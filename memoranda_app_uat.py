
# Fund Management Group - Memoranda Management System (Streamlit)
# Author: Adonis Hautea
# Date: 2025-09-10 (admin approval + st.rerun + admin-only delete + PDF attachments)

import os
import io
import sqlite3
import hashlib
from datetime import datetime, date
from typing import Optional, List, Tuple

import secrets  # For generating secure random tokens
import smtplib  # For sending reset emails
from datetime import timedelta  # For token expiry calculation
from email.message import EmailMessage  # For constructing reset email messages

import pandas as pd
import streamlit as st

# ----------------------------- App Config -----------------------------
st.set_page_config(page_title="Fund Management Group (FMG) - Correspondence Management System (CMS)", layout="wide")

DB_PATH = os.environ.get("MMS_DB_PATH", "memos.db")
# Separate database path for users and units. This isolates user credentials and
# unit/role metadata from memoranda records. If unspecified, defaults to
# ``users.db`` in the working directory. Keeping a separate DB allows
# administrators to manage user information independently of memo data.
USERS_DB_PATH = os.environ.get("USERS_DB_PATH", "users.db")

FILES_DIR = os.environ.get("MMS_FILES_DIR", "memo_files")
os.makedirs(FILES_DIR, exist_ok=True)

# ----------------------------- DB Helpers -----------------------------
def get_conn():
    """Return a connection to the memoranda database with foreign keys on."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# ----------------------------- User DB Helpers -----------------------------
def get_user_conn() -> sqlite3.Connection:
    """Return a connection to the user database (separate from memos DB)."""
    conn = sqlite3.connect(USERS_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_user_db():
    """Initialize the user database with tables for users, units and user_units."""
    conn = get_user_conn(); c = conn.cursor()
    # Users table: stores credentials and role. Role can be 'admin', 'super', or 'viewer'.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            email TEXT UNIQUE,
            password_hash TEXT,
            password_salt TEXT,
            role TEXT DEFAULT 'user',
            is_active INTEGER DEFAULT 0,
            created_at TEXT
        )
        """
    )
    # Units table: holds all distinct units available in the system.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    # Many-to-many relationship: associates users with one or more units.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS user_units (
            user_id INTEGER,
            unit_id INTEGER,
            PRIMARY KEY (user_id, unit_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
        )
        """
    )

    # Table to store password reset tokens. Each record links a user to a token and
    # specifies an expiry timestamp. When a user initiates a password reset, a
    # token is generated and inserted here. Tokens older than their expiry are
    # considered invalid and may be purged. A user may have multiple pending
    # tokens, but validation will accept only those that have not expired.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            token TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )
    conn.commit()
    seed_user_defaults(conn)
    conn.close()

def seed_user_defaults(conn: sqlite3.Connection):
    """
    Seed the user database with a default admin account if none exists.
    The default credentials are username ``admin`` and password ``admin``. The account
    is marked active so that an administrator can immediately sign in and
    begin approving new registrations.
    """
    c = conn.cursor()
    # Create default admin if not present. Use email OR username match to avoid duplicate insertions.
    c.execute(
        "SELECT id FROM users WHERE username=? OR email=?",
        ("admin", "admin@example.com")
    )
    row = c.fetchone()
    if not row:
        salt, pw = hash_password("admin")
        created_at = today_ts()
        c.execute(
            "INSERT INTO users (username,email,password_hash,password_salt,role,is_active,created_at) VALUES (?,?,?,?,?,1,?)",
            ("admin", "admin@example.com", pw, salt, "admin", created_at)
        )
        user_id = c.lastrowid
        # Mirror admin into memo DB for audit trail
        try:
            mconn = get_conn(); mc = mconn.cursor()
            mc.execute(
                "INSERT OR REPLACE INTO users (id, username, email, password_hash, password_salt, role, is_active, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (user_id, "admin", "admin@example.com", pw, salt, "admin", 1, created_at)
            )
            mconn.commit(); mconn.close()
        except Exception:
            pass
    conn.commit()


def init_db():
    # Initialize both the memoranda database and the separate user database.
    # The memo database stores all correspondence records, categories, files, and settings.
    # The user database stores user credentials, roles, and unit associations.
    conn = get_conn(); c = conn.cursor()
    # settings
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    # categories
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER DEFAULT 1
        )
        """
    )
    # memos
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS memos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            control_no TEXT UNIQUE,
            date_log TEXT,    -- YYYY-MM-DD
            date_doc TEXT,    -- YYYY-MM-DD
            memo_from TEXT,
            thru TEXT,
            memo_for TEXT,
            subject TEXT,
            category TEXT,
            status TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    # files
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS memo_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memo_id INTEGER NOT NULL,
            filename TEXT,
            filepath TEXT,
            uploaded_at TEXT,
            FOREIGN KEY (memo_id) REFERENCES memos(id) ON DELETE CASCADE
        )
        """
    )
    # statuses
    # Maintains configurable statuses for correspondence records. Each status has an
    # auto-incrementing id, a unique name, and an active flag. Statuses are stored
    # separately from the memos table to allow administrators to add, delete and
    # rename statuses without altering the memo schema. When a status is renamed,
    # the update_status_name function will propagate changes to existing memos.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS statuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER DEFAULT 1
        )
        """
    )

    # Control prefixes per unit. This table allows administrators to assign
    # distinct control number prefixes to individual divisions/units. The
    # column `unit_name` stores the unit name as a primary key, and `prefix`
    # stores the prefix string. When generating control numbers, the prefix
    # corresponding to the first selected unit will be used. If no prefix is
    # defined for a unit, the default prefix from settings (key
    # ``control_prefix``) is used.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS control_prefixes (
            unit_name TEXT PRIMARY KEY,
            prefix TEXT NOT NULL
        )
        """
    )
    # users (legacy table retained for audit trail references). New user roles include 'viewer' and 'super'.
    # Unique constraints on username/email are intentionally omitted to allow multiple records with the
    # same email or username. The audit trail only references users by id; this simplifies mirroring
    # from the user database without running into UNIQUE constraint errors.
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            email TEXT,
            password_hash TEXT,
            password_salt TEXT,
            role TEXT DEFAULT 'user', -- 'admin', 'super', 'user', or 'viewer'
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )
        """
    )
    # audit
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            user_id INTEGER,
            action TEXT,
            memo_id INTEGER,
            details TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        """
    )
    conn.commit()
    seed_defaults(conn)
    conn.close()
    # Ensure user DB exists and seeded
    init_user_db()

def seed_defaults(conn: sqlite3.Connection):
    c = conn.cursor()
    # Prefix
    c.execute("SELECT value FROM settings WHERE key=?", ("control_prefix",))
    if not c.fetchone():
        c.execute("INSERT INTO settings (key,value) VALUES (?,?)", ("control_prefix", "MEMO"))
    # Categories and statuses are intentionally not seeded here. Administrators
    # can define categories and statuses through the Settings interface. Leaving
    # these tables empty ensures the system starts with blank options and users
    # can create their own taxonomy.
    conn.commit()

# ----------------------------- Utilities -----------------------------
# Legacy default statuses list retained for backwards compatibility. The application no
# longer seeds default statuses; administrators should configure statuses via the Settings tab.
STATUSES = []

def today_ts() -> str:
    return datetime.now().isoformat()

def get_setting(key: str, default: Optional[str]=None) -> Optional[str]:
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = c.fetchone(); conn.close()
    return row[0] if row else default

def set_setting(key: str, value: str) -> None:
    conn = get_conn(); c = conn.cursor()
    c.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit(); conn.close()

def get_active_categories() -> List[str]:
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT name FROM categories WHERE is_active=1 ORDER BY name ASC")
    rows = c.fetchall(); conn.close()
    return [r[0] for r in rows]

def get_all_categories_df() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("SELECT id, name, is_active FROM categories ORDER BY name ASC", conn)
    conn.close(); return df

def add_category(name: str):
    conn = get_conn(); c = conn.cursor()
    try:
        c.execute("INSERT INTO categories(name, is_active) VALUES (?,1)", (name.strip(),))
        conn.commit()
    except sqlite3.IntegrityError:
        st.warning(f"Category '{name}' already exists.")
    finally:
        conn.close()

def set_category_active(cat_id: int, active: bool):
    conn = get_conn(); c = conn.cursor()
    c.execute("UPDATE categories SET is_active=? WHERE id=?", (1 if active else 0, cat_id))
    conn.commit(); conn.close()

def delete_category(cat_id: int):
    conn = get_conn(); c = conn.cursor()
    c.execute("DELETE FROM categories WHERE id=?", (cat_id,))
    conn.commit(); conn.close()

# ----------------------------- Status Utilities -----------------------------
def get_active_statuses() -> List[str]:
    """
    Return a list of names for all active statuses, sorted alphabetically. These
    statuses are used when creating or editing memos, and in the template
    generation. Only statuses with ``is_active=1`` are returned.
    """
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT name FROM statuses WHERE is_active=1 ORDER BY name ASC")
    rows = c.fetchall(); conn.close()
    return [r[0] for r in rows]

def get_all_status_names(active_only: bool=False) -> List[str]:
    """
    Return a list of status names. If ``active_only`` is True, return only
    active statuses; otherwise return all statuses (active and inactive).
    The names are sorted alphabetically.
    """
    conn = get_conn(); c = conn.cursor()
    if active_only:
        c.execute("SELECT name FROM statuses WHERE is_active=1 ORDER BY name ASC")
    else:
        c.execute("SELECT name FROM statuses ORDER BY name ASC")
    rows = c.fetchall(); conn.close()
    return [r[0] for r in rows]

def get_all_statuses_df() -> pd.DataFrame:
    """
    Return a pandas DataFrame of all statuses with columns ``id``, ``name`` and
    ``is_active``. Rows are ordered alphabetically by name. This helper is used
    by the Settings > Statuses tab to display and manage statuses.
    """
    conn = get_conn()
    df = pd.read_sql("SELECT id, name, is_active FROM statuses ORDER BY name ASC", conn)
    conn.close(); return df

def add_status(name: str):
    """
    Add a new status to the statuses table. Leading/trailing whitespace is
    stripped from the name. If a status with the same name already exists
    (case-insensitive), a warning is shown via ``st.warning``. The function
    commits the change on success and closes the connection.
    """
    nm = (name or "").strip()
    if not nm:
        return
    conn = get_conn(); c = conn.cursor()
    # Check for existing status (case-insensitive)
    c.execute("SELECT id FROM statuses WHERE LOWER(name)=LOWER(?)", (nm,))
    exists = c.fetchone() is not None
    if exists:
        st.warning(f"Status '{nm}' already exists.")
        conn.close(); return
    try:
        c.execute("INSERT INTO statuses(name, is_active) VALUES (?,1)", (nm,))
        conn.commit()
    except sqlite3.IntegrityError:
        st.warning(f"Status '{nm}' already exists.")
    finally:
        conn.close()

def set_status_active(stat_id: int, active: bool):
    """
    Toggle the ``is_active`` flag for a status by id. If active is True, the
    status becomes available in drop-downs for new memos and editing. If False,
    the status remains in the database but is hidden from these lists.
    """
    conn = get_conn(); c = conn.cursor()
    c.execute("UPDATE statuses SET is_active=? WHERE id=?", (1 if active else 0, stat_id))
    conn.commit(); conn.close()

def delete_status(stat_id: int):
    """
    Delete a status record from the database. This does not alter existing
    memo records that reference the deleted status; such memos will continue
    to display the old status name. Use with caution.
    """
    conn = get_conn(); c = conn.cursor()
    c.execute("DELETE FROM statuses WHERE id=?", (stat_id,))
    conn.commit(); conn.close()

def rename_status(stat_id: int, new_name: str):
    """
    Rename a status and propagate the change to existing memos. If another
    status with the new name already exists (case-insensitive), the rename is
    aborted and a warning is displayed. On success, all memos referencing the
    old status name are updated to the new name.
    """
    nm = (new_name or "").strip()
    if not nm:
        return
    conn = get_conn(); c = conn.cursor()
    # Fetch the current name of this status
    c.execute("SELECT name FROM statuses WHERE id=?", (stat_id,))
    row = c.fetchone()
    if not row:
        conn.close(); return
    old_name = row[0]
    # If the name hasn't changed, nothing to do
    if old_name == nm:
        conn.close(); return
    # Check for conflict
    c.execute("SELECT id FROM statuses WHERE LOWER(name)=LOWER(?) AND id<>?", (nm, stat_id))
    conflict = c.fetchone() is not None
    if conflict:
        st.warning(f"A status named '{nm}' already exists.")
        conn.close(); return
    # Update the status name
    c.execute("UPDATE statuses SET name=? WHERE id=?", (nm, stat_id))
    # Propagate to memos
    c.execute("UPDATE memos SET status=? WHERE status=?", (nm, old_name))
    conn.commit(); conn.close()

# ----------------------------- Control Prefix Utilities -----------------------------
def get_unit_prefix(unit_name: str) -> str:
    """
    Retrieve the control prefix for a given unit name. If no prefix is
    configured for the unit, fall back to the global default prefix stored
    under settings key ``control_prefix`` (defaulting to 'MEMO'). Unit names
    are matched exactly (case-sensitive) for prefix retrieval.
    """
    if not unit_name:
        # If unit name is blank, return global default
        return get_setting("control_prefix", "MEMO")
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT prefix FROM control_prefixes WHERE unit_name=?", (unit_name,))
    row = c.fetchone(); conn.close()
    if row:
        return row[0]
    return get_setting("control_prefix", "MEMO")

def set_unit_prefix(unit_name: str, prefix: str) -> None:
    """
    Create or update the control prefix for a specific unit. This helper
    inserts a new row into the ``control_prefixes`` table or updates the
    existing prefix if one already exists for the given unit name. The
    operation is case-sensitive on unit_name.
    """
    if not unit_name or not prefix:
        return
    conn = get_conn(); c = conn.cursor()
    c.execute(
        "INSERT INTO control_prefixes(unit_name,prefix) VALUES (?,?) ON CONFLICT(unit_name) DO UPDATE SET prefix=excluded.prefix",
        (unit_name, prefix.strip())
    )
    conn.commit(); conn.close()

def get_all_unit_prefixes() -> dict:
    """
    Return a dictionary mapping unit names to their configured control prefixes.
    If a unit has no specific prefix configured, it will not appear in the
    returned dictionary.
    """
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT unit_name, prefix FROM control_prefixes")
    rows = c.fetchall(); conn.close()
    return {unit: prefix for unit, prefix in rows}

def yymm(dt: datetime) -> str:
    return dt.strftime("%y")

def parse_control_sequence(control_no: str) -> Optional[int]:
    try:
        seq_part = control_no.split("-")[-1].strip()
        return int(seq_part)
    except Exception:
        return None

def next_control_no(prefix: str) -> str:
    yy = yymm(datetime.now())
    conn = get_conn(); c = conn.cursor()
    like_pattern = f"{prefix} {yy}-%"
    c.execute("SELECT control_no FROM memos WHERE control_no LIKE ? ORDER BY control_no DESC", (like_pattern,))
    rows = c.fetchall(); conn.close()
    last_seq = 0
    for (cn,) in rows:
        seq = parse_control_sequence(cn)
        if seq and seq > last_seq:
            last_seq = seq
    return f"{prefix} {yy}-{last_seq + 1:03d}"

def parse_date_cell(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    dt = pd.to_datetime(val, errors="coerce")
    if dt is None or pd.isna(dt):
        return None
    return dt.date().isoformat()  # date-only

# ----------------------------- Auth & Audit -----------------------------
def hash_password(password: str, salt_hex: Optional[str]=None) -> Tuple[str,str]:
    if not salt_hex:
        salt = os.urandom(16); salt_hex = salt.hex()
    else:
        salt = bytes.fromhex(salt_hex)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return salt_hex, dk.hex()

def verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    _, calc = hash_password(password, salt_hex)
    return calc == hash_hex

def get_user_by_key(user_key: str):
    """Retrieve a user by username or email from the user database.

    Returns a dict with user fields and a list of unit names under key ``units``. If
    no such user exists, returns ``None``. This helper centralizes user lookups
    and includes unit memberships for downstream authorization checks.
    """
    conn = get_user_conn(); c = conn.cursor()
    c.execute(
        "SELECT id, username, email, password_hash, password_salt, role, is_active FROM users WHERE username=? OR email=?",
        (user_key, user_key)
    )
    row = c.fetchone()
    if not row:
        conn.close(); return None
    keys = ["id","username","email","password_hash","password_salt","role","is_active"]
    user_dict = dict(zip(keys, row))
    # Fetch units for the user
    c.execute(
        """
        SELECT u.name
        FROM units AS u
        INNER JOIN user_units AS uu ON u.id = uu.unit_id
        WHERE uu.user_id=? AND u.is_active=1
        """,
        (user_dict["id"],)
    )
    unit_rows = c.fetchall(); conn.close()
    user_dict["units"] = [u[0] for u in unit_rows]
    return user_dict

def ensure_units_exist(units: List[str], conn: Optional[sqlite3.Connection] = None) -> List[int]:
    """
    Ensure each unit name exists in the units table, creating it if necessary.
    Returns the corresponding unit IDs. Case-insensitive duplicates are
    collapsed, but the stored unit names preserve their original casing.

    A connection may be provided to avoid locking issues; if not provided,
    a new connection is opened for this operation and closed upon completion.
    """
    ids: List[int] = []
    if not units:
        return ids
    close_conn = False
    if conn is None:
        conn = get_user_conn()
        close_conn = True
    c = conn.cursor()
    for unit in units:
        name = unit.strip()
        if not name:
            continue
        # Try to fetch existing unit (case-insensitive)
        c.execute("SELECT id FROM units WHERE LOWER(name)=LOWER(?)", (name,))
        row = c.fetchone()
        if row:
            unit_id = row[0]
        else:
            # Insert new unit as active
            c.execute("INSERT INTO units (name,is_active) VALUES (?,1)", (name,))
            unit_id = c.lastrowid
        ids.append(unit_id)
    conn.commit()
    if close_conn:
        conn.close()
    return ids

def get_active_unit_names() -> List[str]:
    """
    Return a list of active unit names from the user database, sorted alphabetically.
    These are used for tagging memoranda to specific divisions/units.
    """
    conn = get_user_conn(); c = conn.cursor()
    c.execute("SELECT name FROM units WHERE is_active=1 ORDER BY name ASC")
    rows = c.fetchall(); conn.close()
    return [r[0] for r in rows]

def create_user(username: str, email: str, password: str, units: Optional[List[str]] = None, role: str="user", is_active: int=0) -> Tuple[bool,str]:
    """
    Create a new user in the user database. Accepts an optional list of unit
    names to assign to the user. Units will be created if they do not already
    exist. By default, new accounts are set to role 'viewer' and require
    administrator activation.
    """
    if not username or not email or not password:
        return False, "Missing fields"
    conn = get_user_conn(); c = conn.cursor()
    try:
        salt, pw = hash_password(password)
        created_at = today_ts()
        # Insert the new user
        c.execute(
            "INSERT INTO users (username,email,password_hash,password_salt,role,is_active,created_at) VALUES (?,?,?,?,?,?,?)",
            (username.strip(), email.strip(), pw, salt, role, is_active, created_at)
        )
        user_id = c.lastrowid
        # Ensure units exist and associate them using the same connection to avoid database locks
        unit_ids: List[int] = ensure_units_exist(units or [], conn)
        for uid in unit_ids:
            c.execute("INSERT OR IGNORE INTO user_units (user_id, unit_id) VALUES (?, ?)", (user_id, uid))
        # Commit changes to user DB
        conn.commit()
        # Mirror this user into the memoranda database's users table for audit references
        try:
            mconn = get_conn(); mc = mconn.cursor()
            mc.execute(
                "INSERT OR REPLACE INTO users (id, username, email, password_hash, password_salt, role, is_active, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (user_id, username.strip(), email.strip(), pw, salt, role, is_active, created_at)
            )
            mconn.commit(); mconn.close()
        except Exception:
            pass
        ok=True; msg="Registered"
    except sqlite3.IntegrityError:
        ok=False; msg="Username or email already exists"
    finally:
        conn.close()
    return ok, msg

def log_action(action: str, memo_id: Optional[int]=None, details: str=""):
    try:
        uid = None
        if isinstance(st.session_state.get("user"), dict):
            uid = st.session_state["user"].get("id")
        conn = get_conn(); c = conn.cursor()
        c.execute(
            "INSERT INTO audit_trail (ts, user_id, action, memo_id, details) VALUES (?,?,?,?,?)",
            (today_ts(), uid, action, memo_id, details)
        )
        conn.commit(); conn.close()
    except Exception:
        pass

# ----------------------------- File Helpers -----------------------------
def save_files(files, memo_id: int):
    saved = []
    if not files: return saved
    memo_dir = os.path.join(FILES_DIR, f"memo_{memo_id}")
    os.makedirs(memo_dir, exist_ok=True)
    conn = get_conn(); c = conn.cursor()
    for f in files:
        fname = getattr(f, "name", "upload.bin")
        path = os.path.join(memo_dir, fname)
        data = f.read() if hasattr(f, "read") else f
        with open(path, "wb") as out:
            out.write(data)
        c.execute(
            "INSERT INTO memo_files (memo_id, filename, filepath, uploaded_at) VALUES (?,?,?,?)",
            (memo_id, fname, path, today_ts())
        )
        saved.append((fname, path))
    conn.commit(); conn.close()
    return saved

def list_files(memo_id: int) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql("SELECT id, filename, filepath, uploaded_at FROM memo_files WHERE memo_id=? ORDER BY uploaded_at DESC", conn, params=(memo_id,))
    conn.close(); return df

def delete_file(file_id: int):
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT filepath FROM memo_files WHERE id=?", (file_id,))
    row = c.fetchone()
    if row:
        try: os.remove(row[0])
        except Exception: pass
    c.execute("DELETE FROM memo_files WHERE id=?", (file_id,))
    conn.commit(); conn.close()

# ----------------------------- Password Reset Helpers -----------------------------
def purge_expired_reset_tokens(conn: Optional[sqlite3.Connection] = None) -> None:
    """
    Remove password reset tokens that have expired. If a connection is provided,
    use it; otherwise, open and close a new connection. Tokens are considered
    expired when their `expires_at` timestamp is earlier than the current UTC
    time. This helper is called before inserting a new reset token and when
    validating tokens to keep the table clean.
    """
    close_conn = False
    if conn is None:
        conn = get_user_conn()
        close_conn = True
    c = conn.cursor()
    now_iso = datetime.utcnow().isoformat()
    c.execute("DELETE FROM password_reset_tokens WHERE expires_at < ?", (now_iso,))
    conn.commit()
    if close_conn:
        conn.close()


def generate_reset_token(length: int = 6) -> str:
    """
    Generate a secure random numeric token of the specified length. The token
    comprises digits only to make entry simple when sent via email.
    """
    return ''.join(secrets.choice('0123456789') for _ in range(length))


def send_reset_email(to_email: str, token: str) -> Tuple[bool, str]:
    """
    Send a password reset token to the specified email address using SMTP
    configuration provided in the Streamlit secrets. Returns a tuple
    (success, message). On success, the message indicates the token was
    dispatched. On failure, it contains the error description.
    """
    try:
        smtp_conf = st.secrets.get("smtp")
    except Exception:
        return False, "SMTP configuration not found. Please define [smtp] in .streamlit/secrets.toml."
    if not smtp_conf:
        return False, "SMTP configuration not found. Please define [smtp] in .streamlit/secrets.toml."
    try:
        host = smtp_conf.get("host")
        port = smtp_conf.get("port")
        username = smtp_conf.get("username")
        password = smtp_conf.get("password")
        use_tls = smtp_conf.get("use_tls", True)
        sender = smtp_conf.get("sender", username)
        if not (host and port and username and password):
            return False, "Incomplete SMTP configuration. Ensure host, port, username, and password are set."
        # Construct the email
        msg = EmailMessage()
        msg["Subject"] = "Password Reset Token"
        msg["From"] = sender
        msg["To"] = to_email
        msg.set_content(
            f"Your password reset token is: {token}\n\n"
            "If you did not request a password reset, you can ignore this email."
        )
        # Connect and send
        with smtplib.SMTP(host, port) as server:
            if use_tls:
                server.starttls()
            server.login(username, password)
            server.send_message(msg)
        return True, "A reset token has been sent to your email."
    except Exception as e:
        return False, f"Failed to send email: {e}"


def create_password_reset_token(email: str) -> Tuple[bool, str]:
    """
    Initiate a password reset for the user with the given email. Generates a
    token, stores it in the database with an expiry of 30 minutes, and
    dispatches it to the user's email. Returns (success, message).
    """
    if not email:
        return False, "Email is required."
    # Look up the user by email
    conn = get_user_conn(); c = conn.cursor()
    c.execute("SELECT id FROM users WHERE LOWER(email)=LOWER(?)", (email.strip(),))
    row = c.fetchone()
    if not row:
        conn.close()
        return False, "No account found with that email."
    user_id = row[0]
    # Purge expired tokens first
    purge_expired_reset_tokens(conn)
    # Generate a new token and expiry
    token = generate_reset_token()
    expires_at = (datetime.utcnow() + timedelta(minutes=30)).isoformat()
    created_at = datetime.utcnow().isoformat()
    # Insert the token record
    c.execute(
        "INSERT INTO password_reset_tokens (user_id, token, expires_at, created_at) VALUES (?,?,?,?)",
        (user_id, token, expires_at, created_at)
    )
    conn.commit(); conn.close()
    # Send the email
    sent, msg = send_reset_email(email.strip(), token)
    if sent:
        return True, msg
    else:
        return False, msg


def validate_password_reset_token(email: str, token: str) -> Optional[int]:
    """
    Validate that the provided token matches an unexpired reset token for the
    user with the specified email. Returns the user's ID if valid, or None
    otherwise.
    """
    if not (email and token):
        return None
    conn = get_user_conn(); c = conn.cursor()
    # Purge expired tokens prior to validation
    purge_expired_reset_tokens(conn)
    c.execute(
        """
        SELECT pr.user_id
        FROM password_reset_tokens pr
        JOIN users u ON pr.user_id = u.id
        WHERE LOWER(u.email)=LOWER(?) AND pr.token = ? AND pr.expires_at > ?
        ORDER BY pr.created_at DESC
        LIMIT 1
        """,
        (email.strip(), token.strip(), datetime.utcnow().isoformat())
    )
    row = c.fetchone(); conn.close()
    return row[0] if row else None


def update_user_password(user_id: int, new_password: str) -> Tuple[bool, str]:
    """
    Update the password for the specified user. Generates a new salt and hash
    for the provided password and writes it to both the user database and the
    mirrored record in the memoranda database for audit. Returns (success,
    message).
    """
    if not new_password:
        return False, "Password must not be empty."
    try:
        salt, pw_hash = hash_password(new_password)
        now_iso = today_ts()
        # Update user database
        conn = get_user_conn(); c = conn.cursor()
        # Some installations may not have an updated_at column; attempt update
        try:
            c.execute(
                "UPDATE users SET password_hash=?, password_salt=?, updated_at=? WHERE id=?",
                (pw_hash, salt, now_iso, user_id)
            )
        except Exception:
            # Fallback if updated_at column does not exist
            c.execute(
                "UPDATE users SET password_hash=?, password_salt=? WHERE id=?",
                (pw_hash, salt, user_id)
            )
        # Remove all reset tokens for this user
        c.execute("DELETE FROM password_reset_tokens WHERE user_id=?", (user_id,))
        conn.commit(); conn.close()
        # Update mirrored record in memo DB users table for audit
        try:
            mconn = get_conn(); mc = mconn.cursor()
            mc.execute(
                "UPDATE users SET password_hash=?, password_salt=? WHERE id=?",
                (pw_hash, salt, user_id)
            )
            mconn.commit(); mconn.close()
        except Exception:
            # If memo DB update fails, continue silently as this table is for audit only
            pass
        return True, "Password updated successfully."
    except Exception as e:
        return False, f"Failed to update password: {e}"

# ----------------------------- Zip Helpers -----------------------------
def zip_memo_files(memo_id: int) -> Optional[bytes]:
    """
    Create a zip archive of all files attached to a given memo. Returns the
    bytes of the zip, or None if no files exist. This helper is used by
    super users and admins to download attachments in bulk.
    """
    fdf = list_files(memo_id)
    if fdf.empty:
        return None
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for row in fdf.itertuples(index=False):
            # add the file using its filename to avoid exposing directory structure
            try:
                zf.write(row.filepath, arcname=row.filename)
            except Exception:
                pass
    return buf.getvalue()

# ----------------------------- Admin Helpers -----------------------------
def is_admin() -> bool:
    u = st.session_state.get("user")
    return isinstance(u, dict) and u.get("role") == "admin"

def is_super() -> bool:
    """Return True if the currently logged in user has the 'super' role."""
    u = st.session_state.get("user")
    return isinstance(u, dict) and u.get("role") == "super"

def is_user() -> bool:
    """Return True if the currently logged in user has the 'user' role."""
    u = st.session_state.get("user")
    return isinstance(u, dict) and u.get("role") == "user"

def clear_memo_contents(memo_id: int):
    """Wipe core fields but keep the record and control number for traceability."""

    conn = get_conn(); c = conn.cursor()
    c.execute(
        """
        UPDATE memos SET
            date_log=NULL, date_doc=NULL,
            memo_from='', thru='', memo_for='',
            subject='', notes='',
            updated_at=?
        WHERE id=?
        """,
        (today_ts(), memo_id)
    )
    conn.commit(); conn.close()


def _current_role():
    u = st.session_state.get("user")
    if isinstance(u, dict):
        # Default to 'viewer' when no role specified
        return u.get("role", "user")
    return "guest"

def _is_guest():
    # Treat non-logged-in users as "guest" for download gating.
    return _current_role() == "guest" or st.session_state.get("user") is None
# ----------------------------- Import/Export Utils -----------------------------
# Template columns for import/export. The column formerly named "Memo For"
# (used to designate the target divisions/units for a memo) has been renamed
# to "Division(s)/Unit(s)" to align with the UI terminology. This name
# clearly indicates that multiple divisions or units may be specified,
# separated by commas.
TEMPLATE_COLUMNS = [
    "Control No",
    "Date of Log",
    "Date of Document",
    "Memo From",
    "Thru",
    "Division(s)/Unit(s)",
    "Subject",
    "Category",
    "Status",
    "Notes",
]
# The 'Category' and 'Status' fields are optional in imports. Only the basic fields (dates, from, subject)
# are mandatory. Users can leave Category and Status blank or add their own values.
REQUIRED_IMPORT_COLS = ["Date of Log","Date of Document","Memo From","Subject"]

def build_template_xlsx() -> bytes:
    buf = io.BytesIO()
    tmpl = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        tmpl.to_excel(w, index=False, sheet_name="Template")
        # Build guidance. Fetch current active statuses for the status notes. If
        # there are no active statuses, fall back to all statuses or a placeholder.
        active_stats = get_active_statuses()
        status_note = "One of: " + (", ".join(active_stats) if active_stats else "Refer to your configured statuses")
        guide = pd.DataFrame({
            "Field": TEMPLATE_COLUMNS,
            # Only the date fields, Memo From and Subject are mandatory. Category, Status,
            # and Division(s)/Unit(s) are optional on import.
            "Required": [False, True, True, True, False, False, True, False, False, False],
            "Notes": [
                "Leave blank to auto-generate",
                "Date (YYYY-MM-DD)",
                "Date (YYYY-MM-DD)",
                "Originator",
                "Optional",
                "Target divisions/units (comma-separated)",
                "Subject line",
                "Optional category name (will be auto-created if necessary)",
                "Optional status name (must match an existing status if provided)",
                "Optional free text",
            ]
        })
        guide.to_excel(w, index=False, sheet_name="Guidance")
    return buf.getvalue()

def ensure_category(name: str, auto_create: bool) -> bool:
    nm = (name or "").strip()
    if not nm: return False
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT 1 FROM categories WHERE name=?", (nm,))
    ok = c.fetchone() is not None
    if not ok and auto_create:
        try:
            c.execute("INSERT INTO categories(name,is_active) VALUES (?,1)", (nm,))
            conn.commit(); ok=True
        except sqlite3.IntegrityError:
            ok=False
    conn.close(); return ok

def validate_import_df(df: pd.DataFrame, auto_create_cats: bool) -> pd.DataFrame:
    errors = []
    missing = [c for c in REQUIRED_IMPORT_COLS if c not in df.columns]
    if missing:
        errors.append({"Row":"-","Field":"Columns","Issue":f"Missing columns: {', '.join(missing)}"})
        return pd.DataFrame(errors)
    # Retrieve list of valid statuses once for efficient lookup. We include
    # inactive statuses here because imported records may reference older
    # statuses that are no longer active. Unknown statuses are flagged as
    # errors.
    valid_statuses = get_all_status_names(active_only=False)
    for idx, row in df.iterrows():
        issues = []
        # Validate dates
        for fld in ["Date of Log", "Date of Document"]:
            if pd.isna(row.get(fld)) or parse_date_cell(row.get(fld)) is None:
                issues.append(f"{fld} invalid or missing")
        # Mandatory fields
        if not str(row.get("Memo From") or "").strip():
            issues.append("Memo From is required")
        if not str(row.get("Subject") or "").strip():
            issues.append("Subject is required")
        # Category validation: if provided, ensure it exists or create it (depending on auto_create). Blank categories are allowed.
        cat = str(row.get("Category") or "").strip()
        if cat:
            if not ensure_category(cat, auto_create_cats):
                issues.append(f"Category '{cat}' not found (auto-create OFF)")
        # Status validation: blank statuses are allowed; if provided, it must exist in the list of valid statuses
        stt = str(row.get("Status") or "").strip()
        if stt and (stt not in valid_statuses):
            issues.append(f"Invalid Status '{stt}'")
        if issues:
            errors.append({"Row": int(idx) + 2, "Field": "Multiple", "Issue": "; ".join(issues)})
    return pd.DataFrame(errors)

def import_from_df(df: pd.DataFrame, auto_create_cats: bool, auto_gen_cn: bool) -> Tuple[int,int,int]:
    # If a Status column exists, allow NaN/blank values; do not fill with defaults since statuses are optional.
    if not df.empty and "Status" in df.columns:
        df["Status"] = df["Status"].fillna("")
    # Prepare status lookup once. Blank values will be accepted without lookup.
    valid_statuses = get_all_status_names(active_only=False)
    # Determine fallback status when an invalid status is provided. Use blank if there are no valid statuses.
    fallback_status = "" if not valid_statuses else (valid_statuses[0])
    inserted = skipped = failed = 0
    for _, row in df.iterrows():
        # Determine control number; auto-generate if requested and blank
        control = str(row.get("Control No")) if pd.notna(row.get("Control No")) else ""
        control = control.strip()
        if not control and auto_gen_cn:
            prefix = get_setting("control_prefix", "MEMO")
            control = next_control_no(prefix)
        # Skip duplicates
        if control:
            conn = get_conn(); c = conn.cursor()
            c.execute("SELECT 1 FROM memos WHERE control_no=?", (control,))
            exists = c.fetchone() is not None
            conn.close()
            if exists:
                skipped += 1
                continue
        data = {
            "control_no": control or None,
            "date_log": parse_date_cell(row.get("Date of Log")),
            "date_doc": parse_date_cell(row.get("Date of Document")),
            "memo_from": str(row.get("Memo From") or "").strip(),
            "thru": str(row.get("Thru") or "").strip(),
            # For the memo_for field, prefer the new "Division(s)/Unit(s)" column. If that
            # column is missing or NaN, fall back to the legacy "Memo For" column.
            # This ensures backward compatibility with old templates.
            "memo_for": str(
                (row.get("Division(s)/Unit(s)") if (not pd.isna(row.get("Division(s)/Unit(s)")) and row.get("Division(s)/Unit(s)") is not None) else row.get("Memo For"))
                or ""
            ).strip(),
            "subject": str(row.get("Subject") or "").strip(),
            "category": str(row.get("Category") or "").strip(),
            "status": None,
            "notes": str(row.get("Notes") or "").strip(),
        }
        # If critical fields are missing, count as failed. Category and status are optional.
        if not all([data["date_log"], data["date_doc"], data["memo_from"], data["subject"]]):
            failed += 1
            continue
        # Determine status: if provided and exists in lookup, use it; otherwise fallback (blank or first valid status)
        stt_val = str(row.get("Status") or "").strip()
        if stt_val and stt_val in valid_statuses:
            data["status"] = stt_val
        else:
            data["status"] = fallback_status
        # Ensure category exists only if provided (auto-create if allowed)
        if data["category"]:
            ensure_category(data["category"], auto_create_cats)
        # Insert record
        try:
            conn = get_conn(); c = conn.cursor()
            now_iso = today_ts()
            c.execute(
                """
                INSERT INTO memos (
                    control_no, date_log, date_doc, memo_from, thru, memo_for, subject, category, status, notes, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    data["control_no"], data["date_log"], data["date_doc"], data["memo_from"], data["thru"], data["memo_for"],
                    data["subject"], data["category"], data["status"], data["notes"], now_iso, now_iso
                )
            )
            conn.commit(); conn.close()
            inserted += 1
        except sqlite3.IntegrityError:
            # Duplicate control numbers cause skip
            skipped += 1
            try:
                conn.close()
            except Exception:
                pass
        except Exception:
            failed += 1
            try:
                conn.close()
            except Exception:
                pass
    return inserted, skipped, failed

# ----------------------------- INIT & Session -----------------------------
init_db()
if "import_df" not in st.session_state: st.session_state["import_df"] = None
if "import_validated" not in st.session_state: st.session_state["import_validated"] = False
if "import_errors" not in st.session_state: st.session_state["import_errors"] = pd.DataFrame()
if "user" not in st.session_state: st.session_state["user"] = None

# Sidebar & Auth
st.sidebar.title("FMG-CMS")
if st.session_state["user"]:
    u = st.session_state["user"]
    st.sidebar.success(f"Signed in as {u['username']} ({u['role']})")
    if st.sidebar.button("Logout"):
        log_action("logout")
        st.session_state["user"] = None
        st.rerun()
else:
    st.sidebar.info("Please sign in.")

# Determine pages based on auth/role
# Display customizable sidebar title
sidebar_title = get_setting("sidebar_title", "FMG-CMS")
st.sidebar.title(sidebar_title)
if st.session_state["user"] is None:
    page = st.sidebar.radio("Navigate", ["Auth"], index=0)
else:
    role = st.session_state["user"].get("role", "user")
    # Base pages available to all authenticated users
    base_pages: List[str] = ["Dashboard"]
    # Users (user/super/admin) can create new memos
    if role in ("user", "super", "admin"):
        base_pages.append("New Memorandum")
    # All roles can monitor/manage; editing permissions will be enforced on page
    base_pages.append("Monitor / Manage")
    # Import/Export is allowed for super and admin roles
    if role in ("super", "admin"):
        base_pages.append("Import / Export")
    # Settings is accessible to super and admin users. Categories are now
    # managed within Settings and no longer appear as a separate page.
    if role in ("super", "admin"):
        base_pages.append("Settings")
    # Admin-only page
    if role == "admin":
        base_pages.append("Admin")
    page = st.sidebar.radio("Navigate", base_pages, index=0)

# ----------------------------- Page: Auth -----------------------------
if page == "Auth":
    st.title("Sign in to the FMG Correspondence Management System")
    # Present three tabs: Login, Register, and Reset Password. The Reset Password tab
    # allows users to initiate a password reset by entering their registered email,
    # verifying a token sent via email, and setting a new password.
    tab_login, tab_register, tab_reset = st.tabs(["Login","Register","Reset Password"])
    with tab_login:
        with st.form("login_form"):
            user_key = st.text_input("Email or Username")
            pw = st.text_input("Password", type="password")
            ok = st.form_submit_button("Login", type="primary")
        if ok:
            user = get_user_by_key(user_key.strip()) if user_key else None
            if not user:
                st.error("Invalid credentials.")
            elif not user.get("is_active"):
                st.error("Your account is pending admin approval.")
            else:
                if verify_password(pw, user["password_salt"], user["password_hash"]):
                    # Preserve units list on session for authorization checks
                    st.session_state["user"] = {
                        "id": user["id"],
                        "username": user["username"],
                        "role": user.get("role", "viewer"),
                        "units": user.get("units", [])
                    }
                    log_action("login")
                    st.success("Logged in successfully.")
                    st.rerun()
                else:
                    st.error("Invalid credentials.")
    with tab_register:
        with st.form("register_form"):
            r_user = st.text_input("Username")
            r_email = st.text_input("Email")
            r_pw1 = st.text_input("Password", type="password")
            r_pw2 = st.text_input("Confirm Password", type="password")
            r_units_raw = st.text_input(
                "Division(s)/Unit(s) (comma-separated)",
                help="Enter one or more divisions or units separated by commas (e.g. FMG, EID, CMG, IS)."
            )
            r_ok = st.form_submit_button("Create Account")
        if r_ok:
            if not r_user or not r_email or not r_pw1:
                st.error("All fields are required.")
            elif r_pw1 != r_pw2:
                st.error("Passwords do not match.")
            else:
                # Parse units: split by comma and strip whitespace
                units_list = [u.strip() for u in r_units_raw.split(",") if u.strip()] if r_units_raw else []
                ok, msg = create_user(r_user.strip(), r_email.strip(), r_pw1, units=units_list, role="user", is_active=0)
                if ok:
                    log_action("register", details=f"username={r_user.strip()}")
                    st.success("Registration submitted. An admin must approve your account before you can log in.")
                else:
                    st.error(msg)
    with tab_reset:
        # Initialize reset workflow stage state on first load. Possible stages:
        # 'email' -> enter email, send token. 'token' -> enter token. 'password' -> set new password.
        if "reset_stage" not in st.session_state:
            st.session_state["reset_stage"] = "email"
            st.session_state["reset_email"] = ""
            st.session_state["reset_user_id"] = None
        stage = st.session_state["reset_stage"]
        if stage == "email":
            st.subheader("Request Password Reset")
            with st.form("reset_email_form"):
                email_input = st.text_input("Enter your registered email")
                send_tok = st.form_submit_button("Send Reset Token", type="primary")
            if send_tok:
                if not email_input:
                    st.error("Please enter your email.")
                else:
                    ok, msg = create_password_reset_token(email_input.strip())
                    if ok:
                        # Save the email and advance to the token stage. Immediately
                        # rerun the app to display the token input field; without
                        # this, the user might not see the token form until the
                        # next interaction.
                        st.session_state["reset_email"] = email_input.strip()
                        st.session_state["reset_stage"] = "token"
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)
        elif stage == "token":
            st.subheader("Verify Reset Token")
            with st.form("reset_token_form"):
                token_input = st.text_input("Enter the reset token sent to your email")
                verify_tok = st.form_submit_button("Verify Token", type="primary")
            if verify_tok:
                if not token_input:
                    st.error("Please enter the token.")
                else:
                    user_id = validate_password_reset_token(st.session_state.get("reset_email",""), token_input.strip())
                    if user_id:
                        # Store user_id and advance to the password stage. Rerun
                        # to render the password form immediately.
                        st.session_state["reset_user_id"] = user_id
                        st.session_state["reset_stage"] = "password"
                        st.success("Token verified. Please enter a new password.")
                        st.rerun()
                    else:
                        st.error("Invalid or expired token. Please try again.")
        elif stage == "password":
            st.subheader("Set New Password")
            with st.form("reset_password_form"):
                new_pw = st.text_input("New Password", type="password")
                new_pw2 = st.text_input("Confirm New Password", type="password")
                change_pw = st.form_submit_button("Change Password", type="primary")
            if change_pw:
                if not new_pw:
                    st.error("Password is required.")
                elif new_pw != new_pw2:
                    st.error("Passwords do not match.")
                else:
                    user_id = st.session_state.get("reset_user_id")
                    if not user_id:
                        st.error("Unexpected error: user not found. Please restart the reset process.")
                    else:
                        ok, msg = update_user_password(user_id, new_pw)
                        if ok:
                            st.success(msg + " You can now log in with your new password.")
                            # Reset the stage so the form returns to the email entry and rerun
                            st.session_state["reset_stage"] = "email"
                            st.session_state["reset_email"] = ""
                            st.session_state["reset_user_id"] = None
                            st.rerun()
                        else:
                            st.error(msg)
# ----------------------------- Page: Dashboard -----------------------------
elif page == "Dashboard":
    # Use configurable dashboard title from settings
    st.title(get_setting("dashboard_title", "FMG - Correspondence Management System (CMS) Dashboard"))
    with st.expander("Filters", expanded=True):
        today = date.today(); start_default = today.replace(month=1, day=1)
        d1, d2, d3 = st.columns([1,1,2])
        with d1:
            start = st.date_input("Start (Date of Log)", value=start_default, key="dash_start")
        with d2:
            end = st.date_input("End (Date of Log)", value=today, key="dash_end")
        with d3:
            q = st.text_input("Search (Subject / From / For / Notes)", key="dash_q")
        d4, d5 = st.columns([1,1])
        with d4:
            cat_filter = st.multiselect("Category", options=get_active_categories(), key="dash_cat")
        with d5:
            # Offer status filter using all statuses (active + inactive). This ensures that
            # users can filter on historical statuses even if they have been retired.
            status_opts = get_all_status_names(active_only=False)
            status_filter = st.multiselect("Status", options=status_opts, key="dash_status")
    # query
    conn = get_conn()
    params = [str(start), str(end)]
    sql = ("SELECT id, control_no, date_log, date_doc, memo_from, memo_for, subject, notes, category, status "
           "FROM memos WHERE date_log >= ? AND date_log <= ?")
    if q:
        sql += " AND (subject LIKE ? OR memo_from LIKE ? OR memo_for LIKE ? OR notes LIKE ?)"
        like = f"%{q}%"; params.extend([like, like, like, like])
    if cat_filter:
        marks = ",".join(["?"]*len(cat_filter)); sql += f" AND category IN ({marks})"; params.extend(cat_filter)
    if status_filter:
        marks = ",".join(["?"]*len(status_filter)); sql += f" AND status IN ({marks})"; params.extend(status_filter)
    # Restrict records by user's assigned units for non-admin roles
    current_user = st.session_state.get("user")
    if current_user and current_user.get("role") != "admin":
        units = current_user.get("units", [])
        if units:
            # Build condition to match memo_for field containing any of the user's units
            unit_conds = " OR ".join(["memo_for LIKE ?" for _ in units])
            sql += f" AND ({unit_conds})"
            params.extend([f"%{u}%" for u in units])
        else:
            # If no units assigned, return an empty result set by adding a false condition
            sql += " AND 1=0"
    sql += " ORDER BY date_log DESC, control_no DESC"
    df = pd.read_sql(sql, conn, params=params); conn.close()
    total = int(df.shape[0])
    m1,m2,m3 = st.columns(3)
    with m1: st.metric("Total memoranda", total)
    with m2: st.metric("Unique categories", int(df["category"].nunique()) if total else 0)
    with m3: st.metric("Unique signatories (From)", int(df["memo_from"].nunique()) if total else 0)
    if total:
        df_status = df.groupby("status").size().rename("count").reset_index().sort_values("count", ascending=False)
        df_cat = df.groupby("category").size().rename("count").reset_index().sort_values("count", ascending=False)
        tmp = df.copy(); tmp["date"] = pd.to_datetime(tmp["date_log"]).dt.date
        df_daily = tmp.groupby("date").size().rename("count").reset_index()
        c1,c2 = st.columns(2)
        with c1:
            st.subheader("By Status")
            st.bar_chart(df_status.set_index("status"), use_container_width=True)
        with c2:
            st.subheader("By Category")
            st.bar_chart(df_cat.set_index("category"), use_container_width=True)
        st.subheader("Daily New (Date of Log)")
        st.line_chart(df_daily.set_index("date"), use_container_width=True)
        st.subheader("Records (Subject, Notes, From/For, Dates)")
        view_cols = ["control_no","date_log","date_doc","memo_from","memo_for","subject","notes","category","status"]
        st.dataframe(df[view_cols], use_container_width=True, height=360)
        def dash_xlsx_bytes():
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="Raw")
                df_status.to_excel(w, index=False, sheet_name="By Status")
                df_cat.to_excel(w, index=False, sheet_name="By Category")
                df_daily.to_excel(w, index=False, sheet_name="Daily Trend")
            return buf.getvalue()
        st.download_button("Download Dashboard (.xlsx)", dash_xlsx_bytes(), file_name="memoranda_dashboard.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("No data for the selected filters.")

# ----------------------------- Page: New Memorandum -----------------------------
elif page == "New Memorandum":
    st.title("New Correspondence / Memorandum")
    # Only users, super users and admins can create new memoranda
    if not (is_admin() or is_super() or (st.session_state.get("user", {}).get("role") == "user")):
        st.warning("Only Users, Super Users and Admins can create new memoranda.")
    else:
        # Prepare unit options and defaults before rendering fields so we can compute the control number prefix later.
        current_user = st.session_state.get("user") or {}
        current_units = current_user.get("units", [])
        role_curr = current_user.get("role")
        # Build list of available units: include all active units plus the user's units (in case some are inactive).
        active_units = get_active_unit_names()
        unit_options_list = active_units.copy()
        for u in current_units:
            if u not in unit_options_list:
                unit_options_list.append(u)
        # -- Row 1: Dates and units selection --
        col_d1, col_d2, col_u = st.columns([1,1,1])
        with col_d1:
            date_log = st.date_input("Date of Log", value=date.today())
        with col_d2:
            date_doc = st.date_input("Date of Document", value=date.today())
        with col_u:
            # Super/admin can select units; regular users see their units but cannot change them.
            if role_curr in ("super", "admin"):
                st.multiselect(
                    "Division(s)/Unit(s)",
                    options=unit_options_list,
                    default=st.session_state.get("new_memo_units", current_units),
                    help="Select one or more divisions/units that this memorandum is for.",
                    key="new_memo_units"
                )
            else:
                # For regular users, display the units but do not allow modification. Use a separate key so as not to overwrite new_memo_units.
                st.multiselect(
                    "Division(s)/Unit(s)",
                    options=unit_options_list,
                    default=current_units,
                    disabled=True,
                    key="display_memo_units",
                    help="Your memoranda are automatically tagged with your assigned divisions/units."
                )
        # -- Row 2: memo details (memo from, thru) --
        col_m1, col_m2, col_m3 = st.columns([1,1,1])
        with col_m1:
            memo_from = st.text_input("Memo From")
            thru = st.text_input("Thru")
        with col_m2:
            # Category selection: include a blank option so category is optional.
            cat_options = [""] + get_active_categories()
            category = st.selectbox("Category", options=cat_options)
            # Status selection: include a blank option. The first blank entry means no status.
            active_stats = get_active_statuses()
            status_options = [""] + active_stats
            status = st.selectbox("Status", options=status_options, index=0)
            subject = st.text_input("Subject")
        with col_m3:
            # Provide a persistent key for the auto-control toggle so Streamlit properly updates state.
            auto_ctrl = st.checkbox("Auto-generate Control No.", value=True, key="memo_auto_ctrl")
            # Determine prefix based on the selected unit(s). For super/admin we use the selections stored in session state.
            if role_curr in ("super", "admin"):
                sel_units = st.session_state.get("new_memo_units", [])
            else:
                # Regular users use their assigned units.
                sel_units = current_units
            # Determine prefix: if units are selected, use the first selected unit.
            if sel_units:
                unit_prefix = get_unit_prefix(sel_units[0])
            else:
                # Fallback: if the user has assigned units, use the prefix from the first one; otherwise use global default.
                if current_units:
                    unit_prefix = get_unit_prefix(current_units[0])
                else:
                    unit_prefix = get_setting("control_prefix", "MEMO")
            suggested = next_control_no(unit_prefix)
            # Maintain the control number in session state using a dedicated key. When auto generation is
            # enabled, we override the session state value with the suggested number before rendering the text input.
            ctrl_key = "memo_control_no"
            # Initialize the key if not present.
            if ctrl_key not in st.session_state:
                st.session_state[ctrl_key] = suggested
            if auto_ctrl:
                # Override the stored value with the suggested control number when auto generation is on.
                st.session_state[ctrl_key] = suggested
            # Render control number field outside of a form so its disabled state updates immediately when toggled.
            control_no = st.text_input(
                "Memorandum Control No.",
                key=ctrl_key,
                disabled=auto_ctrl
            )
            # Display a hint only when auto generation is enabled.
            if auto_ctrl:
                if sel_units:
                    hint_unit = sel_units[0]
                else:
                    hint_unit = current_units[0] if current_units else "Default"
                st.caption(f"Suggested based on prefix '{unit_prefix}' (Unit: {hint_unit}). Uncheck to override.")
        # -- Notes and attachments --
        notes = st.text_area("Notes / Description (optional)", height=120)
        files = st.file_uploader(
            "Attach files (PDF, Images, TXT, XLSX, DOC/DOCX, PPT/PPTX)",
            type=["pdf","png","jpg","jpeg","webp","gif","txt","xlsx","doc","docx","ppt","pptx"],
            accept_multiple_files=True
        )
        # -- Save button --
        save_btn = st.button("Save Memorandum", type="primary")
        if save_btn:
            # Determine the units to save with the memorandum. Super/admin use the selected units stored in session state;
            # regular users are limited to their own units.
            if role_curr in ("super", "admin"):
                memo_for_units = st.session_state.get("new_memo_units", current_units)
            else:
                memo_for_units = current_units
            # Validate required fields
            if not st.session_state.get(ctrl_key):
                st.error("Memorandum Control No. is required.")
            elif not subject:
                st.error("Subject is required.")
            elif not memo_for_units:
                st.error("At least one division/unit is required to tag this memorandum.")
            else:
                conn = get_conn(); c = conn.cursor()
                now_iso = today_ts()
                memo_for_str = ", ".join([u.strip() for u in memo_for_units])
                try:
                    c.execute(
                        """
INSERT INTO memos (
    control_no, date_log, date_doc, memo_from, thru, memo_for, subject, category, status, notes, created_at, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
""",
                        (
                            st.session_state[ctrl_key].strip(),
                            str(date_log), str(date_doc),
                            memo_from.strip(), thru.strip(), memo_for_str,
                            subject.strip(), category, status, notes.strip(),
                            now_iso, now_iso
                        )
                    )
                    memo_id = c.lastrowid; conn.commit(); conn.close()
                    saved = save_files(files, memo_id)
                    log_action("memo_create", memo_id, f"control={st.session_state[ctrl_key].strip()}")
                    st.success(f"Memorandum saved (ID: {memo_id}). {len(saved)} file(s) attached.")
                    st.balloons()
                except sqlite3.IntegrityError:
                    st.error("Control No. already exists. Please use another.")
                    conn.close()

# ----------------------------- Page: Monitor / Manage -----------------------------
elif page == "Monitor / Manage":
    st.title("Monitor & Manage Correspondence")
    with st.expander("Filters", expanded=True):
        today = date.today(); start_default = today.replace(month=1, day=1)
        f_col1, f_col2, f_col3 = st.columns([1,1,1])
        with f_col1:
            start = st.date_input("Start (Date of Log)", value=start_default)
        with f_col2:
            end = st.date_input("End (Date of Log)", value=today)
        with f_col3:
            q = st.text_input("Search (Subject / From / For / Notes)")
        f_col4, f_col5 = st.columns([1,1])
        with f_col4:
            cat_filter = st.multiselect("Category", options=get_active_categories())
        with f_col5:
            # Populate status filter with all statuses (active + inactive) to allow
            # filtering historical records
            status_opts = get_all_status_names(active_only=False)
            status_filter = st.multiselect("Status", options=status_opts)
    conn = get_conn()
    params = [str(start), str(end)]
    base_sql = "SELECT id, control_no, date_log, date_doc, subject, category, status, memo_from, memo_for, thru, notes, updated_at FROM memos WHERE date_log >= ? AND date_log <= ?"
    if q:
        base_sql += " AND (subject LIKE ? OR memo_from LIKE ? OR memo_for LIKE ? OR notes LIKE ?)"
        like = f"%{q}%"; params.extend([like, like, like, like])
    if cat_filter:
        placeholders = ",".join(["?"]*len(cat_filter))
        base_sql += f" AND category IN ({placeholders})"; params.extend(cat_filter)
    if status_filter:
        placeholders = ",".join(["?"]*len(status_filter))
        base_sql += f" AND status IN ({placeholders})"; params.extend(status_filter)
    # Apply unit-based restriction for non-admin roles
    current_user = st.session_state.get("user")
    if current_user and current_user.get("role") != "admin":
        units = current_user.get("units", [])
        if units:
            unit_conds = " OR ".join(["memo_for LIKE ?" for _ in units])
            base_sql += f" AND ({unit_conds})"
            params.extend([f"%{u}%" for u in units])
        else:
            base_sql += " AND 1=0"
    base_sql += " ORDER BY date_log DESC, control_no DESC"
    df = pd.read_sql(base_sql, conn, params=params); conn.close()
    st.dataframe(df, use_container_width=True, height=360)
    if not df.empty:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV (filtered)", data=csv, file_name="memos_filtered.csv", mime="text/csv")
    st.markdown("### View / Edit Selected Memorandum")
    if df.empty:
        st.info("No records match your filters.")
    else:
        idx_map = {f"{r.control_no} — {r.subject}": int(r.id) for r in df.itertuples(index=False)}
        selected_label = st.selectbox("Choose a memorandum", options=list(idx_map.keys()))
        sel_id = idx_map[selected_label]
        conn = get_conn(); c = conn.cursor()
        c.execute("SELECT * FROM memos WHERE id=?", (sel_id,))
        memo_row = c.fetchone(); cols = [d[0] for d in c.description]
        conn.close()
        if memo_row:
            memo = dict(zip(cols, memo_row))
            # Determine if the current user can edit this memo. Admins can always edit.
            # Super users can edit only if at least one of their units matches the memo's memo_for field.
            current_user = st.session_state.get("user") or {}
            user_units = current_user.get("units", [])
            memo_units = [u.strip() for u in (memo.get("memo_for") or "").split(",") if u.strip()]
            role_curr = current_user.get("role")
            can_edit = False
            # Admins can always edit
            if role_curr == "admin":
                can_edit = True
            # Super users and Users can edit if at least one of their units matches the memo's target unit(s)
            elif role_curr in ("super", "user") and user_units:
                memo_units_lower = [mu.lower() for mu in memo_units]
                can_edit = any(unit.lower() in memo_units_lower for unit in user_units)
            if can_edit:
                # Editable form
                e1,e2,e3 = st.columns([1,1,1])
                with e1:
                    # Build list of available statuses. Include a blank option and include inactive statuses.
                    all_stats = get_all_status_names(active_only=False)
                    current_status = memo.get("status") or ""
                    # Start options with blank to allow no status
                    options_status: List[str] = [""]
                    # Ensure current status is present
                    if current_status and current_status not in options_status:
                        options_status.append(current_status)
                    # Append all statuses, avoiding duplicates
                    for st_name in all_stats:
                        if st_name not in options_status:
                            options_status.append(st_name)
                    # Determine default index
                    try:
                        status_idx = options_status.index(current_status)
                    except ValueError:
                        status_idx = 0
                    new_status = st.selectbox("Status", options=options_status, index=status_idx)
                    # Build list of active categories, but include a blank option and the memo's current category
                    actives = get_active_categories()
                    cat_value = memo.get("category") or ""
                    cat_options: List[str] = [""]
                    if cat_value and cat_value not in cat_options:
                        cat_options.append(cat_value)
                    for cat in actives:
                        if cat not in cat_options:
                            cat_options.append(cat)
                    try:
                        cat_idx = cat_options.index(cat_value)
                    except ValueError:
                        cat_idx = 0
                    new_category = st.selectbox("Category", options=cat_options, index=cat_idx)
                with e2:
                    new_subject = st.text_input("Subject", value=memo.get("subject") or "")
                    new_control = st.text_input("Control No.", value=memo.get("control_no") or "")
                with e3:
                    _dlog = pd.to_datetime(memo.get("date_log"), errors="coerce")
                    _ddoc = pd.to_datetime(memo.get("date_doc"), errors="coerce")
                    new_date_log = st.date_input("Date of Log", value=(_dlog.date() if _dlog is not None and not pd.isna(_dlog) else date.today()))
                    new_date_doc = st.date_input("Date of Document", value=(_ddoc.date() if _ddoc is not None and not pd.isna(_ddoc) else date.today()))
                e4,e5,e6 = st.columns([1,1,1])
                with e4:
                    new_from = st.text_input("Memo From", value=memo.get("memo_from") or "")
                with e5:
                    new_thru = st.text_input("Thru", value=memo.get("thru") or "")
                with e6:
                    # Multi-select for divisions/units with role-based permissions. Prepopulate with current memo units.
                    current_for_list = [u.strip() for u in (memo.get("memo_for") or "").split(",") if u.strip()]
                    # Compute available units: start with active units and include current memo units to avoid default errors
                    active_units_edit = get_active_unit_names()
                    unit_options_edit = active_units_edit.copy()
                    for u in current_for_list:
                        if u not in unit_options_edit:
                            unit_options_edit.append(u)
                    # Determine role; only admin and super can modify units
                    cu_role = current_user.get("role")
                    if cu_role in ("admin", "super"):
                        new_for_units = st.multiselect(
                            "Division(s)/Unit(s)",
                            options=unit_options_edit,
                            default=current_for_list,
                            help="Edit the divisions/units tags for this memorandum."
                        )
                    else:
                        # For regular users, show as disabled multi-select and do not allow modification
                        new_for_units = current_for_list
                        st.multiselect(
                            "Division(s)/Unit(s)",
                            options=unit_options_edit,
                            default=current_for_list,
                            disabled=True,
                            help="You cannot modify the divisions/units for this memorandum."
                        )
                new_notes = st.text_area("Notes / Description", value=memo.get("notes") or "", height=120)
                u1,u2 = st.columns([1,1])
                with u1:
                    if st.button("Update", type="primary", use_container_width=True):
                        # Validate at least one division/unit selected
                        if not new_for_units:
                            st.error("At least one division/unit is required to tag this memorandum.")
                        else:
                            conn = get_conn(); c = conn.cursor()
                            try:
                                # Join selected units into comma-separated string
                                new_for_str = ", ".join([u.strip() for u in new_for_units])
                                c.execute(
                                    """
                                    UPDATE memos SET
                                        control_no=?, date_log=?, date_doc=?, memo_from=?, thru=?, memo_for=?,
                                        subject=?, category=?, status=?, notes=?, updated_at=?
                                    WHERE id=?
                                    """,
                                    (
                                        new_control.strip(), str(new_date_log), str(new_date_doc),
                                        new_from.strip(), new_thru.strip(), new_for_str,
                                        new_subject.strip(), new_category, new_status, new_notes.strip(),
                                        today_ts(), sel_id
                                    )
                                )
                                conn.commit(); conn.close()
                                st.success("Memorandum updated.")
                                log_action("memo_update", sel_id, f"control={new_control.strip()}")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("Control No. must be unique. Update aborted.")
                                conn.close()
                with u2:
                    # Admin-only danger zone
                    if is_admin():
                        with st.expander("Danger zone — Admin only", expanded=False):
                            colA, colB = st.columns([1,1])
                            with colA:
                                del_conf = st.text_input("Type DELETE to confirm hard delete", key=f"del_conf_{sel_id}")
                                del_disabled = (del_conf.strip().upper() != "DELETE")
                                if st.button("Hard delete memorandum", use_container_width=True, disabled=del_disabled, key=f"hard_del_{sel_id}"):
                                    conn = get_conn(); c = conn.cursor()
                                    c.execute("DELETE FROM memos WHERE id=?", (sel_id,))
                                    conn.commit(); conn.close()
                                    memo_dir = os.path.join(FILES_DIR, f"memo_{sel_id}")
                                    if os.path.isdir(memo_dir):
                                        for root, _, files in os.walk(memo_dir, topdown=False):
                                            for name in files:
                                                try: os.remove(os.path.join(root, name))
                                                except Exception: pass
                                        try: os.rmdir(memo_dir)
                                        except Exception: pass
                                    log_action("memo_delete", sel_id)
                                    st.warning("Memorandum deleted.")
                                    st.rerun()
                            with colB:
                                clr_conf = st.text_input("Type CLEAR to confirm content wipe", key=f"clr_conf_{sel_id}")
                                clr_disabled = (clr_conf.strip().upper() != "CLEAR")
                                if st.button("Wipe contents (keep record)", use_container_width=True, disabled=clr_disabled, key=f"wipe_{sel_id}"):
                                    clear_memo_contents(sel_id)
                                    log_action("memo_clear_contents", sel_id)
                                    st.success("Memorandum contents wiped (control number and record retained).")
                                    st.rerun()
                    else:
                        st.caption("Only admins can delete or wipe memo contents (Monitor / Manage).")
            else:
                # Read-only view for viewers or unauthorized units
                st.subheader("Memo Details (read-only)")
                rd_cols = [
                    ("Control No.", memo.get("control_no") or ""),
                    ("Date of Log", memo.get("date_log") or ""),
                    ("Date of Document", memo.get("date_doc") or ""),
                    ("Subject", memo.get("subject") or ""),
                    ("Status", memo.get("status") or ""),
                    ("Category", memo.get("category") or ""),
                    ("Memo From", memo.get("memo_from") or ""),
                    ("Thru", memo.get("thru") or ""),
                    ("Memo For", memo.get("memo_for") or ""),
                    ("Notes", memo.get("notes") or "")
                ]
                for label, value in rd_cols:
                    st.write(f"**{label}:** {value}")
            
    if 'sel_id' in locals():
        st.markdown("#### Attachments")
        # Determine if current user can edit this memo (same logic as above)
        cu = st.session_state.get("user") or {}
        user_units = cu.get("units", [])
        memo_for_str = memo.get("memo_for") or ""
        memo_units_list = [u.strip() for u in memo_for_str.split(",") if u.strip()]
        # Determine if attachments can be managed (upload/delete). Same logic as can_edit but without viewer.
        role_curr = cu.get("role")
        can_edit_attach = False
        if role_curr == "admin":
            can_edit_attach = True
        elif role_curr in ("super", "user") and user_units:
            memo_units_lower = [mu.lower() for mu in memo_units_list]
            can_edit_attach = any(unit.lower() in memo_units_lower for unit in user_units)
        # Show uploader only if user can edit
        if can_edit_attach:
            add_files = st.file_uploader(
                "Add files (PDF, Images, TXT, XLSX, DOC/DOCX, PPT/PPTX)",
                type=["pdf","png","jpg","jpeg","webp","gif","txt","xlsx","doc","docx","ppt","pptx"],
                accept_multiple_files=True
            )
            if st.button("Upload New File(s)"):
                saved = save_files(add_files, sel_id)
                log_action("file_upload", sel_id, f"count={len(saved)}")
                st.success(f"Uploaded {len(saved)} file(s).")
                st.rerun()

        fdf = list_files(sel_id)
        if fdf.empty:
            st.info("No files attached.")
        else:
            # Provide option to download all attachments as a zip for super/admin users
            if (is_admin() or is_super()) and not _is_guest():
                zip_bytes = zip_memo_files(sel_id)
                if zip_bytes:
                    st.download_button(
                        "Download all attachments (.zip)",
                        data=zip_bytes,
                        file_name=f"memo_{sel_id}_attachments.zip",
                        mime="application/zip",
                        key=f"zipdl_{sel_id}"
                    )

            # Split images vs other docs
            img_exts = {".png",".jpg",".jpeg",".webp",".gif"}
            image_rows = [r for r in fdf.itertuples(index=False) if os.path.splitext(r.filename)[1].lower() in img_exts]
            doc_rows = [r for r in fdf.itertuples(index=False) if os.path.splitext(r.filename)[1].lower() not in img_exts]

            # Image gallery (wrapped, 3 columns)
            if image_rows:
                st.subheader("Images")
                cols = st.columns(3)
                for i, row in enumerate(image_rows):
                    with cols[i % 3]:
                        try:
                            st.image(row.filepath, caption=row.filename, use_container_width=True)
                        except Exception:
                            st.caption(row.filename)
                        # Download gating: disabled for guest
                        disabled = _is_guest()
                        try:
                            with open(row.filepath, "rb") as fbin:
                                st.download_button(
                                    "Download image", fbin, file_name=row.filename, disabled=disabled, key=f"imgdl_{row.id}")
                        except Exception:
                            pass

            # Other documents
            if doc_rows:
                st.subheader("Documents")
                for row in doc_rows:
                    cA, cB = st.columns([6,2])
                    with cA:
                        st.write(f"📄 **{row.filename}**")
                        st.caption(f"Uploaded: {row.uploaded_at}")
                    with cB:
                        disabled = _is_guest()
                        try:
                            with open(row.filepath, "rb") as fbin:
                                st.download_button(
                                    "Download", fbin, file_name=row.filename, disabled=disabled, key=f"docdl_{row.id}")
                        except Exception:
                            st.caption("File unavailable")
                    # Allow per-file deletion only if editing rights are granted
                    if can_edit_attach:
                        if st.button("Delete", key=f"del_{row.id}"):
                            delete_file(int(row.id))
                            log_action("file_delete", sel_id, f"file_id={row.id}")
                            st.rerun()
# ----------------------------- Page: Import / Export -----------------------------
elif page == "Import / Export":
    st.title("Import / Export")
    # Restrict import/export to super and admin users
    if not (is_super() or is_admin()):
        st.warning("Only Super Users and Admins can import or export data.")
    else:
        tcol1,tcol2 = st.columns([2,1])
        with tcol1:
            st.subheader("Download Excel Template")
            tmpl_bytes = build_template_xlsx()
            st.download_button("Download Template (.xlsx)", tmpl_bytes, file_name="memoranda_template.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
            st.caption("Columns: " + ", ".join(TEMPLATE_COLUMNS))
            # Show currently configured active statuses as guidance for import
            active_stats_list = get_active_statuses()
            st.caption("Allowed Status: " + (", ".join(active_stats_list) if active_stats_list else "Refer to Status settings"))
        with tcol2:
            st.info("Attachments/images are uploaded separately in the memo view after import.")
        st.divider()
        st.subheader("Upload Excel to Import")
        up = st.file_uploader("Choose .xlsx file", type=["xlsx"], accept_multiple_files=False)
        auto_create = st.toggle("Auto-create missing categories", value=True, key="imp_auto_cat")
        auto_gen_cn = st.toggle("Auto-generate Control No. for blanks", value=True, key="imp_auto_cn")
        if up is not None:
            try:
                df_in = pd.read_excel(up)
                st.session_state["import_df"] = df_in
                st.session_state["import_validated"] = False
                st.session_state["import_errors"] = pd.DataFrame()
            except Exception as e:
                st.error(f"Failed to read Excel: {e}")
                st.session_state["import_df"] = None
        if st.session_state.get("import_df") is not None:
            st.write("Preview (first 100 rows):")
            st.dataframe(st.session_state["import_df"].head(100), use_container_width=True, height=300)
            cval,cimp = st.columns([1,1])
            with cval:
                if st.button("Validate", type="primary", key="btn_validate"):
                    err_df = validate_import_df(st.session_state["import_df"], auto_create_cats=auto_create)
                    st.session_state["import_errors"] = err_df
                    st.session_state["import_validated"] = err_df.empty
            with cimp:
                imp_disabled = not st.session_state.get("import_validated", False)
                if st.button("Import Rows", key="btn_import", disabled=imp_disabled):
                    ins, skip, fail = import_from_df(st.session_state["import_df"], auto_create_cats=auto_create, auto_gen_cn=auto_gen_cn)
                    log_action("import_rows", None, f"inserted={ins}, skipped={skip}, failed={fail}")
                    st.success(f"Imported: {ins} | Duplicates skipped: {skip} | Failed: {fail}")
                    st.session_state["import_df"] = None
                    st.session_state["import_validated"] = False
                    st.session_state["import_errors"] = pd.DataFrame()
        else:
            st.info("Upload an .xlsx file to begin.")

# ----------------------------- Page: Categories -----------------------------
elif page == "Categories":
    st.title("Manage Categories")
    with st.form("add_cat"):
        new_cat = st.text_input("New Category Name")
        add_ok = st.form_submit_button("Add Category")
    if add_ok and new_cat and new_cat.strip():
        add_category(new_cat); log_action("category_add", None, f"name={new_cat.strip()}")
        st.success(f"Added category: {new_cat.strip()}")
    st.markdown("### Existing Categories")
    cdf = get_all_categories_df()
    if cdf.empty:
        st.info("No categories yet.")
    else:
        # Header row for clarity
        h1, h2, h3 = st.columns([6, 2, 2])
        with h1:
            st.markdown("**Category Name**")
        with h2:
            st.markdown("**Active**")
        with h3:
            st.markdown("**Delete**")
        for row in cdf.itertuples(index=False):
            cc1, cc2, cc3 = st.columns([6, 2, 2])
            with cc1:
                st.write(f"**{row.name}**")
            with cc2:
                active = bool(row.is_active)
                toggled = st.toggle("", value=active, key=f"active_{row.id}")
                if toggled != active:
                    set_category_active(int(row.id), toggled)
                    log_action("category_toggle", None, f"id={row.id}, active={toggled}")
                    st.rerun()
            with cc3:
                if st.button("Delete", key=f"del_cat_{row.id}"):
                    delete_category(int(row.id))
                    log_action("category_delete", None, f"id={row.id}")
                    st.rerun()

# ----------------------------- Page: Settings -----------------------------
elif page == "Settings":
    st.title("Settings")
    user_role = st.session_state.get("user", {}).get("role") if st.session_state.get("user") else None
    if user_role not in ("admin", "super"):
        st.warning("Only Admin and Super users can access Settings.")
    else:
        # Provide separate tabs for different settings areas
        tab_ctrl, tab_cats, tab_stats = st.tabs(["Control No Scheme", "Categories", "Statuses"])
        # --- Control Number Scheme Tab ---
        with tab_ctrl:
            st.subheader("Control Number Scheme")
            # Default prefix: used when no unit-specific prefix is defined.
            default_prefix = get_setting("control_prefix", "MEMO")
            col_dp, col_ex = st.columns([2, 1])
            with col_dp:
                new_default_prefix = st.text_input(
                    "Default Prefix",
                    value=default_prefix,
                    help="Fallback prefix used when a division/unit has no specific control prefix. Format: '{PREFIX} YY-####' (e.g., FMG 25-0048).",
                    key="ctrl_default_prefix_input",
                )
            with col_ex:
                st.caption("Example next number:")
                try:
                    ex_no_default = next_control_no(new_default_prefix)
                except Exception:
                    ex_no_default = "N/A"
                st.code(ex_no_default, language="text")
            if st.button("Save Default Prefix", type="primary", key="save_default_prefix_btn"):
                set_setting("control_prefix", new_default_prefix.strip() or "MEMO")
                log_action("settings_update", None, f"control_prefix={new_default_prefix.strip()}")
                st.success("Default prefix saved.")
                st.rerun()

            st.divider()
            st.markdown("### Per-unit Control Prefixes")
            st.caption("Assign a unique control number prefix for each division/unit. If a prefix is not specified for a unit, the default prefix will be used. The first selected unit when creating a memo determines the prefix.")
            # List all active units and allow editing their prefixes
            unit_names = get_active_unit_names()
            if not unit_names:
                st.info("No active divisions/units found. Define units via the Users/Admin page.")
            else:
                # Header row for clarity
                hp1, hp2, hp3 = st.columns([4, 3, 2])
                with hp1:
                    st.markdown("**Division/Unit**")
                with hp2:
                    st.markdown("**Prefix**")
                with hp3:
                    st.markdown("**Action**")
                for uname in unit_names:
                    pcol1, pcol2, pcol3 = st.columns([4, 3, 2])
                    with pcol1:
                        st.write(uname)
                    with pcol2:
                        current_prefix = get_unit_prefix(uname)
                        new_unit_prefix = st.text_input(
                            "",
                            value=current_prefix,
                            key=f"unit_prefix_input_{uname}",
                        )
                    with pcol3:
                        if st.button("Save", key=f"save_unit_prefix_{uname}"):
                            set_unit_prefix(uname, new_unit_prefix.strip() or default_prefix)
                            log_action("unit_prefix_update", None, f"unit={uname}, prefix={new_unit_prefix.strip()}")
                            st.success(f"Prefix for {uname} saved.")
                            st.rerun()
        # --- Categories Management Tab ---
        with tab_cats:
            st.subheader("Manage Categories")
            # Add new category
            with st.form("add_cat_form"):
                new_cat = st.text_input("New Category Name", key="add_cat_name")
                add_ok = st.form_submit_button("Add Category")
            if add_ok and new_cat and new_cat.strip():
                add_category(new_cat)
                log_action("category_add", None, f"name={new_cat.strip()}")
                st.success(f"Added category: {new_cat.strip()}")
                st.rerun()
            st.markdown("### Existing Categories")
            cdf = get_all_categories_df()
            if cdf.empty:
                st.info("No categories yet.")
            else:
                # Header row for clarity
                h1, h2, h3 = st.columns([6, 2, 2])
                with h1:
                    st.markdown("**Category Name**")
                with h2:
                    st.markdown("**Active**")
                with h3:
                    st.markdown("**Delete**")
                for row in cdf.itertuples(index=False):
                    cc1, cc2, cc3 = st.columns([6, 2, 2])
                    # Display the category name as plain text. Categories cannot be renamed via UI.
                    with cc1:
                        st.write(f"**{row.name}**")
                    # Active toggle aligned with name
                    with cc2:
                        active = bool(row.is_active)
                        # Use a blank label so toggles align neatly under the header
                        toggled = st.toggle(
                            "",
                            value=active,
                            key=f"cat_active_{row.id}_settings",
                        )
                        if toggled != active:
                            set_category_active(int(row.id), toggled)
                            log_action("category_toggle", None, f"id={row.id}, active={toggled}")
                            st.rerun()
                    # Delete button aligned with name
                    with cc3:
                        if st.button("Delete", key=f"del_cat_{row.id}_settings"):
                            delete_category(int(row.id))
                            log_action("category_delete", None, f"id={row.id}")
                            st.rerun()
        # --- Status Management Tab ---
        with tab_stats:
            st.subheader("Manage Statuses")
            # Add new status
            with st.form("add_status_form"):
                new_status_name = st.text_input("New Status Name", key="add_status_name")
                add_status_ok = st.form_submit_button("Add Status")
            if add_status_ok and new_status_name and new_status_name.strip():
                add_status(new_status_name)
                log_action("status_add", None, f"name={new_status_name.strip()}")
                st.success(f"Added status: {new_status_name.strip()}")
                st.rerun()
            st.markdown("### Existing Statuses")
            sdf = get_all_statuses_df()
            if sdf.empty:
                st.info("No statuses configured.")
            else:
                # Header row for clarity. Each column aligns with the corresponding input/toggle/button.
                h1, h2, h3 = st.columns([6, 2, 2])
                with h1:
                    st.markdown("**Status Name**")
                with h2:
                    st.markdown("**Active**")
                with h3:
                    st.markdown("**Delete**")
                for row in sdf.itertuples(index=False):
                    c1, c2, c3 = st.columns([6, 2, 2])
                    # Editable name input
                    with c1:
                        new_name_val = st.text_input(
                            "",
                            value=row.name,
                            key=f"status_name_{row.id}_settings",
                        )
                        if new_name_val != row.name:
                            rename_status(int(row.id), new_name_val.strip())
                            log_action("status_rename", None, f"id={row.id}, new_name={new_name_val.strip()}")
                            st.rerun()
                    # Active toggle aligned with name
                    with c2:
                        active = bool(row.is_active)
                        toggled = st.toggle(
                            "",
                            value=active,
                            key=f"status_active_{row.id}_settings",
                        )
                        if toggled != active:
                            set_status_active(int(row.id), toggled)
                            log_action("status_toggle", None, f"id={row.id}, active={toggled}")
                            st.rerun()
                    # Delete button aligned with name
                    with c3:
                        if st.button("Delete", key=f"del_status_{row.id}_settings"):
                            delete_status(int(row.id))
                            log_action("status_delete", None, f"id={row.id}")
                            st.rerun()

# ----------------------------- Page: Admin -----------------------------
elif page == "Admin":
    st.title("Admin Panel")
    user_role = st.session_state.get("user", {}).get("role") if st.session_state.get("user") else None
    if user_role not in ("admin", "super"):
        st.warning("Only Admin and Super users can access Settings.")
    else:
        # Three tabs: Users, Audit Trail, and UI Settings
        tab_users, tab_audit, tab_ui = st.tabs(["Users", "Audit Trail", "UI Settings"])
        with tab_users:
            st.subheader("Manage Users")
            # Fetch users from the user database along with their units
            conn = get_user_conn()
            dfu = pd.read_sql(
                """
                SELECT u.id, u.username, u.email, u.role, u.is_active, u.created_at,
                       GROUP_CONCAT(units.name, ', ') AS units
                FROM users AS u
                LEFT JOIN user_units AS uu ON u.id = uu.user_id
                LEFT JOIN units ON units.id = uu.unit_id AND units.is_active=1
                GROUP BY u.id
                ORDER BY u.created_at DESC
                """,
                conn
            )
            conn.close()
            st.dataframe(dfu, use_container_width=True, height=300)
            if not dfu.empty:
                sel_map = {f"{r.username} ({r.email})": int(r.id) for r in dfu.itertuples(index=False)}
                sel_label = st.selectbox("Select user", options=list(sel_map.keys()))
                sel_id = sel_map[sel_label]
                row = dfu[dfu.id == sel_id].iloc[0]
                col1,col2,col3 = st.columns([1,1,2])
                with col1:
                    # Allow role selection among admin, super, user and viewer
                    role_options = ["admin", "super", "user", "viewer"]
                    # Determine default index; fallback to last (viewer) if unknown
                    try:
                        role_index = role_options.index(row["role"])
                    except ValueError:
                        role_index = role_options.index("viewer")
                    role_new = st.selectbox("Role", options=role_options, index=role_index)
                with col2:
                    active_new = st.checkbox("Active", value=bool(row["is_active"]))
                with col3:
                    # Manage units: multiselect from existing units and allow new units
                    # Fetch all active unit names
                    conn = get_user_conn(); c = conn.cursor()
                    c.execute("SELECT name FROM units WHERE is_active=1 ORDER BY name ASC")
                    all_unit_rows = c.fetchall(); conn.close()
                    all_units = [r[0] for r in all_unit_rows]
                    current_units = [u.strip() for u in (row["units"] or "").split(",") if u.strip()]
                    units_selected = st.multiselect(
                        "Divisions/Units",
                        options=all_units,
                        default=current_units,
                        help="Select one or more divisions or units for this user"
                    )
                    new_units_raw = st.text_input("Add new divisions/units (comma-separated)")
                if st.button("Apply Changes"):
                    # Create or update user record in users DB
                    conn = get_user_conn(); c = conn.cursor()
                    # Update role and activation status
                    c.execute("UPDATE users SET role=?, is_active=? WHERE id=?", (role_new, 1 if active_new else 0, sel_id))
                    # Prepare new units list
                    new_units_list = [u.strip() for u in new_units_raw.split(",") if u.strip()]
                    # Ensure any new units exist, then combine with selected
                    # Use the same connection to avoid database locking when ensuring units exist
                    unit_ids = ensure_units_exist(units_selected + new_units_list, conn)
                    # Remove old associations
                    c.execute("DELETE FROM user_units WHERE user_id=?", (sel_id,))
                    # Insert new associations
                    for uid_val in unit_ids:
                        c.execute("INSERT OR IGNORE INTO user_units (user_id, unit_id) VALUES (?,?)", (sel_id, uid_val))
                    conn.commit(); conn.close()
                    # Mirror changes to the memoranda database's users table
                    try:
                        mconn = get_conn(); mc = mconn.cursor()
                        mc.execute(
                            "UPDATE users SET role=?, is_active=? WHERE id=?",
                            (role_new, 1 if active_new else 0, sel_id)
                        )
                        mconn.commit(); mconn.close()
                    except Exception:
                        pass
                    log_action("user_update", None, f"id={sel_id}, role={role_new}, active={active_new}, units={units_selected + new_units_list}")
                    st.success("User updated."); st.rerun()
        with tab_audit:
            st.subheader("Audit Trail (latest 500)")
            conn = get_conn()
            dfa = pd.read_sql("SELECT ts, user_id, action, memo_id, details FROM audit_trail ORDER BY id DESC LIMIT 500", conn)
            conn.close()
            st.dataframe(dfa, use_container_width=True, height=400)

        # UI Settings tab for customizing sidebar and dashboard titles
        with tab_ui:
            st.subheader("UI Branding / Titles")
            st.markdown("Customize the sidebar and dashboard titles shown in the application.")
            sidebar_title_current = get_setting("sidebar_title", "FMG-CMS")
            dashboard_title_current = get_setting("dashboard_title", "FMG - Correspondence Management System (CMS) Dashboard")
            new_sidebar_title = st.text_input(
                "Sidebar Title",
                value=sidebar_title_current,
                help="This title appears at the top of the sidebar navigation.",
                key="ui_sidebar_title_input",
            )
            new_dashboard_title = st.text_input(
                "Dashboard Header",
                value=dashboard_title_current,
                help="This heading appears on the dashboard page.",
                key="ui_dashboard_title_input",
            )
            if st.button("Save UI Titles", key="save_ui_titles_btn"):
                set_setting("sidebar_title", new_sidebar_title.strip() or "FMG-CMS")
                set_setting("dashboard_title", new_dashboard_title.strip() or "FMG - Correspondence Management System (CMS) Dashboard")
                log_action(
                    "ui_titles_update",
                    None,
                    f"sidebar_title={new_sidebar_title.strip()}, dashboard_title={new_dashboard_title.strip()}"
                )
                st.success("UI titles updated.")
                st.rerun()
