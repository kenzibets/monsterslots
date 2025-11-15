# main.py
"""
FastAPI leaderboard backend for Kenzies Fridge (Postgres primary, local-seed fallback).
Schema-based Postgres persistence:
 - users (username PK)
 - auth_users (username PK)
 - sessions (token PK)
 - recent_trades (id PK)
 - monthly_winners (month PK, data JSONB)
If DATABASE_URL or psycopg2 is unavailable, falls back to existing local JSON seed file behavior.
"""
import os
import json
import threading
import hashlib
import hmac
from fastapi import Body
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Header, Depends, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import logging
import shutil
import decimal

# Postgres client
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # we'll handle absence gracefully

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---- config ----
SEED_DB_PATH = os.path.join(os.path.dirname(__file__), "leaderboard.json")
START_BALANCE = 5000.0
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "leaderboard.json")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR, exist_ok=True)

_lock = threading.Lock()
SESSION_TTL_SECONDS = int(os.environ.get("SESSION_TTL_SECONDS", 7 * 24 * 3600))  # default 7 days

def _now_iso():
    # produce ISO with trailing Z (UTC)
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _now_ts():
    return datetime.now(timezone.utc)

def _get_month_key(dt: Optional[datetime]=None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.strftime("%Y-%m")

def _prev_month_key(dt: Optional[datetime]=None) -> str:
    dt = dt or datetime.now(timezone.utc)
    first = dt.replace(day=1)
    prev_last = first - timedelta(days=1)
    return prev_last.strftime("%Y-%m")

# ---------------------------
# File fallback helpers (unchanged original behavior)
# ---------------------------
def _default_db():
    return {
        "users": {},
        "monthly_winners": {},
        "last_month_closed": None,
        "recent_trades": [],
        "auth": {"users": {}, "sessions": {}}
    }

def _read_db_file_fallback() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        try:
            if os.path.exists(SEED_DB_PATH):
                os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
                shutil.copyfile(SEED_DB_PATH, DB_PATH)
                logger.info(f"Copied seed DB from {SEED_DB_PATH} -> {DB_PATH}")
                with open(DB_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    data.setdefault("auth", {"users": {}, "sessions": {}})
                    return data
        except Exception as e:
            logger.warning(f"Failed to copy seed DB from {SEED_DB_PATH}: {e}")
        default = _default_db()
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=2)
            logger.info(f"Created new default DB at {DB_PATH}")
        except Exception as e:
            logger.warning(f"Failed to create DB file at {DB_PATH}: {e}")
        return default
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("auth", {"users": {}, "sessions": {}})
            return data
    except Exception as e:
        logger.warning(f"Failed to read DB at {DB_PATH} ({e}), returning default DB")
        return _default_db()

def _write_db_file_fallback(data: Dict[str, Any]):
    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    except Exception:
        pass
    tmp = DB_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DB_PATH)
    except Exception as e:
        logger.warning(f"Atomic write failed ({e}), attempting fallback")
        try:
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e2:
            logger.error(f"Failed to write DB file fallback: {e2}")

# ---------------------------
# Postgres helpers and schema
# ---------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_PG = bool(DATABASE_URL and psycopg2)

def _pg_connect():
    if not USE_PG:
        return None
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    try:
        conn = psycopg2.connect(dsn=url, sslmode="require")
        return conn
    except Exception as e:
        logger.warning(f"Could not connect to Postgres: {e}")
        return None

def _init_schema(conn):
    with conn.cursor() as cur:
        # users
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            nickname TEXT,
            balance NUMERIC,
            last_update TIMESTAMPTZ,
            trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            period_start_balance NUMERIC DEFAULT %s
        );
        """, (decimal.Decimal(str(START_BALANCE)),))
        # auth users
        cur.execute("""
        CREATE TABLE IF NOT EXISTS auth_users (
            username TEXT PRIMARY KEY,
            salt TEXT,
            passhash TEXT,
            created_at TIMESTAMPTZ,
            nickname TEXT
        );
        """)
        # sessions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            username TEXT,
            created_at TIMESTAMPTZ,
            expires_at TIMESTAMPTZ
        );
        """)
        # recent trades
        cur.execute("""
        CREATE TABLE IF NOT EXISTS recent_trades (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ,
            username TEXT,
            nickname TEXT,
            result TEXT,
            amount NUMERIC
        );
        """)
        # monthly winners
        cur.execute("""
        CREATE TABLE IF NOT EXISTS monthly_winners (
            month TEXT PRIMARY KEY,
            data JSONB,
            closed_at TIMESTAMPTZ
        );
        """)
        conn.commit()

def _seed_db_to_postgres_if_empty(conn):
    """
    If users table is empty and SEED_DB_PATH exists, load seed JSON into PG.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users LIMIT 1;")
            if cur.fetchone():
                return  # not empty
        # Load seed
        if os.path.exists(SEED_DB_PATH):
            try:
                with open(SEED_DB_PATH, "r", encoding="utf-8") as f:
                    seed = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load seed JSON for PG seeding: {e}")
                seed = None
            if seed:
                with conn.cursor() as cur:
                    users = seed.get("users", {})
                    for uname, u in users.items():
                        cur.execute("""
                            INSERT INTO users (username, nickname, balance, last_update, trades, wins, period_start_balance)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (username) DO NOTHING;
                        """, (
                            uname,
                            u.get("nickname",""),
                            decimal.Decimal(str(u.get("balance", START_BALANCE))),
                            _iso_to_dt(u.get("last_update")),
                            int(u.get("trades",0) or 0),
                            int(u.get("wins",0) or 0),
                            decimal.Decimal(str(u.get("period_start_balance", START_BALANCE)))
                        ))
                    # recent trades
                    for rt in reversed(seed.get("recent_trades", [])[:500]):  # reversed so IDs increase in original order
                        cur.execute("""
                            INSERT INTO recent_trades (ts, username, nickname, result, amount)
                            VALUES (%s,%s,%s,%s,%s);
                        """, (
                            _iso_to_dt(rt.get("ts")),
                            rt.get("username"),
                            rt.get("nickname",""),
                            rt.get("result"),
                            decimal.Decimal(str(rt.get("amount",0.0)))
                        ))
                    # monthly winners
                    for m, w in (seed.get("monthly_winners") or {}).items():
                        cur.execute("""
                            INSERT INTO monthly_winners (month, data, closed_at)
                            VALUES (%s, %s::jsonb, %s)
                            ON CONFLICT (month) DO NOTHING;
                        """, (m, json.dumps(w), _iso_to_dt(w.get("closed_at"))))
                    # auth users
                    auth = seed.get("auth", {}).get("users", {})
                    now = _now_ts()
                    for uname, au in auth.items():
                        cur.execute("""
                            INSERT INTO auth_users (username, salt, passhash, created_at, nickname)
                            VALUES (%s,%s,%s,%s,%s) ON CONFLICT (username) DO NOTHING;
                        """, (uname, au.get("salt"), au.get("passhash"), _iso_to_dt(au.get("created_at")) or now, au.get("nickname","")))
                    conn.commit()
                logger.info("Seeded Postgres from seed JSON")
    except Exception as e:
        logger.warning(f"Seeding Postgres failed: {e}")

def _iso_to_dt(s: Optional[str]):
    if not s:
        return None
    try:
        # Accept trailing Z as UTC marker
        if s.endswith("Z"):
            s2 = s[:-1]
            try:
                dt = datetime.fromisoformat(s2)
            except Exception:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        return None

# ---------------------------
# Postgres operations (CRUD) - convert DB rows <-> app data
# ---------------------------
def _get_user_pg(conn, username: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT username, nickname, balance, last_update, trades, wins, period_start_balance "
            "FROM users WHERE username = %s;",
            (username,)
        )
        r = cur.fetchone()
        if not r:
            return None

        # Safely convert numeric/database values to Python primitives
        balance = float(r.get("balance")) if r.get("balance") is not None else float(START_BALANCE)
        period_start_balance = (
            float(r.get("period_start_balance"))
            if r.get("period_start_balance") is not None
            else float(START_BALANCE)
        )
        last_update = (r.get("last_update").astimezone(timezone.utc).isoformat().replace("+00:00", "Z")) if r.get("last_update") else None
        trades = int(r.get("trades") or 0)
        wins = int(r.get("wins") or 0)

        return {
            "nickname": r.get("nickname") or "",
            "balance": balance,
            "last_update": last_update,
            "trades": trades,
            "wins": wins,
            "period_start_balance": period_start_balance,
        }

def _upsert_user_pg(conn, username: str, user_obj: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (username,nickname,balance,last_update,trades,wins,period_start_balance)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (username) DO UPDATE
            SET nickname = EXCLUDED.nickname,
                balance = EXCLUDED.balance,
                last_update = EXCLUDED.last_update,
                trades = EXCLUDED.trades,
                wins = EXCLUDED.wins,
                period_start_balance = EXCLUDED.period_start_balance;
        """, (
            username,
            user_obj.get("nickname",""),
            decimal.Decimal(str(user_obj.get("balance", START_BALANCE))),
            _iso_to_dt(user_obj.get("last_update")) or _now_ts(),
            int(user_obj.get("trades", 0) or 0),
            int(user_obj.get("wins", 0) or 0),
            decimal.Decimal(str(user_obj.get("period_start_balance", START_BALANCE)))
        ))
        conn.commit()

def _insert_recent_trade_pg(conn, entry: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recent_trades (ts, username, nickname, result, amount)
            VALUES (%s,%s,%s,%s,%s);
        """, (
            _iso_to_dt(entry.get("ts")) or _now_ts(),
            entry.get("username"),
            entry.get("nickname",""),
            entry.get("result"),
            decimal.Decimal(str(entry.get("amount", 0.0)))
        ))
        conn.commit()
        # trim to latest 500
        cur.execute("DELETE FROM recent_trades WHERE id NOT IN (SELECT id FROM recent_trades ORDER BY ts DESC LIMIT 500);")
        conn.commit()

def _get_recent_trades_pg(conn, limit=100, minutes: Optional[int]=None, nickname: Optional[str]=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        q = "SELECT ts,username,nickname,result,amount FROM recent_trades"
        params = []
        where = []
        if minutes is not None:
            where.append("ts >= %s")
            params.append(datetime.now(timezone.utc) - timedelta(minutes=minutes))
        if nickname:
            where.append("lower(nickname) = lower(%s)")
            params.append(nickname)
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY ts DESC LIMIT %s;"
        params.append(limit)
        cur.execute(q, params)
        rows = cur.fetchall()
        out = []
        for r in rows:
            ts_val = r.get("ts")
            ts_str = (ts_val.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")) if ts_val else None
            out.append({
                "ts": ts_str,
                "username": r.get("username"),
                "nickname": r.get("nickname") or "",
                "result": r.get("result"),
                "amount": float(r.get("amount") or 0.0)
            })
        return out

def _get_leaderboard_pg(conn, limit=100):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT username,nickname,balance,trades,wins,period_start_balance,last_update
            FROM users
            ORDER BY balance::numeric DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        arr = []
        for r in rows:
            bal = float(r.get("balance") or START_BALANCE)
            trades = int(r.get("trades") or 0)
            wins = int(r.get("wins") or 0)
            start = float(r.get("period_start_balance") or START_BALANCE)
            if start == 0:
                perf = 0.0
            else:
                perf = ((bal - start) / start) * 100.0
            win_rate = (wins / trades * 100.0) if trades > 0 else 0.0
            arr.append({
                "username": r.get("username"),
                "nickname": r.get("nickname") or "",
                "balance": round(bal, 2),
                "performance": round(perf, 2),
                "win_rate": round(win_rate, 2),
                "trades_this_period": trades
            })
        return arr

def _create_auth_user_pg(conn, username: str, salt: str, passhash: str, nickname: Optional[str]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO auth_users (username, salt, passhash, created_at, nickname)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (username) DO NOTHING;
        """, (username, salt, passhash, _now_ts(), nickname or ""))
        conn.commit()

def _get_auth_user_pg(conn, username: str):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT username,salt,passhash,created_at,nickname FROM auth_users WHERE username=%s;", (username,))
        return cur.fetchone()

def _create_session_pg(conn, token: str, username: str, expires_at: datetime):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sessions (token, username, created_at, expires_at)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (token) DO UPDATE SET username = EXCLUDED.username, created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at;
        """, (token, username, _now_ts(), expires_at))
        conn.commit()

def _cleanup_expired_sessions_pg(conn):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM sessions WHERE expires_at < %s;", (datetime.now(timezone.utc),))
        conn.commit()

def _get_session_username_pg(conn, token: str) -> Optional[str]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT username,expires_at FROM sessions WHERE token=%s;", (token,))
        r = cur.fetchone()
        if not r:
            return None
        exp = r.get("expires_at")
        if exp and exp < datetime.now(timezone.utc):
            # expired: delete and return None
            with conn.cursor() as cur2:
                cur2.execute("DELETE FROM sessions WHERE token=%s;", (token,))
                conn.commit()
            return None
        return r.get("username")

def _get_monthly_winner_pg(conn, month: str):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT data,closed_at FROM monthly_winners WHERE month=%s;", (month,))
        r = cur.fetchone()
        if not r:
            return None
        closed_at = r.get("closed_at")
        closed_at_str = closed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if closed_at else None
        return {"podium": r.get("data").get("podium"), "closed_at": closed_at_str}

def _get_all_monthly_winners_pg(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT month, data FROM monthly_winners ORDER BY month;")
        rows = cur.fetchall()
        out = {}
        for r in rows:
            out[r.get("month")] = r.get("data")
        return out

def _insert_monthly_winner_pg(conn, month: str, podium: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO monthly_winners (month, data, closed_at)
            VALUES (%s, %s::jsonb, %s)
            ON CONFLICT (month) DO UPDATE SET data = EXCLUDED.data, closed_at = EXCLUDED.closed_at;
        """, (month, json.dumps({"podium": podium}), _now_ts()))
        conn.commit()

# ---------------------------
# Compatibility wrappers: use PG if available, else file fallback
# ---------------------------
def _read_db():
    """
    When PG is configured, we don't return a full JSON object; endpoints directly query PG.
    But some limited logic expects the dict (startup seeding). For compatibility, when PG is present
    return a shallow dict with metadata; otherwise return the file fallback full dict.
    """
    if USE_PG:
        conn = _pg_connect()
        if not conn:
            logger.warning("USE_PG set but connection failed; falling back to file")
            return _read_db_file_fallback()
        try:
            _init_schema(conn)
            _seed_db_to_postgres_if_empty(conn)
            # return a very small dict (endpoints use PG functions instead)
            return {"_pg_connected": True}
        finally:
            try:
                conn.close()
            except Exception:
                pass
    else:
        return _read_db_file_fallback()

def _write_db(_data: Dict[str, Any]):
    """
    With PG primary, writes are handled per-operation. For compatibility we keep a file copy.
    We'll write the full JSON file fallback if provided (best-effort).
    """
    if isinstance(_data, dict):
        try:
            _write_db_file_fallback(_data)
        except Exception:
            pass
    # If using PG, authoritative writes happen in per-endpoint PG functions.

# ---------------------------
# Leaderboard utilities (PG-backed)
# ---------------------------
def compute_podium_snapshot_from_users_rows(rows):
    podium = []
    for i, r in enumerate(rows):
        nickname = r.get("nickname") or ""
        balance = float(r.get("balance") or START_BALANCE)
        podium.append({
            "position": i+1,
            "username": r.get("username"),
            "nickname": nickname,
            "balance": round(balance, 2)
        })
    return podium

def compute_user_metrics_from_record(user_record: Dict[str, Any]) -> Dict[str, Any]:
    try:
        balance = float(user_record.get("balance", START_BALANCE))
    except Exception:
        balance = START_BALANCE
    try:
        start = float(user_record.get("period_start_balance", START_BALANCE))
    except Exception:
        start = START_BALANCE
    if start == 0:
        performance = 0.0
    else:
        performance = ((balance - start) / start) * 100.0
    trades = int(user_record.get("trades", 0) or 0)
    wins = int(user_record.get("wins", 0) or 0)
    if trades <= 0:
        win_rate = 0.0
    else:
        win_rate = (wins / trades) * 100.0
    return {
        "performance": round(performance, 2),
        "win_rate": round(win_rate, 2),
        "trades_this_period": trades,
        "wins": wins,
        "period_start_balance": round(start, 2),
        "balance": round(balance, 2)
    }

# ---------------------------
# Simple password hashing
# ---------------------------
def _gen_salt() -> str:
    return secrets.token_hex(16)

def _hash_password(password: str, salt: str, iterations: int = 100_000) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return dk.hex()

def verify_password(password: str, salt: str, expected_hex: str) -> bool:
    return hmac.compare_digest(_hash_password(password, salt), expected_hex)

def generate_token() -> str:
    return secrets.token_urlsafe(32)

# ---------------------------
# Pydantic models
# ---------------------------
class RegisterBody(BaseModel):
    username: str
    password: str
    nickname: Optional[str] = None

class LoginBody(BaseModel):
    username: str
    password: str

class UserUpdate(BaseModel):
    nickname: Optional[str] = None
    balance: Optional[float] = None
    trades: Optional[int] = None
    wins: Optional[int] = None
    period_start_balance: Optional[float] = None

class TradeRecord(BaseModel):
    result: str = Field(..., description='Either "win" or "lose"')
    amount: Optional[float] = None
    nickname: Optional[str] = None

# ---------------------------
# FastAPI app & endpoints
# ---------------------------
app = FastAPI(title="Kenzies Fridge Leaderboard API (Postgres primary)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=FileResponse)
def root_index():
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path, media_type="text/html")
    return JSONResponse({"message": "Kenzies Fridge API - static index not found"}, status_code=200)

@app.on_event("startup")
def startup_info():
    index_path = os.path.join(STATIC_DIR, "index.html")
    logger.info(f"Starting Kenzies Fridge API (Postgres primary, file fallback)")
    logger.info(f"STATIC_DIR = {os.path.abspath(STATIC_DIR)}")
    logger.info(f"index.html exists: {os.path.exists(index_path)} -> {index_path}")
    try:
        files = os.listdir(STATIC_DIR)
        logger.info(f"static/*: {files[:30]}")
    except Exception as e:
        logger.info(f"Could not list static dir: {e}")
    # Ensure DB exists; if missing, _read_db will seed fallback or PG
    with _lock:
        db = _read_db()
        # If PG connected, seeding attempted in _read_db

@app.get("/_debug/static-files")
def debug_static_files():
    index_path = os.path.join(STATIC_DIR, "index.html")
    exists = os.path.exists(index_path)
    try:
        listing = sorted(os.listdir(STATIC_DIR))
    except Exception as e:
        listing = f"error: {e}"
    return {
        "static_dir": os.path.abspath(STATIC_DIR),
        "index_exists": exists,
        "index_path": index_path,
        "listing_sample": listing[:200] if isinstance(listing, list) else listing,
        "use_postgres": USE_PG
    }

# ---------------------------
# Auth helpers (now PG-backed when available)
# ---------------------------
def _create_auth_user(username: str, password: str, nickname: Optional[str] = None) -> Dict[str, Any]:
    salt = _gen_salt()
    hashhex = _hash_password(password, salt)
    now = _now_iso()
    return {"salt": salt, "passhash": hashhex, "created_at": now, "nickname": nickname or ""}

def _create_session_for_user_pg(db_conn, username: str) -> str:
    token = generate_token()
    now_ts = int(time.time())
    expires_ts = now_ts + SESSION_TTL_SECONDS
    expires_dt = datetime.utcfromtimestamp(expires_ts).replace(tzinfo=timezone.utc)
    _create_session_pg(db_conn, token, username, expires_dt)
    return token

def _cleanup_expired_sessions_db(db_conn=None, fallback_db=None):
    if USE_PG and db_conn:
        _cleanup_expired_sessions_pg(db_conn)
    else:
        # fallback: cleanup in-file sessions
        if fallback_db:
            sess = fallback_db.setdefault("auth", {}).setdefault("sessions", {})
            to_del = []
            for t, info in list(sess.items()):
                exp = info.get("expires_at")
                try:
                    if exp and exp.endswith("Z"):
                        exp_dt = datetime.fromisoformat(exp[:-1])
                        exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                    else:
                        exp_dt = datetime.fromisoformat(exp)
                        if exp_dt.tzinfo is None:
                            exp_dt = exp_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    exp_dt = None
                if exp_dt and exp_dt < datetime.now(timezone.utc):
                    to_del.append(t)
            for t in to_del:
                sess.pop(t, None)

def _get_db_and_user_from_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    if USE_PG:
        conn = _pg_connect()
        if not conn:
            return None
        try:
            _cleanup_expired_sessions_pg(conn)
            username = _get_session_username_pg(conn, token)
            return username
        finally:
            conn.close()
    else:
        db = _read_db_file_fallback()
        _cleanup_expired_sessions_db(None, db)
        sess = db.get("auth", {}).get("sessions", {})
        info = sess.get(token)
        if not info:
            return None
        return info.get("username")

async def get_current_username(authorization: Optional[str] = Header(None)):
    username = _get_db_and_user_from_token(authorization)
    if not username:
        raise HTTPException(status_code=401, detail="Invalid or missing token")
    return username

# ---------------------------
# Register / Login endpoints (PG primary)
# ---------------------------
@app.post("/api/register")
def register(body: RegisterBody):
    username = (body.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")
    password = (body.password or "")
    if not password or len(password) < 6:
        raise HTTPException(status_code=400, detail="password required (min 6 chars)")
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                _init_schema(conn)
                existing = _get_auth_user_pg(conn, username)
                if existing:
                    raise HTTPException(status_code=409, detail="username already exists")
                au = _create_auth_user(username, password, body.nickname)
                _create_auth_user_pg(conn, username, au["salt"], au["passhash"], au.get("nickname"))
                # create users row
                user_obj = {
                    "nickname": body.nickname or username,
                    "balance": START_BALANCE,
                    "last_update": _now_iso(),
                    "trades": 0,
                    "wins": 0,
                    "period_start_balance": START_BALANCE
                }
                _upsert_user_pg(conn, username, user_obj)
                token = _create_session_for_user_pg(conn, username)
                return {"status": "ok", "username": username, "token": token, "message": "registered and logged in"}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            auth_users = db.setdefault("auth", {}).setdefault("users", {})
            if username in auth_users:
                raise HTTPException(status_code=409, detail="username already exists")
            auth_users[username] = _create_auth_user(username, password, body.nickname)
            users = db.setdefault("users", {})
            if username not in users:
                users[username] = {
                    "nickname": body.nickname or username,
                    "balance": START_BALANCE,
                    "last_update": _now_iso(),
                    "trades": 0,
                    "wins": 0,
                    "period_start_balance": START_BALANCE
                }
            # create session
            token = generate_token()
            now_ts = int(time.time())
            expires_ts = now_ts + SESSION_TTL_SECONDS
            db.setdefault("auth", {}).setdefault("sessions", {})[token] = {
                "username": username,
                "created_at": _now_iso(),
                "expires_at": datetime.utcfromtimestamp(expires_ts).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            }
            _write_db_file_fallback(db)
            return {"status": "ok", "username": username, "token": token, "message": "registered and logged in"}

@app.post("/api/login")
def login(body: LoginBody):
    username = (body.username or "").strip()
    password = (body.password or "")
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                _init_schema(conn)
                user = _get_auth_user_pg(conn, username)
                if not user:
                    raise HTTPException(status_code=401, detail="invalid credentials")
                salt = user.get("salt")
                ph = user.get("passhash")
                if not verify_password(password, salt, ph):
                    raise HTTPException(status_code=401, detail="invalid credentials")
                token = generate_token()
                expires_dt = datetime.utcfromtimestamp(int(time.time()) + SESSION_TTL_SECONDS).replace(tzinfo=timezone.utc)
                _create_session_pg(conn, token, username, expires_dt)
                return {"status": "ok", "token": token, "username": username, "expires_in": SESSION_TTL_SECONDS}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            auth_users = db.setdefault("auth", {}).setdefault("users", {})
            user = auth_users.get(username)
            if not user:
                raise HTTPException(status_code=401, detail="invalid credentials")
            salt = user.get("salt")
            ph = user.get("passhash")
            if not verify_password(password, salt, ph):
                raise HTTPException(status_code=401, detail="invalid credentials")
            token = generate_token()
            now_ts = int(time.time())
            expires_ts = now_ts + SESSION_TTL_SECONDS
            db.setdefault("auth", {}).setdefault("sessions", {})[token] = {
                "username": username,
                "created_at": _now_iso(),
                "expires_at": datetime.utcfromtimestamp(expires_ts).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
            }
            _write_db_file_fallback(db)
            return {"status": "ok", "token": token, "username": username, "expires_in": SESSION_TTL_SECONDS}

@app.post("/api/logout")
def logout(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1].strip()
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM sessions WHERE token=%s;", (token,))
                    conn.commit()
                    return {"status": "ok", "message": "logged out"}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            sess = db.setdefault("auth", {}).setdefault("sessions", {})
            if token in sess:
                del sess[token]
                _write_db_file_fallback(db)
                return {"status": "ok", "message": "logged out"}
            raise HTTPException(status_code=401, detail="invalid token")

# ---------------------------
# User endpoints (PG primary)
# ---------------------------
@app.post("/api/user/me/trade")
def record_trade_me(tr: TradeRecord, username: str = Depends(get_current_username)):
    user_key = username
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                _init_schema(conn)
                # fetch or create user row
                cur_user = _get_user_pg(conn, user_key)
                if not cur_user:
                    cur_user = {
                        "nickname": "",
                        "balance": START_BALANCE,
                        "last_update": None,
                        "trades": 0,
                        "wins": 0,
                        "period_start_balance": START_BALANCE
                    }
                changed_nick = None
                if tr.nickname is not None:
                    n = (tr.nickname or "").strip()[:40]
                    if n != cur_user.get("nickname", ""):
                        changed_nick = n
                        cur_user["nickname"] = n
                cur_user.setdefault("trades", 0)
                cur_user.setdefault("wins", 0)
                cur_user.setdefault("period_start_balance", START_BALANCE)
                cur_user.setdefault("balance", START_BALANCE)
                res = (tr.result or "").lower()
                if res not in ("win", "lose"):
                    raise HTTPException(status_code=400, detail='result must be "win" or "lose"')
                cur_user["trades"] = int(cur_user.get("trades", 0)) + 1
                if res == "win":
                    cur_user["wins"] = int(cur_user.get("wins", 0)) + 1
                amt = 0.0
                if tr.amount is not None:
                    try:
                        amt = float(tr.amount)
                    except Exception:
                        amt = 0.0
                if tr.amount is not None:
                    if res == "win":
                        cur_user["balance"] = round(float(cur_user.get("balance", START_BALANCE)) + amt, 2)
                    else:
                        # subtract on loss and clamp >= 0.0
                        cur_user["balance"] = round(max(0.0, float(cur_user.get("balance", START_BALANCE)) - amt), 2)
                cur_user["last_update"] = _now_iso()
                # upsert user to PG (authoritative)
                _upsert_user_pg(conn, user_key, cur_user)
                # update recent_trades
                trade_entry = {
                    "ts": cur_user["last_update"],
                    "username": user_key,
                    "nickname": cur_user.get("nickname", "") or "",
                    "result": res,
                    "amount": round(amt, 2)
                }
                _insert_recent_trade_pg(conn, trade_entry)
                metrics = compute_user_metrics_from_record({
                    "balance": cur_user["balance"],
                    "period_start_balance": cur_user.get("period_start_balance", START_BALANCE),
                    "trades": cur_user.get("trades", 0),
                    "wins": cur_user.get("wins", 0)
                })
                resp = {
                    "status": "ok",
                    "user": {
                        "username": user_key,
                        "nickname": cur_user.get("nickname", "") or "",
                        "balance": metrics["balance"],
                        "performance": metrics["performance"],
                        "win_rate": metrics["win_rate"],
                        "trades_this_period": metrics["trades_this_period"],
                        "wins": metrics["wins"],
                        "period_start_balance": metrics["period_start_balance"],
                        "last_update": cur_user.get("last_update")
                    }
                }
                if changed_nick:
                    resp["message"] = f"nickname set to {changed_nick}"
                return resp
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            users = db.setdefault("users", {})
            u = users.setdefault(user_key, {
                "nickname": "",
                "balance": START_BALANCE,
                "last_update": None,
                "trades": 0,
                "wins": 0,
                "period_start_balance": START_BALANCE
            })
            changed_nick = None
            if tr.nickname is not None:
                n = (tr.nickname or "").strip()[:40]
                if n != u.get("nickname", ""):
                    changed_nick = n
                    u["nickname"] = n
                    recent = db.setdefault("recent_trades", [])
                    for ent in recent:
                        try:
                            if ent.get("username") == user_key:
                                ent["nickname"] = changed_nick
                        except Exception:
                            pass
            u.setdefault("trades", 0)
            u.setdefault("wins", 0)
            u.setdefault("period_start_balance", START_BALANCE)
            u.setdefault("balance", START_BALANCE)
            res = (tr.result or "").lower()
            if res not in ("win", "lose"):
                raise HTTPException(status_code=400, detail='result must be "win" or "lose"')
            u["trades"] = int(u.get("trades", 0)) + 1
            if res == "win":
                u["wins"] = int(u.get("wins", 0)) + 1
            amt = 0.0
            if tr.amount is not None:
                try:
                    amt = float(tr.amount)
                except Exception:
                    amt = 0.0
            if tr.amount is not None:
                if res == "win":
                    u["balance"] = round(float(u.get("balance", START_BALANCE)) + amt, 2)
                else:
                    u["balance"] = round(max(0.0, float(u.get("balance", START_BALANCE)) - amt), 2)
            u["last_update"] = _now_iso()
            trade_entry = {
                "ts": u["last_update"],
                "username": user_key,
                "nickname": u.get("nickname", "") or "",
                "result": res,
                "amount": round(amt, 2)
            }
            recent = db.setdefault("recent_trades", [])
            recent.insert(0, trade_entry)
            MAX_RECENT_TRADES = 500
            if len(recent) > MAX_RECENT_TRADES:
                del recent[MAX_RECENT_TRADES:]
            _write_db_file_fallback(db)
            metrics = compute_user_metrics_from_record(u)
            resp = {
                "status": "ok",
                "user": {
                    "username": user_key,
                    "nickname": u.get("nickname", "") or "",
                    "balance": metrics["balance"],
                    "performance": metrics["performance"],
                    "win_rate": metrics["win_rate"],
                    "trades_this_period": metrics["trades_this_period"],
                    "wins": metrics["wins"],
                    "period_start_balance": metrics["period_start_balance"],
                    "last_update": u.get("last_update")
                }
            }
            if changed_nick:
                resp["message"] = f"nickname set to {changed_nick}"
            return resp

# ---------------------------
# live-wins, winners, close_month endpoints
# ---------------------------
@app.get("/api/live-wins")
def get_live_wins(limit: int = 100, minutes: Optional[int] = None, nickname: Optional[str] = None):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be > 0")
    limit = min(limit, 500)
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                recent = _get_recent_trades_pg(conn, limit=limit, minutes=minutes, nickname=nickname)
                # build summary
                summary = {}
                for e in recent:
                    nick = (e.get("nickname") or "")[:40]
                    key = nick if nick.strip() else 'Anon'
                    s = summary.setdefault(key, {"net": 0.0, "wins": 0, "losses": 0, "trades": 0})
                    amt = float(e.get("amount", 0.0) or 0.0)
                    if e.get("result") == "win":
                        s["net"] = round(s["net"] + amt, 2)
                        s["wins"] += 1
                    else:
                        s["net"] = round(s["net"] - amt, 2)
                        s["losses"] += 1
                    s["trades"] += 1
                return {"recent_trades": recent, "summary": summary, "timestamp": _now_iso()}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            recent = list(db.get("recent_trades", []))
            cutoff = None
            if minutes is not None:
                try:
                    minutes = int(minutes)
                    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
                except Exception:
                    raise HTTPException(status_code=400, detail="invalid minutes parameter")
            def parse_ts(s):
                try:
                    if s.endswith("Z"):
                        s2 = s[:-1]
                    else:
                        s2 = s
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    return None
            filtered = []
            lower_filter = nickname.lower().strip() if nickname else None
            for entry in recent:
                if cutoff:
                    ts = parse_ts(entry.get("ts", ""))
                    if not ts or ts < cutoff:
                        continue
                if lower_filter:
                    if (entry.get("nickname") or "").strip().lower() != lower_filter:
                        continue
                filtered.append(entry)
                if len(filtered) >= limit:
                    break
            summary = {}
            for e in filtered:
                nick = (e.get("nickname") or "")[:40]
                key = nick if nick.strip() else 'Anon'
                s = summary.setdefault(key, {"net": 0.0, "wins": 0, "losses": 0, "trades": 0})
                amt = float(e.get("amount", 0.0) or 0.0)
                if e.get("result") == "win":
                    s["net"] = round(s["net"] + amt, 2)
                    s["wins"] += 1
                else:
                    s["net"] = round(s["net"] - amt, 2)
                    s["losses"] += 1
                s["trades"] += 1
            return {"recent_trades": filtered, "summary": summary, "timestamp": _now_iso()}

@app.post("/api/close_month")
def post_close_month():
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                _init_schema(conn)
                prev_month = _prev_month_key()
                # check if already closed
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM monthly_winners WHERE month=%s;", (prev_month,))
                    if cur.fetchone():
                        return {"status": "already_closed", "month": prev_month}
                # compute podium
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT username,nickname,balance FROM users ORDER BY balance::numeric DESC LIMIT 3;
                    """)
                    rows = cur.fetchall()
                podium = compute_podium_snapshot_from_users_rows(rows)
                _insert_monthly_winner_pg(conn, prev_month, podium)
                # reset balances/trades/wins for all users
                now_iso = _now_iso()
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE users SET balance=%s, period_start_balance=%s, trades=0, wins=0, last_update=%s;
                    """, (decimal.Decimal(str(START_BALANCE)), decimal.Decimal(str(START_BALANCE)), _iso_to_dt(now_iso)))
                    conn.commit()
                return {"status": "closed", "month": prev_month, "podium": podium}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            prev_month = _prev_month_key()
            if prev_month in db.get("monthly_winners", {}):
                return {"status": "already_closed", "month": prev_month}
            podium = compute_podium_snapshot_from_users_rows(sorted(
                [{"username": u, "nickname": v.get("nickname",""), "balance": v.get("balance", START_BALANCE)} for u,v in db.get("users", {}).items()],
                key=lambda x: x["balance"], reverse=True)[:3])
            db.setdefault("monthly_winners", {})[prev_month] = {"podium": podium, "closed_at": _now_iso()}
            db["last_month_closed"] = _get_month_key()
            for uname, u in db.get("users", {}).items():
                u["balance"] = round(float(START_BALANCE), 2)
                u["period_start_balance"] = round(float(START_BALANCE), 2)
                u["trades"] = 0
                u["wins"] = 0
                u["last_update"] = _now_iso()
            _write_db_file_fallback(db)
            return {"status": "closed", "month": prev_month, "podium": podium}

@app.get("/api/winners/{month}")
def get_winners(month: str):
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                winners = _get_monthly_winner_pg(conn, month)
                if not winners:
                    raise HTTPException(status_code=404, detail="No winners for that month")
                return {"month": month, "winners": winners}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            winners = db.get("monthly_winners", {}).get(month)
            if not winners:
                raise HTTPException(status_code=404, detail="No winners for that month")
            return {"month": month, "winners": winners}

@app.get("/api/winners")
def get_latest_winners():
    with _lock:
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                all_winners = _get_all_monthly_winners_pg(conn)
                if not all_winners:
                    return {"latest": None, "monthly_winners": {}}
                last_month = sorted(all_winners.keys())[-1]
                return {"latest": last_month, "winners": all_winners[last_month], "monthly_winners": all_winners}
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            mw = db.get("monthly_winners", {})
            if not mw:
                return {"latest": None, "monthly_winners": {}}
            last_month = sorted(mw.keys())[-1]
            return {"latest": last_month, "winners": mw[last_month], "monthly_winners": mw}

@app.post("/api/user/{user_key}/trade")
def record_trade_by_key(user_key: str, tr: TradeRecord = Body(...), authorization: Optional[str] = Header(None)):
    auth_username = _get_db_and_user_from_token(authorization)
    if auth_username and auth_username != user_key:
        raise HTTPException(status_code=403, detail="token does not match username")
    user_key = user_key or 'guest'
    with _lock:
        # Reuse record_trade_me flow but without auth dependency
        if USE_PG:
            conn = _pg_connect()
            if not conn:
                raise HTTPException(status_code=500, detail="Postgres connection failed")
            try:
                _init_schema(conn)
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT username,nickname,balance,last_update,trades,wins,period_start_balance FROM users WHERE username=%s;", (user_key,))
                    row = cur.fetchone()
                if row:
                    u = {
                        "nickname": row.get("nickname") or "",
                        "balance": float(row.get("balance") or START_BALANCE),
                        "last_update": (row.get("last_update").astimezone(timezone.utc).isoformat().replace("+00:00","Z")) if row.get("last_update") else None,
                        "trades": int(row.get("trades") or 0),
                        "wins": int(row.get("wins") or 0),
                        "period_start_balance": float(row.get("period_start_balance") or START_BALANCE)
                    }
                else:
                    u = {
                        "nickname": "",
                        "balance": START_BALANCE,
                        "last_update": None,
                        "trades": 0,
                        "wins": 0,
                        "period_start_balance": START_BALANCE
                    }
                changed_nick = None
                if tr.nickname is not None:
                    n = (tr.nickname or "").strip()[:40]
                    if n != u.get("nickname", ""):
                        changed_nick = n
                        u["nickname"] = n
                u.setdefault("trades", 0)
                u.setdefault("wins", 0)
                u.setdefault("period_start_balance", START_BALANCE)
                u.setdefault("balance", START_BALANCE)
                res = (tr.result or "").lower()
                if res not in ("win", "lose"):
                    raise HTTPException(status_code=400, detail='result must be "win" or "lose"')
                u["trades"] = int(u.get("trades", 0)) + 1
                if res == "win":
                    u["wins"] = int(u.get("wins", 0)) + 1
                amt = 0.0
                if tr.amount is not None:
                    try:
                        amt = float(tr.amount)
                    except Exception:
                        amt = 0.0
                if tr.amount is not None:
                    if res == "win":
                        u["balance"] = round(float(u.get("balance", START_BALANCE)) + amt, 2)
                    else:
                        u["balance"] = round(max(0.0, float(u.get("balance", START_BALANCE)) - amt), 2)
                u["last_update"] = _now_iso()
                _upsert_user_pg(conn, user_key, u)
                trade_entry = {
                    "ts": u["last_update"],
                    "username": user_key,
                    "nickname": u.get("nickname", "") or "",
                    "result": res,
                    "amount": round(amt, 2)
                }
                _insert_recent_trade_pg(conn, trade_entry)
                metrics = compute_user_metrics_from_record(u)
                resp = {
                    "status": "ok",
                    "user": {
                        "username": user_key,
                        "nickname": u.get("nickname", "") or "",
                        "balance": metrics["balance"],
                        "performance": metrics["performance"],
                        "win_rate": metrics["win_rate"],
                        "trades_this_period": metrics["trades_this_period"],
                        "wins": metrics["wins"],
                        "period_start_balance": metrics["period_start_balance"],
                        "last_update": u.get("last_update")
                    }
                }
                if changed_nick:
                    resp["message"] = f"nickname set to {changed_nick}"
                return resp
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            db = _read_db_file_fallback()
            users = db.setdefault("users", {})
            u = users.setdefault(user_key, {
                "nickname": "",
                "balance": START_BALANCE,
                "last_update": None,
                "trades": 0,
                "wins": 0,
                "period_start_balance": START_BALANCE
            })
            changed_nick = None
            if tr.nickname is not None:
                n = (tr.nickname or "").strip()[:40]
                if n != u.get("nickname", ""):
                    changed_nick = n
                    u["nickname"] = n
                    recent = db.setdefault("recent_trades", [])
                    for ent in recent:
                        try:
                            if ent.get("username") == user_key:
                                ent["nickname"] = changed_nick
                        except Exception:
                            pass
            u.setdefault("trades", 0)
            u.setdefault("wins", 0)
            u.setdefault("period_start_balance", START_BALANCE)
            u.setdefault("balance", START_BALANCE)
            res = (tr.result or "").lower()
            if res not in ("win", "lose"):
                raise HTTPException(status_code=400, detail='result must be "win" or "lose"')
            u["trades"] = int(u.get("trades", 0)) + 1
            if res == "win":
                u["wins"] = int(u.get("wins", 0)) + 1
            amt = 0.0
            if tr.amount is not None:
                try:
                    amt = float(tr.amount)
                except Exception:
                    amt = 0.0
            if tr.amount is not None:
                if res == "win":
                    u["balance"] = round(float(u.get("balance", START_BALANCE)) + amt, 2)
                else:
                    u["balance"] = round(max(0.0, float(u.get("balance", START_BALANCE)) - amt), 2)
            u["last_update"] = _now_iso()
            trade_entry = {
                "ts": u["last_update"],
                "username": user_key,
                "nickname": u.get("nickname", "") or "",
                "result": res,
                "amount": round(amt, 2)
            }
            recent = db.setdefault("recent_trades", [])
            recent.insert(0, trade_entry)
            MAX_RECENT_TRADES = 500
            if len(recent) > MAX_RECENT_TRADES:
                del recent[MAX_RECENT_TRADES:]
            _write_db_file_fallback(db)
            metrics = compute_user_metrics_from_record(u)
            resp = {
                "status": "ok",
                "user": {
                    "username": user_key,
                    "nickname": u.get("nickname", "") or "",
                    "balance": metrics["balance"],
                    "performance": metrics["performance"],
                    "win_rate": metrics["win_rate"],
                    "trades_this_period": metrics["trades_this_period"],
                    "wins": metrics["wins"],
                    "period_start_balance": metrics["period_start_balance"],
                    "last_update": u.get("last_update")
                }
            }
            if changed_nick:
                resp["message"] = f"nickname set to {changed_nick}"
            return resp
