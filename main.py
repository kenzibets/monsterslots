# main.py
"""
FastAPI leaderboard backend for Kenzies Fridge (S3 removed).
Uses a committed seed JSON (leaderboard.json next to main.py) to
restore data/leaderboard.json on startup if missing. No env keys needed.
"""
import os
import json
import threading
import hashlib
import hmac
from fastapi import Body
import secrets
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Header, Depends, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import logging
import shutil

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---- S3 removed: use local-seed approach ----
# Path to the seed DB file that's committed into your repository.
# Place a seed file named `leaderboard.json` next to main.py (project root).
SEED_DB_PATH = os.path.join(os.path.dirname(__file__), "leaderboard.json")

# default starting balance for new users
START_BALANCE = 5000.0

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DATA_DIR, "leaderboard.json")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR, exist_ok=True)

_lock = threading.Lock()

def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def _get_month_key(dt: Optional[datetime]=None) -> str:
    dt = dt or datetime.utcnow()
    return dt.strftime("%Y-%m")

def _prev_month_key(dt: Optional[datetime]=None) -> str:
    dt = dt or datetime.utcnow()
    first = dt.replace(day=1)
    prev_last = first - timedelta(days=1)
    return prev_last.strftime("%Y-%m")

# ---------------------------
# File DB helpers (read/write) with seed fallback
# ---------------------------
def _default_db():
    return {
        "users": {},  # leaderboard users: username -> { nickname, balance, last_update, trades, wins, period_start_balance }
        "monthly_winners": {},  # "YYYY-MM" -> { "podium": [...], "closed_at": ISO }
        "last_month_closed": None,
        "recent_trades": [],  # newest-first, each entry includes "username", "nickname", ...
        # auth area:
        "auth": {
            "users": {},  # username -> { salt, passhash, created_at, nickname_optional }
            "sessions": {}  # token -> { username, created_at, expires_at }
        }
    }

def _read_db() -> Dict[str, Any]:
    """
    Read DB from DATA_DIR. If missing, attempt to copy a committed seed file (SEED_DB_PATH)
    into DB_PATH. If seed is missing or copy fails, create and return a default DB.
    """
    # If DB file doesn't exist, try to create it from the seed file committed with the repo.
    if not os.path.exists(DB_PATH):
        try:
            if os.path.exists(SEED_DB_PATH):
                # ensure data dir exists
                os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
                shutil.copyfile(SEED_DB_PATH, DB_PATH)
                logger.info(f"Copied seed DB from {SEED_DB_PATH} -> {DB_PATH}")
                with open(DB_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    data.setdefault("auth", {"users": {}, "sessions": {}})
                    return data
        except Exception as e:
            logger.warning(f"Failed to copy seed DB from {SEED_DB_PATH}: {e}")

        # fallback: create a fresh default DB file
        default = _default_db()
        try:
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump(default, f, indent=2)
            logger.info(f"Created new default DB at {DB_PATH}")
        except Exception as e:
            logger.warning(f"Failed to create DB file at {DB_PATH}: {e}")
        return default

    # if DB exists, load it
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("auth", {"users": {}, "sessions": {}})
            return data
    except Exception as e:
        logger.warning(f"Failed to read DB at {DB_PATH} ({e}), returning default DB")
        return _default_db()

def _write_db(data: Dict[str, Any]):
    """
    Atomically write DB to DB_PATH. No external uploads (no S3).
    """
    tmp = DB_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, DB_PATH)
    except Exception as e:
        logger.warning(f"Atomic write failed ({e}), attempting fallback write")
        try:
            with open(DB_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e2:
            logger.error(f"Failed to write DB to {DB_PATH}: {e2}")

# ---------------------------
# Leaderboard helper utilities
# ---------------------------
def compute_podium_snapshot(users: Dict[str, Any], top_n=3):
    arr = []
    for uname, u in users.items():
        try:
            bal = float(u.get("balance", START_BALANCE))
        except Exception:
            bal = START_BALANCE
        arr.append((uname, u.get("nickname", ""), bal))
    arr.sort(key=lambda x: x[2], reverse=True)
    podium = []
    for i in range(min(top_n, len(arr))):
        uname, nick, bal = arr[i]
        podium.append({
            "position": i+1,
            "username": uname,
            "nickname": nick,
            "balance": round(bal, 2)
        })
    return podium

def compute_user_metrics(user_record: Dict[str, Any]) -> Dict[str, Any]:
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
# Simple password hashing (PBKDF2)
# ---------------------------
def _gen_salt() -> str:
    return secrets.token_hex(16)

def _hash_password(password: str, salt: str, iterations: int = 100_000) -> str:
    # returns hex digest
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    return dk.hex()

def verify_password(password: str, salt: str, expected_hex: str) -> bool:
    return hmac.compare_digest(_hash_password(password, salt), expected_hex)

def generate_token() -> str:
    return secrets.token_urlsafe(32)

SESSION_TTL_SECONDS = int(os.environ.get("SESSION_TTL_SECONDS", 7 * 24 * 3600))  # default 7 days

# ---------------------------
# Pydantic models for auth
# ---------------------------
class RegisterBody(BaseModel):
    username: str
    password: str
    nickname: Optional[str] = None

class LoginBody(BaseModel):
    username: str
    password: str

# Existing models (unchanged)
class UserUpdate(BaseModel):
    nickname: Optional[str] = None
    balance: Optional[float] = None
    trades: Optional[int] = None
    wins: Optional[int] = None
    period_start_balance: Optional[float] = None

class TradeRecord(BaseModel):
    result: str = Field(..., description='Either "win" or "lose"')
    amount: Optional[float] = None
    nickname: Optional[str] = None  # optional nickname to update on trade

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Kenzies Fridge Leaderboard API (local-seed)")

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
    logger.info(f"Starting Kenzies Fridge API (local-seed mode)")
    logger.info(f"STATIC_DIR = {os.path.abspath(STATIC_DIR)}")
    logger.info(f"index.html exists: {os.path.exists(index_path)} -> {index_path}")
    try:
        files = os.listdir(STATIC_DIR)
        logger.info(f"static/*: {files[:30]}")
    except Exception as e:
        logger.info(f"Could not list static dir: {e}")
    # Ensure DB exists; if missing, _read_db will copy the seed file if present
    with _lock:
        _read_db()

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
    }

# ---------------------------
# Auth helpers
# ---------------------------
def _create_auth_user(username: str, password: str, nickname: Optional[str] = None) -> Dict[str, Any]:
    salt = _gen_salt()
    hashhex = _hash_password(password, salt)
    now = _now_iso()
    return {"salt": salt, "passhash": hashhex, "created_at": now, "nickname": nickname or ""}

def _create_session_for_user(db: Dict[str, Any], username: str) -> str:
    token = generate_token()
    now_ts = int(time.time())
    expires_ts = now_ts + SESSION_TTL_SECONDS
    db.setdefault("auth", {}).setdefault("sessions", {})[token] = {
        "username": username,
        "created_at": _now_iso(),
        "expires_at": datetime.utcfromtimestamp(expires_ts).isoformat() + "Z"
    }
    return token

def _cleanup_expired_sessions(db: Dict[str, Any]):
    sess = db.setdefault("auth", {}).setdefault("sessions", {})
    now = datetime.utcnow()
    to_delete = []
    for token, info in list(sess.items()):
        exp = info.get("expires_at")
        try:
            if exp and exp.endswith("Z"):
                exp_dt = datetime.fromisoformat(exp[:-1])
            else:
                exp_dt = datetime.fromisoformat(exp)
        except Exception:
            exp_dt = None
        if exp_dt and exp_dt < now:
            to_delete.append(token)
    for t in to_delete:
        if t in sess:
            del sess[t]

def _get_db_and_user_from_token(authorization: Optional[str]) -> Optional[str]:
    # expects header "Bearer <token>"
    if not authorization:
        return None
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    with _lock:
        db = _read_db()
        _cleanup_expired_sessions(db)
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
# Register / Login endpoints
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
        db = _read_db()
        auth_users = db.setdefault("auth", {}).setdefault("users", {})
        if username in auth_users:
            raise HTTPException(status_code=409, detail="username already exists")
        auth_users[username] = _create_auth_user(username, password, body.nickname)
        # Also create a leaderboard user record (optional)
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
        _write_db(db)
        token = _create_session_for_user(db, username)
        _write_db(db)
    return {"status": "ok", "username": username, "token": token, "message": "registered and logged in"}

@app.post("/api/login")
def login(body: LoginBody):
    username = (body.username or "").strip()
    password = (body.password or "")
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    with _lock:
        db = _read_db()
        auth_users = db.setdefault("auth", {}).setdefault("users", {})
        user = auth_users.get(username)
        if not user:
            raise HTTPException(status_code=401, detail="invalid credentials")
        salt = user.get("salt")
        ph = user.get("passhash")
        if not verify_password(password, salt, ph):
            raise HTTPException(status_code=401, detail="invalid credentials")
        # create session
        token = _create_session_for_user(db, username)
        _write_db(db)
    return {"status": "ok", "token": token, "username": username, "expires_in": SESSION_TTL_SECONDS}

@app.post("/api/logout")
def logout(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing token")
    token = authorization.split(" ", 1)[1].strip()
    with _lock:
        db = _read_db()
        sess = db.setdefault("auth", {}).setdefault("sessions", {})
        if token in sess:
            del sess[token]
            _write_db(db)
            return {"status": "ok", "message": "logged out"}
    raise HTTPException(status_code=401, detail="invalid token")

# ---------------------------
# User-scoped endpoints (NO user_id in paths; use token username)
# ---------------------------
@app.post("/api/user/me/trade")
def record_trade_me(tr: TradeRecord, username: str = Depends(get_current_username)):
    # username is derived from the Bearer token via get_current_username
    user_key = username
    with _lock:
        db = _read_db()
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
                u["balance"] = round(float(u.get("balance", START_BALANCE)) - amt, 2)

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

        _write_db(db)

        metrics = compute_user_metrics(u)

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

@app.get("/api/leaderboard")
def get_leaderboard(limit: int = 100):
    with _lock:
        db = _read_db()
    users = db.get("users", {})
    arr = []
    for uname, u in users.items():
        metrics = compute_user_metrics(u)
        arr.append({
            "username": uname,
            "nickname": u.get("nickname", "") or "",
            "balance": metrics["balance"],
            "performance": metrics["performance"],
            "win_rate": metrics["win_rate"],
            "trades_this_period": metrics["trades_this_period"]
        })
    arr.sort(key=lambda x: x["balance"], reverse=True)
    return {"leaderboard": arr[:max(0, min(limit, 1000))], "timestamp": _now_iso()}

@app.get("/api/user/me")
def get_user_me(username: str = Depends(get_current_username)):
    with _lock:
        db = _read_db()
    user = db.get("users", {}).get(username)
    if user:
        metrics = compute_user_metrics(user)
        return {
            "username": username,
            "nickname": user.get("nickname", "") or "",
            "balance": metrics["balance"],
            "performance": metrics["performance"],
            "win_rate": metrics["win_rate"],
            "trades_this_period": metrics["trades_this_period"],
            "wins": metrics["wins"],
            "period_start_balance": metrics["period_start_balance"],
            "last_update": user.get("last_update")
        }
    else:
        return {
            "username": username,
            "nickname": "",
            "balance": round(START_BALANCE, 2),
            "performance": 0.0,
            "win_rate": 0.0,
            "trades_this_period": 0,
            "wins": 0,
            "period_start_balance": round(START_BALANCE, 2),
            "last_update": None
        }

@app.post("/api/user/me")
def update_user_me(upd: UserUpdate, username: str = Depends(get_current_username)):
    # Only the authenticated user can update their record; username comes from token.
    user_key = username
    with _lock:
        db = _read_db()
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
        if upd.nickname is not None:
            n = (upd.nickname or "").strip()[:40]
            if n != u.get("nickname", ""):
                changed_nick = n
                u["nickname"] = n

        if upd.balance is not None:
            try:
                u["balance"] = round(float(upd.balance), 2)
            except Exception:
                u["balance"] = START_BALANCE

        if upd.trades is not None:
            try:
                u["trades"] = int(upd.trades)
            except Exception:
                u["trades"] = int(u.get("trades", 0) or 0)

        if upd.wins is not None:
            try:
                u["wins"] = int(upd.wins)
            except Exception:
                u["wins"] = int(u.get("wins", 0) or 0)

        if upd.period_start_balance is not None:
            try:
                u["period_start_balance"] = round(float(upd.period_start_balance), 2)
            except Exception:
                u["period_start_balance"] = START_BALANCE

        u["last_update"] = _now_iso()

        if changed_nick:
            recent = db.setdefault("recent_trades", [])
            for ent in recent:
                try:
                    if ent.get("username") == user_key:
                        ent["nickname"] = changed_nick
                except Exception:
                    pass

        _write_db(db)
        metrics = compute_user_metrics(u)

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

# Removed any /api/user/{user_id}/trade style endpoints — /api/user/me/trade is authoritative.

@app.get("/api/live-wins")
def get_live_wins(limit: int = 100, minutes: Optional[int] = None, nickname: Optional[str] = None):
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be > 0")
    limit = min(limit, 500)

    with _lock:
        db = _read_db()
        recent = list(db.get("recent_trades", []))

    cutoff = None
    if minutes is not None:
        try:
            minutes = int(minutes)
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid minutes parameter")

    def parse_ts(s):
        try:
            if s.endswith("Z"):
                s2 = s[:-1]
            else:
                s2 = s
            return datetime.fromisoformat(s2)
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

    return {
        "recent_trades": filtered,
        "summary": summary,
        "timestamp": _now_iso()
    }

@app.post("/api/close_month")
def post_close_month():
    with _lock:
        db = _read_db()
        prev_month = _prev_month_key()
        if prev_month in db.get("monthly_winners", {}):
            return {"status": "already_closed", "month": prev_month}
        podium = compute_podium_snapshot(db.get("users", {}), top_n=3)
        db.setdefault("monthly_winners", {})[prev_month] = {"podium": podium, "closed_at": _now_iso()}
        db["last_month_closed"] = _get_month_key()

        # === reset every user's balance to START_BALANCE for new period ===
        users = db.get("users", {})
        now_iso = _now_iso()
        for uname, u in users.items():
            try:
                u["balance"] = round(float(START_BALANCE), 2)
            except Exception:
                u["balance"] = round(START_BALANCE, 2)
            try:
                u["period_start_balance"] = round(float(START_BALANCE), 2)
            except Exception:
                u["period_start_balance"] = round(START_BALANCE, 2)
            u["trades"] = 0
            u["wins"] = 0
            u["last_update"] = now_iso

        _write_db(db)
    return {"status": "closed", "month": prev_month, "podium": podium}

@app.get("/api/winners/{month}")
def get_winners(month: str):
    with _lock:
        db = _read_db()
    winners = db.get("monthly_winners", {}).get(month)
    if not winners:
        raise HTTPException(status_code=404, detail="No winners for that month")
    return {"month": month, "winners": winners}

@app.get("/api/winners")
def get_latest_winners():
    with _lock:
        db = _read_db()
    mw = db.get("monthly_winners", {})
    if not mw:
        return {"latest": None, "monthly_winners": {}}
    last_month = sorted(mw.keys())[-1]
    return {"latest": last_month, "winners": mw[last_month], "monthly_winners": mw}

@app.post("/api/user/{user_key}/trade")
def record_trade_by_key(user_key: str, tr: TradeRecord = Body(...), authorization: Optional[str] = Header(None)):
    # If token present, it must match the path username (prevent impersonation).
    auth_username = _get_db_and_user_from_token(authorization)
    if auth_username and auth_username != user_key:
        raise HTTPException(status_code=403, detail="token does not match username")

    user_key = user_key or 'guest'
    with _lock:
        db = _read_db()
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
                u["balance"] = round(float(u.get("balance", START_BALANCE)) - amt, 2)

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

        _write_db(db)
        metrics = compute_user_metrics(u)

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
