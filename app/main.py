# app/main.py
import os
import json
import time
import subprocess
import hashlib
from typing import Optional
import math
import sys

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from pydantic import BaseModel
from jose import JWTError, jwt
from app.system_status import load_status
from app.auth import (
    authenticate_user,
    create_access_token,
    get_current_user,
    require_admin,
    User,
)

from app.agent import chat




# ==========================================================
# CONFIG
# ==========================================================

BASE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
METRICS_UI_PATH = os.path.join(DATA_DIR, "metrics_ui.json")
SYSTEM_STATUS_FILE = os.path.join(DATA_DIR, "system_status.json")

SECRET_KEY = os.environ.get("JWT_SECRET", "dev_secret_change_me")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 3600

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

# ==========================================================
# FASTAPI APP
# ==========================================================

app = FastAPI(title="Mutual Fund Advisor API")

app.include_router(chat.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================================
# SIMPLE AUTH (SHA256 – SAFE FOR INTERNAL TOOL)
# ==========================================================

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hash_password(plain_password) == hashed_password


USERS = {
    "admin": {
        "username": "admin",
        "full_name": "Administrator",
        "hashed_password": hash_password("adminpass"),
        "role": "admin",
    },
    "user": {
        "username": "user",
        "full_name": "Normal User",
        "hashed_password": hash_password("userpass"),
        "role": "user",
    },
}

# ==========================================================
# MODELS
# ==========================================================

class Token(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    username: str
    role: str

class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[str] = None

class User(BaseModel):
    username: str
    full_name: Optional[str]
    role: str

# ==========================================================
# AUTH HELPERS
# ==========================================================

def authenticate_user(username: str, password: str) -> Optional[User]:
    u = USERS.get(username)
    if not u:
        return None
    if not verify_password(password, u["hashed_password"]):
        return None
    return User(
        username=u["username"],
        full_name=u.get("full_name"),
        role=u["role"]
    )

def create_access_token(data: dict, expires_seconds: int):
    payload = data.copy()
    expire = int(time.time()) + expires_seconds
    payload.update({"exp": expire})
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token, expire



# ==========================================================
# AUTH ENDPOINT
# ==========================================================

@app.post("/api/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    token, expiry = create_access_token(
        {"sub": user.username, "role": user.role},
        ACCESS_TOKEN_EXPIRE_SECONDS
    )

    return Token(
        access_token=token,
        token_type="bearer",
        expires_in=ACCESS_TOKEN_EXPIRE_SECONDS,
        username=user.username,
        role=user.role,
    )

# ==========================================================
# DATA ENDPOINTS
# ==========================================================

@app.get("/api/metrics")
async def get_metrics(user: User = Depends(get_current_user)):
    if not os.path.exists(METRICS_UI_PATH):
        raise HTTPException(
            status_code=404,
            detail="metrics_ui.json not found. Run admin generate UI."
        )
    with open(METRICS_UI_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    return sanitize_for_json(data)

# ==========================================================
# ADMIN HELPERS
# ==========================================================

def run_script(cmd: list, timeout: int = 3600):
    """
    Runs a python module using the SAME interpreter
    that FastAPI is running on (virtualenv-safe).
    """
    full_cmd = [sys.executable] + cmd[1:] if cmd[0] == "python" else cmd

    try:
        proc = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=BASE_DIR,
        )
        return {
            "command": " ".join(full_cmd),
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            "command": " ".join(full_cmd),
            "returncode": -1,
            "stdout": "",
            "stderr": "Execution timed out",
        }

# ==========================================================
# ADMIN ENDPOINTS
# ==========================================================

@app.post("/api/admin/rebuild-masterlist")
async def rebuild_masterlist(admin: User = Depends(require_admin)):
    return run_script(["python", "-m", "app.masterlist"])

@app.post("/api/admin/extract-sids")
async def extract_sids(admin: User = Depends(require_admin)):
    return run_script(
        ["python", "-m", "app.doc_extractor", "--process-all"],
        timeout=7200
    )

@app.post("/api/admin/build-metrics")
async def build_metrics(admin: User = Depends(require_admin)):
    return run_script(
        ["python", "-m", "app.build_all_scheme_metrics", "--workers", "8"],
        timeout=14400
    )

@app.post("/api/admin/generate-ui")
async def generate_ui(admin: User = Depends(require_admin)):
    return run_script(["python", "-m", "app.scheme_info"])

@app.post("/api/admin/run-full-pipeline")
async def run_full_pipeline(admin: User = Depends(require_admin)):
    steps = [
        (["python", "-m", "app.masterlist"], 600),
        (["python", "-m", "app.doc_extractor", "--process-all"], 7200),
        (["python", "-m", "app.build_all_scheme_metrics", "--workers", "8"], 14400),
        (["python", "-m", "app.scheme_info"], 600),
    ]

    results = []
    for cmd, to in steps:
        res = run_script(cmd, timeout=to)
        results.append(res)
        if res["returncode"] != 0:
            break

    return {"steps": results}

# ==========================================================
# HEALTH
# ==========================================================

@app.get("/api/health")
async def health():
    return {"status": "ok"}

# ==========================================================
# sanitizer utility : Sanitize NaN / Infinity BEFORE returning JSON
# ==========================================================

def sanitize_for_json(obj):
    """
    Recursively replace NaN / Inf values with None
    so JSON serialization doesn't fail.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    else:
        return obj
    
# ==========================================================
# Expose status via FastAPI
# ==========================================================

@app.get("/api/status")
def system_status(_: User = Depends(get_current_user)):
    return load_status()

# ==========================================================
# AUTH DEBUG (TEMPORARY – SAFE)
# ==========================================================

@app.get("/api/debug/auth")
def debug_auth(user: User = Depends(get_current_user)):
    """
    Debug endpoint to verify:
    - Token is being sent
    - Token is decoded correctly
    - Role is what backend sees
    """
    return {
        "username": user.username,
        "role": user.role,
        "message": "Authentication OK"
    }
