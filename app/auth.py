# app/auth.py
import time
import hashlib
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel

# ==========================================================
# CONFIG
# ==========================================================

SECRET_KEY = "dev_secret_change_me"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_SECONDS = 3600

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/token")

# ==========================================================
# MODELS
# ==========================================================

class User(BaseModel):
    username: str
    full_name: Optional[str]
    role: str

class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[str] = None

# ==========================================================
# USERS (simple in-memory)
# ==========================================================

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

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
# AUTH HELPERS
# ==========================================================

def authenticate_user(username: str, password: str) -> Optional[User]:
    u = USERS.get(username)
    if not u:
        return None
    if hash_password(password) != u["hashed_password"]:
        return None
    return User(
        username=u["username"],
        full_name=u.get("full_name"),
        role=u["role"],
    )

def create_access_token(data: dict, expires_seconds: int):
    payload = data.copy()
    payload["exp"] = int(time.time()) + expires_seconds
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    auth_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        role = payload.get("role")
        if not username:
            raise auth_error
    except JWTError:
        raise auth_error

    u = USERS.get(username)
    if not u:
        raise auth_error

    return User(
        username=u["username"],
        full_name=u.get("full_name"),
        role=u["role"],
    )

async def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
