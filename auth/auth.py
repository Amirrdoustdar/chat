"""
auth/auth.py  –  Authentication & authorisation helpers
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt

from database.repository import SessionRepo, UserRepo

logger = logging.getLogger(__name__)

TOKEN_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", "24"))


# ──────────────────────────────────────────────────────────────
# Password helpers
# ──────────────────────────────────────────────────────────────
def hash_password(plain: str) -> str:
    """Return a bcrypt hash string for *plain*."""
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_password(plain: str, hashed: str) -> bool:
    """Constant-time comparison against stored bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Token helpers
# ──────────────────────────────────────────────────────────────
def _generate_token() -> str:
    """32-byte cryptographically random hex token."""
    return secrets.token_hex(32)


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────
class AuthError(Exception):
    """Raised on authentication / authorisation failure."""


def register(username: str, email: str, password: str) -> dict:
    """
    Create a new account.

    Returns ``{"user_id": int, "token": str}`` on success.
    Raises :class:`AuthError` if the username or email is taken.
    """
    username = username.strip().lower()
    email    = email.strip().lower()

    if len(password) < 8:
        raise AuthError("Password must be at least 8 characters.")

    if UserRepo.get_by_username(username):
        raise AuthError("Username already taken.")
    if UserRepo.get_by_email(email):
        raise AuthError("Email already registered.")

    pw_hash = hash_password(password)
    user_id = UserRepo.create(username, email, pw_hash)

    token, expires = _new_session(user_id)
    logger.info("Registered user_id=%d username=%s", user_id, username)
    return {"user_id": user_id, "username": username, "token": token, "expires_at": expires}


def login(username: str, password: str,
          ip: str = None, ua: str = None) -> dict:
    """
    Validate credentials and create a session.

    Returns ``{"user_id": int, "username": str, "token": str}`` on success.
    Raises :class:`AuthError` on bad credentials.
    """
    username = username.strip().lower()
    user = UserRepo.get_by_username(username)

    if not user or not verify_password(password, user["password_hash"]):
        raise AuthError("Invalid username or password.")

    UserRepo.set_status(user["id"], "online")
    token, expires = _new_session(user["id"], ip=ip, ua=ua)

    logger.info("Login user_id=%d ip=%s", user["id"], ip)
    return {
        "user_id":    user["id"],
        "username":   user["username"],
        "token":      token,
        "expires_at": expires,
    }


def logout(token: str, user_id: int) -> None:
    SessionRepo.revoke(token)
    UserRepo.set_status(user_id, "offline")
    logger.info("Logout user_id=%d", user_id)


def validate_token(token: str) -> Optional[dict]:
    """
    Verify *token* is valid and not expired.

    Returns ``{"user_id": int, "username": str}`` or ``None``.
    """
    return SessionRepo.get_valid(token)


def require_auth(token: str) -> dict:
    """
    Like :func:`validate_token` but raises :class:`AuthError` on failure.
    """
    payload = validate_token(token)
    if not payload:
        raise AuthError("Invalid or expired session token.")
    return payload


# ──────────────────────────────────────────────────────────────
# Private helpers
# ──────────────────────────────────────────────────────────────
def _new_session(user_id: int, ip: str = None, ua: str = None) -> tuple[str, datetime]:
    token = _generate_token()
    expires = datetime.now(timezone.utc) + timedelta(hours=TOKEN_TTL_HOURS)
    SessionRepo.create(user_id, token, expires, ip=ip, ua=ua)
    return token, expires
