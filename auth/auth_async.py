"""
auth/auth_async.py  –  Async auth with rate limiting + refresh tokens
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import bcrypt

from auth.rate_limiter import RateLimitExceeded, RateLimiter
from auth.token_store import TokenStore
from database.repository_async import SessionRepo, UserRepo

logger = logging.getLogger(__name__)


class AuthError(Exception):
    pass


# ── Password ─────────────────────────────────────────────────
def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ── Public API ───────────────────────────────────────────────
async def register(username: str, email: str, password: str,
                   ip: str = None, redis=None) -> dict:
    username = username.strip().lower()
    email    = email.strip().lower()

    # Rate limit by IP
    if redis and ip:
        limiter = RateLimiter(redis)
        try:
            await limiter.check("register", ip)
        except RateLimitExceeded as exc:
            raise AuthError(str(exc))

    if len(password) < 8:
        raise AuthError("Password must be at least 8 characters.")
    if await UserRepo.get_by_username(username):
        raise AuthError("Username already taken.")
    if await UserRepo.get_by_email(email):
        raise AuthError("Email already registered.")

    pw_hash = hash_password(password)
    user_id = await UserRepo.create(username, email, pw_hash)

    # Issue token pair
    tokens = {}
    if redis:
        store  = TokenStore(redis)
        tokens = await store.issue_pair(user_id, username)

    logger.info("Registered user_id=%d username=%s", user_id, username)
    return {"user_id": user_id, "username": username, **tokens}


async def login(username: str, password: str,
                ip: str = None, redis=None) -> dict:
    username = username.strip().lower()

    # Rate limit by IP
    if redis and ip:
        limiter = RateLimiter(redis)
        try:
            await limiter.check("login", ip)
        except RateLimitExceeded as exc:
            raise AuthError(str(exc))

    user = await UserRepo.get_by_username(username)

    if not user or not verify_password(password, user["password_hash"]):
        raise AuthError("Invalid username or password.")

    await UserRepo.set_status(user["id"], "online")

    # Reset rate limit counter on success
    if redis and ip:
        await RateLimiter(redis).reset("login", ip)

    # Issue token pair
    tokens = {}
    if redis:
        store  = TokenStore(redis)
        tokens = await store.issue_pair(user["id"], user["username"])

    logger.info("Login user_id=%d ip=%s", user["id"], ip)
    return {"user_id": user["id"], "username": user["username"], **tokens}


async def refresh_tokens(refresh_token: str, redis=None) -> dict:
    """Exchange a refresh token for a new token pair."""
    if not redis:
        raise AuthError("Token refresh not available.")

    store = TokenStore(redis)
    pair  = await store.refresh(refresh_token)
    if not pair:
        raise AuthError("Invalid or expired refresh token.")
    return pair


async def logout(access_token: str, refresh_token: str,
                 user_id: int, username: str, redis=None) -> None:
    if redis:
        store = TokenStore(redis)
        await store.revoke_access(access_token)
        await store.revoke_refresh(refresh_token)
    await UserRepo.set_status(user_id, "offline")
    logger.info("Logout user_id=%d", user_id)


async def validate_token(token: str, redis=None) -> Optional[dict]:
    """Validate access token. Returns {user_id, username} or None."""
    if not redis:
        return None
    store = TokenStore(redis)
    return await store.validate_access(token)


async def require_auth(token: str, redis=None) -> dict:
    payload = await validate_token(token, redis)
    if not payload:
        raise AuthError("Invalid or expired access token.")
    return payload
