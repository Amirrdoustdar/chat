"""
auth/token_store.py  –  Redis-backed refresh token store

Token pair model
────────────────
access_token   short-lived (15 min)  stored in session table
refresh_token  long-lived  (7 days)  stored in Redis only

On refresh:
  1. Validate refresh_token in Redis
  2. Issue new access_token + new refresh_token  (rotation)
  3. Invalidate old refresh_token immediately
"""
from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

ACCESS_TTL_SECONDS  = 15 * 60          # 15 minutes
REFRESH_TTL_SECONDS = 7 * 24 * 3600    # 7 days
REFRESH_KEY_PREFIX  = "refresh:"
ACCESS_KEY_PREFIX   = "access:"


def _generate_token() -> str:
    return secrets.token_hex(32)


class TokenStore:
    def __init__(self, redis) -> None:
        self._r = redis

    # ── Issue ────────────────────────────────────────────────
    async def issue_pair(self, user_id: int, username: str) -> dict:
        """
        Create a new access + refresh token pair.
        Returns dict with both tokens and their expiry times.
        """
        access_token  = _generate_token()
        refresh_token = _generate_token()

        now = datetime.now(timezone.utc)
        access_expires  = now + timedelta(seconds=ACCESS_TTL_SECONDS)
        refresh_expires = now + timedelta(seconds=REFRESH_TTL_SECONDS)

        # Store access token in Redis
        await self._r.setex(
            f"{ACCESS_KEY_PREFIX}{access_token}",
            ACCESS_TTL_SECONDS,
            f"{user_id}:{username}",
        )

        # Store refresh token in Redis
        await self._r.setex(
            f"{REFRESH_KEY_PREFIX}{refresh_token}",
            REFRESH_TTL_SECONDS,
            f"{user_id}:{username}",
        )

        logger.debug("Issued token pair for user_id=%d", user_id)
        return {
            "access_token":      access_token,
            "access_expires_at": access_expires.isoformat(),
            "refresh_token":     refresh_token,
            "refresh_expires_at": refresh_expires.isoformat(),
            "token_type":        "bearer",
        }

    # ── Validate access token ────────────────────────────────
    async def validate_access(self, token: str) -> Optional[dict]:
        """Return {user_id, username} or None if invalid/expired."""
        val = await self._r.get(f"{ACCESS_KEY_PREFIX}{token}")
        if not val:
            return None
        user_id, username = val.decode().split(":", 1)
        return {"user_id": int(user_id), "username": username}

    # ── Refresh ──────────────────────────────────────────────
    async def refresh(self, refresh_token: str) -> Optional[dict]:
        """
        Rotate tokens:
          - validate old refresh_token
          - revoke it immediately
          - issue a new pair
        Returns new token pair dict, or None if token invalid.
        """
        key = f"{REFRESH_KEY_PREFIX}{refresh_token}"
        val = await self._r.get(key)
        if not val:
            return None

        user_id, username = val.decode().split(":", 1)

        # Revoke old refresh token immediately (rotation)
        await self._r.delete(key)

        # Issue fresh pair
        pair = await self.issue_pair(int(user_id), username)
        logger.info("Rotated refresh token for user_id=%s", user_id)
        return pair

    # ── Revoke ───────────────────────────────────────────────
    async def revoke_access(self, token: str) -> None:
        await self._r.delete(f"{ACCESS_KEY_PREFIX}{token}")

    async def revoke_refresh(self, token: str) -> None:
        await self._r.delete(f"{REFRESH_KEY_PREFIX}{token}")

    async def revoke_all(self, user_id: int, username: str) -> None:
        """
        Revoke all tokens for a user.
        Uses SCAN to find matching keys — safe for large Redis keyspaces.
        """
        patterns = [
            f"{ACCESS_KEY_PREFIX}*",
            f"{REFRESH_KEY_PREFIX}*",
        ]
        target = f"{user_id}:{username}"
        for pattern in patterns:
            async for key in self._r.scan_iter(pattern):
                val = await self._r.get(key)
                if val and val.decode() == target:
                    await self._r.delete(key)
        logger.info("Revoked all tokens for user_id=%d", user_id)
