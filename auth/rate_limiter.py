"""
auth/rate_limiter.py  –  Redis-backed rate limiter
Sliding window counter per (action, key) pair.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Limits ───────────────────────────────────────────────────
LIMITS: dict[str, tuple[int, int]] = {
    # action          max_attempts  window_seconds
    "login":         (5,  60),   # 5 attempts per minute
    "register":      (3,  60),   # 3 registrations per minute
    "token_refresh": (10, 60),   # 10 refreshes per minute
}

LOCKOUT_SECONDS = 300   # 5 minute lockout after limit exceeded


class RateLimitExceeded(Exception):
    def __init__(self, action: str, retry_after: int) -> None:
        self.action      = action
        self.retry_after = retry_after
        super().__init__(
            f"Too many {action} attempts. Try again in {retry_after} seconds."
        )


class RateLimiter:
    def __init__(self, redis) -> None:
        self._r = redis

    # ── Public API ───────────────────────────────────────────
    async def check(self, action: str, key: str) -> None:
        """
        Raise :class:`RateLimitExceeded` if *key* has exceeded
        the limit for *action*.
        """
        # Check if key is locked out
        lockout_key = f"lockout:{action}:{key}"
        if await self._r.exists(lockout_key):
            ttl = await self._r.ttl(lockout_key)
            raise RateLimitExceeded(action, ttl)

        max_attempts, window = LIMITS.get(action, (20, 60))
        counter_key = f"ratelimit:{action}:{key}"

        # Increment counter
        count = await self._r.incr(counter_key)

        # Set expiry on first increment
        if count == 1:
            await self._r.expire(counter_key, window)

        if count > max_attempts:
            # Lock out the key
            await self._r.setex(lockout_key, LOCKOUT_SECONDS, 1)
            await self._r.delete(counter_key)
            logger.warning("Rate limit exceeded: action=%s key=%s", action, key)
            raise RateLimitExceeded(action, LOCKOUT_SECONDS)

    async def reset(self, action: str, key: str) -> None:
        """Clear rate limit counter for *key* (call on successful login)."""
        await self._r.delete(f"ratelimit:{action}:{key}")
        await self._r.delete(f"lockout:{action}:{key}")

    async def remaining(self, action: str, key: str) -> int:
        """Return how many attempts are left for *key*."""
        max_attempts, _ = LIMITS.get(action, (20, 60))
        counter_key     = f"ratelimit:{action}:{key}"
        count           = int(await self._r.get(counter_key) or 0)
        return max(0, max_attempts - count)
