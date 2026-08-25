"""
tests/test_security.py  –  Tests for rate limiter and token store
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from auth.rate_limiter import RateLimitExceeded, RateLimiter
from auth.token_store import TokenStore


# ── Helpers ──────────────────────────────────────────────────
def make_redis_mock():
    """Minimal async Redis mock with in-memory store."""
    store = {}
    ttls  = {}

    mock = AsyncMock()

    async def incr(key):
        store[key] = store.get(key, 0) + 1
        return store[key]

    async def expire(key, ttl):
        ttls[key] = ttl

    async def exists(key):
        return key in store

    async def ttl(key):
        return ttls.get(key, -1)

    async def setex(key, ttl_, val):
        store[key] = val
        ttls[key]  = ttl_

    async def get(key):
        val = store.get(key)
        return val.encode() if isinstance(val, str) else val

    async def delete(*keys):
        for k in keys:
            store.pop(k, None)

    async def scan_iter(pattern):
        prefix = pattern.rstrip("*")
        for k in list(store.keys()):
            if k.startswith(prefix):
                yield k.encode() if isinstance(k, str) else k

    mock.incr      = incr
    mock.expire    = expire
    mock.exists    = exists
    mock.ttl       = ttl
    mock.setex     = setex
    mock.get       = get
    mock.delete    = delete
    mock.scan_iter = scan_iter

    return mock


# ── Rate Limiter Tests ────────────────────────────────────────
class TestRateLimiter:

    @pytest.mark.asyncio
    async def test_allows_within_limit(self):
        r       = make_redis_mock()
        limiter = RateLimiter(r)
        # Should not raise for first attempt
        await limiter.check("login", "192.168.1.1")

    @pytest.mark.asyncio
    async def test_blocks_after_limit(self):
        r       = make_redis_mock()
        limiter = RateLimiter(r)
        # Exhaust all 5 login attempts
        for _ in range(5):
            try:
                await limiter.check("login", "192.168.1.2")
            except RateLimitExceeded:
                pass
        # 6th attempt must raise
        with pytest.raises(RateLimitExceeded):
            await limiter.check("login", "192.168.1.2")

    @pytest.mark.asyncio
    async def test_reset_clears_counter(self):
        r       = make_redis_mock()
        limiter = RateLimiter(r)
        await limiter.check("login", "192.168.1.3")
        await limiter.reset("login",  "192.168.1.3")
        # After reset, should allow again
        await limiter.check("login", "192.168.1.3")

    @pytest.mark.asyncio
    async def test_different_ips_independent(self):
        r       = make_redis_mock()
        limiter = RateLimiter(r)
        # Exhaust IP A
        for _ in range(6):
            try:
                await limiter.check("login", "10.0.0.1")
            except RateLimitExceeded:
                pass
        # IP B should still work
        await limiter.check("login", "10.0.0.2")


# ── Token Store Tests ─────────────────────────────────────────
class TestTokenStore:

    @pytest.mark.asyncio
    async def test_issue_pair(self):
        r     = make_redis_mock()
        store = TokenStore(r)
        pair  = await store.issue_pair(1, "alice")

        assert "access_token"  in pair
        assert "refresh_token" in pair
        assert "access_expires_at"  in pair
        assert "refresh_expires_at" in pair
        assert pair["token_type"] == "bearer"

    @pytest.mark.asyncio
    async def test_validate_access_token(self):
        r     = make_redis_mock()
        store = TokenStore(r)
        pair  = await store.issue_pair(1, "alice")

        result = await store.validate_access(pair["access_token"])
        assert result is not None
        assert result["user_id"]  == 1
        assert result["username"] == "alice"

    @pytest.mark.asyncio
    async def test_invalid_token_returns_none(self):
        r      = make_redis_mock()
        store  = TokenStore(r)
        result = await store.validate_access("nonexistent_token")
        assert result is None

    @pytest.mark.asyncio
    async def test_refresh_rotates_tokens(self):
        r     = make_redis_mock()
        store = TokenStore(r)
        pair  = await store.issue_pair(2, "bob")

        new_pair = await store.refresh(pair["refresh_token"])
        assert new_pair is not None
        # New tokens must be different
        assert new_pair["access_token"]  != pair["access_token"]
        assert new_pair["refresh_token"] != pair["refresh_token"]

    @pytest.mark.asyncio
    async def test_old_refresh_token_invalid_after_rotation(self):
        r     = make_redis_mock()
        store = TokenStore(r)
        pair  = await store.issue_pair(3, "carol")

        await store.refresh(pair["refresh_token"])
        # Old token must be invalid now
        result = await store.refresh(pair["refresh_token"])
        assert result is None

    @pytest.mark.asyncio
    async def test_revoke_access_token(self):
        r     = make_redis_mock()
        store = TokenStore(r)
        pair  = await store.issue_pair(4, "dave")

        await store.revoke_access(pair["access_token"])
        result = await store.validate_access(pair["access_token"])
        assert result is None
