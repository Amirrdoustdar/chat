"""
database/cache.py  –  Redis cache layer
Caches expensive PostgreSQL queries with TTL-based invalidation
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Cache TTLs (seconds)
TTL_USER        = 300    # 5 min
TTL_GROUP       = 600    # 10 min
TTL_CONV_LIST   = 60     # 1 min
TTL_MESSAGES    = 30     # 30 sec
TTL_GROUP_DISC  = 120    # 2 min (group discovery)


class CacheLayer:
    def __init__(self, redis) -> None:
        self._r = redis

    # ── Generic ──────────────────────────────────────────────
    async def get(self, key: str) -> Optional[Any]:
        val = await self._r.get(key)
        if val:
            return json.loads(val)
        return None

    async def set(self, key: str, value: Any, ttl: int) -> None:
        await self._r.setex(key, ttl, json.dumps(value, default=str))

    async def delete(self, *keys: str) -> None:
        if keys:
            await self._r.delete(*keys)

    async def delete_pattern(self, pattern: str) -> None:
        async for key in self._r.scan_iter(pattern):
            await self._r.delete(key)

    # ── User cache ───────────────────────────────────────────
    async def get_user(self, user_id: int) -> Optional[dict]:
        return await self.get(f"user:{user_id}")

    async def set_user(self, user_id: int, data: dict) -> None:
        await self.set(f"user:{user_id}", data, TTL_USER)

    async def invalidate_user(self, user_id: int) -> None:
        await self.delete(f"user:{user_id}")

    # ── Group cache ──────────────────────────────────────────
    async def get_group(self, group_id: int) -> Optional[dict]:
        return await self.get(f"group:{group_id}")

    async def set_group(self, group_id: int, data: dict) -> None:
        await self.set(f"group:{group_id}", data, TTL_GROUP)

    async def invalidate_group(self, group_id: int) -> None:
        await self.delete(f"group:{group_id}", f"group:members:{group_id}")

    # ── Conversation list cache ───────────────────────────────
    async def get_conv_list(self, user_id: int) -> Optional[list]:
        return await self.get(f"convlist:{user_id}")

    async def set_conv_list(self, user_id: int, data: list) -> None:
        await self.set(f"convlist:{user_id}", data, TTL_CONV_LIST)

    async def invalidate_conv_list(self, user_id: int) -> None:
        await self.delete(f"convlist:{user_id}")

    # ── Recent messages cache (last 50) ───────────────────────
    async def get_messages(self, key: str) -> Optional[list]:
        return await self.get(f"msgs:{key}")

    async def set_messages(self, key: str, messages: list) -> None:
        await self.set(f"msgs:{key}", messages, TTL_MESSAGES)

    async def invalidate_messages(self, key: str) -> None:
        await self.delete(f"msgs:{key}")

    # ── Group discovery cache ─────────────────────────────────
    async def get_group_search(self, query: str) -> Optional[list]:
        return await self.get(f"gsearch:{query.lower()}")

    async def set_group_search(self, query: str, results: list) -> None:
        await self.set(f"gsearch:{query.lower()}", results, TTL_GROUP_DISC)

    # ── Typing indicator ──────────────────────────────────────
    async def set_typing(self, from_id: int, to_id: int) -> None:
        """Mark user as typing. Auto-expires in 4 seconds."""
        await self._r.setex(f"typing:{from_id}:{to_id}", 4, "1")

    async def is_typing(self, from_id: int, to_id: int) -> bool:
        return bool(await self._r.exists(f"typing:{from_id}:{to_id}"))
