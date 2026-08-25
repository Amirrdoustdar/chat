"""
database/db_postgres.py  –  Async PostgreSQL pool using asyncpg
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",     "127.0.0.1"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "database": os.getenv("PG_NAME",     "chat_platform"),
    "user":     os.getenv("PG_USER",     "chat_user"),
    "password": os.getenv("PG_PASSWORD", "change_me"),
}

POOL_MIN  = int(os.getenv("PG_POOL_MIN", "2"))
POOL_MAX  = int(os.getenv("PG_POOL_MAX", "10"))

_pool: Optional[asyncpg.Pool] = None


# ── Pool lifecycle ────────────────────────────────────────────
async def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            min_size=POOL_MIN,
            max_size=POOL_MAX,
            **PG_CONFIG,
        )
        logger.info("asyncpg pool ready (min=%d max=%d)", POOL_MIN, POOL_MAX)


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("asyncpg pool closed")


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised")
    return _pool


# ── Context manager ───────────────────────────────────────────
@asynccontextmanager
async def get_connection() -> AsyncGenerator[asyncpg.Connection, None]:
    async with get_pool().acquire() as conn:
        async with conn.transaction():
            yield conn


# ── Helpers ───────────────────────────────────────────────────
async def fetchone(sql: str, *args) -> Optional[asyncpg.Record]:
    async with get_pool().acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def fetchall(sql: str, *args) -> list:
    async with get_pool().acquire() as conn:
        return await conn.fetch(sql, *args)


async def execute(sql: str, *args) -> str:
    async with get_pool().acquire() as conn:
        return await conn.execute(sql, *args)


async def fetchval(sql: str, *args) -> Any:
    async with get_pool().acquire() as conn:
        return await conn.fetchval(sql, *args)


async def ping() -> bool:
    try:
        val = await fetchval("SELECT 1")
        return val == 1
    except Exception as exc:
        logger.error("DB ping failed: %s", exc)
        return False
