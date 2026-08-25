"""
database/db_async.py  –  Async MySQL connection pool using aiomysql
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import aiomysql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DB_CONFIG: dict[str, Any] = {
    "host":    os.getenv("DB_HOST",     "127.0.0.1"),
    "port":    int(os.getenv("DB_PORT", "3306")),
    "db":      os.getenv("DB_NAME",     "chat_platform"),
    "user":    os.getenv("DB_USER",     "chat_user"),
    "password":os.getenv("DB_PASSWORD", "change_me"),
    "charset": "utf8mb4",
    "autocommit": False,
}

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
_pool: Optional[aiomysql.Pool] = None


# ──────────────────────────────────────────────────────────────
# Pool lifecycle
# ──────────────────────────────────────────────────────────────
async def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await aiomysql.create_pool(
            minsize=2,
            maxsize=POOL_SIZE,
            cursorclass=aiomysql.DictCursor,
            **DB_CONFIG,
        )
        logger.info("aiomysql pool initialised (maxsize=%d)", POOL_SIZE)


async def close_pool() -> None:
    global _pool
    if _pool:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("aiomysql pool closed")


def get_pool() -> aiomysql.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialised — call await init_pool() first")
    return _pool


# ──────────────────────────────────────────────────────────────
# Context manager
# ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def get_connection() -> AsyncGenerator[aiomysql.Connection, None]:
    async with get_pool().acquire() as conn:
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


# ──────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────
async def execute_one(sql: str, params: tuple = ()) -> Optional[dict]:
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchone()


async def execute_all(sql: str, params: tuple = ()) -> list:
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


async def execute_write(sql: str, params: tuple = ()) -> int:
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.lastrowid or cur.rowcount


async def execute_many(sql: str, params_list: list[tuple]) -> int:
    async with get_connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params_list)
            return cur.rowcount


# ──────────────────────────────────────────────────────────────
# Health-check
# ──────────────────────────────────────────────────────────────
async def ping() -> bool:
    try:
        row = await execute_one("SELECT 1 AS ok")
        return bool(row and row.get("ok") == 1)
    except Exception as exc:
        logger.error("DB ping failed: %s", exc)
        return False
