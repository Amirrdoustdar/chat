"""
database/db.py  –  Thread-safe MySQL connection pool
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from typing import Any, Generator, Optional

import mysql.connector
from mysql.connector import pooling, Error as MySQLError

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Configuration  (override via environment variables)
# ──────────────────────────────────────────────────────────────
DB_CONFIG: dict[str, Any] = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME",     "chat_platform"),
    "user":     os.getenv("DB_USER",     "chat_user"),
    "password": os.getenv("DB_PASSWORD", "change_me"),
    "charset":  "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
    "autocommit": False,
    "time_zone": "+00:00",
}

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
_pool: Optional[pooling.MySQLConnectionPool] = None
_pool_lock = threading.Lock()


# ──────────────────────────────────────────────────────────────
# Pool initialisation
# ──────────────────────────────────────────────────────────────
def init_pool() -> None:
    """Create the global connection pool.  Call once at startup."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = pooling.MySQLConnectionPool(
                pool_name="chat_pool",
                pool_size=POOL_SIZE,
                pool_reset_session=True,
                **DB_CONFIG,
            )
            logger.info("MySQL pool initialised (size=%d)", POOL_SIZE)


def get_pool() -> pooling.MySQLConnectionPool:
    if _pool is None:
        init_pool()
    return _pool  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────
# Context manager – auto commit/rollback, auto return to pool
# ──────────────────────────────────────────────────────────────
@contextmanager
def get_connection() -> Generator[mysql.connector.MySQLConnection, None, None]:
    """
    Usage::

        with get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT …")
    """
    conn = get_pool().get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()   # returns to pool


# ──────────────────────────────────────────────────────────────
# Convenience helpers
# ──────────────────────────────────────────────────────────────
def execute_one(
    sql: str,
    params: tuple = (),
    *,
    dictionary: bool = True,
) -> Optional[dict | tuple]:
    """Execute a SELECT and return the first row (or None)."""
    with get_connection() as conn:
        cur = conn.cursor(dictionary=dictionary)
        cur.execute(sql, params)
        return cur.fetchone()


def execute_all(
    sql: str,
    params: tuple = (),
    *,
    dictionary: bool = True,
) -> list:
    """Execute a SELECT and return all rows."""
    with get_connection() as conn:
        cur = conn.cursor(dictionary=dictionary)
        cur.execute(sql, params)
        return cur.fetchall()


def execute_write(sql: str, params: tuple = ()) -> int:
    """Execute an INSERT / UPDATE / DELETE and return lastrowid or rowcount."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.lastrowid or cur.rowcount


def execute_many(sql: str, params_list: list[tuple]) -> int:
    """Bulk INSERT / UPDATE using executemany."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, params_list)
        return cur.rowcount


# ──────────────────────────────────────────────────────────────
# Health-check
# ──────────────────────────────────────────────────────────────
def ping() -> bool:
    try:
        result = execute_one("SELECT 1 AS ok")
        return bool(result and result.get("ok") == 1)
    except MySQLError as exc:
        logger.error("DB ping failed: %s", exc)
        return False
