"""
database/repository.py  –  Data-access layer (repository pattern)
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Optional

from .db import execute_all, execute_one, execute_write, execute_many, get_connection


# ══════════════════════════════════════════════════════════════
#  Users
# ══════════════════════════════════════════════════════════════
class UserRepo:

    @staticmethod
    def create(username: str, email: str, password_hash: str) -> int:
        return execute_write(
            "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
            (username, email, password_hash),
        )

    @staticmethod
    def get_by_id(user_id: int) -> Optional[dict]:
        return execute_one(
            "SELECT id, username, email, status, created_at, last_seen "
            "FROM users WHERE id = %s AND is_active = 1",
            (user_id,),
        )

    @staticmethod
    def get_by_username(username: str) -> Optional[dict]:
        return execute_one(
            "SELECT id, username, email, password_hash, status "
            "FROM users WHERE username = %s AND is_active = 1",
            (username,),
        )

    @staticmethod
    def get_by_email(email: str) -> Optional[dict]:
        return execute_one(
            "SELECT id, username, email, password_hash "
            "FROM users WHERE email = %s AND is_active = 1",
            (email,),
        )

    @staticmethod
    def set_status(user_id: int, status: str) -> None:
        execute_write(
            "UPDATE users SET status = %s WHERE id = %s",
            (status, user_id),
        )

    @staticmethod
    def search(query: str, limit: int = 20) -> list:
        like = f"%{query}%"
        return execute_all(
            "SELECT id, username, status FROM users "
            "WHERE username LIKE %s AND is_active = 1 LIMIT %s",
            (like, limit),
        )


# ══════════════════════════════════════════════════════════════
#  Sessions
# ══════════════════════════════════════════════════════════════
class SessionRepo:

    @staticmethod
    def create(user_id: int, token: str, expires_at: datetime,
               ip: str = None, ua: str = None) -> int:
        return execute_write(
            "INSERT INTO sessions (user_id, token, ip_address, user_agent, expires_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (user_id, token, ip, ua, expires_at),
        )

    @staticmethod
    def get_valid(token: str) -> Optional[dict]:
        return execute_one(
            "SELECT s.user_id, s.expires_at, u.username "
            "FROM sessions s JOIN users u ON u.id = s.user_id "
            "WHERE s.token = %s AND s.expires_at > NOW() AND u.is_active = 1",
            (token,),
        )

    @staticmethod
    def revoke(token: str) -> None:
        execute_write("DELETE FROM sessions WHERE token = %s", (token,))

    @staticmethod
    def revoke_all_for_user(user_id: int) -> None:
        execute_write("DELETE FROM sessions WHERE user_id = %s", (user_id,))


# ══════════════════════════════════════════════════════════════
#  Groups
# ══════════════════════════════════════════════════════════════
class GroupRepo:

    @staticmethod
    def create(name: str, owner_id: int, description: str = "") -> int:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO groups_ (name, owner_id, description) VALUES (%s, %s, %s)",
                (name, owner_id, description),
            )
            group_id = cur.lastrowid
            # Owner is automatically a member
            cur.execute(
                "INSERT INTO group_members (group_id, user_id, role) VALUES (%s, %s, 'owner')",
                (group_id, owner_id),
            )
        return group_id

    @staticmethod
    def get_by_id(group_id: int) -> Optional[dict]:
        return execute_one(
            "SELECT id, name, description, owner_id, created_at "
            "FROM groups_ WHERE id = %s AND is_active = 1",
            (group_id,),
        )

    @staticmethod
    def add_member(group_id: int, user_id: int, role: str = "member") -> None:
        execute_write(
            "INSERT IGNORE INTO group_members (group_id, user_id, role) VALUES (%s, %s, %s)",
            (group_id, user_id, role),
        )

    @staticmethod
    def remove_member(group_id: int, user_id: int) -> None:
        execute_write(
            "DELETE FROM group_members WHERE group_id = %s AND user_id = %s",
            (group_id, user_id),
        )

    @staticmethod
    def get_members(group_id: int) -> list:
        return execute_all(
            "SELECT u.id, u.username, u.status, gm.role "
            "FROM group_members gm JOIN users u ON u.id = gm.user_id "
            "WHERE gm.group_id = %s",
            (group_id,),
        )

    @staticmethod
    def is_member(group_id: int, user_id: int) -> bool:
        row = execute_one(
            "SELECT 1 FROM group_members WHERE group_id = %s AND user_id = %s",
            (group_id, user_id),
        )
        return row is not None

    @staticmethod
    def get_user_groups(user_id: int) -> list:
        return execute_all(
            "SELECT g.id, g.name, gm.role FROM groups_ g "
            "JOIN group_members gm ON gm.group_id = g.id "
            "WHERE gm.user_id = %s AND g.is_active = 1",
            (user_id,),
        )


# ══════════════════════════════════════════════════════════════
#  Messages
# ══════════════════════════════════════════════════════════════
class MessageRepo:

    @staticmethod
    def create_dm(sender_id: int, recipient_id: int, body: str,
                  msg_type: str = "text", media_id: int = None,
                  ttl_seconds: int = None) -> int:
        expires_at = None
        if ttl_seconds:
            from datetime import timedelta
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        return execute_write(
            "INSERT INTO messages "
            "(sender_id, recipient_id, msg_type, body, media_id, ttl_seconds, expires_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (sender_id, recipient_id, msg_type, body, media_id, ttl_seconds, expires_at),
        )

    @staticmethod
    def create_group_msg(sender_id: int, group_id: int, body: str,
                         msg_type: str = "text", media_id: int = None,
                         ttl_seconds: int = None) -> int:
        expires_at = None
        if ttl_seconds:
            from datetime import timedelta
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        return execute_write(
            "INSERT INTO messages "
            "(sender_id, group_id, msg_type, body, media_id, ttl_seconds, expires_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (sender_id, group_id, msg_type, body, media_id, ttl_seconds, expires_at),
        )

    @staticmethod
    def get_dm_history(user_a: int, user_b: int, limit: int = 50, offset: int = 0) -> list:
        return execute_all(
            "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_id, "
            "       m.ttl_seconds, m.expires_at, m.sent_at, u.username AS sender_name "
            "FROM messages m JOIN users u ON u.id = m.sender_id "
            "WHERE ((m.sender_id=%s AND m.recipient_id=%s) "
            "    OR (m.sender_id=%s AND m.recipient_id=%s)) "
            "  AND m.is_deleted = 0 "
            "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
            "ORDER BY m.sent_at DESC LIMIT %s OFFSET %s",
            (user_a, user_b, user_b, user_a, limit, offset),
        )

    @staticmethod
    def get_group_history(group_id: int, limit: int = 50, offset: int = 0) -> list:
        return execute_all(
            "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_id, "
            "       m.ttl_seconds, m.expires_at, m.sent_at, u.username AS sender_name "
            "FROM messages m JOIN users u ON u.id = m.sender_id "
            "WHERE m.group_id = %s "
            "  AND m.is_deleted = 0 "
            "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
            "ORDER BY m.sent_at DESC LIMIT %s OFFSET %s",
            (group_id, limit, offset),
        )

    @staticmethod
    def soft_delete(message_id: int, requesting_user_id: int) -> bool:
        rows = execute_write(
            "UPDATE messages SET is_deleted = 1 "
            "WHERE id = %s AND sender_id = %s",
            (message_id, requesting_user_id),
        )
        return rows > 0

    @staticmethod
    def mark_read(message_id: int, user_id: int) -> None:
        execute_write(
            "INSERT IGNORE INTO read_receipts (message_id, user_id) VALUES (%s, %s)",
            (message_id, user_id),
        )


# ══════════════════════════════════════════════════════════════
#  Media Files
# ══════════════════════════════════════════════════════════════
class MediaRepo:

    @staticmethod
    def save(uploader_id: int, filename: str, mime_type: str,
             file_size: int, storage_path: str, data: bytes) -> int:
        checksum = hashlib.sha256(data).hexdigest()
        return execute_write(
            "INSERT INTO media_files "
            "(uploader_id, filename, mime_type, file_size, storage_path, checksum) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (uploader_id, filename, mime_type, file_size, storage_path, checksum),
        )

    @staticmethod
    def get_by_id(media_id: int) -> Optional[dict]:
        return execute_one(
            "SELECT id, uploader_id, filename, mime_type, file_size, storage_path, checksum "
            "FROM media_files WHERE id = %s",
            (media_id,),
        )
