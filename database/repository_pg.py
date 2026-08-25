"""
database/repository_pg.py  –  PostgreSQL repository layer
Includes: chat_key, cache layer, Yahoo groups, invite codes, MinIO media
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

from .db_postgres import execute, fetchall, fetchone, fetchval, get_connection

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
#  Invite Codes
# ══════════════════════════════════════════════════════════════
class InviteRepo:

    @staticmethod
    def _generate_code(length: int = 12) -> str:
        chars = string.ascii_uppercase + string.digits
        return "".join(secrets.choice(chars) for _ in range(length))

    @staticmethod
    async def create(created_by: int, expires_hours: int = None) -> str:
        code = InviteRepo._generate_code()
        expires_at = None
        if expires_hours:
            expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_hours)
        await execute(
            "INSERT INTO invite_codes (code, created_by, expires_at) VALUES ($1, $2, $3)",
            code, created_by, expires_at,
        )
        return code

    @staticmethod
    async def validate(code: str) -> Optional[dict]:
        """Return invite record if valid and unused, else None."""
        row = await fetchone(
            "SELECT id, code, used_by, expires_at FROM invite_codes "
            "WHERE code = $1 AND is_active = TRUE AND used_by IS NULL "
            "AND (expires_at IS NULL OR expires_at > NOW())",
            code,
        )
        return dict(row) if row else None

    @staticmethod
    async def mark_used(code: str, user_id: int) -> None:
        await execute(
            "UPDATE invite_codes SET used_by=$1, used_at=NOW(), is_active=FALSE "
            "WHERE code=$2",
            user_id, code,
        )

    @staticmethod
    async def list_all(limit: int = 50) -> list:
        rows = await fetchall(
            "SELECT ic.code, ic.created_at, ic.expires_at, ic.is_active, "
            "       u.username AS used_by_username "
            "FROM invite_codes ic "
            "LEFT JOIN users u ON u.id = ic.used_by "
            "ORDER BY ic.created_at DESC LIMIT $1",
            limit,
        )
        return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════
#  Users
# ══════════════════════════════════════════════════════════════
class UserRepo:

    @staticmethod
    async def create(username: str, email: str, password_hash: str,
                     invite_code: str) -> int:
        async with get_connection() as conn:
            user_id = await conn.fetchval(
                "INSERT INTO users (username, email, password_hash, system_id, is_verified) "
                "VALUES ($1, $2, $3, $4, TRUE) RETURNING id",
                username, email, password_hash, invite_code,
            )
            # Mark invite code as used
            await conn.execute(
                "UPDATE invite_codes SET used_by=$1, used_at=NOW(), is_active=FALSE "
                "WHERE code=$2",
                user_id, invite_code,
            )
        return user_id

    @staticmethod
    async def get_by_id(user_id: int) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, username, email, status, avatar_key, is_verified, "
            "       created_at, last_seen "
            "FROM users WHERE id=$1 AND is_active=TRUE",
            user_id,
        )
        return dict(row) if row else None

    @staticmethod
    async def get_by_username(username: str) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, username, email, password_hash, status, is_verified "
            "FROM users WHERE username=$1 AND is_active=TRUE",
            username,
        )
        return dict(row) if row else None

    @staticmethod
    async def get_by_email(email: str) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, username, email FROM users WHERE email=$1 AND is_active=TRUE",
            email,
        )
        return dict(row) if row else None

    @staticmethod
    async def set_status(user_id: int, status: str) -> None:
        await execute(
            "UPDATE users SET status=$1, last_seen=NOW() WHERE id=$2",
            status, user_id,
        )

    @staticmethod
    async def set_avatar(user_id: int, object_key: str) -> None:
        await execute(
            "UPDATE users SET avatar_key=$1 WHERE id=$2",
            object_key, user_id,
        )

    @staticmethod
    async def search(query: str, limit: int = 20) -> list:
        rows = await fetchall(
            "SELECT id, username, status, avatar_key "
            "FROM users WHERE username ILIKE $1 AND is_active=TRUE LIMIT $2",
            f"%{query}%", limit,
        )
        return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════
#  Conversations (Chat Key)
# ══════════════════════════════════════════════════════════════
class ConversationRepo:

    @staticmethod
    async def get_or_create(user_a: int, user_b: int) -> str:
        """Get or create a conversation. Returns UUID string (chat key)."""
        chat_key = await fetchval(
            "SELECT get_or_create_conversation($1, $2)",
            user_a, user_b,
        )
        return str(chat_key)

    @staticmethod
    async def get_by_key(chat_key: str) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, user_a, user_b, created_at, last_msg_at "
            "FROM conversations WHERE id=$1",
            chat_key,
        )
        return dict(row) if row else None

    @staticmethod
    async def get_user_conversations(user_id: int) -> list:
        """Get all conversations for a user with last message."""
        rows = await fetchall(
            "SELECT c.id AS chat_key, "
            "       CASE WHEN c.user_a=$1 THEN c.user_b ELSE c.user_a END AS other_user_id, "
            "       u.username AS other_username, u.status AS other_status, "
            "       c.last_msg_at "
            "FROM conversations c "
            "JOIN users u ON u.id = CASE WHEN c.user_a=$1 THEN c.user_b ELSE c.user_a END "
            "WHERE (c.user_a=$1 OR c.user_b=$1) "
            "ORDER BY c.last_msg_at DESC NULLS LAST",
            user_id,
        )
        return [dict(r) for r in rows]

    @staticmethod
    async def update_last_msg(chat_key: str) -> None:
        await execute(
            "UPDATE conversations SET last_msg_at=NOW() WHERE id=$1",
            chat_key,
        )


# ══════════════════════════════════════════════════════════════
#  Groups (Yahoo Messenger style)
# ══════════════════════════════════════════════════════════════
class GroupRepo:

    @staticmethod
    def _generate_invite_code() -> str:
        chars = string.ascii_uppercase + string.digits
        return "".join(secrets.choice(chars) for _ in range(8))

    @staticmethod
    async def create(name: str, owner_id: int, description: str = "",
                     topic: str = "", is_public: bool = True,
                     max_members: int = 500) -> int:
        invite_code = GroupRepo._generate_invite_code()
        async with get_connection() as conn:
            group_id = await conn.fetchval(
                "INSERT INTO groups_ (name, owner_id, description, topic, "
                "                    is_public, invite_code, max_members) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
                name, owner_id, description, topic,
                is_public, invite_code, max_members,
            )
            await conn.execute(
                "INSERT INTO group_members (group_id, user_id, role) "
                "VALUES ($1,$2,'owner')",
                group_id, owner_id,
            )
        return group_id

    @staticmethod
    async def get_by_id(group_id: int) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, name, description, topic, owner_id, is_public, "
            "       invite_code, max_members, member_count, created_at "
            "FROM groups_ WHERE id=$1 AND is_active=TRUE",
            group_id,
        )
        return dict(row) if row else None

    @staticmethod
    async def get_by_invite_code(code: str) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, name, description, topic, member_count, max_members "
            "FROM groups_ WHERE invite_code=$1 AND is_active=TRUE",
            code,
        )
        return dict(row) if row else None

    @staticmethod
    async def search_public(query: str, limit: int = 20) -> list:
        """Search public groups — Yahoo Messenger style discovery."""
        rows = await fetchall(
            "SELECT id, name, description, topic, member_count, invite_code "
            "FROM groups_ "
            "WHERE is_public=TRUE AND is_active=TRUE "
            "  AND (name ILIKE $1 OR topic ILIKE $1 OR description ILIKE $1) "
            "ORDER BY member_count DESC LIMIT $2",
            f"%{query}%", limit,
        )
        return [dict(r) for r in rows]

    @staticmethod
    async def join_by_invite(code: str, user_id: int) -> Optional[int]:
        """Join a group by invite code. Returns group_id or None."""
        group = await GroupRepo.get_by_invite_code(code)
        if not group:
            return None
        if group["member_count"] >= group["max_members"]:
            return None
        async with get_connection() as conn:
            await conn.execute(
                "INSERT INTO group_members (group_id, user_id) VALUES ($1,$2) "
                "ON CONFLICT DO NOTHING",
                group["id"], user_id,
            )
            await conn.execute(
                "UPDATE groups_ SET member_count=member_count+1 WHERE id=$1",
                group["id"],
            )
        return group["id"]

    @staticmethod
    async def add_member(group_id: int, user_id: int,
                         role: str = "member") -> None:
        async with get_connection() as conn:
            await conn.execute(
                "INSERT INTO group_members (group_id, user_id, role) "
                "VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                group_id, user_id, role,
            )
            await conn.execute(
                "UPDATE groups_ SET member_count=member_count+1 WHERE id=$1",
                group_id,
            )

    @staticmethod
    async def remove_member(group_id: int, user_id: int) -> None:
        async with get_connection() as conn:
            await conn.execute(
                "DELETE FROM group_members WHERE group_id=$1 AND user_id=$2",
                group_id, user_id,
            )
            await conn.execute(
                "UPDATE groups_ SET member_count=GREATEST(member_count-1,0) WHERE id=$1",
                group_id,
            )

    @staticmethod
    async def ban_member(group_id: int, user_id: int) -> None:
        await execute(
            "UPDATE group_members SET is_banned=TRUE WHERE group_id=$1 AND user_id=$2",
            group_id, user_id,
        )

    @staticmethod
    async def is_member(group_id: int, user_id: int) -> bool:
        row = await fetchone(
            "SELECT 1 FROM group_members "
            "WHERE group_id=$1 AND user_id=$2 AND is_banned=FALSE",
            group_id, user_id,
        )
        return row is not None

    @staticmethod
    async def get_members(group_id: int) -> list:
        rows = await fetchall(
            "SELECT u.id, u.username, u.status, u.avatar_key, gm.role "
            "FROM group_members gm JOIN users u ON u.id=gm.user_id "
            "WHERE gm.group_id=$1 AND gm.is_banned=FALSE",
            group_id,
        )
        return [dict(r) for r in rows]

    @staticmethod
    async def get_user_groups(user_id: int) -> list:
        rows = await fetchall(
            "SELECT g.id, g.name, g.topic, g.member_count, g.invite_code, gm.role "
            "FROM groups_ g JOIN group_members gm ON gm.group_id=g.id "
            "WHERE gm.user_id=$1 AND g.is_active=TRUE AND gm.is_banned=FALSE",
            user_id,
        )
        return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════
#  Messages
# ══════════════════════════════════════════════════════════════
class MessageRepo:

    @staticmethod
    async def create_dm(chat_key: str, sender_id: int, body: str,
                        msg_type: str = "text", media_key: str = None,
                        ttl_seconds: int = None,
                        reply_to_id: int = None) -> int:
        expires_at = None
        if ttl_seconds:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        async with get_connection() as conn:
            msg_id = await conn.fetchval(
                "INSERT INTO messages "
                "(conversation_id, sender_id, msg_type, body, media_key, "
                " ttl_seconds, expires_at, reply_to_id) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                chat_key, sender_id, msg_type, body,
                media_key, ttl_seconds, expires_at, reply_to_id,
            )
            await conn.execute(
                "UPDATE conversations SET last_msg_at=NOW() WHERE id=$1",
                chat_key,
            )
        return msg_id

    @staticmethod
    async def create_group_msg(group_id: int, sender_id: int, body: str,
                               msg_type: str = "text", media_key: str = None,
                               ttl_seconds: int = None,
                               reply_to_id: int = None) -> int:
        expires_at = None
        if ttl_seconds:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        return await fetchval(
            "INSERT INTO messages "
            "(group_id, sender_id, msg_type, body, media_key, "
            " ttl_seconds, expires_at, reply_to_id) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
            group_id, sender_id, msg_type, body,
            media_key, ttl_seconds, expires_at, reply_to_id,
        )

    @staticmethod
    async def get_dm_history(chat_key: str,
                             limit: int = 50, before_id: int = None) -> list:
        if before_id:
            rows = await fetchall(
                "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_key, "
                "       m.ttl_seconds, m.expires_at, m.sent_at, m.reactions, "
                "       m.reply_to_id, m.is_edited, u.username AS sender_name "
                "FROM messages m JOIN users u ON u.id=m.sender_id "
                "WHERE m.conversation_id=$1 AND m.is_deleted=FALSE "
                "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
                "  AND m.id < $2 "
                "ORDER BY m.sent_at DESC LIMIT $3",
                chat_key, before_id, limit,
            )
        else:
            rows = await fetchall(
                "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_key, "
                "       m.ttl_seconds, m.expires_at, m.sent_at, m.reactions, "
                "       m.reply_to_id, m.is_edited, u.username AS sender_name "
                "FROM messages m JOIN users u ON u.id=m.sender_id "
                "WHERE m.conversation_id=$1 AND m.is_deleted=FALSE "
                "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
                "ORDER BY m.sent_at DESC LIMIT $2",
                chat_key, limit,
            )
        return [dict(r) for r in rows]

    @staticmethod
    async def get_group_history(group_id: int,
                                limit: int = 50, before_id: int = None) -> list:
        if before_id:
            rows = await fetchall(
                "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_key, "
                "       m.ttl_seconds, m.expires_at, m.sent_at, m.reactions, "
                "       m.reply_to_id, m.is_edited, u.username AS sender_name "
                "FROM messages m JOIN users u ON u.id=m.sender_id "
                "WHERE m.group_id=$1 AND m.is_deleted=FALSE "
                "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
                "  AND m.id < $2 "
                "ORDER BY m.sent_at DESC LIMIT $3",
                group_id, before_id, limit,
            )
        else:
            rows = await fetchall(
                "SELECT m.id, m.sender_id, m.body, m.msg_type, m.media_key, "
                "       m.ttl_seconds, m.expires_at, m.sent_at, m.reactions, "
                "       m.reply_to_id, m.is_edited, u.username AS sender_name "
                "FROM messages m JOIN users u ON u.id=m.sender_id "
                "WHERE m.group_id=$1 AND m.is_deleted=FALSE "
                "  AND (m.expires_at IS NULL OR m.expires_at > NOW()) "
                "ORDER BY m.sent_at DESC LIMIT $2",
                group_id, limit,
            )
        return [dict(r) for r in rows]

    @staticmethod
    async def edit_message(message_id: int, user_id: int,
                           new_body: str) -> bool:
        async with get_connection() as conn:
            old = await conn.fetchrow(
                "SELECT body FROM messages WHERE id=$1 AND sender_id=$2 AND is_deleted=FALSE",
                message_id, user_id,
            )
            if not old:
                return False
            await conn.execute(
                "INSERT INTO message_edits (message_id, old_body) VALUES ($1,$2)",
                message_id, old["body"],
            )
            await conn.execute(
                "UPDATE messages SET body=$1, is_edited=TRUE WHERE id=$2",
                new_body, message_id,
            )
        return True

    @staticmethod
    async def add_reaction(message_id: int, user_id: int, emoji: str) -> dict:
        """Add or remove emoji reaction. Returns updated reactions."""
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT reactions FROM messages WHERE id=$1",
                message_id,
            )
            if not row:
                return {}
            reactions = dict(row["reactions"])
            users = reactions.get(emoji, [])
            if user_id in users:
                users.remove(user_id)   # toggle off
            else:
                users.append(user_id)   # toggle on
            if not users:
                reactions.pop(emoji, None)
            else:
                reactions[emoji] = users
            await conn.execute(
                "UPDATE messages SET reactions=$1 WHERE id=$2",
                json.dumps(reactions), message_id,
            )
        return reactions

    @staticmethod
    async def soft_delete(message_id: int, user_id: int) -> bool:
        result = await execute(
            "UPDATE messages SET is_deleted=TRUE "
            "WHERE id=$1 AND sender_id=$2",
            message_id, user_id,
        )
        return result == "UPDATE 1"

    @staticmethod
    async def mark_read(message_id: int, user_id: int) -> None:
        await execute(
            "INSERT INTO read_receipts (message_id, user_id) VALUES ($1,$2) "
            "ON CONFLICT DO NOTHING",
            message_id, user_id,
        )

    @staticmethod
    async def search(query: str, user_id: int, limit: int = 20) -> list:
        """Full-text search across user's messages."""
        rows = await fetchall(
            "SELECT m.id, m.body, m.sent_at, u.username AS sender_name, "
            "       m.conversation_id, m.group_id "
            "FROM messages m JOIN users u ON u.id=m.sender_id "
            "WHERE m.body ILIKE $1 AND m.is_deleted=FALSE "
            "  AND (m.conversation_id IN ("
            "    SELECT id FROM conversations WHERE user_a=$2 OR user_b=$2"
            "  ) OR m.group_id IN ("
            "    SELECT group_id FROM group_members WHERE user_id=$2"
            "  )) "
            "ORDER BY m.sent_at DESC LIMIT $3",
            f"%{query}%", user_id, limit,
        )
        return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════
#  Media (MinIO-based)
# ══════════════════════════════════════════════════════════════
class MediaRepo:

    @staticmethod
    async def save(uploader_id: int, object_key: str, bucket: str,
                   filename: str, mime_type: str,
                   file_size: int, checksum: str) -> int:
        return await fetchval(
            "INSERT INTO media_files "
            "(uploader_id, object_key, bucket, filename, mime_type, file_size, checksum) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
            uploader_id, object_key, bucket,
            filename, mime_type, file_size, checksum,
        )

    @staticmethod
    async def get_by_key(object_key: str) -> Optional[dict]:
        row = await fetchone(
            "SELECT id, uploader_id, object_key, bucket, filename, "
            "       mime_type, file_size, checksum "
            "FROM media_files WHERE object_key=$1",
            object_key,
        )
        return dict(row) if row else None


# ── Block / Unblock (appended) ────────────────────────────────
# Add these to UserRepo class manually or use directly:

async def block_user(blocker_id: int, blocked_id: int) -> None:
    await execute(
        "INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1,$2) "
        "ON CONFLICT DO NOTHING",
        blocker_id, blocked_id,
    )

async def unblock_user(blocker_id: int, blocked_id: int) -> None:
    await execute(
        "DELETE FROM user_blocks WHERE blocker_id=$1 AND blocked_id=$2",
        blocker_id, blocked_id,
    )

async def is_blocked(blocker_id: int, blocked_id: int) -> bool:
    row = await fetchone(
        "SELECT 1 FROM user_blocks WHERE blocker_id=$1 AND blocked_id=$2",
        blocker_id, blocked_id,
    )
    return row is not None
