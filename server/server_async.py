"""
server/server_async.py  –  Complete async TCP chat server
  - asyncio + asyncpg (PostgreSQL)
  - Redis pub/sub + cache layer
  - Rate limiting + refresh tokens
  - Invite code registration
  - Chat key (UUID per conversation)
  - Yahoo-style public groups
  - MinIO media storage
  - Typing indicators
  - Reactions
  - Message edit + reply-to
  - User blocking
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
import sys
from typing import Dict, Optional, Set

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from auth.auth_pg import (AuthError, login, logout, refresh_tokens,
                           register, require_auth)
from auth.rate_limiter import RateLimitExceeded
from database.db_postgres import close_pool, init_pool
from database.repository_pg import (
    ConversationRepo, GroupRepo, InviteRepo,
    MessageRepo, MediaRepo, UserRepo,
)
from database.cache import CacheLayer
from media.minio_handler import MediaError, save_media, load_media, get_presigned_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ChatServer")

HOST      = os.getenv("SERVER_HOST", "0.0.0.0")
PORT      = int(os.getenv("SERVER_PORT", "9000"))
REDIS_URL = os.getenv("REDIS_URL",    "redis://127.0.0.1:6379")
REDIS_CHAN = "chat:broadcast"

# ── Wire protocol ─────────────────────────────────────────────
_HEADER = struct.Struct("<I")


def _encode(payload: dict) -> bytes:
    body = json.dumps(payload, default=str).encode()
    return _HEADER.pack(len(body)) + body


async def _read_packet(reader: asyncio.StreamReader) -> Optional[dict]:
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError:
        return None
    (length,) = _HEADER.unpack(header)
    if length == 0 or length > 64 * 1024 * 1024:
        raise ConnectionError(f"Bad packet length: {length}")
    body = await reader.readexactly(length)
    return json.loads(body.decode())


# ── Client registry ───────────────────────────────────────────
class ClientRegistry:
    def __init__(self) -> None:
        self._writers: Dict[int, asyncio.StreamWriter] = {}
        self._groups:  Dict[int, Set[int]]             = {}
        self._lock = asyncio.Lock()

    async def add(self, user_id: int, writer: asyncio.StreamWriter) -> None:
        async with self._lock:
            self._writers[user_id] = writer

    async def remove(self, user_id: int) -> None:
        async with self._lock:
            self._writers.pop(user_id, None)
            for members in self._groups.values():
                members.discard(user_id)

    async def join_group(self, group_id: int, user_id: int) -> None:
        async with self._lock:
            self._groups.setdefault(group_id, set()).add(user_id)

    async def leave_group(self, group_id: int, user_id: int) -> None:
        async with self._lock:
            self._groups.get(group_id, set()).discard(user_id)

    async def send_to_user(self, user_id: int, packet: dict) -> bool:
        async with self._lock:
            writer = self._writers.get(user_id)
        if writer:
            try:
                writer.write(_encode(packet))
                await writer.drain()
                return True
            except Exception:
                pass
        return False

    async def broadcast_group(self, group_id: int, packet: dict,
                              exclude_id: int = None) -> None:
        async with self._lock:
            members = set(self._groups.get(group_id, set()))
        for uid in members:
            if uid != exclude_id:
                await self.send_to_user(uid, packet)


registry: ClientRegistry
redis_pub: aioredis.Redis
redis_sub: aioredis.Redis
cache: CacheLayer


# ── Redis pub/sub ─────────────────────────────────────────────
async def redis_listener() -> None:
    pubsub = redis_pub.pubsub()
    await pubsub.subscribe(REDIS_CHAN)
    logger.info("Redis subscriber ready on '%s'", REDIS_CHAN)
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        try:
            env     = json.loads(message["data"])
            target  = env.get("target")
            payload = env["payload"]
            if target == "user":
                await registry.send_to_user(env["user_id"], payload)
            elif target == "group":
                await registry.broadcast_group(
                    env["group_id"], payload,
                    exclude_id=env.get("exclude_id"),
                )
        except Exception as exc:
            logger.error("Redis listener error: %s", exc)


async def _pub_user(user_id: int, packet: dict) -> None:
    env = json.dumps({"target": "user", "user_id": user_id,
                      "payload": packet}, default=str)
    await redis_pub.publish(REDIS_CHAN, env)


async def _pub_group(group_id: int, packet: dict,
                     exclude_id: int = None) -> None:
    env = json.dumps({"target": "group", "group_id": group_id,
                      "exclude_id": exclude_id, "payload": packet},
                     default=str)
    await redis_pub.publish(REDIS_CHAN, env)


# ── Per-client handler ────────────────────────────────────────
class ClientHandler:
    def __init__(self, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.reader        = reader
        self.writer        = writer
        self.addr          = writer.get_extra_info("peername")
        self.user_id:      Optional[int] = None
        self.username:     Optional[str] = None
        self.access_token: Optional[str] = None
        self.refresh_token:Optional[str] = None

    async def run(self) -> None:
        logger.info("Connection from %s:%d", *self.addr)
        try:
            while True:
                pkt = await _read_packet(self.reader)
                if pkt is None:
                    break
                await self._dispatch(pkt)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        except Exception as exc:
            logger.exception("Unhandled error for %s: %s", self.addr, exc)
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        if self.user_id:
            await registry.remove(self.user_id)
            await UserRepo.set_status(self.user_id, "offline")
            await cache.invalidate_user(self.user_id)
            logger.info("User %d (%s) disconnected", self.user_id, self.username)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

    # ── dispatcher ───────────────────────────────────────────
    async def _dispatch(self, pkt: dict) -> None:
        t = pkt.get("type")

        # Unauthenticated routes
        if t == "register":         return await self._register(pkt)
        if t == "login":            return await self._login(pkt)
        if t == "ping":             return await self._reply({"type": "pong", "ok": True})
        if t == "invite_validate":  return await self._invite_validate(pkt)
        if t == "token_refresh":    return await self._token_refresh(pkt)
        if t == "group_search":     return await self._group_search(pkt)

        if not self.user_id:
            return await self._reply({"type": "auth_err", "ok": False,
                                      "error": "Not authenticated"})

        routes = {
            # auth
            "logout":               self._logout,
            # DM
            "dm_send":              self._dm_send,
            "dm_history":           self._dm_history,
            "conv_list":            self._conv_list,
            # groups
            "group_create":         self._group_create,
            "group_join":           self._group_join,
            "group_join_by_code":   self._group_join_by_code,
            "group_leave":          self._group_leave,
            "group_send":           self._group_send,
            "group_history":        self._group_history,
            "group_list":           self._group_list,
            "group_members":        self._group_members,
            # messages
            "msg_edit":             self._msg_edit,
            "msg_delete":           self._msg_delete,
            "msg_react":            self._msg_react,
            "read_receipt":         self._read_receipt,
            "msg_search":           self._msg_search,
            # media
            "media_upload":         self._media_upload,
            "media_fetch":          self._media_fetch,
            "media_url":            self._media_url,
            # users
            "user_search":          self._user_search,
            "user_block":           self._user_block,
            "typing":               self._typing,
            # admin
            "invite_create":        self._invite_create,
            "invite_list":          self._invite_list,
        }
        handler = routes.get(t)
        if handler:
            await handler(pkt)
        else:
            await self._reply({"type": "error", "ok": False,
                               "error": f"Unknown type: {t}"})

    # ══════════════════════════════════════════════════════════
    #  Auth handlers
    # ══════════════════════════════════════════════════════════
    async def _invite_validate(self, pkt: dict) -> None:
        code = pkt.get("code", "").strip().upper()
        record = await InviteRepo.validate(code)
        if record:
            await self._reply({"type": "invite_valid", "ok": True,
                               "code": code})
        else:
            await self._reply({"type": "invite_invalid", "ok": False,
                               "error": "Invalid or expired invite code"})

    async def _register(self, pkt: dict) -> None:
        try:
            result = await register(
                pkt["username"], pkt["email"],
                pkt["password"], pkt["invite_code"],
                ip=str(self.addr[0]), redis=redis_pub,
            )
            await self._reply({"type": "auth_ok", "ok": True, **result})
        except (AuthError, KeyError) as exc:
            await self._reply({"type": "auth_err", "ok": False, "error": str(exc)})

    async def _login(self, pkt: dict) -> None:
        try:
            result = await login(
                pkt["username"], pkt["password"],
                ip=str(self.addr[0]), redis=redis_pub,
            )
            self.user_id       = result["user_id"]
            self.username      = result["username"]
            self.access_token  = result.get("access_token")
            self.refresh_token = result.get("refresh_token")

            await registry.add(self.user_id, self.writer)
            for grp in await GroupRepo.get_user_groups(self.user_id):
                await registry.join_group(grp["id"], self.user_id)

            await self._reply({"type": "auth_ok", "ok": True, **result})
            logger.info("User %s logged in from %s", self.username, self.addr)
        except (AuthError, KeyError) as exc:
            await self._reply({"type": "auth_err", "ok": False, "error": str(exc)})

    async def _token_refresh(self, pkt: dict) -> None:
        try:
            pair = await refresh_tokens(pkt["refresh_token"], redis=redis_pub)
            self.access_token  = pair["access_token"]
            self.refresh_token = pair["refresh_token"]
            await self._reply({"type": "token_refresh", "ok": True, **pair})
        except AuthError as exc:
            await self._reply({"type": "auth_err", "ok": False, "error": str(exc)})

    async def _logout(self, _pkt: dict) -> None:
        await logout(self.access_token, self.refresh_token,
                     self.user_id, self.username, redis=redis_pub)
        await self._reply({"type": "auth_ok", "ok": True, "message": "Logged out"})
        raise ConnectionError("Logged out")

    # ══════════════════════════════════════════════════════════
    #  DM handlers
    # ══════════════════════════════════════════════════════════
    async def _dm_send(self, pkt: dict) -> None:
        try:
            rid       = int(pkt["recipient_id"])
            body      = pkt.get("body", "")
            msg_type  = pkt.get("msg_type", "text")
            media_key = pkt.get("media_key")
            ttl       = pkt.get("ttl_seconds")
            reply_to  = pkt.get("reply_to_id")

            # Get or create chat key
            chat_key = await ConversationRepo.get_or_create(self.user_id, rid)

            msg_id = await MessageRepo.create_dm(
                chat_key, self.user_id, body,
                msg_type=msg_type, media_key=media_key,
                ttl_seconds=ttl, reply_to_id=reply_to,
            )

            # Invalidate message cache
            await cache.invalidate_messages(chat_key)
            await cache.invalidate_conv_list(self.user_id)
            await cache.invalidate_conv_list(rid)

            outbound = {
                "type":        "dm_recv",
                "ok":          True,
                "message_id":  msg_id,
                "chat_key":    chat_key,
                "sender_id":   self.user_id,
                "sender_name": self.username,
                "recipient_id":rid,
                "body":        body,
                "msg_type":    msg_type,
                "media_key":   media_key,
                "ttl_seconds": ttl,
                "reply_to_id": reply_to,
            }
            if not await registry.send_to_user(rid, outbound):
                await _pub_user(rid, outbound)
            await self._reply(outbound)
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _dm_history(self, pkt: dict) -> None:
        try:
            rid      = int(pkt["other_user_id"])
            limit    = min(int(pkt.get("limit", 50)), 200)
            before   = pkt.get("before_id")

            chat_key = await ConversationRepo.get_or_create(self.user_id, rid)

            # Try cache first
            cached = await cache.get_messages(chat_key)
            if cached and not before:
                return await self._reply({
                    "type": "dm_history_resp", "ok": True,
                    "chat_key": chat_key, "messages": cached,
                })

            msgs = await MessageRepo.get_dm_history(chat_key, limit, before)
            if not before:
                await cache.set_messages(chat_key, msgs)

            await self._reply({
                "type": "dm_history_resp", "ok": True,
                "chat_key": chat_key, "messages": msgs,
            })
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _conv_list(self, _pkt: dict) -> None:
        cached = await cache.get_conv_list(self.user_id)
        if cached:
            return await self._reply({
                "type": "conv_list_resp", "ok": True,
                "conversations": cached,
            })
        convs = await ConversationRepo.get_user_conversations(self.user_id)
        await cache.set_conv_list(self.user_id, convs)
        await self._reply({
            "type": "conv_list_resp", "ok": True,
            "conversations": convs,
        })

    # ══════════════════════════════════════════════════════════
    #  Group handlers
    # ══════════════════════════════════════════════════════════
    async def _group_search(self, pkt: dict) -> None:
        query = pkt.get("query", "")
        cached = await cache.get_group_search(query)
        if cached:
            return await self._reply({
                "type": "group_search_resp", "ok": True,
                "groups": cached,
            })
        groups = await GroupRepo.search_public(query)
        await cache.set_group_search(query, groups)
        await self._reply({
            "type": "group_search_resp", "ok": True,
            "groups": groups,
        })

    async def _group_create(self, pkt: dict) -> None:
        try:
            gid = await GroupRepo.create(
                pkt["name"], self.user_id,
                description=pkt.get("description", ""),
                topic=pkt.get("topic", ""),
                is_public=pkt.get("is_public", True),
                max_members=pkt.get("max_members", 500),
            )
            grp = await GroupRepo.get_by_id(gid)
            await registry.join_group(gid, self.user_id)
            await self._reply({
                "type": "group_create", "ok": True,
                "group_id": gid, "invite_code": grp["invite_code"],
                "name": pkt["name"],
            })
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_join(self, pkt: dict) -> None:
        try:
            gid = int(pkt["group_id"])
            grp = await GroupRepo.get_by_id(gid)
            if not grp:
                return await self._reply({"type": "error", "ok": False,
                                          "error": "Group not found"})
            await GroupRepo.add_member(gid, self.user_id)
            await registry.join_group(gid, self.user_id)
            await cache.invalidate_group(gid)
            notice = {"type": "group_recv", "ok": True, "group_id": gid,
                      "msg_type": "system",
                      "body": f"{self.username} joined the group."}
            await registry.broadcast_group(gid, notice)
            await _pub_group(gid, notice)
            await self._reply({"type": "group_join", "ok": True, "group_id": gid})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_join_by_code(self, pkt: dict) -> None:
        try:
            code = pkt.get("invite_code", "").strip().upper()
            gid  = await GroupRepo.join_by_invite(code, self.user_id)
            if not gid:
                return await self._reply({"type": "error", "ok": False,
                                          "error": "Invalid code or group full"})
            await registry.join_group(gid, self.user_id)
            await cache.invalidate_group(gid)
            grp = await GroupRepo.get_by_id(gid)
            notice = {"type": "group_recv", "ok": True, "group_id": gid,
                      "msg_type": "system",
                      "body": f"{self.username} joined via invite link."}
            await registry.broadcast_group(gid, notice)
            await _pub_group(gid, notice)
            await self._reply({"type": "group_join", "ok": True,
                               "group_id": gid, "name": grp["name"]})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_leave(self, pkt: dict) -> None:
        try:
            gid = int(pkt["group_id"])
            await GroupRepo.remove_member(gid, self.user_id)
            await registry.leave_group(gid, self.user_id)
            await cache.invalidate_group(gid)
            notice = {"type": "group_recv", "ok": True, "group_id": gid,
                      "msg_type": "system",
                      "body": f"{self.username} left the group."}
            await registry.broadcast_group(gid, notice)
            await _pub_group(gid, notice)
            await self._reply({"type": "group_leave", "ok": True, "group_id": gid})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_send(self, pkt: dict) -> None:
        try:
            gid       = int(pkt["group_id"])
            body      = pkt.get("body", "")
            msg_type  = pkt.get("msg_type", "text")
            media_key = pkt.get("media_key")
            ttl       = pkt.get("ttl_seconds")
            reply_to  = pkt.get("reply_to_id")

            if not await GroupRepo.is_member(gid, self.user_id):
                return await self._reply({"type": "error", "ok": False,
                                          "error": "Not a member"})

            msg_id = await MessageRepo.create_group_msg(
                gid, self.user_id, body,
                msg_type=msg_type, media_key=media_key,
                ttl_seconds=ttl, reply_to_id=reply_to,
            )
            await cache.invalidate_messages(f"group:{gid}")

            outbound = {
                "type":        "group_recv",
                "ok":          True,
                "message_id":  msg_id,
                "group_id":    gid,
                "sender_id":   self.user_id,
                "sender_name": self.username,
                "body":        body,
                "msg_type":    msg_type,
                "media_key":   media_key,
                "ttl_seconds": ttl,
                "reply_to_id": reply_to,
            }
            await registry.broadcast_group(gid, outbound, exclude_id=self.user_id)
            await _pub_group(gid, outbound, exclude_id=self.user_id)
            await self._reply(outbound)
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_history(self, pkt: dict) -> None:
        try:
            gid    = int(pkt["group_id"])
            limit  = min(int(pkt.get("limit", 50)), 200)
            before = pkt.get("before_id")
            msgs   = await MessageRepo.get_group_history(gid, limit, before)
            await self._reply({
                "type": "group_history_resp", "ok": True,
                "group_id": gid, "messages": msgs,
            })
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _group_list(self, _pkt: dict) -> None:
        groups = await GroupRepo.get_user_groups(self.user_id)
        await self._reply({"type": "group_list_resp", "ok": True, "groups": groups})

    async def _group_members(self, pkt: dict) -> None:
        try:
            gid = int(pkt["group_id"])
            cached = await cache.get(f"group:members:{gid}")
            if cached:
                return await self._reply({
                    "type": "group_members_resp", "ok": True,
                    "group_id": gid, "members": cached,
                })
            members = await GroupRepo.get_members(gid)
            await cache.set(f"group:members:{gid}", members, 120)
            await self._reply({
                "type": "group_members_resp", "ok": True,
                "group_id": gid, "members": members,
            })
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    # ══════════════════════════════════════════════════════════
    #  Message actions
    # ══════════════════════════════════════════════════════════
    async def _msg_edit(self, pkt: dict) -> None:
        try:
            mid      = int(pkt["message_id"])
            new_body = pkt["body"]
            success  = await MessageRepo.edit_message(mid, self.user_id, new_body)
            if not success:
                return await self._reply({"type": "error", "ok": False,
                                          "error": "Message not found or not yours"})
            await self._reply({"type": "msg_edited", "ok": True,
                               "message_id": mid, "body": new_body})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _msg_delete(self, pkt: dict) -> None:
        deleted = await MessageRepo.soft_delete(
            int(pkt["message_id"]), self.user_id
        )
        if deleted:
            await self._reply({"type": "msg_deleted", "ok": True,
                               "message_id": pkt["message_id"]})
        else:
            await self._reply({"type": "error", "ok": False,
                               "error": "Not found or not yours"})

    async def _msg_react(self, pkt: dict) -> None:
        try:
            mid   = int(pkt["message_id"])
            emoji = pkt["emoji"]
            updated = await MessageRepo.add_reaction(mid, self.user_id, emoji)
            await self._reply({"type": "msg_reacted", "ok": True,
                               "message_id": mid, "reactions": updated})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _read_receipt(self, pkt: dict) -> None:
        await MessageRepo.mark_read(int(pkt["message_id"]), self.user_id)
        await self._reply({"type": "read_receipt", "ok": True,
                           "message_id": pkt["message_id"]})

    async def _msg_search(self, pkt: dict) -> None:
        try:
            results = await MessageRepo.search(
                pkt.get("query", ""), self.user_id,
                limit=int(pkt.get("limit", 20)),
            )
            await self._reply({"type": "msg_search_resp", "ok": True,
                               "results": results})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    # ══════════════════════════════════════════════════════════
    #  Media handlers
    # ══════════════════════════════════════════════════════════
    async def _media_upload(self, pkt: dict) -> None:
        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(
                None, lambda: save_media(
                    self.user_id, pkt["filename"],
                    pkt["mime_type"], pkt["data_b64"],
                )
            )
            media_id = await MediaRepo.save(
                uploader_id=self.user_id,
                object_key=info["object_key"],
                bucket=info["bucket"],
                filename=pkt["filename"],
                mime_type=pkt["mime_type"],
                file_size=info["file_size"],
                checksum=info["checksum"],
            )
            await self._reply({"type": "media_upload_ok", "ok": True,
                               "media_id": media_id,
                               "media_key": info["object_key"]})
        except MediaError as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _media_fetch(self, pkt: dict) -> None:
        try:
            key    = pkt["media_key"]
            loop   = asyncio.get_event_loop()
            data   = await loop.run_in_executor(None, lambda: load_media(key))
            if not data:
                return await self._reply({"type": "error", "ok": False,
                                          "error": "Media not found"})
            await self._reply({"type": "media_fetch_resp", "ok": True, **data})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _media_url(self, pkt: dict) -> None:
        """Return a presigned URL for direct browser access."""
        try:
            key  = pkt["media_key"]
            loop = asyncio.get_event_loop()
            url  = await loop.run_in_executor(
                None, lambda: get_presigned_url(key, expires_seconds=3600)
            )
            await self._reply({"type": "media_url_resp", "ok": True,
                               "url": url, "media_key": key})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    # ══════════════════════════════════════════════════════════
    #  User handlers
    # ══════════════════════════════════════════════════════════
    async def _user_search(self, pkt: dict) -> None:
        users = await UserRepo.search(pkt.get("query", ""), limit=20)
        await self._reply({"type": "user_search_resp", "ok": True, "users": users})

    async def _user_block(self, pkt: dict) -> None:
        try:
            target_id = int(pkt["user_id"])
            action    = pkt.get("action", "block")
            if action == "block":
                await UserRepo.block(self.user_id, target_id)
                await self._reply({"type": "user_blocked", "ok": True,
                                   "user_id": target_id})
            else:
                await UserRepo.unblock(self.user_id, target_id)
                await self._reply({"type": "user_unblocked", "ok": True,
                                   "user_id": target_id})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _typing(self, pkt: dict) -> None:
        """Typing indicator — stores in Redis with 4s TTL."""
        target_id = pkt.get("recipient_id") or pkt.get("group_id")
        is_group  = "group_id" in pkt

        await cache.set_typing(self.user_id, target_id)

        notice = {
            "type":        "typing",
            "sender_id":   self.user_id,
            "sender_name": self.username,
        }
        if is_group:
            notice["group_id"] = target_id
            await registry.broadcast_group(
                target_id, notice, exclude_id=self.user_id
            )
            await _pub_group(target_id, notice, exclude_id=self.user_id)
        else:
            notice["recipient_id"] = target_id
            if not await registry.send_to_user(target_id, notice):
                await _pub_user(target_id, notice)

    # ══════════════════════════════════════════════════════════
    #  Admin / invite handlers
    # ══════════════════════════════════════════════════════════
    async def _invite_create(self, pkt: dict) -> None:
        try:
            expires = pkt.get("expires_hours")
            code    = await InviteRepo.create(self.user_id, expires)
            await self._reply({"type": "invite_created", "ok": True, "code": code})
        except Exception as exc:
            await self._reply({"type": "error", "ok": False, "error": str(exc)})

    async def _invite_list(self, _pkt: dict) -> None:
        invites = await InviteRepo.list_all()
        await self._reply({"type": "invite_list_resp", "ok": True,
                           "invites": invites})

    # ── Reply helper ─────────────────────────────────────────
    async def _reply(self, packet: dict) -> None:
        self.writer.write(_encode(packet))
        await self.writer.drain()


# ── Server entry-point ────────────────────────────────────────
async def handle_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> None:
    await ClientHandler(reader, writer).run()


async def main() -> None:
    global registry, redis_pub, redis_sub, cache

    registry = ClientRegistry()

    await init_pool()
    logger.info("PostgreSQL pool ready")

    redis_pub = await aioredis.from_url(REDIS_URL, decode_responses=False)
    redis_sub = await aioredis.from_url(REDIS_URL, decode_responses=False)
    cache     = CacheLayer(redis_pub)
    logger.info("Redis connected: %s", REDIS_URL)

    asyncio.create_task(redis_listener())

    server = await asyncio.start_server(handle_client, HOST, PORT)
    logger.info("Chat server listening on %s:%d", HOST, PORT)

    async with server:
        await server.serve_forever()

    await close_pool()
    await redis_pub.aclose()
    await redis_sub.aclose()


if __name__ == "__main__":
    asyncio.run(main())
