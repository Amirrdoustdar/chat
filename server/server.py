"""
server/server.py  –  Multi-threaded TCP chat server
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from typing import Dict, Optional, Set

from auth.auth import AuthError, login, logout, register, require_auth
from database.db import init_pool
from database.repository import GroupRepo, MessageRepo, UserRepo
from media.media_handler import MediaError, load_media, save_media
from utils.protocol import MsgType, err, ok, recv_packet, send_packet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ChatServer")

HOST = os.getenv("SERVER_HOST", "0.0.0.0")
PORT = int(os.getenv("SERVER_PORT", "9000"))


# ──────────────────────────────────────────────────────────────
# Connected-client registry
# ──────────────────────────────────────────────────────────────
class ClientRegistry:
    """Thread-safe map: user_id → (socket, username)."""

    def __init__(self) -> None:
        self._lock: threading.RLock = threading.RLock()
        self._by_id:   Dict[int, socket.socket]   = {}
        self._by_sock: Dict[socket.socket, int]   = {}
        self._group_members: Dict[int, Set[int]]  = {}  # group_id → {user_ids}

    # ── user registration ────────────────────────────────────
    def add(self, user_id: int, sock: socket.socket) -> None:
        with self._lock:
            self._by_id[user_id]   = sock
            self._by_sock[sock]    = user_id

    def remove_sock(self, sock: socket.socket) -> Optional[int]:
        with self._lock:
            uid = self._by_sock.pop(sock, None)
            if uid is not None:
                self._by_id.pop(uid, None)
                # leave all group presence lists
                for members in self._group_members.values():
                    members.discard(uid)
            return uid

    def get_sock(self, user_id: int) -> Optional[socket.socket]:
        with self._lock:
            return self._by_id.get(user_id)

    def get_user_id(self, sock: socket.socket) -> Optional[int]:
        with self._lock:
            return self._by_sock.get(sock)

    # ── group presence ───────────────────────────────────────
    def join_group(self, group_id: int, user_id: int) -> None:
        with self._lock:
            self._group_members.setdefault(group_id, set()).add(user_id)

    def leave_group(self, group_id: int, user_id: int) -> None:
        with self._lock:
            self._group_members.get(group_id, set()).discard(user_id)

    def group_sockets(self, group_id: int) -> list[socket.socket]:
        """Return sockets for all *online* members of a group."""
        with self._lock:
            members = self._group_members.get(group_id, set())
            return [
                self._by_id[uid]
                for uid in members
                if uid in self._by_id
            ]

    # ── broadcast helpers ────────────────────────────────────
    def send_to_user(self, user_id: int, packet: dict) -> bool:
        sock = self.get_sock(user_id)
        if sock:
            try:
                send_packet(sock, packet)
                return True
            except Exception:
                pass
        return False

    def broadcast_group(self, group_id: int, packet: dict,
                        exclude_id: int = None) -> None:
        for sock in self.group_sockets(group_id):
            uid = self.get_user_id(sock)
            if uid == exclude_id:
                continue
            try:
                send_packet(sock, packet)
            except Exception:
                pass


registry = ClientRegistry()


# ──────────────────────────────────────────────────────────────
# Per-client connection handler
# ──────────────────────────────────────────────────────────────
class ClientHandler(threading.Thread):
    """Handles one client connection in its own thread."""

    daemon = True

    def __init__(self, conn: socket.socket, addr: tuple) -> None:
        super().__init__()
        self.conn       = conn
        self.addr       = addr
        self.user_id:   Optional[int] = None
        self.username:  Optional[str] = None
        self.token:     Optional[str] = None

    # ── lifecycle ────────────────────────────────────────────
    def run(self) -> None:
        logger.info("Connection from %s:%d", *self.addr)
        try:
            while True:
                packet = recv_packet(self.conn)
                if packet is None:
                    break
                self._dispatch(packet)
        except ConnectionError as exc:
            logger.debug("Connection error (%s): %s", self.addr, exc)
        except Exception as exc:
            logger.exception("Unhandled error for %s: %s", self.addr, exc)
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        uid = registry.remove_sock(self.conn)
        if uid:
            UserRepo.set_status(uid, "offline")
            registry.broadcast_group(
                -1,          # placeholder – we'd iterate user's groups in prod
                ok(MsgType.STATUS_CHANGE, user_id=uid, status="offline"),
                exclude_id=uid,
            )
            logger.info("User %d (%s) disconnected", uid, self.username)
        try:
            self.conn.close()
        except Exception:
            pass

    # ── dispatcher ───────────────────────────────────────────
    def _dispatch(self, pkt: dict) -> None:
        t = pkt.get("type")

        # Un-authed routes
        if t == MsgType.REGISTER:
            return self._handle_register(pkt)
        if t == MsgType.LOGIN:
            return self._handle_login(pkt)
        if t == MsgType.PING:
            return self._reply(ok(MsgType.PONG))

        # All other routes require authentication
        if not self.user_id:
            return self._reply(err("Not authenticated", MsgType.AUTH_ERR))

        handlers = {
            MsgType.LOGOUT:          self._handle_logout,
            MsgType.DM_SEND:         self._handle_dm_send,
            MsgType.DM_HISTORY:      self._handle_dm_history,
            MsgType.GROUP_CREATE:    self._handle_group_create,
            MsgType.GROUP_JOIN:      self._handle_group_join,
            MsgType.GROUP_LEAVE:     self._handle_group_leave,
            MsgType.GROUP_SEND:      self._handle_group_send,
            MsgType.GROUP_HISTORY:   self._handle_group_history,
            MsgType.GROUP_LIST:      self._handle_group_list,
            MsgType.GROUP_MEMBERS:   self._handle_group_members,
            MsgType.MEDIA_UPLOAD:    self._handle_media_upload,
            MsgType.MEDIA_FETCH:     self._handle_media_fetch,
            MsgType.USER_SEARCH:     self._handle_user_search,
            MsgType.DELETE_MSG:      self._handle_delete_msg,
            MsgType.READ_RECEIPT:    self._handle_read_receipt,
        }
        handler = handlers.get(t)
        if handler:
            handler(pkt)
        else:
            self._reply(err(f"Unknown message type: {t}"))

    # ── auth handlers ────────────────────────────────────────
    def _handle_register(self, pkt: dict) -> None:
        try:
            result = register(
                pkt["username"], pkt["email"], pkt["password"]
            )
            self._reply(ok(MsgType.AUTH_OK, **result))
        except (AuthError, KeyError) as exc:
            self._reply(err(str(exc), MsgType.AUTH_ERR))

    def _handle_login(self, pkt: dict) -> None:
        try:
            result = login(
                pkt["username"], pkt["password"],
                ip=str(self.addr[0]),
            )
            self.user_id  = result["user_id"]
            self.username = result["username"]
            self.token    = result["token"]

            registry.add(self.user_id, self.conn)

            # Re-join saved groups
            for grp in GroupRepo.get_user_groups(self.user_id):
                registry.join_group(grp["id"], self.user_id)

            self._reply(ok(MsgType.AUTH_OK, **result))
            logger.info("User %s logged in from %s", self.username, self.addr)
        except (AuthError, KeyError) as exc:
            self._reply(err(str(exc), MsgType.AUTH_ERR))

    def _handle_logout(self, _pkt: dict) -> None:
        logout(self.token, self.user_id)
        self._reply(ok(MsgType.AUTH_OK, message="Logged out."))
        raise ConnectionError("Logged out")

    # ── DM handlers ──────────────────────────────────────────
    def _handle_dm_send(self, pkt: dict) -> None:
        try:
            recipient_id = int(pkt["recipient_id"])
            body         = pkt.get("body", "")
            msg_type     = pkt.get("msg_type", "text")
            media_id     = pkt.get("media_id")
            ttl          = pkt.get("ttl_seconds")

            msg_id = MessageRepo.create_dm(
                self.user_id, recipient_id, body,
                msg_type=msg_type, media_id=media_id, ttl_seconds=ttl,
            )

            outbound = ok(
                MsgType.DM_RECV,
                message_id=msg_id,
                sender_id=self.user_id,
                sender_name=self.username,
                recipient_id=recipient_id,
                body=body,
                msg_type=msg_type,
                media_id=media_id,
                ttl_seconds=ttl,
            )
            # Deliver to recipient if online
            registry.send_to_user(recipient_id, outbound)
            # Echo to sender too (for multi-device / confirmation)
            self._reply(outbound)
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_dm_history(self, pkt: dict) -> None:
        try:
            other = int(pkt["other_user_id"])
            limit = min(int(pkt.get("limit", 50)), 200)
            offset= int(pkt.get("offset", 0))
            msgs  = MessageRepo.get_dm_history(self.user_id, other, limit, offset)
            self._reply(ok(MsgType.DM_HISTORY_RESP, messages=msgs))
        except Exception as exc:
            self._reply(err(str(exc)))

    # ── Group handlers ───────────────────────────────────────
    def _handle_group_create(self, pkt: dict) -> None:
        try:
            gid = GroupRepo.create(
                pkt["name"], self.user_id,
                pkt.get("description", ""),
            )
            registry.join_group(gid, self.user_id)
            self._reply(ok(MsgType.GROUP_CREATE, group_id=gid, name=pkt["name"]))
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_group_join(self, pkt: dict) -> None:
        try:
            gid = int(pkt["group_id"])
            grp = GroupRepo.get_by_id(gid)
            if not grp:
                return self._reply(err("Group not found."))
            GroupRepo.add_member(gid, self.user_id)
            registry.join_group(gid, self.user_id)
            # Notify existing members
            registry.broadcast_group(
                gid,
                ok(MsgType.GROUP_RECV,
                   group_id=gid,
                   msg_type="system",
                   body=f"{self.username} joined the group."),
            )
            self._reply(ok(MsgType.GROUP_JOIN, group_id=gid))
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_group_leave(self, pkt: dict) -> None:
        try:
            gid = int(pkt["group_id"])
            GroupRepo.remove_member(gid, self.user_id)
            registry.leave_group(gid, self.user_id)
            registry.broadcast_group(
                gid,
                ok(MsgType.GROUP_RECV,
                   group_id=gid,
                   msg_type="system",
                   body=f"{self.username} left the group."),
            )
            self._reply(ok(MsgType.GROUP_LEAVE, group_id=gid))
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_group_send(self, pkt: dict) -> None:
        try:
            gid      = int(pkt["group_id"])
            body     = pkt.get("body", "")
            msg_type = pkt.get("msg_type", "text")
            media_id = pkt.get("media_id")
            ttl      = pkt.get("ttl_seconds")

            if not GroupRepo.is_member(gid, self.user_id):
                return self._reply(err("Not a member of this group."))

            msg_id = MessageRepo.create_group_msg(
                self.user_id, gid, body,
                msg_type=msg_type, media_id=media_id, ttl_seconds=ttl,
            )
            outbound = ok(
                MsgType.GROUP_RECV,
                message_id=msg_id,
                group_id=gid,
                sender_id=self.user_id,
                sender_name=self.username,
                body=body,
                msg_type=msg_type,
                media_id=media_id,
                ttl_seconds=ttl,
            )
            registry.broadcast_group(gid, outbound, exclude_id=self.user_id)
            self._reply(outbound)
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_group_history(self, pkt: dict) -> None:
        try:
            gid    = int(pkt["group_id"])
            limit  = min(int(pkt.get("limit", 50)), 200)
            offset = int(pkt.get("offset", 0))
            msgs   = MessageRepo.get_group_history(gid, limit, offset)
            self._reply(ok(MsgType.GROUP_HISTORY_RESP, group_id=gid, messages=msgs))
        except Exception as exc:
            self._reply(err(str(exc)))

    def _handle_group_list(self, _pkt: dict) -> None:
        groups = GroupRepo.get_user_groups(self.user_id)
        self._reply(ok(MsgType.GROUP_LIST_RESP, groups=groups))

    def _handle_group_members(self, pkt: dict) -> None:
        try:
            gid     = int(pkt["group_id"])
            members = GroupRepo.get_members(gid)
            self._reply(ok(MsgType.GROUP_MEMBERS_RESP, group_id=gid, members=members))
        except Exception as exc:
            self._reply(err(str(exc)))

    # ── Media handlers ───────────────────────────────────────
    def _handle_media_upload(self, pkt: dict) -> None:
        try:
            media_id = save_media(
                self.user_id,
                pkt["filename"],
                pkt["mime_type"],
                pkt["data_b64"],
            )
            self._reply(ok(MsgType.MEDIA_UPLOAD_OK, media_id=media_id))
        except MediaError as exc:
            self._reply(err(str(exc)))

    def _handle_media_fetch(self, pkt: dict) -> None:
        try:
            media_id = int(pkt["media_id"])
            data     = load_media(media_id)
            if not data:
                return self._reply(err("Media not found."))
            self._reply(ok(MsgType.MEDIA_FETCH_RESP, **data))
        except Exception as exc:
            self._reply(err(str(exc)))

    # ── Misc handlers ────────────────────────────────────────
    def _handle_user_search(self, pkt: dict) -> None:
        results = UserRepo.search(pkt.get("query", ""), limit=20)
        self._reply(ok(MsgType.USER_SEARCH_RESP, users=results))

    def _handle_delete_msg(self, pkt: dict) -> None:
        deleted = MessageRepo.soft_delete(int(pkt["message_id"]), self.user_id)
        if deleted:
            self._reply(ok(MsgType.DELETE_MSG, message_id=pkt["message_id"]))
        else:
            self._reply(err("Message not found or not yours."))

    def _handle_read_receipt(self, pkt: dict) -> None:
        MessageRepo.mark_read(int(pkt["message_id"]), self.user_id)
        self._reply(ok(MsgType.READ_RECEIPT, message_id=pkt["message_id"]))

    # ── helpers ──────────────────────────────────────────────
    def _reply(self, packet: dict) -> None:
        send_packet(self.conn, packet)


# ──────────────────────────────────────────────────────────────
# Server entry-point
# ──────────────────────────────────────────────────────────────
def run_server() -> None:
    init_pool()
    logger.info("DB pool ready.")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen(128)
        logger.info("Chat server listening on %s:%d", HOST, PORT)

        while True:
            conn, addr = srv.accept()
            ClientHandler(conn, addr).start()


if __name__ == "__main__":
    run_server()
