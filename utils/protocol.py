"""
utils/protocol.py  –  Wire protocol (JSON-framed over TCP)

Every packet is:   4-byte little-endian length  |  UTF-8 JSON payload
"""
from __future__ import annotations

import json
import socket
import struct
from enum import Enum
from typing import Any, Optional


# ──────────────────────────────────────────────────────────────
# Packet types (client → server  /  server → client)
# ──────────────────────────────────────────────────────────────
class MsgType(str, Enum):
    # Auth
    REGISTER        = "register"
    LOGIN           = "login"
    LOGOUT          = "logout"
    AUTH_OK         = "auth_ok"
    AUTH_ERR        = "auth_err"

    # DM
    DM_SEND         = "dm_send"
    DM_RECV         = "dm_recv"
    DM_HISTORY      = "dm_history"
    DM_HISTORY_RESP = "dm_history_resp"

    # Group
    GROUP_CREATE    = "group_create"
    GROUP_JOIN      = "group_join"
    GROUP_LEAVE     = "group_leave"
    GROUP_SEND      = "group_send"
    GROUP_RECV      = "group_recv"
    GROUP_HISTORY   = "group_history"
    GROUP_HISTORY_RESP = "group_history_resp"
    GROUP_LIST      = "group_list"
    GROUP_LIST_RESP = "group_list_resp"
    GROUP_MEMBERS   = "group_members"
    GROUP_MEMBERS_RESP = "group_members_resp"

    # Media
    MEDIA_UPLOAD    = "media_upload"       # client sends base64 blob
    MEDIA_UPLOAD_OK = "media_upload_ok"    # server replies with media_id
    MEDIA_FETCH     = "media_fetch"        # client requests file by media_id
    MEDIA_FETCH_RESP= "media_fetch_resp"   # server returns base64 blob

    # Misc
    USER_SEARCH     = "user_search"
    USER_SEARCH_RESP= "user_search_resp"
    STATUS_CHANGE   = "status_change"
    ERROR           = "error"
    PING            = "ping"
    PONG            = "pong"
    DELETE_MSG      = "delete_msg"
    READ_RECEIPT    = "read_receipt"


# ──────────────────────────────────────────────────────────────
# Frame helpers
# ──────────────────────────────────────────────────────────────
_HEADER = struct.Struct("<I")   # 4-byte unsigned int, little-endian


def encode_packet(payload: dict) -> bytes:
    """Serialise *payload* to a length-prefixed JSON frame."""
    body = json.dumps(payload, default=str).encode("utf-8")
    return _HEADER.pack(len(body)) + body


def send_packet(sock: socket.socket, payload: dict) -> None:
    """Send a single packet over *sock*."""
    sock.sendall(encode_packet(payload))


def recv_packet(sock: socket.socket) -> Optional[dict]:
    """
    Receive one packet from *sock*.

    Returns ``None`` on clean disconnect.
    Raises :class:`ConnectionError` on partial / corrupt frames.
    """
    header = _recv_exact(sock, 4)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length == 0 or length > 64 * 1024 * 1024:   # 64 MiB guard
        raise ConnectionError(f"Bad packet length: {length}")
    body = _recv_exact(sock, length)
    if body is None:
        raise ConnectionError("Connection closed mid-packet")
    return json.loads(body.decode("utf-8"))


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ──────────────────────────────────────────────────────────────
# Packet builder helpers  (avoids typos in field names)
# ──────────────────────────────────────────────────────────────
def ok(type_: MsgType, **kwargs) -> dict:
    return {"type": type_, "ok": True, **kwargs}


def err(msg: str, type_: MsgType = MsgType.ERROR) -> dict:
    return {"type": type_, "ok": False, "error": msg}
