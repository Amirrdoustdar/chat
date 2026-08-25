"""
bridge/bridge.py  –  FastAPI WebSocket bridge
Translates browser WebSocket connections → TCP JSON packets to the async chat server.

Architecture:
    Browser (React)
        ↕  WebSocket (ws://localhost:8000/ws)
    FastAPI bridge  (this file)
        ↕  TCP socket (localhost:9000)
    Python async chat server
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import struct
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("WSBridge")

CHAT_HOST = os.getenv("SERVER_HOST", "127.0.0.1")
CHAT_PORT = int(os.getenv("SERVER_PORT", "9000"))
BRIDGE_PORT = int(os.getenv("BRIDGE_PORT", "8000"))

# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(title="Chat Platform WebSocket Bridge", version="1.0.0")

# CORS — allow React dev server and production domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite dev server
        "http://localhost:3000",   # CRA dev server
        "http://127.0.0.1:5173",
        os.getenv("FRONTEND_URL", "http://localhost:5173"),
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Wire protocol helpers (same as server) ────────────────────
_HEADER = struct.Struct("<I")


def _encode(payload: dict) -> bytes:
    body = json.dumps(payload, default=str).encode()
    return _HEADER.pack(len(body)) + body


async def _recv_tcp(reader: asyncio.StreamReader) -> Optional[dict]:
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError:
        return None
    (length,) = _HEADER.unpack(header)
    if length == 0 or length > 64 * 1024 * 1024:
        raise ConnectionError(f"Bad packet length: {length}")
    body = await reader.readexactly(length)
    return json.loads(body.decode())


# ── WebSocket endpoint ────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    client = ws.client
    logger.info("Browser connected: %s:%d", client.host, client.port)

    # Open TCP connection to chat server
    try:
        reader, writer = await asyncio.open_connection(CHAT_HOST, CHAT_PORT)
        logger.info("TCP tunnel opened to %s:%d", CHAT_HOST, CHAT_PORT)
    except ConnectionRefusedError:
        await ws.send_json({
            "type": "error",
            "ok": False,
            "error": "Chat server is not running. Start server_async.py first.",
        })
        await ws.close()
        return

    # ── Two coroutines running concurrently ──────────────────
    # 1. browser → TCP server
    async def browser_to_tcp():
        try:
            while True:
                data = await ws.receive_text()
                try:
                    packet = json.loads(data)
                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "error": "Invalid JSON"})
                    continue
                writer.write(_encode(packet))
                await writer.drain()
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            logger.error("browser→tcp error: %s", exc)
        finally:
            writer.close()

    # 2. TCP server → browser
    async def tcp_to_browser():
        try:
            while True:
                packet = await _recv_tcp(reader)
                if packet is None:
                    break
                await ws.send_json(packet)
        except Exception as exc:
            logger.error("tcp→browser error: %s", exc)
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    # Run both directions concurrently
    await asyncio.gather(
        browser_to_tcp(),
        tcp_to_browser(),
        return_exceptions=True,
    )

    logger.info("Browser disconnected: %s:%d", client.host, client.port)


# ── Health check endpoint ─────────────────────────────────────
@app.get("/health")
async def health():
    """Check if bridge and chat server are both reachable."""
    try:
        reader, writer = await asyncio.open_connection(CHAT_HOST, CHAT_PORT)
        writer.close()
        await writer.wait_closed()
        server_ok = True
    except Exception:
        server_ok = False

    return {
        "bridge": "ok",
        "chat_server": "ok" if server_ok else "unreachable",
        "chat_server_addr": f"{CHAT_HOST}:{CHAT_PORT}",
    }


# ── Packet reference endpoint (for Ali's dev) ─────────────────
@app.get("/docs/packets")
async def packet_reference():
    """Full packet type reference for the frontend developer."""
    return {
        "protocol": "JSON over WebSocket",
        "connect_to": "ws://localhost:8000/ws",
        "packets": {
            "auth": {
                "register":      {"fields": ["username", "email", "password"]},
                "login":         {"fields": ["username", "password"]},
                "logout":        {"fields": []},
                "token_refresh": {"fields": ["refresh_token"]},
                "auth_ok":       {"direction": "server→client", "fields": ["user_id", "username", "access_token", "refresh_token"]},
                "auth_err":      {"direction": "server→client", "fields": ["error"]},
            },
            "dm": {
                "dm_send":        {"fields": ["recipient_id", "body", "msg_type?", "media_id?", "ttl_seconds?"]},
                "dm_recv":        {"direction": "server→client", "fields": ["message_id", "sender_id", "sender_name", "body", "ttl_seconds?"]},
                "dm_history":     {"fields": ["other_user_id", "limit?", "offset?"]},
                "dm_history_resp":{"direction": "server→client", "fields": ["messages[]"]},
            },
            "groups": {
                "group_create":       {"fields": ["name", "description?"]},
                "group_join":         {"fields": ["group_id"]},
                "group_leave":        {"fields": ["group_id"]},
                "group_send":         {"fields": ["group_id", "body", "ttl_seconds?"]},
                "group_recv":         {"direction": "server→client", "fields": ["group_id", "sender_id", "sender_name", "body"]},
                "group_history":      {"fields": ["group_id", "limit?", "offset?"]},
                "group_list":         {"fields": []},
                "group_members":      {"fields": ["group_id"]},
            },
            "media": {
                "media_upload":     {"fields": ["filename", "mime_type", "data_b64"]},
                "media_upload_ok":  {"direction": "server→client", "fields": ["media_id"]},
                "media_fetch":      {"fields": ["media_id"]},
                "media_fetch_resp": {"direction": "server→client", "fields": ["filename", "mime_type", "data_b64"]},
            },
            "misc": {
                "user_search":      {"fields": ["query"]},
                "delete_msg":       {"fields": ["message_id"]},
                "read_receipt":     {"fields": ["message_id"]},
                "ping":             {"fields": []},
                "status_change":    {"direction": "server→client", "fields": ["user_id", "status"]},
            },
        },
    }


# ── Run directly ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "bridge.bridge:app",
        host="0.0.0.0",
        port=BRIDGE_PORT,
        reload=False,
        log_level="info",
    )
