"""
tests/test_protocol.py  –  Tests for wire protocol encode/decode
"""
from __future__ import annotations

import io
import json
import socket
import struct
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.protocol import MsgType, encode_packet, err, ok, recv_packet, send_packet


class TestEncodePacket:
    def test_roundtrip(self) -> None:
        payload = {"type": MsgType.PING, "data": "hello"}
        frame   = encode_packet(payload)
        length  = struct.unpack_from("<I", frame)[0]
        body    = frame[4:]
        assert len(body) == length
        assert json.loads(body) == payload

    def test_empty_payload(self) -> None:
        frame = encode_packet({})
        assert len(frame) > 4


class TestHelpers:
    def test_ok_has_ok_true(self) -> None:
        p = ok(MsgType.PONG, foo="bar")
        assert p["ok"] is True
        assert p["type"] == MsgType.PONG
        assert p["foo"] == "bar"

    def test_err_has_ok_false(self) -> None:
        p = err("Something went wrong")
        assert p["ok"] is False
        assert "error" in p


class TestSendRecv:
    """Use a loopback socket pair to verify send/recv."""

    def _make_pair(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        cli = socket.socket()
        cli.connect(("127.0.0.1", port))
        conn, _ = srv.accept()
        srv.close()
        return cli, conn

    def test_single_packet(self) -> None:
        cli, srv = self._make_pair()
        try:
            payload = {"type": "test", "value": 42}
            send_packet(cli, payload)
            received = recv_packet(srv)
            assert received == payload
        finally:
            cli.close()
            srv.close()

    def test_multiple_packets(self) -> None:
        cli, srv = self._make_pair()
        try:
            messages = [{"i": i} for i in range(5)]
            for m in messages:
                send_packet(cli, m)
            for expected in messages:
                got = recv_packet(srv)
                assert got == expected
        finally:
            cli.close()
            srv.close()

    def test_recv_returns_none_on_close(self) -> None:
        cli, srv = self._make_pair()
        cli.close()
        result = recv_packet(srv)
        assert result is None
        srv.close()
