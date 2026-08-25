"""
tests/test_auth.py  –  Unit tests for auth module (no DB required; mocked)
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from auth.auth import AuthError, hash_password, verify_password


# ── Password helpers ─────────────────────────────────────────
class TestPasswordHashing:
    def test_hash_and_verify(self) -> None:
        pw = "S3cur3P@ss!"
        h  = hash_password(pw)
        assert h != pw
        assert verify_password(pw, h)

    def test_wrong_password_fails(self) -> None:
        h = hash_password("correct")
        assert not verify_password("wrong", h)

    def test_hash_is_unique(self) -> None:
        pw = "same_password"
        assert hash_password(pw) != hash_password(pw)  # bcrypt salts differ

    def test_empty_password(self) -> None:
        h = hash_password("")
        assert verify_password("", h)
        assert not verify_password("x", h)


# ── Register ─────────────────────────────────────────────────
class TestRegister:
    @patch("auth.auth.SessionRepo")
    @patch("auth.auth.UserRepo")
    def test_successful_register(self, mock_user, mock_sess) -> None:
        from auth.auth import register

        mock_user.get_by_username.return_value = None
        mock_user.get_by_email.return_value    = None
        mock_user.create.return_value          = 42
        mock_sess.create.return_value          = 1

        result = register("alice", "alice@example.com", "password123")

        assert result["user_id"] == 42
        assert result["username"] == "alice"
        assert "token" in result

    @patch("auth.auth.UserRepo")
    def test_short_password_raises(self, mock_user) -> None:
        from auth.auth import register
        with pytest.raises(AuthError, match="8 characters"):
            register("bob", "bob@x.com", "short")

    @patch("auth.auth.UserRepo")
    def test_duplicate_username_raises(self, mock_user) -> None:
        from auth.auth import register
        mock_user.get_by_username.return_value = {"id": 1}
        with pytest.raises(AuthError, match="taken"):
            register("alice", "a@x.com", "password123")

    @patch("auth.auth.UserRepo")
    def test_duplicate_email_raises(self, mock_user) -> None:
        from auth.auth import register
        mock_user.get_by_username.return_value = None
        mock_user.get_by_email.return_value    = {"id": 1}
        with pytest.raises(AuthError, match="registered"):
            register("newuser", "taken@x.com", "password123")


# ── Login ────────────────────────────────────────────────────
class TestLogin:
    @patch("auth.auth.SessionRepo")
    @patch("auth.auth.UserRepo")
    def test_successful_login(self, mock_user, mock_sess) -> None:
        from auth.auth import login

        pw_hash = hash_password("pass1234")
        mock_user.get_by_username.return_value = {
            "id": 7, "username": "bob", "password_hash": pw_hash
        }
        mock_user.set_status.return_value = None
        mock_sess.create.return_value     = 1

        result = login("bob", "pass1234")
        assert result["user_id"] == 7
        assert "token" in result

    @patch("auth.auth.UserRepo")
    def test_bad_password_raises(self, mock_user) -> None:
        from auth.auth import login

        mock_user.get_by_username.return_value = {
            "id": 1, "username": "x",
            "password_hash": hash_password("correct"),
        }
        with pytest.raises(AuthError, match="Invalid"):
            login("x", "wrong")

    @patch("auth.auth.UserRepo")
    def test_unknown_user_raises(self, mock_user) -> None:
        from auth.auth import login
        mock_user.get_by_username.return_value = None
        with pytest.raises(AuthError, match="Invalid"):
            login("nobody", "anything")
