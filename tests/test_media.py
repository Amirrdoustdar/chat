"""
tests/test_media.py  –  Tests for media upload / download
"""
from __future__ import annotations

import base64
import hashlib
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from media.media_handler import MAX_FILE_BYTES, MediaError, load_media, save_media


SAMPLE_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
    b"\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18"
    b"\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
)


class TestSaveMedia:
    @patch("media.media_handler.MediaRepo")
    def test_valid_upload(self, mock_repo, tmp_path) -> None:
        import media.media_handler as mh
        # Patch MEDIA_ROOT directly on the already-imported module
        original_root = mh.MEDIA_ROOT
        mh.MEDIA_ROOT = tmp_path
        mock_repo.save.return_value = 1
        data_b64 = base64.b64encode(SAMPLE_PNG).decode()
        try:
            media_id = mh.save_media(
                uploader_id=1,
                filename="test.png",
                mime_type="image/png",
                data_b64=data_b64,
            )
            assert isinstance(media_id, int)
        finally:
            mh.MEDIA_ROOT = original_root

    def test_invalid_mime_raises(self) -> None:
        with pytest.raises(MediaError, match="MIME"):
            save_media(1, "x.exe", "application/x-msdownload",
                       base64.b64encode(b"MZ").decode())

    @patch("media.media_handler.MediaRepo")
    def test_oversized_file_raises(self, _mock, tmp_path) -> None:
        import media.media_handler as mh
        original_root  = mh.MEDIA_ROOT
        original_limit = mh.MAX_FILE_BYTES
        mh.MEDIA_ROOT    = tmp_path
        mh.MAX_FILE_BYTES = 10          # tiny limit
        try:
            with pytest.raises(MediaError, match="large"):
                mh.save_media(1, "big.png", "image/png",
                              base64.b64encode(b"x" * 1024).decode())
        finally:
            mh.MEDIA_ROOT    = original_root
            mh.MAX_FILE_BYTES = original_limit

    def test_bad_base64_raises(self) -> None:
        with pytest.raises(MediaError, match="base64"):
            save_media(1, "f.png", "image/png", "!!!not_base64!!!")


class TestLoadMedia:
    @patch("media.media_handler.MediaRepo")
    def test_returns_none_for_missing_record(self, mock_repo) -> None:
        mock_repo.get_by_id.return_value = None
        result = load_media(999)
        assert result is None

    @patch("media.media_handler.MediaRepo")
    def test_loads_correctly(self, mock_repo, tmp_path) -> None:
        file_path = tmp_path / "1" / "test.png"
        file_path.parent.mkdir(parents=True)
        file_path.write_bytes(SAMPLE_PNG)
        checksum = hashlib.sha256(SAMPLE_PNG).hexdigest()

        mock_repo.get_by_id.return_value = {
            "id": 1,
            "filename": "test.png",
            "mime_type": "image/png",
            "storage_path": str(file_path),
            "checksum": checksum,
        }

        result = load_media(1)
        assert result is not None
        assert result["filename"] == "test.png"
        assert base64.b64decode(result["data_b64"]) == SAMPLE_PNG
