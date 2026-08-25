"""
media/media_handler.py  –  Store & retrieve media files on disk
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import uuid
from pathlib import Path
from typing import Optional

from database.repository import MediaRepo

logger = logging.getLogger(__name__)

MEDIA_ROOT = Path(os.getenv("MEDIA_ROOT", "media_store"))
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_MB", "20")) * 1024 * 1024   # default 20 MB

ALLOWED_MIME = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
    "text/plain",
    "application/zip",
}


class MediaError(Exception):
    """Raised on media validation / storage failure."""


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_media(
    uploader_id: int,
    filename: str,
    mime_type: str,
    data_b64: str,
) -> int:
    """
    Decode base64 *data_b64*, persist to disk, record in DB.

    Returns the new ``media_files.id``.
    Raises :class:`MediaError` on validation failure.
    """
    if mime_type not in ALLOWED_MIME:
        raise MediaError(f"MIME type not allowed: {mime_type}")

    try:
        raw = base64.b64decode(data_b64)
    except Exception as exc:
        raise MediaError(f"Invalid base64 data: {exc}") from exc

    if len(raw) > MAX_FILE_BYTES:
        raise MediaError(
            f"File too large ({len(raw)} bytes > {MAX_FILE_BYTES})"
        )

    # Build storage path: media_store/<uploader_id>/<uuid>_<filename>
    dest_dir = MEDIA_ROOT / str(uploader_id)
    _ensure_dir(dest_dir)
    safe_name = f"{uuid.uuid4().hex}_{Path(filename).name}"
    dest_path = dest_dir / safe_name

    dest_path.write_bytes(raw)
    logger.info("Saved media %s (%d bytes)", dest_path, len(raw))

    media_id = MediaRepo.save(
        uploader_id=uploader_id,
        filename=filename,
        mime_type=mime_type,
        file_size=len(raw),
        storage_path=str(dest_path),
        data=raw,
    )
    return media_id


def load_media(media_id: int) -> Optional[dict]:
    """
    Retrieve a media record and return its contents as base64.

    Returns::

        {
          "media_id": int,
          "filename": str,
          "mime_type": str,
          "data_b64": str,   # base64-encoded file bytes
        }

    or ``None`` if not found.
    """
    record = MediaRepo.get_by_id(media_id)
    if not record:
        return None

    path = Path(record["storage_path"])
    if not path.exists():
        logger.error("Media file missing on disk: %s", path)
        return None

    raw = path.read_bytes()
    # Verify integrity
    expected = record["checksum"]
    actual   = hashlib.sha256(raw).hexdigest()
    if expected != actual:
        logger.error("Checksum mismatch for media_id=%d", media_id)
        return None

    return {
        "media_id":  media_id,
        "filename":  record["filename"],
        "mime_type": record["mime_type"],
        "data_b64":  base64.b64encode(raw).decode(),
    }
