"""
media/minio_handler.py  –  MinIO-based media storage
Replaces disk-based storage with S3-compatible object storage
"""
from __future__ import annotations

import base64
import hashlib
import io
import logging
import os
import uuid
from typing import Optional

from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── MinIO config ─────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "localhost:9001")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE    = os.getenv("MINIO_SECURE",     "false").lower() == "true"
MEDIA_BUCKET    = os.getenv("MINIO_BUCKET",     "chat-media")
MAX_FILE_BYTES  = int(os.getenv("MAX_FILE_MB",  "20")) * 1024 * 1024

ALLOWED_MIME = {
    "image/jpeg", "image/png", "image/gif", "image/webp",
    "application/pdf",
    "text/plain",
    "application/zip",
}

_client: Optional[Minio] = None


class MediaError(Exception):
    pass


def get_client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS,
            secret_key=MINIO_SECRET,
            secure=MINIO_SECURE,
        )
        # Ensure bucket exists
        if not _client.bucket_exists(MEDIA_BUCKET):
            _client.make_bucket(MEDIA_BUCKET)
            logger.info("Created MinIO bucket: %s", MEDIA_BUCKET)
    return _client


def save_media(uploader_id: int, filename: str,
               mime_type: str, data_b64: str) -> dict:
    """
    Upload base64 media to MinIO.
    Returns dict with object_key, checksum, file_size.
    """
    if mime_type not in ALLOWED_MIME:
        raise MediaError(f"MIME type not allowed: {mime_type}")

    try:
        raw = base64.b64decode(data_b64)
    except Exception as exc:
        raise MediaError(f"Invalid base64: {exc}") from exc

    if len(raw) > MAX_FILE_BYTES:
        raise MediaError(f"File too large: {len(raw)} bytes")

    checksum   = hashlib.sha256(raw).hexdigest()
    ext        = filename.rsplit(".", 1)[-1] if "." in filename else "bin"
    object_key = f"{uploader_id}/{uuid.uuid4().hex}.{ext}"

    client = get_client()
    client.put_object(
        MEDIA_BUCKET,
        object_key,
        io.BytesIO(raw),
        length=len(raw),
        content_type=mime_type,
    )
    logger.info("Uploaded media: %s (%d bytes)", object_key, len(raw))

    return {
        "object_key": object_key,
        "bucket":     MEDIA_BUCKET,
        "checksum":   checksum,
        "file_size":  len(raw),
    }


def load_media(object_key: str) -> Optional[dict]:
    """
    Download media from MinIO and return as base64.
    """
    client = get_client()
    try:
        response = client.get_object(MEDIA_BUCKET, object_key)
        raw = response.read()
        response.close()
        return {
            "object_key": object_key,
            "data_b64":   base64.b64encode(raw).decode(),
        }
    except S3Error as exc:
        logger.error("MinIO get error: %s", exc)
        return None


def get_presigned_url(object_key: str, expires_seconds: int = 3600) -> str:
    """
    Generate a presigned URL for direct browser access.
    Useful for image previews without proxying through the server.
    """
    from datetime import timedelta
    client = get_client()
    return client.presigned_get_object(
        MEDIA_BUCKET,
        object_key,
        expires=timedelta(seconds=expires_seconds),
    )


def delete_media(object_key: str) -> None:
    client = get_client()
    try:
        client.remove_object(MEDIA_BUCKET, object_key)
        logger.info("Deleted media: %s", object_key)
    except S3Error as exc:
        logger.error("MinIO delete error: %s", exc)
