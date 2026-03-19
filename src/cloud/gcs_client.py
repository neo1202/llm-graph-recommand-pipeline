"""Google Cloud Storage utilities for uploading/downloading pipeline data."""

import logging
from pathlib import Path

from src.config import settings

logger = logging.getLogger(__name__)


def get_gcs_client():
    """Get GCS client, or None if not configured."""
    if not settings.gcs_bucket:
        logger.warning("GCS_BUCKET not set — GCS operations will be skipped")
        return None
    from google.cloud import storage
    return storage.Client(project=settings.gcp_project_id)


def upload_to_gcs(local_path: str, gcs_path: str) -> str | None:
    """Upload a local file to GCS bucket."""
    client = get_gcs_client()
    if not client:
        return None

    bucket = client.bucket(settings.gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    uri = f"gs://{settings.gcs_bucket}/{gcs_path}"
    logger.info(f"Uploaded {local_path} → {uri}")
    return uri


def download_from_gcs(gcs_path: str, local_path: str) -> str | None:
    """Download a file from GCS bucket to local path."""
    client = get_gcs_client()
    if not client:
        return None

    bucket = client.bucket(settings.gcs_bucket)
    blob = bucket.blob(gcs_path)
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)

    logger.info(f"Downloaded gs://{settings.gcs_bucket}/{gcs_path} → {local_path}")
    return local_path


def upload_json_to_gcs(data: dict | list, gcs_path: str) -> str | None:
    """Upload a JSON object directly to GCS."""
    import json

    client = get_gcs_client()
    if not client:
        return None

    bucket = client.bucket(settings.gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json",
    )

    uri = f"gs://{settings.gcs_bucket}/{gcs_path}"
    logger.info(f"Uploaded JSON → {uri}")
    return uri
