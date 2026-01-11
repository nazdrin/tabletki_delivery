from __future__ import annotations
from dataclasses import dataclass
from io import BytesIO
from typing import Optional, Iterable

import random
import time
import ssl

import httplib2
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


SCOPES = ["https://www.googleapis.com/auth/drive.readonly",
          "https://www.googleapis.com/auth/drive.file"]

@dataclass
class DriveFile:
    id: str
    name: str
    modified_time: str

class GoogleDriveClient:
    def __init__(self, credentials_path: str):
        creds = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)

    def find_file_in_folder(self, folder_id: str, filename: str) -> Optional[DriveFile]:
        q = (
            f"'{folder_id}' in parents and trashed=false and name='{filename}'"
        )
        res = self.service.files().list(
            q=q,
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=5,
        ).execute()
        files = res.get("files", [])
        if not files:
            return None
        f = files[0]
        return DriveFile(id=f["id"], name=f["name"], modified_time=f["modifiedTime"])

    def find_latest_xlsx_in_folder(self, folder_id: str) -> Optional[DriveFile]:
        q = (
            f"'{folder_id}' in parents and trashed=false and "
            "(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')"
        )
        res = self.service.files().list(
            q=q,
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=1,
        ).execute()
        files = res.get("files", [])
        if not files:
            return None
        f = files[0]
        return DriveFile(id=f["id"], name=f["name"], modified_time=f["modifiedTime"])

    def download_file_bytes(self, file_id: str) -> bytes:
        request = self.service.files().get_media(fileId=file_id)
        buf = BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    def _resumable_upload_bytes(
        self,
        folder_id: str,
        filename: str,
        content: bytes,
        mimetype: str,
        max_attempts: int = 6,
        base_sleep_sec: float = 2.0,
        max_sleep_sec: float = 90.0,
    ) -> str:
        """Upload bytes to Google Drive with a resilient resumable uploader.

        Why: occasional transient network/TLS glitches (e.g., ssl.SSLError) can happen during
        MediaIoBaseUpload/next_chunk. We retry by re-creating the upload request each attempt.

        Notes:
        - This does not guarantee success if the account/permissions are wrong.
        - This is designed to *not lose the computed result* just because upload failed.
        """

        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                # Re-create media each attempt because the underlying stream may be consumed.
                media = MediaIoBaseUpload(
                    BytesIO(content),
                    mimetype=mimetype,
                    resumable=True,
                )

                request = None
                # NOTE: Overwrite-by-name is handled by upsert_* helpers; this uploader only creates new files.
                if request is None:
                    file_metadata = {"name": filename, "parents": [folder_id]}
                    request = self.service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields="id",
                    )

                # For resumable uploads, use next_chunk loop.
                response = None
                while response is None:
                    status, response = request.next_chunk()

                return response["id"]

            except (ssl.SSLError, OSError, httplib2.HttpLib2Error, HttpError) as e:
                last_exc = e

                # Retry only if it looks transient.
                # - 5xx and 429 are usually transient.
                # - 4xx other than 429 are typically permanent (permissions, bad request, etc.).
                if isinstance(e, HttpError):
                    status = getattr(e.resp, "status", None)
                    if status is not None and int(status) not in (429, 500, 502, 503, 504):
                        raise

                if attempt >= max_attempts:
                    break

                # Exponential backoff with jitter
                sleep_for = min(max_sleep_sec, base_sleep_sec * (2 ** (attempt - 1)))
                sleep_for = sleep_for * random.uniform(0.8, 1.3)
                time.sleep(sleep_for)

        assert last_exc is not None
        raise last_exc

    def _resumable_update_bytes(
        self,
        file_id: str,
        content: bytes,
        mimetype: str,
        max_attempts: int = 6,
        base_sleep_sec: float = 2.0,
        max_sleep_sec: float = 90.0,
    ) -> str:
        """Update (overwrite) an existing Drive file by file_id using resumable upload + retries."""

        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                media = MediaIoBaseUpload(
                    BytesIO(content),
                    mimetype=mimetype,
                    resumable=True,
                )

                request = self.service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields="id",
                )

                response = None
                while response is None:
                    status, response = request.next_chunk()

                return response["id"]

            except (ssl.SSLError, OSError, httplib2.HttpLib2Error, HttpError) as e:
                last_exc = e

                if isinstance(e, HttpError):
                    status = getattr(e.resp, "status", None)
                    if status is not None and int(status) not in (429, 500, 502, 503, 504):
                        raise

                if attempt >= max_attempts:
                    break

                sleep_for = min(max_sleep_sec, base_sleep_sec * (2 ** (attempt - 1)))
                sleep_for = sleep_for * random.uniform(0.8, 1.3)
                time.sleep(sleep_for)

        assert last_exc is not None
        raise last_exc

    def upsert_json_bytes(self, folder_id: str, filename: str, content: bytes) -> str:
        """Create JSON if not exists; otherwise overwrite the existing file (no duplicates)."""
        existing = self.find_file_in_folder(folder_id, filename)
        if existing:
            return self._resumable_update_bytes(
                file_id=existing.id,
                content=content,
                mimetype="application/json",
            )
        return self._resumable_upload_bytes(
            folder_id=folder_id,
            filename=filename,
            content=content,
            mimetype="application/json",
        )

    def upload_xlsx_bytes(self, folder_id: str, filename: str, content: bytes) -> str:
        return self._resumable_upload_bytes(
            folder_id=folder_id,
            filename=filename,
            content=content,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def upload_json_bytes(self, folder_id: str, filename: str, content: bytes) -> str:
        """Upload JSON content to Google Drive.

        IMPORTANT: This method UPSERTS by name within the folder:
        - if a file with the same name exists in the folder -> overwrite it (update by file_id)
        - else -> create a new file

        This prevents Drive duplicates like "competitors_delivery_total.json (1)".
        """
        return self.upsert_json_bytes(folder_id=folder_id, filename=filename, content=content)