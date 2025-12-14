from __future__ import annotations
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
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

    def upload_xlsx_bytes(self, folder_id: str, filename: str, content: bytes) -> str:
        media = MediaIoBaseUpload(BytesIO(content),
                                  mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        created = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        return created["id"]

    def upload_json_bytes(self, folder_id: str, filename: str, content: bytes) -> str:
        """
        Upload JSON content to Google Drive.
        """
        media = MediaIoBaseUpload(
            BytesIO(content),
            mimetype="application/json",
            resumable=True
        )
        file_metadata = {
            "name": filename,
            "parents": [folder_id]
        }
        created = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        return created["id"]