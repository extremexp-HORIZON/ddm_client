from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from .file import FileItem


# -------- requests --------

class DeleteFileIds(BaseModel):
    file_ids: List[str]


class DownloadFileIds(BaseModel):
    file_ids: List[str]


class ProjectDownloadRequest(BaseModel):
    project_id: str


class FileUpdateItem(BaseModel):
    """
    Bulk update item. DDM backend expects each dict to have at least 'id'
    and any subset of updatable fields.
    """
    model_config = ConfigDict(extra="allow")

    id: str
    filename: Optional[str] = None
    description: Optional[str] = None
    project_id: Optional[str] = None
    use_case: Optional[List[str]] = None
    path: Optional[str] = None
    file_size: Optional[int] = None
    file_hash: Optional[str] = None
    uploader_metadata: Optional[Dict[str, Any]] = None


class FilesUpdateRequest(BaseModel):
    files: List[FileUpdateItem]


class UploadFileUrlRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    file_url: str
    filename: Optional[str] = None
    description: Optional[str] = ""
    use_cases: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class UploadFileUrlsRequest(BaseModel):
    project_id: str
    files: List[UploadFileUrlRequest]


# -------- responses --------

class FilesUpdateResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: str
    updated_files: List[Dict[str, Any]] = Field(default_factory=list)
    errors: Optional[Any] = None


class BulkUploadResponse(BaseModel):
    """
    /ddm/files/upload returns:
      { "message": "...", "files": [ {file-json + metadata_task_id}, ... ] }
    """
    model_config = ConfigDict(extra="allow")

    message: str
    files: List[Dict[str, Any]]


class UploadFileUrlResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: str
    file_id: str
    zenoh_file_path: str
    fetch_task_id: Optional[str] = None
    process_task_id: Optional[str] = None
    file_url: Optional[str] = None


class UploadFileUrlsResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: str
    files: List[UploadFileUrlResponse]
