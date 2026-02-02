from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator, AliasChoices
import json 

class FileItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    filename: str
    upload_filename: Optional[str] = None
    description: Optional[str] = None
    use_case: List[Any] = Field(default_factory=list)
    path: str
    user_id: str
    created: Optional[str] = None
    parent_files: Optional[Dict[str, Any]] = None
    project_id: Optional[str] = None
    file_size: Optional[int] = None
    file_type: Optional[str] = None
    recdeleted: Optional[bool] = None
    file_metadata: Optional[Dict[str, Any]] = None


class UploadSingleResponseFile(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    filename: str
    upload_filename: Optional[str] = None

    # backend returns "path" but you want "zenoh_file_path"
    zenoh_file_path: str = Field(validation_alias=AliasChoices("file_path", "path"))
    zenoh_metadata_path: Optional[str] = None
    project_id: Optional[str] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)

    file_hash: Optional[str] = None
    metadata_task_id: Optional[str] = None

    @field_validator("metadata", mode="before")
    @classmethod
    def _parse_metadata(cls, v: Any) -> Any:
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return {}
            try:
                parsed = json.loads(s)
                return parsed if isinstance(parsed, dict) else {"value": parsed}
            except Exception:
                # if backend returns non-json string, keep it but wrap
                return {"raw": v}
        # last resort
        return {"value": v}


class UploadSingleResponse(BaseModel):
    message: str
    file: UploadSingleResponseFile


class UpdateFileResponse(BaseModel):
    message: str
    updated_data: FileItem

class TaskChainUploadLinkResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: str
    file_id: str
    zenoh_file_path: str
    fetch_task_id: Optional[str] = None
    process_task_id: Optional[str] = None
    file_url: Optional[str] = None


# ----- request bodies -----

class FileUpdateBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: Optional[str] = None
    project_id: Optional[str] = None
    file_size: Optional[int] = None
    file_hash: Optional[str] = None
    uploader_metadata: Optional[Dict[str, Any]] = None
    filename: Optional[str] = None
    description: Optional[str] = None
    use_case: Optional[List[str]] = None


class UploadLinkBody(BaseModel):
    model_config = ConfigDict(extra="allow")

    file_url: str
    project_id: str
    description: Optional[str] = ""
    use_cases: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AsyncChunkResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: str
    file_id: Optional[str] = None
    zenoh_file_path: Optional[str] = None
    merge_task_id: Optional[str] = None
    metadata_task_id: Optional[str] = None
    project_id: Optional[str] = None
