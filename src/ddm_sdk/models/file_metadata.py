from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class FileIdsRequest(BaseModel):
    file_ids: List[str]


class FileMetadataMapResponse(BaseModel):
    """
    POST /ddm/file_metadata/ returns:
      { "metadata": { "<file_id>": <file.file_metadata>, ... } }
    """
    model_config = ConfigDict(extra="allow")

    metadata: Dict[str, Any] = Field(default_factory=dict)


class FileMetadataAnyResponse(BaseModel):
    """
    GET /ddm/file_metadata/{file_id} returns raw JSON (file.file_metadata),
    not wrapped. We allow any dict.
    """
    model_config = ConfigDict(extra="allow")
