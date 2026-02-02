from __future__ import annotations

from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


class UploaderMetadataJSON(BaseModel):
    """
    Matches your Swagger model:
      { "uploader_metadata": <any json> }
    """
    model_config = ConfigDict(extra="allow")

    uploader_metadata: Optional[Dict[str, Any]] = Field(
        default=None, description="Uploader metadata (JSON)"
    )


class UploaderMetadataResponse(BaseModel):
    """
    Your handlers return different shapes:
      - GET:  { "uploader_metadata": {...} }
      - PUT:  { "message": "...", "file_id": "..." }
      - POST: { "message": "...", "file_id": "..." } (your code returns 201)
      - DELETE:{ "message": "..." }
    So keep it flexible.
    """
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    file_id: Optional[str] = None
    uploader_metadata: Optional[Dict[str, Any]] = None
