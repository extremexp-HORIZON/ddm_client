from __future__ import annotations

from typing import Any, Dict, Optional, Union

from ..transport.http import HttpTransport
from ..models.uploader_metadata import UploaderMetadataJSON, UploaderMetadataResponse


class UploaderMetadataAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    # POST /ddm/uploader_metadata/{file_id}
    def attach(
        self,
        file_id: str,
        body: Union[UploaderMetadataJSON, Dict[str, Any]],
    ) -> UploaderMetadataResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, UploaderMetadataJSON) else dict(body)
        data = self._http.request("POST", f"/ddm/uploader_metadata/{file_id}", json=payload)
        return UploaderMetadataResponse.model_validate(data)

    # PUT /ddm/uploader_metadata/{file_id}
    def update(
        self,
        file_id: str,
        body: Union[UploaderMetadataJSON, Dict[str, Any]],
    ) -> UploaderMetadataResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, UploaderMetadataJSON) else dict(body)
        data = self._http.request("PUT", f"/ddm/uploader_metadata/{file_id}", json=payload)
        return UploaderMetadataResponse.model_validate(data)

    # GET /ddm/uploader_metadata/{file_id}
    def get(self, file_id: str) -> UploaderMetadataResponse:
        data = self._http.request("GET", f"/ddm/uploader_metadata/{file_id}")
        return UploaderMetadataResponse.model_validate(data)

    # DELETE /ddm/uploader_metadata/{file_id}
    def delete(self, file_id: str) -> UploaderMetadataResponse:
        data = self._http.request("DELETE", f"/ddm/uploader_metadata/{file_id}")
        return UploaderMetadataResponse.model_validate(data)
