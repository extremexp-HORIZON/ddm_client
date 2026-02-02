from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union, BinaryIO

from ..transport.http import HttpTransport
from ..transport.multipart import read_bytes, guess_filename
import json
from ..models.files import (
    BulkUploadResponse,
    UploadFileUrlsRequest,
    UploadFileUrlsResponse,
    FilesUpdateRequest,
    FilesUpdateResponse,
    DeleteFileIds,
    DownloadFileIds,
    ProjectDownloadRequest,
)


class FilesAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def upload(
        self,
        *,
        project_id: str,
        files: Sequence[Union[str, bytes, BinaryIO]],
        user_filenames: Optional[Sequence[str]] = None,
        descriptions: Optional[Sequence[str]] = None,
        use_case: Optional[Sequence[Union[str, List[str]]]] = None,
        metadata_files: Optional[Sequence[Union[str, bytes, BinaryIO]]] = None,
        metadata_filenames: Optional[Sequence[str]] = None,
    ) -> BulkUploadResponse:
        """
        POST /ddm/files/upload (multipart)

        IMPORTANT: backend expects these as multipart form fields (repeated):
          project_id, user_filenames, descriptions, use_case
        plus repeated files=...
        """

        multipart: List[Any] = []

        # ✅ project_id as form field
        multipart.append(("project_id", (None, project_id)))

        # repeat files
        for idx, f in enumerate(files):
            content = read_bytes(f)
            fname = guess_filename(f, fallback=f"file_{idx}")
            multipart.append(("files", (fname, content)))

            # per-file user_filenames/descriptions/use_case as form fields (repeat)
            if user_filenames and idx < len(user_filenames) and user_filenames[idx]:
                multipart.append(("user_filenames", (None, str(user_filenames[idx]))))

            if descriptions and idx < len(descriptions) and descriptions[idx] is not None:
                multipart.append(("descriptions", (None, str(descriptions[idx]))))

            if use_case:
                for uc in use_case:
                    if isinstance(uc, list):
                        uc_json = json.dumps(uc)
                    else:
                        s = str(uc).strip()
                        uc_json = s if s.startswith("[") else json.dumps([s])
                    multipart.append(("use_case", (None, uc_json)))

        #  optional metadata-files
        if metadata_files:
            for j, mf in enumerate(metadata_files):
                mcontent = read_bytes(mf)
                mname = (
                    metadata_filenames[j]
                    if (metadata_filenames and j < len(metadata_filenames))
                    else guess_filename(mf, fallback=f"metadata_{j}.json")
                )
                multipart.append(("metadata-files", (mname, mcontent, "application/json")))

        data = self._http.request(
            "POST",
            "/ddm/files/upload",
            params=None,
            data=None,
            files=multipart,
        )
        return BulkUploadResponse.model_validate(data)


    # ----------------------------
    # /ddm/files/upload-links (json)
    # ----------------------------
    def upload_links(
        self,
        body: Union[UploadFileUrlsRequest, Dict[str, Any]],
    ) -> UploadFileUrlsResponse:
        payload = body.model_dump() if isinstance(body, UploadFileUrlsRequest) else dict(body)
        data = self._http.request("POST", "/ddm/files/upload-links", json=payload)
        return UploadFileUrlsResponse.model_validate(data)

    # ----------------------------
    # /ddm/files/update (bulk patch)
    # ----------------------------
    def update(
        self,
        body: Union[FilesUpdateRequest, Dict[str, Any]],
    ) -> FilesUpdateResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, FilesUpdateRequest) else dict(body)
        data = self._http.request("PATCH", "/ddm/files/update", json=payload)
        return FilesUpdateResponse.model_validate(data)

    # ----------------------------
    # /ddm/files/delete (bulk delete)
    # ----------------------------
    def delete(self, file_ids: Union[DeleteFileIds, Sequence[str], Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(file_ids, DeleteFileIds):
            payload = file_ids.model_dump()
        elif isinstance(file_ids, dict):
            payload = file_ids
        else:
            payload = {"file_ids": list(file_ids)}

        return self._http.request("DELETE", "/ddm/files/delete", json=payload)

    # ----------------------------
    # /ddm/files/download (zip bytes)
    # ----------------------------
    def download_zip(self, file_ids: Union[DownloadFileIds, Sequence[str], Dict[str, Any]]) -> bytes:
        """
        Returns ZIP bytes. Backend uses send_file(..., mimetype="application/zip").
        """
        if isinstance(file_ids, DownloadFileIds):
            payload = file_ids.model_dump()
        elif isinstance(file_ids, dict):
            payload = file_ids
        else:
            payload = {"file_ids": list(file_ids)}

        resp = self._http.request("POST", "/ddm/files/download", json=payload)
        if isinstance(resp, (bytes, bytearray)):
            return bytes(resp)
        raise TypeError("Expected ZIP bytes from /ddm/files/download")

    # ----------------------------
    # /ddm/files/download/project (zip bytes)
    # ----------------------------
    def download_project_zip(self, project_id: Union[ProjectDownloadRequest, str, Dict[str, Any]]) -> bytes:
        if isinstance(project_id, ProjectDownloadRequest):
            payload = project_id.model_dump()
        elif isinstance(project_id, dict):
            payload = project_id
        else:
            payload = {"project_id": project_id}

        resp = self._http.request("POST", "/ddm/files/download/project", json=payload)
        if isinstance(resp, (bytes, bytearray)):
            return bytes(resp)
        raise TypeError("Expected ZIP bytes from /ddm/files/download/project")
