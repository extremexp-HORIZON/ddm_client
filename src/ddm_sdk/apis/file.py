from __future__ import annotations

import json
from typing import Any, Dict, Optional, Union, BinaryIO

from ..transport.http import HttpTransport
from ..transport.multipart import read_bytes, guess_filename, iter_chunks

from ..models.file import (
    UploadSingleResponse,
    UpdateFileResponse,
    TaskChainUploadLinkResponse,
    AsyncChunkResponse,
    FileUpdateBody,
    UploadLinkBody,
)

# NOTE:
# DDM Backend expects multipart fields:
# - /ddm/file/upload: query param project_id REQUIRED + formData file REQUIRED
#   optional query: user_filename, description, use_case (multi)
#   optional formData: metadata-file



class FileAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def upload(
        self,
        *,
        project_id: str,
        file: Union[str, bytes, BinaryIO],
        user_filename: Optional[str] = None,
        description: Optional[str] = None,
        use_case: Optional[list[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        metadata_file: Optional[Union[str, bytes, BinaryIO]] = None,
        metadata_filename: str = "metadata.json",
    ):
        file_bytes = read_bytes(file)
        up_name = guess_filename(file, fallback="upload.bin")

        files: Dict[str, Any] = {"file": (up_name, file_bytes)}

        if metadata_file is not None:
            meta_bytes = read_bytes(metadata_file)
            files["metadata-file"] = (metadata_filename, meta_bytes, "application/json")
        elif metadata is not None:
            meta_bytes = json.dumps(metadata).encode("utf-8")
            files["metadata-file"] = (metadata_filename, meta_bytes, "application/json")

        form: list[tuple[str, str]] = [("project_id", project_id)]
        if user_filename is not None:
            form.append(("user_filename", user_filename))
        if description is not None:
            form.append(("description", description))
        if use_case:
            for uc in use_case:
                form.append(("use_case", uc))

        data = self._http.request(
            "POST",
            "/ddm/file/upload",
            data=form,
            files=files,
        )
        return UploadSingleResponse.model_validate(data)



    # -------- async chunk upload --------

    def upload_async_chunk(
        self,
        *,
        project_id: str,
        file_bytes: bytes,
        filename: str,
        chunk_index: int,
        total_chunks: int,
        file_id: Optional[str] = None,
    ) -> AsyncChunkResponse:
        files = {"file": (filename, file_bytes)}
        data_form: Dict[str, Any] = {
            "chunk_index": str(chunk_index),
            "total_chunks": str(total_chunks),
            "filename": filename,
            "project_id": project_id,
        }
        if file_id:
            data_form["file_id"] = file_id

        resp = self._http.request(
            "POST",
            "/ddm/file/upload/async",
            files=files,
            data=data_form,
        )
        return AsyncChunkResponse.model_validate(resp)

    def upload_async(
        self,
        *,
        project_id: str,
        file: Union[str, bytes, BinaryIO],
        filename: Optional[str] = None,
        chunk_size: int = 2 * 1024 * 1024,
    ) -> AsyncChunkResponse:
        """
        Convenience method that splits a file and calls /upload/async repeatedly.

        Returns the final 202 response on completion (contains merge_task_id, metadata_task_id).
        """
        raw = read_bytes(file)
        fname = filename or guess_filename(file, fallback="upload.bin")

        total_chunks = (len(raw) + chunk_size - 1) // chunk_size
        current_file_id: Optional[str] = None
        last_resp: Optional[AsyncChunkResponse] = None

        for idx, chunk in iter_chunks(raw, chunk_size):
            resp = self.upload_async_chunk(
                project_id=project_id,
                file_bytes=chunk,
                filename=fname,
                chunk_index=idx,
                total_chunks=total_chunks,
                file_id=current_file_id,
            )
            last_resp = resp

            if resp.file_id:
                current_file_id = resp.file_id

        if last_resp is None:
            raise ValueError("Empty upload; no chunks were sent")
        return last_resp

    # -------- upload from link --------

    def upload_link(self, body: Union[UploadLinkBody, Dict[str, Any]]) -> TaskChainUploadLinkResponse:
        payload = body.model_dump() if isinstance(body, UploadLinkBody) else dict(body)
        data = self._http.request("POST", "/ddm/file/upload-link", json=payload)
        return TaskChainUploadLinkResponse.model_validate(data)

    # -------- update file --------

    def update(self, file_id: str, body: Union[FileUpdateBody, Dict[str, Any]]) -> UpdateFileResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, FileUpdateBody) else dict(body)
        data = self._http.request("PATCH", f"/ddm/file/update/{file_id}", json=payload)
        return UpdateFileResponse.model_validate(data)

    # -------- download file --------

    def download(self, file_id: str) -> bytes:
        """
        Returns raw bytes of the file (backend uses send_file).
        """
        data = self._http.request("GET", f"/ddm/file/{file_id}")
        # HttpTransport returns r.content when not JSON
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        raise TypeError("Expected bytes from download endpoint")

    # -------- delete file --------

    def delete(self, file_id: str) -> Dict[str, Any]:
        return self._http.request("DELETE", f"/ddm/file/{file_id}/delete")
