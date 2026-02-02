from __future__ import annotations

from typing import Any, Dict, Sequence, Union

from ..transport.http import HttpTransport
from ..models.file_metadata import FileIdsRequest, FileMetadataMapResponse


class FileMetadataAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    # GET /ddm/file_metadata/{file_id}
    def get(self, file_id: str) -> Dict[str, Any]:
        """
        Returns the raw metadata JSON for that file (whatever file.file_metadata is).
        """
        data = self._http.request("GET", f"/ddm/file_metadata/{file_id}")
        if isinstance(data, dict):
            return data
        raise TypeError("Expected JSON object from /ddm/file_metadata/{file_id}")

    # POST /ddm/file_metadata/
    def get_many(self, file_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        ids = [str(x) for x in file_ids if str(x).strip()]
        if not ids:
            return {}

        payload = {"file_ids": ids}

        try:
            data = self._http.request("POST", "/ddm/file_metadata/", json=payload)

            # expected: { "<id>": {...}, "<id2>": {...} }
            if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
                return data  # mapping ok

            # sometimes wrapped: {"metadata": {...}}
            if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
                return data["metadata"]

        except Exception:
            pass

        # fallback
        out: Dict[str, Dict[str, Any]] = {}
        for fid in ids:
            out[fid] = self.get(fid)
        return out

    # GET /ddm/file_metadata/report/{file_id}  (HTML)
    def get_report_html(self, file_id: str) -> str:
        """
        Backend returns Response(..., mimetype='text/html').
        Your HttpTransport must support returning text for text/html responses.
        """
        data = self._http.request("GET", f"/ddm/file_metadata/report/{file_id}")
        if isinstance(data, str):
            return data
        # Some transports return bytes for non-json; accept that too.
        if isinstance(data, (bytes, bytearray)):
            return bytes(data).decode("utf-8", errors="replace")
        raise TypeError("Expected HTML from /ddm/file_metadata/report/{file_id}")

    # POST /ddm/file_metadata/reports  (ZIP)
    def download_reports_zip(self, file_ids: Union[FileIdsRequest, Sequence[str], Dict[str, Any]]) -> bytes:
        """
        Returns ZIP bytes containing *_profile_report.html files.
        """
        if isinstance(file_ids, FileIdsRequest):
            payload = file_ids.model_dump()
        elif isinstance(file_ids, dict):
            payload = file_ids
        else:
            payload = {"file_ids": list(file_ids)}

        resp = self._http.request("POST", "/ddm/file_metadata/reports", json=payload)
        if isinstance(resp, (bytes, bytearray)):
            return bytes(resp)
        raise TypeError("Expected ZIP bytes from /ddm/file_metadata/reports")
