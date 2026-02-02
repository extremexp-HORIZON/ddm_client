from __future__ import annotations

from typing import Any, Dict, Optional
import requests

from .errors import ApiError, BadRequest, Unauthorized, Forbidden, NotFound, ServerError


class HttpTransport:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 240):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.session = requests.Session()

    def set_token(self, token: Optional[str]) -> None:
        self.token = token

    def _headers(
        self,
        extra: Optional[Dict[str, str]] = None,
        *,
        auth: bool = True,
    ) -> Dict[str, str]:
        h: Dict[str, str] = {"Accept": "application/json"}
        if auth and self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if extra:
            h.update(extra)
        return h

    def _normalize_path(self, path: str) -> str:
        return path if path.startswith("/") else f"/{path}"

    def _pick_exc(self, status_code: int):
        if status_code == 400:
            return BadRequest
        if status_code == 401:
            return Unauthorized
        if status_code == 403:
            return Forbidden
        if status_code == 404:
            return NotFound
        if status_code >= 500:
            return ServerError
        return ApiError

    def _extract_error_message(self, r: requests.Response) -> str:
        try:
            ct = r.headers.get("Content-Type", "")
            if "application/json" in ct and r.content:
                payload = r.json()
                if isinstance(payload, dict):
                    return (
                        payload.get("message")
                        or payload.get("error")
                        or payload.get("detail")
                        or str(payload)
                    )
        except Exception:
            pass
        return (r.text or "").strip()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Any = None,
        headers: Optional[Dict[str, str]] = None,
        files: Any = None,
        data: Any = None,
        auth: bool = True,
    ) -> Any:
        path = self._normalize_path(path)
        url = f"{self.base_url}{path}"

        try:
            r = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=self._headers(headers, auth=auth),
                files=files,
                data=data,
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            raise ApiError(status_code=0, message=str(e), response_text=None) from e

        if 200 <= r.status_code < 300:
            if not r.content:
                return None
            ct = r.headers.get("Content-Type", "")
            if "application/json" in ct:
                return r.json()
            return r.content

        exc = self._pick_exc(r.status_code)
        server_msg = self._extract_error_message(r)

        raise exc(
            status_code=r.status_code,
            message=f"{method} {path} failed" + (f": {server_msg}" if server_msg else ""),
            response_text=r.text,
        )
