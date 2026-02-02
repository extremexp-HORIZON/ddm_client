from __future__ import annotations

from typing import Any, Dict, Optional, Union

from ..transport.http import HttpTransport
from ..models.user import (
    DeletePreferredQueryResponse,
    GetUserProfileResponse,
    MarkAllNotificationsReadResponse,
    MarkNotificationReadResponse,
    PreferredQueryCreateRequest,
    PreferredQueryListResponse,
    SavePreferredQueryResponse,
    UpdateUserProfileResponse,
    UserNotificationListResponse,
)


class UserAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    # ---------- Profile ----------
    def get_profile(self, username: str) -> GetUserProfileResponse:
        resp = self._http.request("GET", f"/ddm/users/user/profile/{username}")
        return GetUserProfileResponse.model_validate(resp)

    def update_profile(
        self,
        username: str,
        *,
        public_key: Optional[str] = None,
        profile_pic_path: Optional[str] = None,
    ) -> UpdateUserProfileResponse:
        """
        POST /ddm/users/user/profile/<username>

        Server expects multipart/form-data:
        - form field: public_key (optional)
        - file field: profile_pic (optional)
        """
        data: Dict[str, Any] = {}
        if public_key is not None:
            data["public_key"] = public_key

        files = None
        if profile_pic_path:
            # HttpTransport should accept requests-style files={...}
            files = {"profile_pic": open(profile_pic_path, "rb")}

        try:
            resp = self._http.request(
                "POST",
                f"/ddm/users/user/profile/{username}",
                data=data if data else None,
                files=files,
            )
            return UpdateUserProfileResponse.model_validate(resp)
        finally:
            if files:
                try:
                    files["profile_pic"].close()
                except Exception:
                    pass

    def get_profile_picture_bytes(self, filename: str) -> bytes:
        """
        GET /ddm/users/user/profile_pic/<filename>
        Returns raw bytes (png/jpg/etc).
        """
        data = self._http.request("GET", f"/ddm/users/user/profile_pic/{filename}")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        # if your transport ever returns text for images (unlikely), convert
        if isinstance(data, str):
            return data.encode("utf-8")
        raise TypeError(f"Expected bytes from profile_pic endpoint, got {type(data)}")


    # ---------- Notifications ----------
    def list_notifications(
        self,
        *,
        onlyUnread: bool = False,
        limit: int = 50,
    ) -> UserNotificationListResponse:
        params = {
            "onlyUnread": "true" if onlyUnread else "false",
            "limit": limit,
        }
        resp = self._http.request("GET", "/ddm/users/user/notifications", params=params)
        return UserNotificationListResponse.model_validate(resp)

    def mark_notification_read(self, notification_id: int) -> MarkNotificationReadResponse:
        resp = self._http.request(
            "POST",
            f"/ddm/users/user/notifications/{notification_id}/read",
        )
        return MarkNotificationReadResponse.model_validate(resp)

    def mark_all_notifications_read(self) -> MarkAllNotificationsReadResponse:
        resp = self._http.request("POST", "/ddm/users/user/notifications/mark_all_read")
        return MarkAllNotificationsReadResponse.model_validate(resp)

    # ---------- Preferred queries ----------
    def list_preferred_queries(self, *, limit: int = 50) -> PreferredQueryListResponse:
        resp = self._http.request("GET", "/ddm/users/user/queries", params={"limit": limit})
        return PreferredQueryListResponse.model_validate(resp)

    def save_preferred_query(
        self,
        body: Union[PreferredQueryCreateRequest, Dict[str, Any]],
    ) -> SavePreferredQueryResponse:
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else dict(body)
        resp = self._http.request("POST", "/ddm/users/user/queries", json=payload)
        return SavePreferredQueryResponse.model_validate(resp)

    def delete_preferred_query(self, query_id: int) -> DeletePreferredQueryResponse:
        resp = self._http.request("POST", f"/ddm/users/user/queries/{query_id}/delete")
        return DeletePreferredQueryResponse.model_validate(resp)
