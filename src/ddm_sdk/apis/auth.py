from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..transport.http import HttpTransport


@dataclass(frozen=True)
class LoginResponse:
    access_token: str
    token_type: str = "Bearer"
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    refresh_expires_in: Optional[int] = None
    id_token: Optional[str] = None
    scope: Optional[str] = None
    session_state: Optional[str] = None

    @classmethod
    def from_json(cls, obj: Dict[str, Any]) -> "LoginResponse":
        return cls(
            access_token=obj["access_token"],
            token_type=obj.get("token_type", "Bearer"),
            expires_in=obj.get("expires_in"),
            refresh_token=obj.get("refresh_token"),
            refresh_expires_in=obj.get("refresh_expires_in"),
            id_token=obj.get("id_token"),
            scope=obj.get("scope"),
            session_state=obj.get("session_state"),
        )


@dataclass(frozen=True)
class UserInfo:
    sub: str
    preferred_username: Optional[str] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    name: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    locale: Optional[str] = None
    groups: Optional[List[str]] = None
    realm_access: Optional[Dict[str, Any]] = None

    @classmethod
    def from_json(cls, obj: Dict[str, Any]) -> "UserInfo":
        return cls(
            sub=obj["sub"],
            preferred_username=obj.get("preferred_username"),
            email=obj.get("email"),
            email_verified=obj.get("email_verified"),
            name=obj.get("name"),
            given_name=obj.get("given_name"),
            family_name=obj.get("family_name"),
            locale=obj.get("locale"),
            groups=obj.get("groups"),
            realm_access=obj.get("realm_access"),
        )


class AuthAPI:
    def __init__(self, auth_http: HttpTransport):
        self._http = auth_http

    def login(self, username: str, password: str) -> LoginResponse:
        payload = {"username": username, "password": password}
        data = self._http.request(
            "POST",
            "/extreme_auth/api/v1/person/login",
            json=payload,
            auth=False,  # no Authorization header
        )
        return LoginResponse.from_json(data)

    def userinfo(self, access_token: Optional[str] = None) -> UserInfo:
        headers = None
        if access_token:
            headers = {"Authorization": f"Bearer {access_token}"}

        data = self._http.request(
            "GET",
            "/extreme_auth/api/v1/person/userinfo",
            headers=headers,
            auth=not bool(headers),
        )
        return UserInfo.from_json(data)
