from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class UserProfile(BaseModel):
    model_config = ConfigDict(extra="allow")

    sub: str
    username: str
    email: Optional[str] = None
    public_key: Optional[str] = None
    profile_pic: Optional[str] = None
    created: Optional[str] = None


class GetUserProfileResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    user: UserProfile


class UpdateUserProfileResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    user: Optional[UserProfile] = None


# ---------- Notifications ----------

class UserNotification(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    user_sub: str
    kind: str

    network: Optional[str] = None
    contract_address: Optional[str] = None
    suite_id: Optional[int] = None
    dataset_fingerprint: Optional[str] = None
    tx_hash: Optional[str] = None
    event_id: Optional[int] = None

    payload: Optional[Dict[str, Any]] = None
    is_read: bool = False
    created_at: Optional[str] = None
    read_at: Optional[str] = None


class UserNotificationListResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: List[UserNotification] = Field(default_factory=list)
    total: int = 0
    unread: int = 0


class MarkNotificationReadResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    notification: Optional[UserNotification] = None


class MarkAllNotificationsReadResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None


# ---------- Preferred queries ----------

class PreferredQuery(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int
    user_sub: str
    name: Optional[str] = None
    query: Dict[str, Any]
    created_at: Optional[str] = None


class PreferredQueryListResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: List[PreferredQuery] = Field(default_factory=list)
    total: int = 0


class PreferredQueryCreateRequest(BaseModel):
    name: Optional[str] = None
    query: Dict[str, Any]


class SavePreferredQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    query: Optional[PreferredQuery] = None


class DeletePreferredQueryResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    id: Optional[int] = None
