from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class ApiError(Exception):
    status_code: int
    message: str
    response_text: Optional[str] = None

    def __post_init__(self):
        super().__init__(self.__str__())

    def __str__(self) -> str:
        if self.response_text:
            return f"{self.status_code}: {self.message} | {self.response_text}"
        return f"{self.status_code}: {self.message}"


class BadRequest(ApiError):
    """400"""


class Unauthorized(ApiError):
    """401"""


class Forbidden(ApiError):
    """403"""


class NotFound(ApiError):
    """404"""


class ServerError(ApiError):
    """5xx"""
