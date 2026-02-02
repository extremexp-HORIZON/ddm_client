from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .transport.http import HttpTransport
from .config import get_settings
from .storage.base import Storage
from .storage.factory import make_storage


from .apis.auth import AuthAPI, LoginResponse, UserInfo
from .apis.blockchain import BlockchainAPI
from .apis.catalog import CatalogAPI
from .apis.file import FileAPI
from .apis.files import FilesAPI
from .apis.file_metadata import FileMetadataAPI
from .apis.uploader_metadata import UploaderMetadataAPI
from .apis.expectations import ExpectationsAPI
from .apis.validations import ValidationsAPI
from .apis.parametrics import ParametricsAPI
from .apis.user import UserAPI
from .apis.tasks import TasksAPI



@dataclass
class DdmClient:
    base_url: str
    auth_url: Optional[str] = None
    token: Optional[str] = None
    timeout: int = 30

    # NEW: optional storage (won't break existing code)
    storage: Optional[Storage] = None

    _http: HttpTransport = field(init=False, repr=False)
    _auth_http: Optional[HttpTransport] = field(init=False, default=None, repr=False)

    # exposed APIs
    auth: Optional[AuthAPI] = field(init=False, default=None)
    blockchain: BlockchainAPI = field(init=False)
    catalog: CatalogAPI = field(init=False)
    file: FileAPI = field(init=False)
    files: FilesAPI = field(init=False)
    file_metadata: FileMetadataAPI = field(init=False)
    uploader_metadata: UploaderMetadataAPI = field(init=False)
    expectations: ExpectationsAPI = field(init=False)
    validations: ValidationsAPI = field(init=False)
    parametrics: ParametricsAPI = field(init=False)
    user: UserAPI = field(init=False)

    def __post_init__(self) -> None:
        self._http = HttpTransport(self.base_url, token=self.token, timeout=self.timeout)

        if self.auth_url:
            self._auth_http = HttpTransport(self.auth_url, token=None, timeout=self.timeout)
            self.auth = AuthAPI(self._auth_http)

        self.blockchain = BlockchainAPI(self._http)
        self.catalog = CatalogAPI(self._http)
        self.file = FileAPI(self._http)
        self.files = FilesAPI(self._http)
        self.file_metadata = FileMetadataAPI(self._http)
        self.uploader_metadata = UploaderMetadataAPI(self._http)
        self.expectations = ExpectationsAPI(self._http)
        self.validations = ValidationsAPI(self._http)
        self.parametrics = ParametricsAPI(self._http)
        self.user = UserAPI(self._http)
        self.tasks = TasksAPI(self._http)

    def set_token(self, token: str) -> None:
        self.token = token
        self._http.set_token(token)

    def load_token_from_storage(self) -> bool:
        """
        Optional convenience: load saved token if storage is configured.
        Does nothing if token already set.
        """
        if self.token or not self.storage:
            return False
        d = self.storage.read_json("auth/token") or {}
        tok = d.get("access_token")
        if isinstance(tok, str) and tok.strip():
            self.set_token(tok.strip())
            return True
        return False

    def login(self, username: str, password: str) -> LoginResponse:
        if not self.auth:
            raise RuntimeError("Auth is not configured. Provide auth_url or set DDM_AUTH_URL.")
        resp = self.auth.login(username, password)
        self.set_token(resp.access_token)
        if self.storage:
            self.storage.write_json("auth/token", {"access_token": resp.access_token, "username": username})
        return resp

    def whoami(self) -> UserInfo:
        if not self.auth:
            raise RuntimeError("Auth is not configured. Provide auth_url or set DDM_AUTH_URL.")
        if not self.token:
            raise RuntimeError("No token set. Call client.login() first or set token.")
        self._auth_http.set_token(self.token) 
        return self.auth.userinfo()

    @classmethod
    def from_env(cls) -> "DdmClient":
        s = get_settings()

        storage = make_storage(s.storage_backend, s.storage_dir)

        c = cls(
            base_url=s.base_url,
            auth_url=s.auth_url,
            token=s.token,
            timeout=s.timeout,
            storage=storage,
        )
        if not c.token:
            c.load_token_from_storage()

        return c