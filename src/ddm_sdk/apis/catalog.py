from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from ..transport.http import HttpTransport
from ..transport.serializers import build_params

from ..models.catalog import PagedFiles, FileOption, TreeResponse


_CSV_KEYS = {
    "filename", "use_case", "project_id", "user_id", "file_type", "parent_files"
}


class CatalogAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def list(
        self,
        *,
        filename: Optional[Sequence[str]] = None,
        use_case: Optional[Sequence[str]] = None,
        project_id: Optional[Sequence[str]] = None,
        created_from: Optional[str] = None,  # ISO string
        created_to: Optional[str] = None,
        user_id: Optional[Sequence[str]] = None,
        file_type: Optional[Sequence[str]] = None,
        parent_files: Optional[Sequence[str]] = None,
        size_from: Optional[int] = None,
        size_to: Optional[int] = None,
        sort: str = "id,asc",
        page: int = 1,
        perPage: int = 10,
    ) -> PagedFiles:
        params = build_params(
            {
                "filename": list(filename) if filename else None,
                "use_case": list(use_case) if use_case else None,
                "project_id": list(project_id) if project_id else None,
                "created_from": created_from,
                "created_to": created_to,
                "user_id": list(user_id) if user_id else None,
                "file_type": list(file_type) if file_type else None,
                "parent_files": list(parent_files) if parent_files else None,
                "size_from": size_from,
                "size_to": size_to,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        data = self._http.request("GET", "/ddm/catalog/list", params=params)
        return PagedFiles.model_validate(data)

    def my_catalog(
        self,
        *,
        filename: Optional[Sequence[str]] = None,
        use_case: Optional[Sequence[str]] = None,
        project_id: Optional[Sequence[str]] = None,
        created_from: Optional[str] = None,
        created_to: Optional[str] = None,
        user_id: Optional[Sequence[str]] = None,
        file_type: Optional[Sequence[str]] = None,
        parent_files: Optional[Sequence[str]] = None,
        size_from: Optional[int] = None,
        size_to: Optional[int] = None,
        sort: str = "id,asc",
        page: int = 1,
        perPage: int = 10,
    ) -> PagedFiles:
        params = build_params(
            {
                "filename": list(filename) if filename else None,
                "use_case": list(use_case) if use_case else None,
                "project_id": list(project_id) if project_id else None,
                "created_from": created_from,
                "created_to": created_to,
                "user_id": list(user_id) if user_id else None,
                "file_type": list(file_type) if file_type else None,
                "parent_files": list(parent_files) if parent_files else None,
                "size_from": size_from,
                "size_to": size_to,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        data = self._http.request("GET", "/ddm/catalog/my-catalog", params=params)
        return PagedFiles.model_validate(data)

    def options(
        self,
        *,
        project_id: Optional[str] = None,
        filename: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> list[FileOption]:
        params: Dict[str, Any] = {}
        if project_id:
            params["project_id"] = project_id
        if filename:
            params["filename"] = filename
        if user_id:
            params["user_id"] = user_id

        data = self._http.request("GET", "/ddm/catalog/options", params=params)
        return [FileOption.model_validate(x) for x in data]

    def tree(
        self,
        *,
        parent: Optional[str] = None,
        name: Optional[str] = None,
        size: Optional[int] = None,
        type: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 0,
        perPage: int = 20,
        filter: Optional[str] = None,
    ) -> TreeResponse:
        params: Dict[str, Any] = {
            "page": page,
            "perPage": perPage,
        }
        if parent is not None:
            params["parent"] = parent
        if name is not None:
            params["name"] = name
        if size is not None:
            params["size"] = size
        if type is not None:
            params["type"] = type
        if sort is not None:
            params["sort"] = sort
        if filter is not None:
            params["filter"] = filter

        data = self._http.request("GET", "/ddm/catalog/tree", params=params)
        return TreeResponse.model_validate(data)

    def advanced(self, filters: Dict[str, Any]) -> Any:
        # Returns a JSON array of file-like dicts 
        return self._http.request("POST", "/ddm/catalog/advanced", json=filters)
