from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, ConfigDict

from .file import FileItem


class PagedFiles(BaseModel):
    data: List[FileItem]
    total: int
    page: int
    perPage: int
    filtered_total: int


class FileOption(BaseModel):
    id: str
    filename: str
    project_id: Optional[str] = None


class TreeNodeData(BaseModel):
    model_config = ConfigDict(extra="allow")

    # folder nodes: name/path/type/size
    # file nodes: id/name/path/size/type
    id: Optional[str] = None
    name: str
    path: Optional[str] = None
    type: Optional[str] = None
    size: Optional[int] = None


class TreeNode(BaseModel):
    model_config = ConfigDict(extra="allow")

    key: str
    data: TreeNodeData
    leaf: bool


class TreeResponse(BaseModel):
    nodes: List[TreeNode]
    totalRecords: int
