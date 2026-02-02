from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


# ---------- Requests ----------



class ExpectationItem(BaseModel):
    expectation_type: str
    kwargs: Dict[str, Any]

class ExpectationSuiteCreate(BaseModel):
    """
    Matches POST /ddm/expectations/suites (UI payload)
    """
    suite_name: str
    dataset_id: str
    file_types: List[str]
    expectations: Dict[str, Any]
    user_id: str

    datasource_name: Optional[str] = "default"
    category: Optional[str] = None
    description: Optional[str] = None
    use_case: Optional[str] = None
    column_names: Optional[List[str]] = None
    column_descriptions: Optional[Dict[str, str]] = None


class UploadSampleResponse(BaseModel):
    """
    POST /ddm/expectations/upload-sample
    """
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    dataset_id: Optional[str] = None
    expectation_task_id: Optional[str] = None
    description_task_id: Optional[str] = None


# ---------- Responses ----------

class OnchainDatasetRequestLite(BaseModel):
    model_config = ConfigDict(extra="allow")

    network: Optional[str] = None
    contract_address: Optional[str] = None
    suite_id: Optional[int] = None  # on-chain suiteId

    bounty_wei: Optional[str] = None
    total_expected: Optional[int] = None
    deadline: Optional[int] = None
    is_closed: Optional[bool] = None
    total_claims: Optional[int] = None


class ExpectationSuiteResponse(BaseModel):
    """
    Matches marshal model ExpectationSuiteResponse
    """
    model_config = ConfigDict(extra="allow")

    id: str
    suite_name: str
    datasource_name: Optional[str] = None
    suite_hash: Optional[str] = None

    file_types: Any = None
    expectations: Any = None

    category: Optional[str] = None
    description: Optional[str] = None
    user_id: Optional[str] = None
    use_case: Optional[str] = None

    column_descriptions: Any = None
    column_names: Any = None
    created: Optional[str] = None

    expectation_descriptions: Any = None

    has_onchain_request: Optional[bool] = None
    onchain_requests: List[OnchainDatasetRequestLite] = Field(default_factory=list)


class ExpectationSuiteListResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: List[ExpectationSuiteResponse] = Field(default_factory=list)
    total: int = 0
    filtered_total: int = 0
    page: int = 1
    perPage: int = 10


class CreateSuiteResponse(BaseModel):
    """
    POST /ddm/expectations/suites returns 202 with:
      { message, suite_id, task_id }
    """
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    suite_id: Optional[str] = None
    task_id: Optional[str] = None
