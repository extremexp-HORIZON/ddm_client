from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, ConfigDict


class DeployedContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: Optional[int] = None
    network: str
    name: str
    address: str
    tx_hash: Optional[str] = None
    start_block: Optional[int] = None
    last_scanned_block: Optional[int] = None
    confirmations: Optional[int] = None
    status: Optional[str] = None
    events_count: Optional[int] = None
    abi: Optional[List[dict]] = None


class ContractEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: Optional[int] = None
    network: str
    address: str
    name: str
    tx_hash: str
    block_number: int
    log_index: int
    args: Dict[str, Any]
    contract_name: Optional[str] = None


class ContractTx(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: Optional[int] = None
    network: str
    tx_hash: str
    block_number: int
    tx_index: Optional[int] = None

    # API returns key "from" (string); keep python-safe name with alias
    frm: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None

    value_wei: Optional[str] = None
    status: Optional[int] = None
    gas_used: Optional[str] = None
    effective_gas_price: Optional[str] = None
    nonce: Optional[int] = None
    contract_address: Optional[str] = None
    block_timestamp: Optional[int] = None
    extra: Optional[Dict[str, Any]] = None
    contract_name: Optional[str] = None


class TaskRef(BaseModel):
    task_id: str


class PagedContracts(BaseModel):
    data: List[DeployedContract]
    total: int
    filtered_total: int
    page: int
    perPage: int


class PagedEvents(BaseModel):
    data: List[ContractEvent]
    total: int
    filtered_total: int
    page: int
    perPage: int


class PagedTxs(BaseModel):
    data: List[ContractTx]
    total: int
    filtered_total: int
    page: int
    perPage: int


# ----- request bodies -----

class IngestTxBody(BaseModel):
    network: str
    address: str
    tx_hash: str


class PrepareSuiteBody(BaseModel):
    network: str = "sepolia"
    requester: str
    suite: Dict[str, Any]
    category: str
    fileFormat: str
    deadline: int
    totalExpected: int
    docs_html: Optional[str] = None
    certificate_json: Optional[Dict[str, Any]] = None
    expectation_suite_id: str
    certificate_json: Dict[str, Any]


class PrepareRewardBody(BaseModel):
    model_config = ConfigDict(extra="allow")
    network: str
    category: str
    dataset_fingerprint: Optional[str] = None
    uploader: Optional[str] = None
    expires_in_sec: Optional[int] = None
    dataset_uri: Optional[str] = None
    suite_hash: Optional[str] = None
    report_uri: Optional[str] = None


class PrepareValidationBody(BaseModel):
    model_config = ConfigDict(extra="allow")
    network: str
    dataset_fingerprint: str
    uploader: str
    validation_json: Dict[str, Any] = Field(default_factory=dict)
    include_report: bool = True