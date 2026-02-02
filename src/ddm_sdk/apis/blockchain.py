from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Union

from ..transport.http import HttpTransport
from ..transport.serializers import build_params

from ..models.blockchain import (
    DeployedContract,
    PagedContracts,
    ContractEvent,
    PagedEvents,
    ContractTx,
    PagedTxs,
    TaskRef,
    IngestTxBody,
    PrepareSuiteBody,
    PrepareRewardBody,
    PrepareValidationBody,
)

# Swagger says many array query params are collectionFormat: csv
_CSV_KEYS = {
    "network", "name", "address", "status",
    "tx_hash", "from", "to",
}


class BlockchainAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    # -------- contracts --------

    def list_contracts(
        self,
        *,
        network: Optional[Sequence[str]] = None,
        name: Optional[Sequence[str]] = None,
        address: Optional[Sequence[str]] = None,
        status: Optional[Sequence[str]] = None,
        withEventsCount: int = 1,
        includeAbi: int = 0,
        sort: str = "id,desc",
        page: int = 1,
        perPage: int = 25,
        x_fields: Optional[str] = None,
    ) -> PagedContracts:
        params = build_params(
            {
                "network": list(network) if network else None,
                "name": list(name) if name else None,
                "address": list(address) if address else None,
                "status": list(status) if status else None,
                "withEventsCount": withEventsCount,
                "includeAbi": includeAbi,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", "/ddm/blockchain/contracts", params=params, headers=headers)
        return PagedContracts.model_validate(data)

    def get_contract(
        self,
        address: str,
        *,
        includeAbi: Union[int, str] = 0,
        withEventsCount: Union[int, str] = 0,
        x_fields: Optional[str] = None,
    ) -> DeployedContract:

        params = {
            "includeAbi": str(includeAbi),
            "withEventsCount": str(withEventsCount),
        }
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", f"/ddm/blockchain/contracts/{address}", params=params, headers=headers)
        return DeployedContract.model_validate(data)

    def registry(self) -> Dict[str, Any]:
        # Returns {"data": [...], "count": N} in your Flask code
        return self._http.request("GET", "/ddm/blockchain/contracts/registry")

    # -------- events --------

    def contract_events(
        self,
        address: str,
        *,
        network: Optional[Sequence[str]] = None,
        name: Optional[Sequence[str]] = None,
        tx_hash: Optional[Sequence[str]] = None,
        block_from: Optional[int] = None,
        block_to: Optional[int] = None,
        search: Optional[str] = None,
        sort: str = "block_number,desc",
        page: int = 1,
        perPage: int = 50,
        x_fields: Optional[str] = None,
    ) -> PagedEvents:
        params = build_params(
            {
                # NOTE: swagger lists address as query csv array, DDM backend uses path param.
                "network": list(network) if network else None,
                "name": list(name) if name else None,
                "tx_hash": list(tx_hash) if tx_hash else None,
                "block_from": block_from,
                "block_to": block_to,
                "search": search,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", f"/ddm/blockchain/contracts/{address}/events", params=params, headers=headers)
        return PagedEvents.model_validate(data)

    def all_events(
        self,
        *,
        network: Optional[Sequence[str]] = None,
        address: Optional[Sequence[str]] = None,
        name: Optional[Sequence[str]] = None,
        tx_hash: Optional[Sequence[str]] = None,
        block_from: Optional[int] = None,
        block_to: Optional[int] = None,
        search: Optional[str] = None,
        sort: str = "block_number,desc",
        page: int = 1,
        perPage: int = 50,
        x_fields: Optional[str] = None,
    ) -> PagedEvents:
        params = build_params(
            {
                "network": list(network) if network else None,
                "address": list(address) if address else None,
                "name": list(name) if name else None,
                "tx_hash": list(tx_hash) if tx_hash else None,
                "block_from": block_from,
                "block_to": block_to,
                "search": search,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", "/ddm/blockchain/events", params=params, headers=headers)
        return PagedEvents.model_validate(data)

    # -------- txs --------

    def all_txs(
        self,
        *,
        network: Optional[Sequence[str]] = None,
        tx_hash: Optional[Sequence[str]] = None,
        address: Optional[Sequence[str]] = None,
        frm: Optional[Sequence[str]] = None,
        to: Optional[Sequence[str]] = None,
        ts_from: Optional[int] = None,
        ts_to: Optional[int] = None,
        status: Optional[int] = None,
        block_from: Optional[int] = None,
        block_to: Optional[int] = None,
        sort: str = "block_number,desc",
        page: int = 1,
        perPage: int = 50,
        x_fields: Optional[str] = None,
    ) -> PagedTxs:
        params = build_params(
            {
                "network": list(network) if network else None,
                "tx_hash": list(tx_hash) if tx_hash else None,
                "address": list(address) if address else None,
                "from": list(frm) if frm else None,  # API expects "from" query name
                "to": list(to) if to else None,
                "ts_from": ts_from,
                "ts_to": ts_to,
                "status": status,
                "block_from": block_from,
                "block_to": block_to,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", "/ddm/blockchain/txs", params=params, headers=headers)
        return PagedTxs.model_validate(data)

    def contract_txs(
        self,
        address: str,
        *,
        network: Optional[Sequence[str]] = None,
        tx_hash: Optional[Sequence[str]] = None,
        frm: Optional[Sequence[str]] = None,
        to: Optional[Sequence[str]] = None,
        ts_from: Optional[int] = None,
        ts_to: Optional[int] = None,
        status: Optional[int] = None,
        block_from: Optional[int] = None,
        block_to: Optional[int] = None,
        sort: str = "block_number,desc",
        page: int = 1,
        perPage: int = 50,
        x_fields: Optional[str] = None,
    ) -> PagedTxs:
        params = build_params(
            {
                "network": list(network) if network else None,
                "tx_hash": list(tx_hash) if tx_hash else None,
                # swagger includes address query, DDM server uses path filter; ok to omit
                "from": list(frm) if frm else None,
                "to": list(to) if to else None,
                "ts_from": ts_from,
                "ts_to": ts_to,
                "status": status,
                "block_from": block_from,
                "block_to": block_to,
                "sort": sort,
                "page": page,
                "perPage": perPage,
            },
            csv_keys=_CSV_KEYS,
        )
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", f"/ddm/blockchain/contracts/{address}/txs", params=params, headers=headers)
        return PagedTxs.model_validate(data)

    def get_tx(self, tx_hash: str, *, x_fields: Optional[str] = None) -> ContractTx:
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("GET", f"/ddm/blockchain/txs/{tx_hash}", headers=headers)
        return ContractTx.model_validate(data)

    # -------- async tasks (celery) --------

    def ingest_tx(self, body: Union[IngestTxBody, Dict[str, Any]]) -> TaskRef:
        payload = body.model_dump() if isinstance(body, IngestTxBody) else dict(body)
        data = self._http.request("POST", "/ddm/blockchain/ingest-tx", json=payload)
        return TaskRef.model_validate(data)

    def prepare_suite(self, body: Union[PrepareSuiteBody, Dict[str, Any]], *,x_fields=None) -> TaskRef:
        if isinstance(body, PrepareSuiteBody):
            payload = body.model_dump(exclude_none=True)
        else:
            payload = dict(body)
        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("POST", "/ddm/blockchain/suites/prepare", json=payload, headers=headers)
        return TaskRef.model_validate(data)


    def prepare_reward(self, body: Union[PrepareRewardBody, Dict[str, Any]], *, x_fields=None) -> TaskRef:
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else dict(body)
        payload = {k: v for k, v in payload.items() if v is not None}

        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("POST", "/ddm/blockchain/rewards/prepare", json=payload, headers=headers)
        return TaskRef.model_validate(data)


    def prepare_validation(
        self,
        body: Union[Dict[str, Any], Any],
        *,
        x_fields: Optional[str] = None,
    ) -> TaskRef:
        """
        POST /ddm/blockchain/validations/prepare

        payload:
          {
            "network": "sepolia",
            "dataset_fingerprint": "0x...",
            "uploader": "0x...",
            "validation_json": {...},
            "include_report": true
          }
        """
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else dict(body)

        # do NOT send None fields (backend hates them)
        payload = {k: v for k, v in payload.items() if v is not None}

        headers = {"X-Fields": x_fields} if x_fields else None
        data = self._http.request("POST", "/ddm/blockchain/validations/prepare", json=payload, headers=headers)
        return TaskRef.model_validate(data)
    
    def prepare_report_ipfs_uri(self,
        *,
        network: str,
        catalog_id: str,
        include_report: bool = True,
        x_fields: Optional[str] = None,
    ) -> TaskRef:

        payload = {"network": network, "catalog_id": catalog_id, "include_report": include_report}
        headers = {"X-Fields": x_fields} if x_fields else None

        last_err: Optional[Exception] = None
        path="/ddm/blockchain/register_datasets/prepare_report"
        try:
            data = self._http.request("POST", path, json=payload, headers=headers)
            return TaskRef.model_validate(data)
        except Exception as e:
            last_err = e

        raise last_err 
    
    def prepare_dataset_ipfs_uri(
        self,
        *,
        network: str,
        catalog_id: str,
        include_report: bool = True,
        x_fields: Optional[str] = None,
    ) -> TaskRef:
        payload = {"network": network, "catalog_id": catalog_id, "include_report": include_report}
        headers = {"X-Fields": x_fields} if x_fields else None
        path="/ddm/blockchain/register_datasets/prepare_report"
        last_err: Optional[Exception] = None
        try:
            data = self._http.request("POST", path, json=payload, headers=headers)
            data=data[0] if isinstance(data, tuple) and len(data) >= 2 else data
            return TaskRef.model_validate(data)
        except Exception as e:
            last_err = e

        raise last_err
