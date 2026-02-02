from __future__ import annotations

import pytest
from helpers import safe_call, write_artifact

def _dump_models(paged):
    data = getattr(paged, "data", None) or []
    out = []
    for x in data:
        if hasattr(x, "model_dump"):
            out.append(x.model_dump())
        else:
            out.append(x.__dict__ if hasattr(x, "__dict__") else x)
    return {"total": getattr(paged, "total", None), "filtered_total": getattr(paged, "filtered_total", None), "data": out}

def test_contract_txs(client, network, picked_contract_address):
    txs = safe_call(
        "blockchain.contract_txs(page=1, perPage=50)",
        lambda: client.blockchain.contract_txs(
            picked_contract_address, network=[network], page=1, perPage=50, sort="block_number,desc"
        ),
    )
    payload = _dump_models(txs)
    write_artifact("contract_txs", payload, subdir="blockchain" )
    assert "data" in payload

def test_all_txs(client, network):
    txs = safe_call(
        "blockchain.all_txs(page=1, perPage=50)",
        lambda: client.blockchain.all_txs(network=[network], page=1, perPage=50, sort="block_number,desc"),
    )
    payload = _dump_models(txs)
    write_artifact("all_txs", payload, subdir="blockchain")
    assert "data" in payload

def test_get_tx_from_contract(client, network, picked_contract_address):
    txs = safe_call(
        "blockchain.contract_txs(page=1, perPage=1)",
        lambda: client.blockchain.contract_txs(picked_contract_address, network=[network], page=1, perPage=1),
    )
    if not getattr(txs, "data", None):
        pytest.skip("No txs to fetch via get_tx")
    tx_hash = txs.data[0].tx_hash
    tx = safe_call("blockchain.get_tx(tx_hash)", lambda: client.blockchain.get_tx(tx_hash))
    write_artifact("get_tx", tx if isinstance(tx, dict) else (tx.model_dump() if hasattr(tx, "model_dump") else tx), subdir="blockchain")
    assert tx is not None
