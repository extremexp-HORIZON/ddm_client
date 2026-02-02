from __future__ import annotations
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

def test_contract_events(client, network, picked_contract_address):
    ev = safe_call(
        "blockchain.contract_events(page=1, perPage=100)",
        lambda: client.blockchain.contract_events(
            picked_contract_address,
            network=[network],
            page=1,
            perPage=100,
            sort="block_number,desc",
        ),
    )
    payload = _dump_models(ev)
    write_artifact("contract_events", payload, subdir="blockchain" )
    assert "data" in payload

def test_all_events(client, network):
    ev = safe_call(
        "blockchain.all_events(page=1, perPage=100)",
        lambda: client.blockchain.all_events(network=[network], page=1, perPage=100, sort="block_number,desc"),
    )
    payload = _dump_models(ev)
    write_artifact("all_events", payload, subdir="blockchain")
    assert "data" in payload
