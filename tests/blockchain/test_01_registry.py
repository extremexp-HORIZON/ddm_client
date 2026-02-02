from __future__ import annotations
from helpers import safe_call, write_artifact

def test_registry(client):
    reg = safe_call("blockchain.registry()", lambda: client.blockchain.registry())
    write_artifact("registry", reg, subdir="blockchain")
    assert isinstance(reg, dict)
