from __future__ import annotations

from helpers import safe_call, write_artifact


def test_02_parametrics_categorized_expectations(client):
    cat = safe_call("parametrics.categorized_expectations", lambda: client.parametrics.categorized_expectations())
    assert cat is not None, "categorized_expectations returned None"

    # model may be {data: ...} or plain dict
    data = getattr(cat, "data", None) or cat
    dump = data.model_dump(mode="json", exclude_none=False) if hasattr(data, "model_dump") else data

    write_artifact("parametrics_categorized_expectations", dump, subdir="parametrics")

    # sanity: should be dict-like with keys (or at least non-empty)
    assert dump is not None
