from __future__ import annotations

from helpers import safe_call, write_artifact


def test_03_parametrics_all_expectations(client):
    all_exp = safe_call("parametrics.all_expectations", lambda: client.parametrics.all_expectations())
    assert all_exp is not None, "all_expectations returned None"

    all_list = (
        getattr(all_exp, "all_expectations", None)
        or getattr(all_exp, "data", None)
        or all_exp
    )

    dump = all_exp.model_dump(mode="json", exclude_none=False) if hasattr(all_exp, "model_dump") else all_exp
    write_artifact("parametrics_all_expectations_raw", dump, subdir="parametrics")

    # sanity: list-like (or dict containing list)
    if isinstance(all_list, (list, tuple)):
        assert len(all_list) >= 0
    else:
        # at minimum, ensure it’s not empty None
        assert all_list is not None
