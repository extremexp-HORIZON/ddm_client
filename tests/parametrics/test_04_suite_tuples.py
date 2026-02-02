from __future__ import annotations

from helpers import safe_call, write_artifact


def test_04_parametrics_suite_tuples(client):
    tuples = safe_call("parametrics.suite_tuples", lambda: client.parametrics.suite_tuples())
    assert tuples is not None, "suite_tuples returned None"

    # dump as json
    if hasattr(tuples, "model_dump"):
        dump = tuples.model_dump(mode="json", exclude_none=False)
    else:
        # list of pydantic models or dicts
        out = []
        for t in tuples if isinstance(tuples, list) else []:
            out.append(t.model_dump(mode="json", exclude_none=False) if hasattr(t, "model_dump") else t)
        dump = out if out else tuples

    write_artifact("parametrics_suite_tuples", dump, subdir="parametrics")

    # sanity
    assert isinstance(tuples, list), f"expected list, got {type(tuples)}"
    if tuples:
        t0 = tuples[0]
        # tolerate different shapes
        _id = getattr(t0, "id", None) if not isinstance(t0, dict) else t0.get("id")
        _name = getattr(t0, "suite_name", None) if not isinstance(t0, dict) else t0.get("suite_name")
        assert _id is not None or _name is not None, "first tuple has neither id nor suite_name"
