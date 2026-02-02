from __future__ import annotations

import json
from helpers import getenv_int, getenv_str, safe_call, write_artifact, OUT_DIR


def _read_validations_suite_id() -> str | None:
    p = OUT_DIR / "validations" / "validations_suite_id.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        v = d.get("suite_id")
        return v if isinstance(v, str) and v.strip() else None
    except Exception:
        return None


def test_04_validations_list_and_get_result(client):
    # ✅ Prefer suite picked by validations (guaranteed “has expectations”)
    suite_id = _read_validations_suite_id()

    # fallback only if artifact missing
    if not suite_id:
        suite_id = getenv_str("DDM_TEST_SUITE_ID") or getenv_str("DDM_TEST_EXPECTATIONS_SUITE_ID")

    page = getenv_int("DDM_TEST_VALIDATIONS_PAGE") or 1
    per_page = getenv_int("DDM_TEST_VALIDATIONS_PER_PAGE") or 20

    resp = safe_call(
        "validations.list_results",
        lambda: client.validations.list_results(
            page=page,
            perPage=per_page,
            sort="run_time,desc",
            suite_id=[suite_id] if suite_id else None,
        ),
    )
    assert resp is not None, "list_results returned None"

    # ✅ dump fully (includes nested detailed_results when data exists)
    write_artifact(
        "validations_list_results",
        resp.model_dump(mode="json", exclude_none=False),
        subdir="validations",
    )

    data = getattr(resp, "data", None) or []
    if not data:
        # Helpful debug artifact so you can see why it filtered to zero
        write_artifact(
            "validations_list_results_debug",
            {
                "used_suite_id": suite_id,
                "note": "No rows returned. If you expected rows, your filter suite_id likely doesn't match the suite_id in those results.",
                "total": getattr(resp, "total", None),
                "filtered_total": getattr(resp, "filtered_total", None),
                "page": getattr(resp, "page", None),
                "perPage": getattr(resp, "perPage", None),
            },
            subdir="validations",
        )
        return

    rid = getattr(data[0], "id", None)
    if not rid:
        return

    got = safe_call("validations.get_result", lambda: client.validations.get_result(rid))
    assert got is not None, "get_result returned None"

    write_artifact(
        "validations_get_result",
        got.model_dump(mode="json", exclude_none=False) if hasattr(got, "model_dump") else got,
        subdir="validations",
    )
