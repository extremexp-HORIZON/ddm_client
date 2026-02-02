from __future__ import annotations

import json
import pytest
from helpers import getenv_str, safe_call, write_artifact, OUT_DIR


def _suite_id_from_expectations_artifact() -> str | None:
    p = OUT_DIR / "expectations" / "expectations_create_suite_ids.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    v = d.get("suite_id")
    return v if isinstance(v, str) and v.strip() else None


def _suite_has_expectations(client, suite_id: str) -> bool:
    """
    Always verify via GET /suites/{id} (list_suites can lie / omit expectations).
    Accept both shapes:
      - expectations: [ ... ]
      - expectations: { expectations: [ ... ], ... }   (UI payload stored as dict)
    """
    suite = safe_call("expectations.get_suite(check)", lambda: client.expectations.get_suite(suite_id))
    if not suite:
        return False

    ex = getattr(suite, "expectations", None)

    # shape A: list
    if isinstance(ex, list):
        return len(ex) > 0

    # shape B: dict (UI-style payload)
    if isinstance(ex, dict):
        inner = ex.get("expectations")
        if isinstance(inner, list):
            return len(inner) > 0
        # sometimes backend stores other keys; treat non-empty dict as "has something"
        return len(ex.keys()) > 0

    # unknown/None
    return False


def _choose_suite_id(client) -> str:
    # 1) Prefer suite created by expectations tests (artifact)
    v_art = _suite_id_from_expectations_artifact()
    if v_art and _suite_has_expectations(client, v_art):
        return v_art

    # 2) Env override (only if it has expectations)
    v_env = getenv_str("DDM_TEST_SUITE_ID") or getenv_str("DDM_TEST_EXPECTATIONS_SUITE_ID")
    if v_env and _suite_has_expectations(client, v_env):
        return v_env

    # 3) list suites by suite_name, pick newest that *actually* has expectations (verified by get_suite)
    suite_name = getenv_str("DDM_TEST_EXPECT_SUITE_NAME", "my_suite")
    resp = safe_call(
        "expectations.list_suites(filter by suite_name)",
        lambda: client.expectations.list_suites(suite_name=[suite_name], sort="created,desc", page=1, perPage=50),
    )
    for s in (getattr(resp, "data", None) or []):
        sid = getattr(s, "id", None)
        if sid and _suite_has_expectations(client, sid):
            return sid

    # 4) last resort: latest suites, pick first that has expectations
    resp2 = safe_call(
        "expectations.list_suites(latest)",
        lambda: client.expectations.list_suites(sort="created,desc", page=1, perPage=50),
    )
    for s in (getattr(resp2, "data", None) or []):
        sid = getattr(s, "id", None)
        if sid and _suite_has_expectations(client, sid):
            return sid

    raise AssertionError("No suite with expectations available (create one first or fix env/artifact)")


def test_01_validations_pick_suite_id(client):
    suite_id = _choose_suite_id(client)

    write_artifact("validations_suite_id", {"suite_id": suite_id}, subdir="validations")

    # sanity: fetch suite
    suite = safe_call("expectations.get_suite", lambda: client.expectations.get_suite(suite_id))
    assert suite is not None, "get_suite returned None"

    # IMPORTANT: dump as JSON mode so you don't lose nested fields
    dump = suite.model_dump(mode="json", exclude_none=False) if hasattr(suite, "model_dump") else suite
    write_artifact("validations_suite", dump, subdir="validations")

    assert getattr(suite, "id", None) == suite_id

    # extra: hard assert we picked a usable suite for validations
    assert _suite_has_expectations(client, suite_id), "Picked suite has no expectations (should not happen)"
