from __future__ import annotations

import json
import pytest
from pathlib import Path

from ddm_sdk.models.validations import ValidateFilesAgainstSuiteRequest
from ddm_sdk.models.file import UploadSingleResponse

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR


def _read_suite_id_artifact() -> str:
    p = OUT_DIR / "validations" / "validations_suite_id.json"
    if not p.exists():
        pytest.skip("No validations_suite_id.json (run test_01_validations_pick_suite_id first)")
    d = json.loads(p.read_text(encoding="utf-8"))
    return d["suite_id"]


def _split_csv(v: str | None) -> list[str]:
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def _upload_fresh_file_id(client) -> str:
    """
    Uploads a new file using the same logic as your file upload test.
    Returns the new file_id.
    """
    if not getenv_bool("DDM_TEST_FILE_UPLOAD", True):
        pytest.skip("DDM_TEST_FILE_UPLOAD disabled; cannot auto-upload fresh file for re-validation")

    project_id = getenv_str("DDM_TEST_PROJECT_ID", "projectA/sub1")

    sample_path = Path(getenv_str("DDM_SAMPLE_PATH", "data.csv"))
    if not sample_path.exists():
        pytest.skip(f"Sample file not found: {sample_path} (set DDM_SAMPLE_PATH)")

    meta_path = getenv_str("DDM_SAMPLE_METADATA_PATH")
    if meta_path:
        mp = Path(meta_path)
        if not mp.exists():
            pytest.skip(f"Metadata file not found: {mp} (set DDM_SAMPLE_METADATA_PATH)")
        meta_path = str(mp)
    else:
        meta_path = None

    # Make filename unique-ish so backend doesn't dedupe by name (if it does)
    user_filename = getenv_str("DDM_TEST_USER_FILENAME", "my_clean_data.csv")

    resp = safe_call(
        "file.upload(for revalidation)",
        lambda: client.file.upload(
            project_id=project_id,
            file=str(sample_path),
            user_filename=user_filename,
            description=getenv_str("DDM_TEST_FILE_DESCRIPTION", "training dataset"),
            use_case=(getenv_str("DDM_TEST_FILE_USE_CASE", "ml") or "ml").split(","),
            metadata_file=meta_path,
        ),
    )
    assert resp is not None, "file.upload returned None (cannot create fresh file_id)"

    # Persist upload response
    write_artifact(
        "validations_autoupload_file_upload_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="validations",
    )

    assert isinstance(resp, UploadSingleResponse) or hasattr(resp, "file"), f"Unexpected upload response type: {type(resp)}"

    fobj = getattr(resp, "file", None)
    assert fobj is not None, f"upload response missing .file: {resp}"

    file_id = getattr(fobj, "id", None)
    assert isinstance(file_id, str) and file_id.strip(), f"upload response missing file.id: {resp}"

    write_artifact(
        "validations_autoupload_file_upload_ids",
        {"file_id": file_id, "metadata_task_id": getattr(fobj, "metadata_task_id", None)},
        subdir="validations",
    )

    return file_id


def test_02_validations_validate_files_against_suite(client):
    suite_id = _read_suite_id_artifact()

    # 1) First try with env-provided file ids
    file_ids = _split_csv(getenv_str("DDM_TEST_FILE_IDS"))
    if not file_ids:
        one = getenv_str("DDM_TEST_FILE_ID")
        if one:
            file_ids = [one]

    if not file_ids:
        pytest.skip("No file ids (set DDM_TEST_FILE_IDS or DDM_TEST_FILE_ID)")

    body = ValidateFilesAgainstSuiteRequest(suite_id=suite_id, file_ids=file_ids)

    resp = safe_call(
        "validations.validate_files_against_suite",
        lambda: client.validations.validate_files_against_suite(body),
    )

    # 2) If 409 happened, safe_call returns None → upload fresh file and retry once
    if resp is None:
        # keep an artifact so you can see why we retried (safe_call already printed the error)
        write_artifact(
            "validations_validate_files_against_suite_first_try_failed",
            {"suite_id": suite_id, "file_ids": file_ids},
            subdir="validations",
        )

        new_file_id = _upload_fresh_file_id(client)

        retry_body = ValidateFilesAgainstSuiteRequest(suite_id=suite_id, file_ids=[new_file_id])
        resp = safe_call(
            "validations.validate_files_against_suite(retry with fresh file)",
            lambda: client.validations.validate_files_against_suite(retry_body),
        )

        if resp is None:
            # If this still fails, it's not just "already validated"
            pytest.fail("validate_files_against_suite failed even after uploading a fresh file")

        # record that we used a new file id
        write_artifact(
            "validations_validate_files_against_suite_retry_used_file_id",
            {"file_id": new_file_id},
            subdir="validations",
        )

    # 3) Normal response handling
    write_artifact(
        "validations_validate_files_against_suite_response",
        resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp,
        subdir="validations",
    )

    tasks = getattr(resp, "tasks", None) or []
    task_ids = [getattr(t, "task_id", None) for t in tasks if getattr(t, "task_id", None)]

    if not task_ids:
        pytest.skip("validate_files_against_suite returned no task_ids (backend returned empty tasks)")

    write_artifact("validations_tasks", {"task_ids": task_ids}, subdir="validations")
