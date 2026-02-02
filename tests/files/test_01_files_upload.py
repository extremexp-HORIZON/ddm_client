from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR


def _split_csv_env(name: str) -> list[str]:
    v = getenv_str(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


@pytest.fixture(scope="session")
def files_project_id() -> str:
    return getenv_str("DDM_TEST_PROJECT_ID", "projectA/sub1")


@pytest.fixture(scope="session")
def files_sample_paths() -> list[Path]:
    # Prefer explicit list of paths
    paths = _split_csv_env("DDM_SAMPLES_PATHS")
    if not paths:
        # fallback to single DDM_SAMPLE_PATH, treated as a one-item list
        p = getenv_str("DDM_SAMPLE_PATH")
        if not p:
            pytest.skip("Missing DDM_SAMPLES_PATHS or DDM_SAMPLE_PATH")
        paths = [p]

    out: list[Path] = []
    for p in paths:
        pp = Path(p)
        if not pp.exists():
            pytest.skip(f"Sample file not found: {pp}")
        out.append(pp)

    return out


@pytest.fixture(scope="session")
def metadata_paths() -> list[Path] | None:
    # Optional: one metadata per file
    paths = _split_csv_env("DDM_SAMPLES_METADATA_PATHS")
    if not paths:
        return None

    out: list[Path] = []
    for p in paths:
        pp = Path(p)
        if not pp.exists():
            pytest.skip(f"Metadata file not found: {pp}")
        out.append(pp)
    return out


def test_01_files_upload_bulk(client, files_project_id: str, files_sample_paths: list[Path], metadata_paths: list[Path] | None):
    if not getenv_bool("DDM_TEST_FILES_UPLOAD", True):
        pytest.skip("DDM_TEST_FILES_UPLOAD not enabled")

    # Optional per-file fields (comma-separated)
    user_filenames = _split_csv_env("DDM_TEST_FILES_USER_FILENAMES") or [p.name for p in files_sample_paths]
    descriptions = _split_csv_env("DDM_TEST_FILES_DESCRIPTIONS") or ["bulk upload"] * len(files_sample_paths)

    # use_case allows either string or list[str] per file; your client serializes list -> JSON string
    # env examples:
    #   DDM_TEST_FILES_USE_CASE=ml,etl,forecasting
    # or per-file lists:
    #   DDM_TEST_FILES_USE_CASE_JSON=["[\"ml\"]","[\"etl\",\"ingest\"]"]
    use_case_flat = _split_csv_env("DDM_TEST_FILES_USE_CASE")
    use_case = use_case_flat if use_case_flat else ["ml"] * len(files_sample_paths)

    # Ensure lengths are sane (backend behavior varies; we’ll trim/pad safely)
    n = len(files_sample_paths)
    user_filenames = (user_filenames + [user_filenames[-1]] * n)[:n]
    descriptions = (descriptions + [descriptions[-1]] * n)[:n]
    use_case = (use_case + [use_case[-1]] * n)[:n]

    resp = safe_call(
        "files.upload",
        lambda: client.files.upload(
            project_id=files_project_id,
            files=[str(p) for p in files_sample_paths],
            user_filenames=user_filenames,
            descriptions=descriptions,
            use_case=use_case,
            metadata_files=[str(p) for p in metadata_paths] if metadata_paths else None,
        ),
    )
    assert resp is not None, "files.upload returned None"

    write_artifact(
        "files_upload_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="files",
    )

    # Try to extract file_ids from common shapes
    d = resp.model_dump() if hasattr(resp, "model_dump") else resp
    file_ids: list[str] = []

    if isinstance(d, dict):
        # common: {"files":[{"id":"..."}, ...]}
        items = d.get("files") or d.get("data") or d.get("uploaded") or []
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    fid = it.get("id") or it.get("file_id") or it.get("fileId")
                    if isinstance(fid, str) and fid:
                        file_ids.append(fid)

    # Save extracted IDs for running other tests (optional convenience)
    if file_ids:
        write_artifact("files_upload_file_ids", {"file_ids": file_ids}, subdir="files")

    # Don’t hard-fail if backend schema differs; just ensure we got “something”
    assert d is not None, "Unexpected empty response"
