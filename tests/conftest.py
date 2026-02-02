from __future__ import annotations

import pytest
import json
from pathlib import Path
from ddm_sdk.client import DdmClient
from helpers import (
    getenv_str,
    safe_call,
    write_artifact,
    poll_task_until_ready,
    unwrap_task_value,
    get_task_value_fallback_from_status,
    OUT_DIR,
)
from tests.expectations.expectations_utils import parse_column_descriptions


@pytest.fixture(scope="session")
def client() -> DdmClient:
    c = DdmClient.from_env()

    username = getenv_str("DDM_USERNAME")
    password = getenv_str("DDM_PASSWORD")
    if username and password:
        safe_call("login", lambda: c.login(username, password))
    else:
        print("⚠️ DDM_USERNAME/DDM_PASSWORD not set; relying on DDM_TOKEN if present")

    return c


@pytest.fixture(scope="session")
def network() -> str:
    return getenv_str("DDM_TEST_NETWORK", "sepolia")


@pytest.fixture(scope="session")
def requester() -> str:
    r = getenv_str("DDM_TEST_REQUESTER")
    if not r:
        pytest.skip("Missing DDM_TEST_REQUESTER")
    return r


@pytest.fixture(scope="session")
def uploader(requester: str) -> str:
    """
    Frontend flow: uploader is an address.
    Prefer explicit env var; fallback to requester.
    """
    up = getenv_str("DDM_TEST_UPLOADER") or requester
    if not up:
        pytest.skip("Missing DDM_TEST_UPLOADER (or DDM_TEST_REQUESTER)")
    return up


@pytest.fixture(scope="session")
def dataset_fingerprint(client, network: str) -> str:
    fp = getenv_str("DDM_TEST_DATASET_FINGERPRINT")
    if fp:
        fp = fp.strip()
        return fp if fp.startswith("0x") else "0x" + fp

    # fallback: pick latest DatasetRegistered fingerprint from events
    evs = client.blockchain.all_events(
        network=[network],
        name=["DatasetRegistered"],
        sort="block_number,desc",
        page=1,
        perPage=1,
    )

    data = getattr(evs, "data", None) or []
    if not data:
        pytest.skip("No DatasetRegistered events found and DDM_TEST_DATASET_FINGERPRINT not set")

    args = getattr(data[0], "args", None) or {}
    fp = args.get("fingerprint") or args.get("datasetFingerprint")
    if not fp:
        pytest.skip("Latest DatasetRegistered event missing fingerprint")
    return fp



# ---------- Web3 fixtures ----------
try:
    from web3 import Web3
except Exception:
    Web3 = None


@pytest.fixture(scope="session")
def web3(network: str):
    if Web3 is None:
        pytest.skip("web3 not installed (pip install web3)")

    rpc = getenv_str("DDM_TEST_RPC_URL")
    if not rpc:
        if network == "sepolia":
            rpc = "https://rpc.sepolia.org"
        else:
            pytest.skip("Set DDM_TEST_RPC_URL for this network")

    w3 = Web3(Web3.HTTPProvider(rpc))
    if not w3.is_connected():
        pytest.skip(f"Cannot connect to RPC {rpc}")
    return w3


@pytest.fixture(scope="session")
def private_key() -> str:
    pk = getenv_str("DDM_TEST_PRIVATE_KEY")
    if not pk:
        pytest.skip("Missing DDM_TEST_PRIVATE_KEY (needed for signing)")
    return pk

@pytest.fixture(scope="session")
def picked_contract_address(contracts_index_path: Path) -> str:
    if not contracts_index_path.exists():
        pytest.skip(f"Contracts index not found: {contracts_index_path} (run dump contracts test first)")
    idx = json.loads(contracts_index_path.read_text(encoding="utf-8"))
    contracts = idx.get("contracts") or []
    if not contracts:
        pytest.skip("Contracts index is empty.")
    return contracts[0]["address"]

@pytest.fixture(scope="session")
def contracts_index_path(network: str) -> Path:
    return OUT_DIR / "contracts" / network / "_index.json"




def _split_csv_env(name: str) -> list[str]:
    v = getenv_str(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def _artifact_file_ids() -> list[str]:
    # Prefer the files upload artifact (created by tests/files)
    p = OUT_DIR / "files" / "files_upload_file_ids.json"
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            ids = d.get("file_ids") or []
            ids = [x for x in ids if isinstance(x, str) and x.strip()]
            if ids:
                return ids
        except Exception:
            pass

    # Fallback: maybe file_metadata tests wrote something similar
    p2 = OUT_DIR / "file_metadata" / "file_metadata_get_many_response.json"
    if p2.exists():
        try:
            d2 = json.loads(p2.read_text(encoding="utf-8"))
            if isinstance(d2, dict):
                ids2 = [k for k in d2.keys() if isinstance(k, str) and k.strip()]
                if ids2:
                    return ids2
        except Exception:
            pass

    return []


@pytest.fixture(scope="session")
def file_ids() -> list[str]:
    # env override
    ids = _split_csv_env("DDM_TEST_FILE_IDS")
    if ids:
        return ids

    fid = getenv_str("DDM_TEST_FILE_ID")
    if fid:
        return [fid]

    ids = _artifact_file_ids()
    if ids:
        return ids

    pytest.skip("Missing DDM_TEST_FILE_ID/DDM_TEST_FILE_IDS and no suitable artifact found")


@pytest.fixture(scope="session")
def file_id(file_ids: list[str]) -> str:
    if not file_ids:
        pytest.skip("No file_ids available")
    return file_ids[0]

@pytest.fixture(scope="session")
def sample_file_path() -> Path:
    p = Path(getenv_str("DDM_SAMPLE_PATH", "data/sample.csv"))
    if not p.exists():
        pytest.skip(f"Sample file not found: {p} (set DDM_SAMPLE_PATH)")
    return p

@pytest.fixture(scope="session")
def expectations_sample_file_path() -> Path:
    p = Path(getenv_str("DDM_EXPECTATIONS_SAMPLE_FILE_PATH", "data/expectations_sample.csv"))
    if not p.exists():
        pytest.skip(f"Expectations sample file not found: {p} (set DDM_EXPECTATIONS_SAMPLE_FILE_PATH)")
    return p

@pytest.fixture(scope="session")
def suite_id(client, suite_name: str) -> str:
    def _has_expectations(sid: str) -> bool:
        s = safe_call("expectations.get_suite(check)", lambda: client.expectations.get_suite(sid))
        return bool(s and getattr(s, "expectations", None))

    # 1) Prefer suite created by expectations test_02 
    p = OUT_DIR / "expectations" / "expectations_create_suite_ids.json"
    if p.exists():
        d = json.loads(p.read_text(encoding="utf-8"))
        v = d.get("suite_id")
        if isinstance(v, str) and v.strip() and _has_expectations(v):
            return v

    # 2) Optional env override
    v = getenv_str("DDM_TEST_EXPECTATIONS_SUITE_ID") or getenv_str("DDM_TEST_SUITE_ID")
    if v and _has_expectations(v):
        return v

    # 3) Fallback: find latest suite with expectations (by name)
    resp = safe_call(
        "expectations.list_suites(fallback pick)",
        lambda: client.expectations.list_suites(
            suite_name=[suite_name] if suite_name else None,
            sort="created,desc",
            page=1,
            perPage=50,
        ),
    )
    data = getattr(resp, "data", None) or []
    for s in data:
        if getattr(s, "expectations", None):
            return s.id

    pytest.skip("No suite with expectations available (run create_suite or fix env/artifact)")




@pytest.fixture(scope="session")
def suite_name() -> str:
    return getenv_str("DDM_TEST_EXPECT_SUITE_NAME", "my_suite")


@pytest.fixture(scope="session")
def expectations_out_dir() -> Path:
    p = OUT_DIR / "expectations"
    p.mkdir(parents=True, exist_ok=True)
    return p

def _dataset_id_from_artifact() -> str | None:
    p = Path("out/tests/expectations/expectations_upload_sample_ids.json")
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    v = data.get("dataset_id")
    return v if isinstance(v, str) and v else None



@pytest.fixture(scope="session")
def column_names(expectations_upload_context: dict) -> list[str]:
    return expectations_upload_context["column_names"]

@pytest.fixture(scope="session")
def column_descriptions(expectations_upload_context: dict) -> dict[str, str]:
    return expectations_upload_context["column_descriptions"]


@pytest.fixture(scope="session")
def expectations_upload_context(client, expectations_sample_file_path: Path, suite_name: str) -> dict:

    """
    Upload sample + poll tasks once.
    Returns: {dataset_id, column_names, column_descriptions}
    """

    up = safe_call(
        "expectations.upload_sample",
    lambda: client.expectations.upload_sample(str(expectations_sample_file_path), suite_name=suite_name),

    )
    assert up is not None, "upload_sample returned None"

    up_dump = up.model_dump() if hasattr(up, "model_dump") else up
    write_artifact("expectations_upload_sample_response", up_dump, subdir="expectations")

    dataset_id = getattr(up, "dataset_id", None)
    expectation_task_id = getattr(up, "expectation_task_id", None)
    description_task_id = getattr(up, "description_task_id", None)

    assert isinstance(dataset_id, str) and dataset_id
    assert isinstance(expectation_task_id, str) and expectation_task_id
    assert isinstance(description_task_id, str) and description_task_id

    write_artifact(
        "expectations_upload_sample_ids",
        {
            "dataset_id": dataset_id,
            "expectation_task_id": expectation_task_id,
            "description_task_id": description_task_id,
        },
        subdir="expectations",
    )

    st1 = poll_task_until_ready(client, expectation_task_id, timeout_s=180, interval_s=1.0)
    assert st1 is not None and st1.is_success()
    val1 = unwrap_task_value(get_task_value_fallback_from_status(client, expectation_task_id))
    write_artifact("expectation_task_value", val1, subdir="expectations")

    st2 = poll_task_until_ready(client, description_task_id, timeout_s=180, interval_s=1.0)
    assert st2 is not None and st2.is_success()

    st2_dump = st2.model_dump() if hasattr(st2, "model_dump") else (st2.__dict__ if hasattr(st2, "__dict__") else st2)
    res = st2_dump.get("result") if isinstance(st2_dump, dict) else None
    if isinstance(res, dict) and res.get("error"):
        # fallback: read columns from CSV header
        import csv
        with expectations_sample_file_path.open("r", encoding="utf-8-sig", newline="") as f:
            header = next(csv.reader(f), [])
        column_names = [h.strip() for h in header if isinstance(h, str) and h.strip()]
        column_descriptions = {c: "" for c in column_names}
        write_artifact("description_task_error", res, subdir="expectations")
    else:
        column_descriptions, column_names = parse_column_descriptions(st2_dump)

    assert column_names, "No columns available (description task failed and CSV header empty)"


    write_artifact("column_names", column_names, subdir="expectations")
    write_artifact("column_descriptions", column_descriptions, subdir="expectations")

    return {
        "dataset_id": dataset_id,
        "column_names": column_names,
        "column_descriptions": column_descriptions,
    }


@pytest.fixture(scope="session")
def dataset_id_for_suite(expectations_upload_context: dict) -> str:
    return expectations_upload_context["dataset_id"]