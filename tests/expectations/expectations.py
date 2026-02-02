from __future__ import annotations

import os
from ddm_sdk.client import DdmClient
from ddm_sdk.models.expectations import ExpectationSuiteCreate


def main():
    client = DdmClient.from_env()

    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    # -------------------------
    # upload sample (required)
    # -------------------------
    sample_path = os.getenv("DDM_EXPECTATIONS_SAMPLE_FILE_PATH", "data/sample.csv")
    if not os.path.exists(sample_path):
        raise SystemExit(
            f"Sample file not found at {sample_path}. "
            f"Set DDM_EXPECTATIONS_SAMPLE_FILE_PATH in .env or create the file."
        )

    up = client.expectations.upload_sample(sample_path, suite_name="my_suite")
    print("Uploaded sample:", up.dataset_id, up.expectation_task_id)

    # -------------------------
    # list suites
    # -------------------------
    lst = client.expectations.list_suites(page=1, perPage=10, sort="created,desc")
    print("Suites:", lst.total, [s.suite_name for s in lst.data])

    # -------------------------
    # create suite (needs dataset_id)
    # -------------------------
    suite = ExpectationSuiteCreate(
        suite_name="my_suite",
        dataset_id=up.dataset_id,  
        file_types=["csv"],
        expectations={"expectations": []},
        user_id=username,
        category="dataset",
    )

    created = client.expectations.create_suite(suite)
    print("Suite created:", created.suite_id, created.task_id)

    detail = client.expectations.get_suite(created.suite_id)
    print("Suite detail:", detail.suite_name, detail.created)


if __name__ == "__main__":
    main()
