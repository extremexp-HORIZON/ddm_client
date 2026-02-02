from __future__ import annotations

import os
from ddm_sdk.client import DdmClient
from ddm_sdk.models.validations import (
    ValidateFilesAgainstSuiteRequest,
    ValidateFileAgainstSuitesRequest,
    ValidationResultCreate,
)

def main():
    client = DdmClient.from_env()

    # login
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    # ---- ids to test with ----
    suite_id = os.getenv("DDM_TEST_SUITE_ID")
    file_id = os.getenv("DDM_TEST_FILE_ID")
    file_ids_csv = os.getenv("DDM_TEST_FILE_IDS", "")
    file_ids = [x.strip() for x in file_ids_csv.split(",") if x.strip()]

    # 1) list results
    lst = client.validations.list_results(page=1, perPage=20, sort="run_time,desc", user_id=[username] if username else None)
    print("Results:", lst.total, "filtered:", getattr(lst, "filtered_total", None))

    # 2) validate many files vs one suite
    if suite_id and file_ids:
        resp = client.validations.validate_files_against_suite(
            ValidateFilesAgainstSuiteRequest(suite_id=suite_id, file_ids=file_ids)
        )
        print("Validate files vs suite:", resp.message)
        print("Tasks:", [t.task_id for t in resp.tasks])
    else:
        print("Skipping validate_files_against_suite (need DDM_TEST_SUITE_ID + DDM_TEST_FILE_IDS)")

    # 3) validate one file vs many suites
    suite_ids_csv = os.getenv("DDM_TEST_SUITE_IDS", "")
    suite_ids = [x.strip() for x in suite_ids_csv.split(",") if x.strip()]

    if file_id and suite_ids:
        resp2 = client.validations.validate_file_against_suites(
            ValidateFileAgainstSuitesRequest(file_id=file_id, suite_ids=suite_ids)
        )
        print("Validate file vs suites task:", resp2.task_id)
    else:
        print("Skipping validate_file_against_suites (need DDM_TEST_FILE_ID + DDM_TEST_SUITE_IDS)")

    # 4) save result (ONLY if your backend supports posting results)
    if os.getenv("DDM_TEST_SAVE_RESULT") == "1" and suite_id and file_id and username:
        saved = client.validations.save_result(
            ValidationResultCreate(
                user_id=username,
                suite_id=suite_id,
                dataset_name=os.getenv("DDM_TEST_DATASET_NAME", "my.csv"),
                dataset_id=file_id,
                result_summary={"ok": True},
                detailed_results={"note": "client-side test"},
                path=f"projects/{os.getenv('DDM_TEST_PROJECT_ID','projectA')}/files/{file_id}/results.json",
            )
        )
        print("Saved result id:", saved.id)
    else:
        print("Skipping save_result (set DDM_TEST_SAVE_RESULT=1 + suite/file ids)")

if __name__ == "__main__":
    main()
