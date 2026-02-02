from __future__ import annotations
import os
from pathlib import Path

from ddm_sdk.client import DdmClient

def main():
    client = DdmClient.from_env()

    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    sample_path = os.getenv("DDM_SAMPLE_FILE_PATH", r"C:\Users\orest\Downloads\result.csv")
    if not Path(sample_path).exists():
        print("Missing sample:", sample_path)
        return

    project_id = os.getenv("DDM_TEST_PROJECT_ID", "projectA/sub1")

    resp = client.files.upload(
        project_id=project_id,
        files=[sample_path],
        user_filenames=["my_clean_data.csv"],
        descriptions=["training dataset"],
        use_case=[["ml", "forecasting"]],
        # metadata_files=[r"C:\Users\orest\Downloads\metadata.json"],  # optional
    )

    print(resp.message)
    print("Uploaded count:", len(resp.files))
    print("First file id:", resp.files[0].get("id"))

if __name__ == "__main__":
    main()
