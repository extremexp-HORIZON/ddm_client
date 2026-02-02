from __future__ import annotations

import os
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.models.file import UploadLinkBody, FileUpdateBody


def main():
    client = DdmClient.from_env()

    # login (recommended: do not store token in env)
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    project_id = os.getenv("DDM_TEST_PROJECT_ID", "projectA/sub1")

    sample_path = os.getenv("DDM_SAMPLE_PATH", "data.csv")
    if not Path(sample_path).exists():
        print(f"Sample file not found at {sample_path}. Set DDM_SAMPLE_PATH or create the file.")
        return

    # --- upload file (multipart) ---
    resp = client.file.upload(
        project_id=project_id,
        file=sample_path,
        user_filename="my_clean_data.csv",
        description="training dataset",
        use_case=["ml", "forecasting"],
        # you can pass dict or bytes; SDK should wrap it as multipart file part named "metadata-file"
        metadata_file=os.getenv("DDM_SAMPLE_METADATA_PATH"),
    )
    
    print("Uploaded:", resp.file.id, "task:", resp.file.metadata_task_id)

    # --- download ---
    blob = client.file.download(resp.file.id)  # bytes
    Path("out").mkdir(exist_ok=True)
    out_path = Path("out") / f"downloaded_{resp.file.id}.bin"
    out_path.write_bytes(blob)
    print("Downloaded to:", out_path)

    # --- update ---
    upd = client.file.update(
        resp.file.id,
        FileUpdateBody(description="updated description", use_case=["ml"]),
    )
    print("Updated description:", upd.updated_data.description)

    # --- upload from link ---
    link_url = os.getenv("DDM_TEST_LINK_URL")
    if link_url:
        link_task = client.file.upload_link(UploadLinkBody(
            file_url=link_url,
            project_id="projectB",
            description="external import",
            use_cases=["etl"],
            metadata={"source": "external"},
        ))
        print("Upload-link tasks:", link_task.fetch_task_id, link_task.process_task_id)
    else:
        print("DDM_TEST_LINK_URL not set; skipping upload_link")

    # --- delete ---
    client.file.delete(resp.file.id)
    print("Deleted:", resp.file.id)

    # --- async chunk upload (only if you have a big file and implemented wrapper) ---
    big_path = os.getenv("DDM_BIG_PATH")
    if big_path and Path(big_path).exists():
        final = client.file.upload_async(project_id="bigProject", file=big_path)
        print("Async upload done:", final.file_id, final.merge_task_id, final.metadata_task_id)
    else:
        print("DDM_BIG_PATH not set or file missing; skipping upload_async")


if __name__ == "__main__":
    main()
