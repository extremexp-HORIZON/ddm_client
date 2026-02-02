from __future__ import annotations

import os
from ddm_sdk.client import DdmClient
from ddm_sdk.models.uploader_metadata import UploaderMetadataJSON


def main():
    client = DdmClient.from_env()

    # login (recommended)
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    file_id = os.getenv("DDM_TEST_FILE_ID")
    if not file_id:
        print("Set DDM_TEST_FILE_ID in .env")
        return

    # --- attach uploader metadata ---
    attach_resp = client.uploader_metadata.attach(
        file_id,
        UploaderMetadataJSON(uploader_metadata={"sensor": "A1"})
    )
    print("Attached:", attach_resp.message)

    # --- get uploader metadata ---
    meta = client.uploader_metadata.get(file_id)
    print("Current metadata:", meta.uploader_metadata)

    # --- update uploader metadata ---
    upd = client.uploader_metadata.update(
        file_id,
        {"uploader_metadata": {"sensor": "A2"}}
    )
    print("Updated:", upd.message)

    # --- delete uploader metadata ---
    client.uploader_metadata.delete(file_id)
    print("Deleted uploader metadata")


if __name__ == "__main__":
    main()
