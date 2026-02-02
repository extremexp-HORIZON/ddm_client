from __future__ import annotations

import os
from pathlib import Path

from ddm_sdk.client import DdmClient


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

    # Optional list for multi calls (comma-separated)
    file_ids_env = os.getenv("DDM_TEST_FILE_IDS")
    if file_ids_env:
        file_ids = [x.strip() for x in file_ids_env.split(",") if x.strip()]
    else:
        # fallback: use single id twice if you didn't set a list
        file_ids = [file_id]

    print("Using file_id:", file_id)
    print("Using file_ids:", file_ids)

    out_dir = Path("out")
    out_dir.mkdir(exist_ok=True)

    # --- file metadata: get ---
    m = client.file_metadata.get(file_id)
    print("Metadata GET ok. Type:", type(m))
    print(m)

    # --- file metadata: get many ---
    many = client.file_metadata.get_many(file_ids)
    # Depending on your model, this might be many.metadata or just dict itself
    # Print safely:
    if hasattr(many, "metadata"):
        print("Many metadata keys:", list(many.metadata.keys()))
    else:
        print("Many response keys:", list(getattr(many, "keys", lambda: [])()))

    # --- report html ---
    html = client.file_metadata.get_report_html(file_id)
    html_path = out_dir / f"report_{file_id}.html"
    html_path.write_text(html, encoding="utf-8")
    print("Wrote HTML report:", html_path)

    # --- reports zip ---
    zip_bytes = client.file_metadata.download_reports_zip(file_ids)
    zip_path = out_dir / "reports.zip"
    zip_path.write_bytes(zip_bytes)
    print("Wrote ZIP:", zip_path)


if __name__ == "__main__":
    main()
