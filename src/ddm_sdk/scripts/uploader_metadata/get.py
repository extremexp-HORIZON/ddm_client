from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.uploader_metadata.utils import (
    norm_project,
    require_file_id,
    store_uploader_metadata_json,
    append_log,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-uploader-meta-get", description="Get uploader metadata for a file")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", "--file-id", required=True, dest="file_id")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.uploader_metadata.get(file_id)

    if client.storage and not args.no_store:
        store_uploader_metadata_json(client, project_id, file_id, resp)
        append_log(client, project_id, file_id, action="uploader_metadata.get", ok=True)

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
