from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.models.uploader_metadata import UploaderMetadataJSON
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.uploader_metadata.utils import (
    norm_project,
    require_file_id,
    store_uploader_metadata_json,
    append_log,
    _load_json_arg,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-uploader-meta-attach", description="Attach uploader metadata to a file")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", "--file-id", required=True, dest="file_id")
    ap.add_argument("--json", default=None, help='Inline JSON object, e.g. \'{"sensor":"A1"}\'')
    ap.add_argument("--json-file", default=None, help="Path to JSON file containing an object")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)
    meta_obj = _load_json_arg(args.json, args.json_file)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    body = UploaderMetadataJSON(uploader_metadata=meta_obj)
    resp = client.uploader_metadata.attach(file_id, body)

    if client.storage and not args.no_store:
        # also fetch current metadata so uploader_metadata.json is the real current state
        current = client.uploader_metadata.get(file_id)
        store_uploader_metadata_json(client, project_id, file_id, current)
        append_log(client, project_id, file_id, action="uploader_metadata.attach", ok=True, details={"set": meta_obj})

    print(json.dumps(resp.model_dump(mode="json", exclude_none=True), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
