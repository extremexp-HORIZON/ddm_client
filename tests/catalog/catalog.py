from __future__ import annotations

import os
from ddm_sdk.client import DdmClient


def main():
    client = DdmClient.from_env()

    # login (token will be set on the client)
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    # --- catalog list ---
    paged = client.catalog.list(
        page=1,
        perPage=10,
        sort="created,desc",
    )
    print("Catalog list:", len(paged.data), "of", paged.total, "(filtered:", paged.filtered_total, ")")
    for f in paged.data:
        print(" -", f.id, f.filename, f.project_id, f.created)

    # --- tree ---
    tree = client.catalog.tree(parent="", page=0, perPage=20, sort="name,asc")
    print("Tree totalRecords:", tree.totalRecords)
    if tree.nodes:
        n0 = tree.nodes[0]
        d0 = n0.data
        d = d0.model_dump() if hasattr(d0, "model_dump") else dict(d0)
        print("First node:", n0.key, d.get("name"), d.get("type"), d.get("size"))


    # --- advanced ---
    # NOTE: your backend File.filter_files expects:
    #  - user_id: list (IN)
    #  - metadata: truthy triggers file_metadata != None
    res = client.catalog.advanced({"metadata": True, "user_id": ["alice"]})
    print("Advanced result count:", len(res))


if __name__ == "__main__":
    main()
