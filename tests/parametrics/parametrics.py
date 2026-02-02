from __future__ import annotations

import os
from ddm_sdk.client import DdmClient


def main() -> None:
    client = DdmClient.from_env()

    # login (recommended: don't hardcode token in env)
    username = os.getenv("DDM_USERNAME")
    password = os.getenv("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    # --- grouped file types ---
    df_types = client.parametrics.df_supported_file_types()
    all_types = client.parametrics.all_supported_file_types()

    print("DF supported types:", df_types)
    print("All supported types:", all_types)

    # --- expectations metadata ---
    cat = client.parametrics.categorized_expectations()
    # depending on your model, cat.data might be dict-like or a pydantic model
    data = getattr(cat, "data", None) or cat
    if isinstance(data, dict):
        print("Categorized expectations keys:", list(data.keys()))
    else:
        # fallback: print whatever it is
        print("Categorized expectations:", data)

    all_exp = client.parametrics.all_expectations()
    all_list = getattr(all_exp, "all_expectations", None) or getattr(all_exp, "data", None) or all_exp
    try:
        print("All expectations count:", len(all_list))
    except Exception:
        print("All expectations:", all_list)

    # --- dropdown tuples ---
    tuples = client.parametrics.suite_tuples()
    print("Suite tuples count:", len(tuples))
    if tuples:
        t0 = tuples[0]
        print("First tuple:", getattr(t0, "id", None), getattr(t0, "suite_name", None))


if __name__ == "__main__":
    main()
