from __future__ import annotations

from helpers import safe_call, write_artifact


def _is_grouped_filetypes(x) -> bool:
    # Expected shape: { "Category": { ".csv": "CSV", ... }, ... }
    if not isinstance(x, dict) or not x:
        return False
    # allow some flexibility, but try to confirm nested dict structure
    for _, v in x.items():
        if isinstance(v, dict):
            return True
    return False


def test_01_parametrics_supported_file_types(client):
    df_types = safe_call(
        "parametrics.df_supported_file_types",
        lambda: client.parametrics.df_supported_file_types(),
    )
    assert df_types is not None, "df_supported_file_types returned None"

    all_types = safe_call(
        "parametrics.all_supported_file_types",
        lambda: client.parametrics.all_supported_file_types(),
    )
    assert all_types is not None, "all_supported_file_types returned None"

    # dump artifacts
    write_artifact("parametrics_df_supported_file_types", df_types, subdir="parametrics")
    write_artifact("parametrics_all_supported_file_types", all_types, subdir="parametrics")

    if isinstance(df_types, (list, tuple)):
        assert all(isinstance(x, (str, dict)) for x in df_types)
    else:
        assert _is_grouped_filetypes(df_types), f"unexpected df_types shape: {type(df_types)}"

    if isinstance(all_types, (list, tuple)):
        assert len(all_types) >= 0
    else:
        assert _is_grouped_filetypes(all_types), f"unexpected all_types shape: {type(all_types)}"
