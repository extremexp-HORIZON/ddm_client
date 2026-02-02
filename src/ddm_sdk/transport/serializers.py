from __future__ import annotations
from typing import Any, Dict, Iterable, Set
from collections.abc import Iterable as IterableABC


def csv_param(values: Iterable[Any]) -> str:
    return ",".join(str(v) for v in values if v is not None)


def build_params(params: Dict[str, Any], *, csv_keys: Set[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    for k, v in params.items():
        if v is None:
            continue

        if isinstance(v, bool):
            out[k] = "true" if v else "false"
            continue

        if (
            k in csv_keys
            and isinstance(v, IterableABC)
            and not isinstance(v, (str, bytes))
        ):
            out[k] = csv_param(v)
        else:
            out[k] = v

    return out
