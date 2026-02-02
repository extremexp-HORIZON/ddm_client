from __future__ import annotations

from dataclasses import dataclass
from typing import IO, Iterator, Optional, Tuple, Union
import os
import math


BytesSource = Union[str, bytes, IO[bytes]]


@dataclass
class FilePart:
    field_name: str
    filename: str
    content: bytes
    content_type: Optional[str] = None


def read_bytes(source: BytesSource, *, rewind: bool = False) -> bytes:
    if isinstance(source, bytes):
        return source
    if isinstance(source, str):
        with open(source, "rb") as f:
            return f.read()

    # file-like
    if rewind and hasattr(source, "seek"):
        try:
            source.seek(0)
        except Exception:
            pass
    return source.read()


def guess_filename(source: BytesSource, fallback: str) -> str:
    if isinstance(source, str):
        base = os.path.basename(source)
        return base or fallback

    # Try file-like .name if present
    name = getattr(source, "name", None)
    if isinstance(name, str) and name:
        base = os.path.basename(name)
        if base:
            return base

    return fallback


def iter_chunks(data: bytes, chunk_size: int) -> Iterator[Tuple[int, bytes]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    for idx in range(0, len(data), chunk_size):
        yield (idx // chunk_size), data[idx : idx + chunk_size]


def count_chunks(nbytes: int, chunk_size: int) -> int:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    return int(math.ceil(nbytes / chunk_size)) if nbytes > 0 else 0


def iter_file_chunks(fileobj: IO[bytes], chunk_size: int) -> Iterator[Tuple[int, bytes]]:
    """
    Stream chunks from a file-like object without reading it all into memory.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    idx = 0
    while True:
        chunk = fileobj.read(chunk_size)
        if not chunk:
            break
        yield idx, chunk
        idx += 1
