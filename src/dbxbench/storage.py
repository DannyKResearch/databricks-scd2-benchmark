"""File writes that are safe on UC Volumes.

Databricks docs (volumes/volume-files): on ``/Volumes`` paths "Direct-append or non-sequential (random)
writes are not supported". Rename and O_EXCL semantics are not documented. Therefore, for such
destinations:

* append  -> append to a local spool file, then sequentially copy the whole spool over the destination
             (the destination is always a complete prefix of the log, so readers tolerate a crash);
* exclusive create -> existence check + sequential write (single writer per path by construction);
* atomic replace   -> plain sequential overwrite.

Local paths keep real append / O_EXCL / rename semantics.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
import threading
from pathlib import Path

_LOCK = threading.Lock()


def append_unsafe(path: Path) -> bool:
    return str(path).startswith("/Volumes/") or os.environ.get("DBXBENCH_NO_APPEND") == "1"


def _spool_for(path: Path) -> Path:
    root = Path(os.environ.get("DBXBENCH_SPOOL_DIR", Path(tempfile.gettempdir()) / "dbxbench_spool"))
    root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(str(path.resolve() if not append_unsafe(path) else path).encode()).hexdigest()[:24]
    return root / f"{digest}.{path.name}"


def append_text(path: Path, text: str, fsync: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not append_unsafe(path):
        with path.open("a", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            if fsync:
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass
        return
    with _LOCK:
        spool = _spool_for(path)
        if not spool.exists() and path.exists():
            shutil.copyfile(path, spool)  # continue an existing log written by this process earlier
        with spool.open("a", encoding="utf-8") as f:
            f.write(text)
        shutil.copyfile(spool, path)  # sequential full write; never an append on the volume


def write_text(path: Path, text: str, *, exclusive: bool = False) -> bool:
    """Write a whole file. Returns False if ``exclusive`` and the file already exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if append_unsafe(path):
        if exclusive and path.exists():
            return False
        with path.open("w", encoding="utf-8") as f:
            f.write(text)
        return True
    if exclusive:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        return True
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    return True
