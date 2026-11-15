"""Unit tests for result-file writing on Unity Catalog Volumes, which do not support appending to files.
In that mode every file must be rewritten whole, never opened for append.
"""

from __future__ import annotations

import builtins
import io
import json

from dbxbench import storage
from dbxbench.barrier import FileBarrier
from dbxbench.results import append_jsonl, write_json
from dbxbench.telemetry import TelemetrySink, read_jsonl


def test_append_unsafe_mode_never_opens_destination_for_append(tmp_path, monkeypatch):
    """UC Volumes do not support appends; in that mode the destination is only ever fully (re)written."""
    monkeypatch.setenv("DBXBENCH_NO_APPEND", "1")
    monkeypatch.setenv("DBXBENCH_SPOOL_DIR", str(tmp_path / "spool"))
    dest = tmp_path / "vol" / "telemetry" / "w000.jsonl"
    real_open = io.open
    seen: list[tuple[str, str]] = []

    def spy(file, mode="r", *a, **k):
        seen.append((str(file), mode))
        return real_open(file, mode, *a, **k)

    monkeypatch.setattr(builtins, "open", spy)
    monkeypatch.setattr(io, "open", spy)
    tel = TelemetrySink(dest, writer_id="w000")
    for i in range(5):
        tel.emit("tick", i=i)
    append_jsonl(tmp_path / "vol" / "pubs.jsonl", {"a": 1})
    append_jsonl(tmp_path / "vol" / "pubs.jsonl", {"a": 2})
    assert [e["i"] for e in read_jsonl(dest)] == list(range(5))
    assert [json.loads(x)["a"] for x in (tmp_path / "vol" / "pubs.jsonl").read_text().splitlines()] == [1, 2]
    assert not any(f.startswith(str(tmp_path / "vol")) and "a" in m for f, m in seen)


def test_exclusive_and_barrier_in_append_unsafe_mode(tmp_path, monkeypatch):
    """Write-once files cannot be overwritten, and the barrier still works without appends."""
    monkeypatch.setenv("DBXBENCH_NO_APPEND", "1")
    p = tmp_path / "m.json"
    write_json(p, {"x": 1}, exclusive=True)
    try:
        write_json(p, {"x": 2}, exclusive=True)
        raise AssertionError("immutable file overwritten")
    except FileExistsError:
        pass
    assert json.loads(p.read_text()) == {"x": 1}
    b = FileBarrier(tmp_path / "b", 0.01)
    b.mark_ready("w0")
    assert b.coordinate({"w0"}, 1, 0)["participants"] == ["w0"]
    assert storage.append_unsafe(tmp_path) is True
