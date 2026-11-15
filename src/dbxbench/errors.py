"""Error classification. We never retry blindly: only documented, classified conflict categories are
retried; everything unrecognised is terminal and recorded verbatim for analysis.

Sources for the retryable set (docs.databricks.com/aws/en/error-messages/error-classes and
.../sqlstates, retrieved 2026-11-14):
* SQLSTATE 2D521 conditions: DELTA_CONCURRENT_APPEND, DELTA_CONCURRENT_DELETE_DELETE,
  DELTA_CONCURRENT_DELETE_READ, DELTA_CONCURRENT_TRANSACTION, DELTA_CONCURRENT_WRITE,
  DELTA_CONFLICTING_METADATA_DOMAIN, DELTA_METADATA_CHANGED, DELTA_PROTOCOL_CHANGED, ...
* SQLSTATE 40001: UC_WRITE_CONFLICT ("Please retry the operation"), UC_ICEBERG_COMMIT_CONFLICT.
* SQLSTATE 40000: TRANSACTION_CONCURRENT_CATALOG_METADATA_CHANGE ("retry this transaction").
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from .sqlgen import (
    CODE_DUPLICATE,
    CODE_FENCE_LOST,
    CODE_FENCE_MISSING,
    CODE_INJECTED,
    CODE_NOT_READY,
    CODE_STAGING,
    CODE_STALE,
    MARKER,
)


class Category(StrEnum):
    RETRYABLE_CONFLICT = "RETRYABLE_CONFLICT"
    RETRYABLE_METADATA_CONFLICT = "RETRYABLE_METADATA_CONFLICT"
    ORDERING_NOT_READY = "ORDERING_NOT_READY"  # retryable wait in sequenced mode
    DUPLICATE_PUBLICATION = "DUPLICATE_PUBLICATION"  # already committed -> reconcile, never re-apply
    REJECTED_STALE = "REJECTED_STALE"  # terminal by design
    INJECTED_FAULT = "INJECTED_FAULT"  # simulated, terminal
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"  # lost reply / timeout / transport: reconcile before any retry
    UNSUPPORTED = "UNSUPPORTED"
    PERMISSION = "PERMISSION"
    SCHEMA_OR_SQL = "SCHEMA_OR_SQL"
    DATA_ERROR = "DATA_ERROR"
    SESSION_UNUSABLE = "SESSION_UNUSABLE"  # broken transaction context / stalled session: stop using it
    UNCLASSIFIED = "UNCLASSIFIED"

    @property
    def retryable(self) -> bool:
        return self in {
            Category.RETRYABLE_CONFLICT,
            Category.RETRYABLE_METADATA_CONFLICT,
            Category.ORDERING_NOT_READY,
        }


RETRYABLE_CONDITIONS = {
    "DELTA_CONCURRENT_APPEND",
    "DELTA_CONCURRENT_DELETE_DELETE",
    "DELTA_CONCURRENT_DELETE_READ",
    "DELTA_CONCURRENT_TRANSACTION",
    "DELTA_CONCURRENT_WRITE",
    "DELTA_CONFLICTING_METADATA_DOMAIN",
    "UC_WRITE_CONFLICT",
    "UC_ICEBERG_COMMIT_CONFLICT",
    "ROW_FILTER_CONCURRENT_MODIFICATION",
}
METADATA_CONDITIONS = {
    "DELTA_METADATA_CHANGED",
    "DELTA_PROTOCOL_CHANGED",
    "TRANSACTION_CONCURRENT_CATALOG_METADATA_CHANGE",
}
RETRYABLE_EXCEPTION_NAMES = {
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "ConcurrentWriteException",
}
METADATA_EXCEPTION_NAMES = {"MetadataChangedException", "ProtocolChangedException"}
UNSUPPORTED_CONDITIONS_PREFIX = ("MULTI_STATEMENT_TRANSACTION_NOT_SUPPORTED", "TRANSACTION_NOT_SUPPORTED")
PERMISSION_CONDITIONS = {"PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS", "UNAUTHORIZED_ACCESS"}
TRANSPORT_HINTS = (
    "Py4JNetworkError",
    "Py4JError: An error occurred while calling",
    "Answer from Java side is empty",
    "Connection reset",
    "StatusCode.UNAVAILABLE",
    "StatusCode.DEADLINE_EXCEEDED",
    "SESSION_CLOSED",
    "INVALID_HANDLE",
    "ConnectionRefusedError",
)

_MARKER_RE = re.compile(re.escape(MARKER) + r"([A-Z_]+)")
_SQLSTATE_RE = re.compile(r"SQLSTATE:\s*([0-9A-Z]{5})")
_COND_RE = re.compile(r"\[([A-Z][A-Z0-9_]{3,})(?:\.([A-Z0-9_]+))?\]")
_PY4J_BOILERPLATE = re.compile(r"^(An error occurred while calling \S+\.?|: )$")


class SimulatedLostAck(Exception):
    """Raised by fault injection AFTER the SQL call returned successfully: the reply was 'lost'."""


class ClientTimeout(Exception):
    """The client stopped waiting; the server-side transaction may still commit."""


@dataclass(frozen=True)
class Classified:
    category: Category
    condition: str | None
    sqlstate: str | None
    marker: str | None
    message: str
    simulated: bool

    @property
    def retryable(self) -> bool:
        return self.category.retryable


def _short_message(message: str) -> str:
    """First informative line: skips Py4J boilerplate like 'An error occurred while calling o58.sql.'."""
    for line in message.strip().splitlines():
        line = line.strip()
        if not line or _PY4J_BOILERPLATE.match(line):
            continue
        return line.removeprefix(": ")[:2000]
    return ""


def _attr(exc: BaseException, *names: str) -> str | None:
    for name in names:
        fn = getattr(exc, name, None)
        if callable(fn):
            try:
                v = fn()
            except Exception:  # noqa: S112 - best-effort extraction
                continue
            if v:
                return str(v)
    return None


def classify(exc: BaseException) -> Classified:
    message = str(exc)
    short = _short_message(message) or type(exc).__name__
    if isinstance(exc, SimulatedLostAck):
        return Classified(Category.UNKNOWN_OUTCOME, None, None, None, short, simulated=True)
    if isinstance(exc, ClientTimeout | TimeoutError):
        return Classified(Category.UNKNOWN_OUTCOME, None, None, None, short, simulated=False)

    # getErrorClass is deprecated since PySpark 4.0; only fall back to it on older runtimes
    condition = _attr(exc, "getCondition") if hasattr(exc, "getCondition") else _attr(exc, "getErrorClass")
    sqlstate = _attr(exc, "getSqlState")
    if condition is None:
        # raw Py4JJavaError (e.g. raised inside SQL scripting) carries the condition deeper in the text
        m = _COND_RE.search(message)
        if m:
            condition = m.group(1)
    if sqlstate is None:
        m2 = _SQLSTATE_RE.search(message)
        if m2:
            sqlstate = m2.group(1)
    mk = _MARKER_RE.search(message)
    marker = mk.group(1) if mk else None

    def out(cat: Category, simulated: bool = False) -> Classified:
        return Classified(cat, condition, sqlstate, marker, short, simulated)

    if "SESSION_UNUSABLE" in message or "TRANSACTION_CONTEXT_MISMATCH" in message:
        return out(Category.SESSION_UNUSABLE)
    # Our own in-transaction signals are checked first: they are deterministic and documented here.
    if marker == CODE_DUPLICATE:
        return out(Category.DUPLICATE_PUBLICATION)
    if marker == CODE_STALE:
        return out(Category.REJECTED_STALE)
    if marker == CODE_NOT_READY:
        return out(Category.ORDERING_NOT_READY)
    if marker == CODE_INJECTED:
        return out(Category.INJECTED_FAULT, simulated=True)
    if marker in {CODE_STAGING, CODE_FENCE_MISSING, CODE_FENCE_LOST}:
        return out(Category.DATA_ERROR)

    if condition in RETRYABLE_CONDITIONS or any(n in message for n in RETRYABLE_EXCEPTION_NAMES):
        return out(Category.RETRYABLE_CONFLICT)
    if condition in METADATA_CONDITIONS or any(n in message for n in METADATA_EXCEPTION_NAMES):
        return out(Category.RETRYABLE_METADATA_CONFLICT)
    if condition and condition.startswith(UNSUPPORTED_CONDITIONS_PREFIX):
        return out(Category.UNSUPPORTED)
    if condition in PERMISSION_CONDITIONS or sqlstate == "42501":
        return out(Category.PERMISSION)
    if any(h in message for h in TRANSPORT_HINTS) or type(exc).__name__ in {
        "Py4JNetworkError",
        "ConnectionError",
        "BrokenPipeError",
    }:
        return out(Category.UNKNOWN_OUTCOME)
    if sqlstate == "0A000":
        return out(Category.UNSUPPORTED)
    if sqlstate and sqlstate.startswith("42"):
        return out(Category.SCHEMA_OR_SQL)
    if (sqlstate and sqlstate.startswith(("22", "23"))) or (condition or "").startswith(
        ("DELTA_VIOLATE_", "DELTA_NOT_NULL_", "CAST_", "NUMERIC_VALUE_OUT_OF_RANGE")
    ):
        return out(Category.DATA_ERROR)
    if sqlstate == "2D521":  # documented concurrency SQLSTATE with an unlisted condition
        return out(Category.RETRYABLE_CONFLICT)
    return out(Category.UNCLASSIFIED)
