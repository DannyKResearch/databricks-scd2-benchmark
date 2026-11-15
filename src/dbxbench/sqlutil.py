"""SQL identifier allowlisting, quoting and literal escaping.

Databricks SQL has no client-side parameter binding for a SQL script submitted through
``spark.sql`` that we can rely on for identifiers, so every identifier is validated against a
strict allowlist *and* backtick-quoted, and every literal is rendered by a typed function.
Free text never reaches SQL except through :func:`sql_string`, which rejects control characters
and escapes quotes/backslashes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.:\-]{1,200}$")


class UnsafeSqlError(ValueError):
    pass


def check_ident(name: str) -> str:
    if not isinstance(name, str) or not _IDENT.match(name):
        raise UnsafeSqlError(f"identifier {name!r} is not in the allowlist {_IDENT.pattern}")
    return name


def quote_ident(name: str) -> str:
    return f"`{check_ident(name)}`"


@dataclass(frozen=True, order=True)
class TableRef:
    catalog: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        for part in (self.catalog, self.schema, self.name):
            check_ident(part)

    @property
    def sql(self) -> str:
        return ".".join(quote_ident(p) for p in (self.catalog, self.schema, self.name))

    @property
    def plain(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.name}"

    @staticmethod
    def parse(text: str) -> TableRef:
        parts = text.replace("`", "").split(".")
        if len(parts) != 3:
            raise UnsafeSqlError(f"expected catalog.schema.table, got {text!r}")
        return TableRef(*parts)


def sql_string(value: str) -> str:
    if not isinstance(value, str):
        raise UnsafeSqlError(f"expected str literal, got {type(value).__name__}")
    if any(ord(c) < 32 and c not in "\t" for c in value):
        raise UnsafeSqlError("string literal contains control characters")
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def sql_token(value: str) -> str:
    """A literal restricted to a conservative token charset (ids, run ids, publication ids)."""
    if not _SAFE_TOKEN.match(value):
        raise UnsafeSqlError(f"token {value!r} does not match {_SAFE_TOKEN.pattern}")
    return sql_string(value)


def sql_int(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise UnsafeSqlError(f"expected int literal, got {value!r}")
    return str(value)


def sql_bool(value: bool) -> str:
    if not isinstance(value, bool):
        raise UnsafeSqlError(f"expected bool literal, got {value!r}")
    return "TRUE" if value else "FALSE"


def sql_timestamp(value: datetime) -> str:
    """Render an explicit-UTC TIMESTAMP literal with microsecond precision.

    The literal carries an explicit ``+00:00`` offset so it does not depend on the session time zone.
    """
    if value.tzinfo is None:
        raise UnsafeSqlError("timestamps must be timezone-aware (UTC)")
    utc = value.astimezone(UTC)
    return f"TIMESTAMP'{utc.strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00'"


def sql_string_array(values: list[str]) -> str:
    return "array(" + ", ".join(sql_token(v) for v in values) + ")"


def parse_utc(text: str) -> datetime:
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp {text!r} has no timezone")
    return dt.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
