"""Structured JSON logging with a defensive secret-redaction filter."""

from __future__ import annotations

import json
import logging
import re
import sys
from typing import Any

_SECRET = re.compile(r"(dapi[0-9a-f]{20,}|AKIA[0-9A-Z]{16}|(?i:bearer)\s+[A-Za-z0-9._\-]{20,})")


def redact(text: str) -> str:
    return _SECRET.sub("<redacted>", text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact(record.getMessage()),
        }
        if record.exc_info:
            payload["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    if any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("py4j", "urllib3", "databricks.sdk"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
