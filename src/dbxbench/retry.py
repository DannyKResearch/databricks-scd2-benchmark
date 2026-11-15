"""Bounded exponential backoff with full jitter, attempt cap and global deadline."""

from __future__ import annotations

import random
import time
import zlib
from dataclasses import dataclass, field


@dataclass
class RetryPolicy:
    max_attempts: int
    initial_backoff_ms: int
    max_backoff_ms: int
    overall_deadline_seconds: float
    per_attempt_timeout_seconds: float
    seed: int = 0
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._rng = random.Random(self.seed)

    def backoff_seconds(self, failed_attempt_number: int) -> float:
        """Full-jitter delay after the given (1-based) failed attempt."""
        cap = min(self.max_backoff_ms, self.initial_backoff_ms * (2 ** (failed_attempt_number - 1)))
        return self._rng.uniform(0, cap) / 1000.0


class Deadline:
    def __init__(self, seconds: float) -> None:
        self._clock = time.monotonic
        self.seconds = seconds
        self.start = self._clock()

    def remaining(self) -> float:
        return self.seconds - (self._clock() - self.start)

    def expired(self) -> bool:
        return self.remaining() <= 0


def stable_seed(*parts: object) -> int:
    """Process-independent seed (Python's hash() of str is randomized per process)."""
    return zlib.crc32(":".join(str(p) for p in parts).encode())
