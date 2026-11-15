from __future__ import annotations

from .config import BenchConfig, TransactionMode
from .sqlgen import DATABRICKS, LOCAL_EMULATION, Dialect


def dialect_for(cfg: BenchConfig) -> Dialect:
    if cfg.compute.transaction_mode is TransactionMode.BEGIN_ATOMIC:
        return DATABRICKS
    return LOCAL_EMULATION


def atomicity_evidence(cfg: BenchConfig) -> bool:
    """Only real BEGIN ATOMIC runs can ever count as evidence of atomicity."""
    return cfg.compute.transaction_mode is TransactionMode.BEGIN_ATOMIC
