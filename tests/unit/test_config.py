"""Unit tests for configuration validation: unknown keys, unsafe identifiers, secrets in YAML, the documented
100-tables-per-transaction cap, local-vs-Databricks transaction modes, CLI overrides and the config hash.
"""

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError
from tests.conftest import EXAMPLE, LOCAL

from dbxbench.config import (
    BenchConfig,
    OrderingMode,
    TransactionMode,
    apply_overrides,
    load_config,
)


def raw_example():
    return yaml.safe_load(EXAMPLE.read_text())


def test_example_and_local_configs_validate():
    """The shipped Databricks and local configs load, with the expected transaction modes."""
    ex = load_config(EXAMPLE)
    assert ex.compute.transaction_mode is TransactionMode.BEGIN_ATOMIC
    assert OrderingMode.OFF in ex.concurrency.strict_ordering_modes
    lo = load_config(LOCAL)
    assert lo.compute.transaction_mode is TransactionMode.LOCAL_NONATOMIC_EMULATION
    assert lo.is_local


def test_unknown_key_is_an_error():
    """A misspelled key fails validation instead of being silently ignored."""
    raw = raw_example()
    raw["workload"]["tabel_count"] = 3
    with pytest.raises(ValidationError, match="tabel_count"):
        BenchConfig.model_validate(raw)


def test_placeholder_catalog_rejected():
    """Placeholder catalog names are rejected so a run never targets a catalog by accident."""
    raw = raw_example()
    raw["project"]["catalog"] = "replace_me"
    with pytest.raises(ValidationError, match="authorized Unity Catalog"):
        BenchConfig.model_validate(raw)


@pytest.mark.parametrize("bad", ["main; DROP", "Main", "a-b", ""])
def test_catalog_identifier_allowlist(bad):
    """Catalog names that could inject SQL or break quoting are rejected."""
    raw = raw_example()
    raw["project"]["catalog"] = bad
    with pytest.raises(ValidationError):
        BenchConfig.model_validate(raw)


def test_secrets_in_yaml_rejected(tmp_path):
    """Secret-looking keys and token-shaped values in the YAML are rejected."""
    raw = raw_example()
    raw["project"]["token"] = "x"
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="looks like a secret"):
        load_config(p)
    raw = raw_example()
    raw["project"]["name"] = "dapi" + "0123456789abcdef0123456789abcdef"
    p.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="credential"):
        load_config(p)


def test_transaction_table_cap_enforced():
    """Tables accessed per transaction (targets + staging + ledger + fence) cannot exceed the documented cap of 100."""
    raw = raw_example()
    raw["workload"]["table_count"] = 49  # 2*49+2 = 100 tables accessed: allowed at the documented cap
    BenchConfig.model_validate(raw)
    raw["project"]["max_tables_per_transaction"] = 50
    with pytest.raises(ValidationError, match="tables accessed per transaction"):
        BenchConfig.model_validate(raw)
    raw = raw_example()
    raw["project"]["max_tables_per_transaction"] = 101
    with pytest.raises(ValidationError, match="documented cap"):
        BenchConfig.model_validate(raw)


def test_begin_atomic_cannot_be_used_locally_and_emulation_only_locally():
    """BEGIN ATOMIC needs Databricks; non-atomic emulation is only allowed locally."""
    raw = raw_example()
    raw["compute"]["preferred_mode"] = "local"
    raw["concurrency"]["launcher"] = "local_process"
    with pytest.raises(ValidationError, match="begin_atomic cannot run on local"):
        BenchConfig.model_validate(raw)
    raw = raw_example()
    raw["compute"]["transaction_mode"] = "local_nonatomic_emulation"
    with pytest.raises(ValidationError, match=r"only allowed with compute\.preferred_mode=local"):
        BenchConfig.model_validate(raw)


def test_inline_launcher_cannot_claim_concurrency():
    """The in-process launcher cannot be used for multi-writer cells, which need independent writers."""
    raw = raw_example()
    raw["concurrency"]["launcher"] = "inline"
    with pytest.raises(ValidationError, match="independent concurrent writers"):
        BenchConfig.model_validate(raw)


def test_yaml_off_boolean_is_coerced():
    """YAML parses an unquoted `off` as False; it is mapped back to ordering mode 'off'."""
    raw = raw_example()
    raw["concurrency"]["strict_ordering_modes"] = [False, "sequenced"]
    raw["benchmark"]["ordering_mode"] = False
    cfg = BenchConfig.model_validate(raw)
    assert cfg.concurrency.strict_ordering_modes[0] is OrderingMode.OFF
    assert cfg.benchmark.ordering_mode is OrderingMode.OFF


def test_staging_must_be_materialized_before_timing():
    """Staging data must be generated before timing starts, so generation never inflates publication times."""
    raw = raw_example()
    raw["workload"]["staging_materialized_before_timing"] = False
    with pytest.raises(ValidationError):
        BenchConfig.model_validate(raw)


def test_overrides_and_hash():
    """CLI overrides apply, unknown override keys fail, and the config hash tracks the effective config."""
    raw = raw_example()
    out = apply_overrides(raw, {"tables": 5, "writers": [1, 10], "formats": ["delta"]})
    cfg = BenchConfig.model_validate(out)
    assert cfg.workload.table_count == 5 and cfg.concurrency.writer_counts == [1, 10]
    assert cfg.config_hash() == BenchConfig.model_validate(out).config_hash()
    assert cfg.config_hash() != load_config(EXAMPLE).config_hash()
    with pytest.raises(KeyError):
        apply_overrides(raw, {"nope": 1})
