"""Invariant tests: env YAML ↔ client mapping, final-master release surfaces vs specimen/FHIR."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_FORBIDDEN_FINAL_MASTER_SUBSTR = (
    "specimen",
    "fhir",
    "genomic",
    "genomics",
    "v_diag",
)


def _load_script_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _assert_tables_exclude_specimen_fhir(names: tuple[str, ...] | list[str], label: str) -> None:
    lower_block = " ".join(names).lower()
    for sub in _FORBIDDEN_FINAL_MASTER_SUBSTR:
        assert sub not in lower_block, f"{label} must not cover specimen/FHIR/diagnostic surfaces: {sub!r}"


def test_config_motherduck_environments_matches_resolve_database(monkeypatch: pytest.MonkeyPatch) -> None:
    from motherduck_client import resolve_database_for_env

    monkeypatch.delenv("MOTHERDUCK_DATABASE", raising=False)
    monkeypatch.delenv("MOTHERDUCK_DB", raising=False)
    raw = (ROOT / "config" / "motherduck_environments.yml").read_text(encoding="utf-8")
    cfg = yaml.safe_load(raw)
    envs = cfg.get("environments", {})
    for key in ("dev", "qa", "prod"):
        expected = envs[key]["database"]
        assert resolve_database_for_env(key) == expected


def test_final_master_table_lists_exclude_specimen_fhir_surfaces() -> None:
    mod115 = _load_script_module(ROOT / "scripts" / "115_release_snapshot.py", "snap115")
    mod118 = _load_script_module(ROOT / "scripts" / "118_parquet_release_bundle.py", "bundle118")
    _assert_tables_exclude_specimen_fhir(mod115.FINAL_MASTER_TABLES, "FINAL_MASTER_TABLES")
    _assert_tables_exclude_specimen_fhir(mod115.CANONICAL_TABLES, "CANONICAL_TABLES")
    _assert_tables_exclude_specimen_fhir(mod118.FINAL_MASTER_MAIN, "FINAL_MASTER_MAIN")
    _assert_tables_exclude_specimen_fhir(mod118.FINAL_MASTER_QA, "FINAL_MASTER_QA")


def test_dev_qa_database_names_match_bootstrap_convention(monkeypatch: pytest.MonkeyPatch) -> None:
    """config/motherduck_environments.yml uses the same dev/qa naming pattern as scripts/130_md_env_bootstrap.py."""
    from motherduck_client import resolve_database_for_env

    monkeypatch.delenv("MOTHERDUCK_DATABASE", raising=False)
    monkeypatch.delenv("MOTHERDUCK_DB", raising=False)
    dev = resolve_database_for_env("dev")
    qa = resolve_database_for_env("qa")
    prefix_dev = "Thyroid 2026 Molecular Dev "
    prefix_qa = "Thyroid 2026 Molecular QA "
    assert dev.startswith(prefix_dev) and qa.startswith(prefix_qa)
    dev_tag = dev.removeprefix(prefix_dev).strip()
    qa_tag = qa.removeprefix(prefix_qa).strip()
    assert dev_tag == qa_tag and len(dev_tag) == 8 and dev_tag.isdigit()
    assert dev == f"{prefix_dev}{dev_tag}"
    assert qa == f"{prefix_qa}{qa_tag}"


def test_144_parse_args_defaults() -> None:
    mod144 = _load_script_module(
        ROOT / "scripts" / "144_md_repo_current_state_summary.py",
        "cur144",
    )
    import sys as _sys

    prev = _sys.argv
    try:
        _sys.argv = ["144_md_repo_current_state_summary.py"]
        args = mod144.parse_args()
    finally:
        _sys.argv = prev
    assert args.md is False
    assert args.stale_days == mod144.STALE_DAYS
    assert args.output == mod144.DEFAULT_OUT
