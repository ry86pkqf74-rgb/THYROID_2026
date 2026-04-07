"""Invariant tests: final-master release artifacts vs specimen/FHIR surfaces.

``115_release_snapshot --final-master`` and ``118_parquet_release_bundle --final-master``
intentionally carry the **manuscript analytic** slice (canonical cores, longitudinal
labs, ``master_*_verified_v1``). Specimen + FHIR objects live in ``main`` and are
gated by ``119_md_formalization_validate.py`` (Check 13), not duplicated into every
``release_*`` schema or Parquet bundle row -- see ``docs/specimen_fhir_contract_review.md``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def _load_module(name: str, rel_path: str):
    path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod_115():
    return _load_module("release_snapshot_115", "scripts/115_release_snapshot.py")


@pytest.fixture(scope="module")
def mod_118():
    return _load_module("parquet_bundle_118", "scripts/118_parquet_release_bundle.py")


def _is_specimen_or_fhir_table(name: str) -> bool:
    n = name.lower()
    return n.startswith("specimen_") or n.startswith("fhir_")


def test_final_master_tables_exclude_specimen_fhir_snapshot(mod_115):
    tables = tuple(mod_115.FINAL_MASTER_TABLES)
    offenders = [t for t in tables if _is_specimen_or_fhir_table(t)]
    assert not offenders, f"Unexpected specimen/FHIR in 115 FINAL_MASTER_TABLES: {offenders}"


def test_final_master_parquet_main_excludes_specimen_fhir(mod_118):
    tables = tuple(mod_118.FINAL_MASTER_MAIN)
    offenders = [t for t in tables if _is_specimen_or_fhir_table(t)]
    assert not offenders, f"Unexpected specimen/FHIR in 118 FINAL_MASTER_MAIN: {offenders}"
