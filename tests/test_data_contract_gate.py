"""Offline tests for scripts/145_data_contract_gate.py — fixtures only, no MotherDuck."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_gate():
    p = ROOT / "scripts" / "145_data_contract_gate.py"
    name = "gate145"
    spec = importlib.util.spec_from_file_location(name, p)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def gate145():
    return _load_gate()


def test_foreign_key_not_in_spine(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = dict(g.load_contract(cpath))
    contract["foreign_keys"] = [
        {
            "name": "patient_spine",
            "local_columns": ["research_id"],
            "ref_columns": ["research_id"],
            "ref_csv": "tests/fixtures/data_contract_gate/patient_spine.csv",
        }
    ]
    df = _minimal_lab_df(1)
    df.at[0, "research_id"] = 9999
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(x.rule_id.startswith("fk_not_found") for x in v)


def test_missing_column(gate145, tmp_path: Path) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    df = pd.DataFrame(
        {
            "research_id": [1],
            "lab_date": [date(2020, 1, 1)],
        }
    )
    rules = [0]
    v = g.validate_all(df, contract, rules_run_counter=rules)
    assert any(x.rule_id == "missing_column" for x in v)


def test_type_drift_int(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    df = _minimal_lab_df(1)
    df["research_id"] = df["research_id"].astype(object)
    df.at[0, "research_id"] = "not_int"
    rules = [0]
    v = g.validate_all(df, contract, rules_run_counter=rules)
    assert any(x.rule_id == "dtype_int" for x in v)


def test_enum_violation(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    df = _minimal_lab_df(1)
    df.loc[0, "data_completeness_tier"] = "invalid_tier"
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(x.rule_id == "enum_violation" for x in v)


def test_numeric_bound(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    df = _minimal_lab_df(1)
    df.loc[0, "lab_name_standardized"] = "thyroglobulin"
    df.loc[0, "value_numeric"] = 200_000.0
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(x.rule_id == "numeric_bound_max" for x in v)


def test_future_date(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    df = _minimal_lab_df(1)
    df.loc[0, "lab_date"] = date(2099, 6, 1)
    rules = [0]
    df = df.reset_index(drop=True)
    v = g.validate_all(df, contract, rules_run_counter=rules)
    assert any(x.rule_id == "future_date" for x in v)


def test_duplicate_composite(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "longitudinal_lab_canonical_v1.yaml"
    contract = g.load_contract(cpath)
    row = _minimal_lab_row()
    df = pd.DataFrame([row, row])
    df = df.reset_index(drop=True)
    rules = [0]
    v = g.validate_all(df, contract, rules_run_counter=rules)
    assert any("duplicate_composite" in x.rule_id for x in v)


def test_provenance_required_canonical(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "canonical_extracted_fact_long_v2.yaml"
    contract = g.load_contract(cpath)
    df = pd.DataFrame(
        [
            {
                "research_id": 1,
                "note_row_id": 10,
                "entity_type": "foo",
                "entity_value_raw": None,
                "entity_value_norm": None,
                "entity_date": None,
                "note_date": None,
                "extraction_run_id": "",
                "extracted_at": "2026-01-01T00:00:00Z",
                "source_file_id": None,
            }
        ]
    )
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(
        x.rule_id in ("provenance_required", "non_null_violation") for x in v
    )


def test_linkage_discordant_laterality(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "imaging_fna_linkage_prep_v1.yaml"
    contract = g.load_contract(cpath)
    df = pd.DataFrame(
        [
            {
                "research_id": 1,
                "imaging_exam_id": "e1",
                "fna_episode_id": 1,
                "imaging_laterality": "left",
                "fna_laterality": "right",
                "imaging_exam_date": date(2024, 1, 1),
                "fna_date": date(2024, 1, 5),
                "surgery_date": date(2024, 6, 1),
                "source_file_id": None,
                "extraction_run_id": "run-1",
            }
        ]
    )
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(x.rule_id == "linkage_discordant_laterality" for x in v)


def test_linkage_fna_after_surgery(gate145) -> None:
    g = gate145
    cpath = ROOT / "config" / "data_contracts" / "imaging_fna_linkage_prep_v1.yaml"
    contract = g.load_contract(cpath)
    df = pd.DataFrame(
        [
            {
                "research_id": 1,
                "imaging_exam_id": "e1",
                "fna_episode_id": 1,
                "imaging_laterality": "left",
                "fna_laterality": "left",
                "imaging_exam_date": date(2024, 1, 1),
                "fna_date": date(2024, 8, 1),
                "surgery_date": date(2024, 6, 1),
                "source_file_id": None,
                "extraction_run_id": "run-1",
            }
        ]
    )
    rules = [0]
    v = g.validate_all(df.reset_index(drop=True), contract, rules_run_counter=rules)
    assert any(x.rule_id == "linkage_fna_after_surgery" for x in v)


def test_audit_hash_chain(gate145) -> None:
    g = gate145
    df = pd.DataFrame({"research_id": [1, 2]})
    viols = [
        g.Violation(
            row_index=0,
            column_name="a",
            rule_id="r1",
            severity="error",
            observed_value="x",
            expected_constraint="y",
            suggested_fix="z",
            row_locator={},
            source_file_id=None,
            extraction_run_id="e1",
        ),
        g.Violation(
            row_index=1,
            column_name="b",
            rule_id="r2",
            severity="error",
            observed_value="x",
            expected_constraint="y",
            suggested_fix="z",
            row_locator={},
            source_file_id=None,
            extraction_run_id=None,
        ),
    ]
    events = g.build_audit_events("run-test", "ds", viols, df, "2026-01-01T00:00:00Z")
    assert len(events) == 2
    assert events[0]["prev_event_hash"] == g.GENESIS_HASH
    assert events[1]["prev_event_hash"] == events[0]["event_hash"]
    h1 = events[0]["event_hash"]
    body0 = {
        k: v for k, v in events[0].items() if k != "event_hash"
    }
    canon = json.dumps(body0, sort_keys=True, default=str, ensure_ascii=True)
    assert hashlib.sha256(canon.encode("utf-8")).hexdigest() == h1


def _minimal_lab_row() -> dict:
    return {
        "research_id": 1,
        "lab_date": date(2020, 1, 1),
        "lab_date_status": "exact_collection_date",
        "lab_name_standardized": "thyroglobulin",
        "value_numeric": 1.0,
        "data_completeness_tier": "current_structured",
        "source_table": "t",
        "source_script": "s",
        "ingestion_wave": "w",
    }


def _minimal_lab_df(n: int) -> pd.DataFrame:
    row = _minimal_lab_row()
    return pd.DataFrame([row.copy() for _ in range(n)])


def test_main_cli_offline_writes_artifacts(tmp_path: Path) -> None:
    df = pd.DataFrame([_minimal_lab_row()])
    inp = tmp_path / "in.parquet"
    df.to_parquet(inp)
    out = tmp_path / "out"
    import subprocess

    r = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "145_data_contract_gate.py"),
            "--contract-name",
            "longitudinal_lab_canonical_v1",
            "--input-path",
            str(inp),
            "--output-dir",
            str(out),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    assert (out / "violations.parquet").exists()
    assert (out / "audit_events.jsonl").exists()
    assert (out / "run_metrics.json").exists()
    assert (out / "summary.md").exists()
