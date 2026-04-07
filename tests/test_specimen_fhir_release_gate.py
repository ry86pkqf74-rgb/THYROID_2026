"""Offline tests for release orchestration alignment with 119 Check 13."""

from __future__ import annotations

from pathlib import Path

import duckdb

from utils.specimen_fhir_release_gate import (
    SpecimenFhirGateDecision,
    assess_specimen_fhir_gate,
    decide_specimen_fhir_gate,
    run_specimen_fhir_release_gate,
    specimen_fhir_release_gate,
)


def test_assess_no_anchor() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.other (x INT);")
    a = assess_specimen_fhir_gate(con)
    assert not a.gate_applies
    assert not a.is_satisfied  # N/A until anchor exists
    con.close()


def test_assess_missing_tables() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.synoptic_tumor_long_v1 (research_id BIGINT);")
    a = assess_specimen_fhir_gate(con)
    assert a.gate_applies
    assert a.needs_full_materialization
    assert not a.needs_diagnostics_deploy_only
    con.close()


def test_decide_fail_early_final_release_without_materialize() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.synoptic_tumor_long_v1 (research_id BIGINT);")
    a = assess_specimen_fhir_gate(con)
    assert (
        decide_specimen_fhir_gate(
            a, enforce=True, materialize=False, skip_gate=False
        )
        == SpecimenFhirGateDecision.FAIL_EARLY
    )
    con.close()


def test_decide_skip_gate() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.synoptic_tumor_long_v1 (research_id BIGINT);")
    a = assess_specimen_fhir_gate(con)
    assert (
        decide_specimen_fhir_gate(
            a, enforce=True, materialize=False, skip_gate=True
        )
        == SpecimenFhirGateDecision.WARN_CONTINUE
    )
    con.close()


def test_decide_non_enforce_warn_path() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE main.synoptic_tumor_long_v1 (research_id BIGINT);")
    a = assess_specimen_fhir_gate(con)
    assert (
        decide_specimen_fhir_gate(
            a, enforce=False, materialize=False, skip_gate=False
        )
        == SpecimenFhirGateDecision.WARN_CONTINUE
    )
    con.close()


def test_run_gate_invokes_runner_for_143(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA qa;")
    con.execute("CREATE TABLE main.synoptic_tumor_long_v1 (research_id BIGINT);")
    for t in (
        "specimen_master_v1",
        "specimen_tumor_focus_v1",
        "specimen_genomic_assay_v1",
        "specimen_source_xref_v1",
        "fhir_patient_deid_map_v1",
        "fhir_specimen_v1",
        "fhir_procedure_collection_v1",
        "fhir_encounter_v1",
        "fhir_episode_of_care_v1",
        "fhir_bundle_specimen_export_v1",
    ):
        con.execute(f"CREATE TABLE main.{t} (stub INT);")

    calls: list[tuple[str, list[str]]] = []

    def runner(name: str, cmd: list[str], log: Path) -> bool:
        calls.append((name, cmd))
        log.write_text("ok", encoding="utf-8")
        return True

    steps: list[dict] = []
    ok = run_specimen_fhir_release_gate(
        con,
        enforce=True,
        materialize=True,
        skip_gate=False,
        dry_run=False,
        materialize_target_md=False,
        audit_dir=tmp_path,
        step_results=steps,
        py="python",
        scripts_dir=Path("/scripts"),
        runner=runner,
        now_iso=lambda: "t",
    )
    assert ok
    assert len(calls) == 1
    assert "143" in calls[0][0]
    assert calls[0][1][-1] != "--md"
    con.close()


def test_module_alias_export() -> None:
    assert specimen_fhir_release_gate is run_specimen_fhir_release_gate
