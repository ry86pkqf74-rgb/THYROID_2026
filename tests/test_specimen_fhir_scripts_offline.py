"""Offline CI tests for specimen/FHIR export, QA diagnostics deploy, and repo state summary."""

from __future__ import annotations

import json
import importlib.util
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"

EXPECTED_QA_DIAG_VIEWS = (
    "v_diag_specimen_duplicate_master_fp_v1",
    "v_diag_specimen_orphan_genomic_master_v1",
    "v_diag_specimen_fhir_broken_refs_v1",
    "v_diag_specimen_provenance_master_v1",
    "v_diag_specimen_provenance_genomic_v1",
    "v_diag_specimen_review_burden_v1",
)


def _load_script_141():
    path = SCRIPTS / "141_fhir_specimen_json_export.py"
    spec = importlib.util.spec_from_file_location("fhir_specimen_json_export_141", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_script_144():
    path = SCRIPTS / "144_md_repo_current_state_summary.py"
    spec = importlib.util.spec_from_file_location("md_repo_current_state_144", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_141_export_offline_minimal_db(tmp_path: Path) -> None:
    """Temp DB + :func:`run_export` writes NDJSON + manifest with correct metadata."""
    mod = _load_script_141()
    db_path = tmp_path / "export.duckdb"
    out_root = tmp_path / "out"
    con = duckdb.connect(str(db_path))
    try:
        for name in mod.FHIR_TABLES:
            con.execute(f"CREATE TABLE main.{name} (stub INT)")
        con.execute("DROP TABLE main.fhir_bundle_specimen_export_v1")
        con.execute(
            "CREATE TABLE main.fhir_bundle_specimen_export_v1 "
            "(specimen_id VARCHAR, bundle_json VARCHAR)"
        )
        bundle = json.dumps(
            {"resourceType": "Bundle", "type": "collection", "entry": []},
            separators=(",", ":"),
        )
        con.execute(
            "INSERT INTO main.fhir_bundle_specimen_export_v1 VALUES (?, ?)",
            ["spec-1", bundle],
        )
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        _, manifest = mod.run_export(
            con, output_root=out_root, limit=0, git_sha="test-sha-abc"
        )
    finally:
        con.close()

    out_dirs = list(out_root.glob("fhir_specimen_*"))
    assert len(out_dirs) == 1
    out_dir = out_dirs[0]
    nd = out_dir / "specimen_bundles.ndjson"
    assert nd.is_file()
    lines = nd.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["resourceType"] == "Bundle"
    assert obj["type"] == "collection"

    mf_path = out_dir / "manifest.json"
    loaded = json.loads(mf_path.read_text(encoding="utf-8"))
    assert loaded["bundle_row_count"] == 1
    assert loaded["git_sha"] == "test-sha-abc"
    assert loaded["export_kind"] == "specimen_fhir_analytic_v1"
    assert loaded["custom_user_agent"] == mod.UA
    stm = loaded["source_tables_main"]
    assert stm["fhir_bundle_specimen_export_v1"] == 1
    for tbl in mod.FHIR_TABLES:
        assert tbl in stm
    assert all(stm[t] == 0 for t in mod.FHIR_TABLES if t != "fhir_bundle_specimen_export_v1")
    readme = (out_dir / "README.md").read_text(encoding="utf-8")
    assert "specimen_fhir_export_v1" in readme
    assert "fhir_bundle_specimen_export_v1" in readme


def test_141_unknown_git_sha_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When absent, manifest still records unknown git SHA like the CLI helper."""
    mod = _load_script_141()
    monkeypatch.setattr(mod, "_git_sha", lambda: "unknown")

    db_path = tmp_path / "g.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        for name in mod.FHIR_TABLES:
            con.execute(f"CREATE TABLE main.{name} (stub INT)")
        con.execute("DROP TABLE main.fhir_bundle_specimen_export_v1")
        con.execute(
            "CREATE TABLE main.fhir_bundle_specimen_export_v1 "
            "(specimen_id VARCHAR, bundle_json VARCHAR)"
        )
        b = json.dumps({"resourceType": "Bundle", "type": "collection"})
        con.execute(
            "INSERT INTO main.fhir_bundle_specimen_export_v1 VALUES ('x', ?)", [b]
        )
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        _, manifest = mod.run_export(
            con, output_root=tmp_path / "o", limit=0, git_sha=mod._git_sha()
        )
    finally:
        con.close()
    assert manifest["git_sha"] == "unknown"


def test_144_introspect_local_release_manifest_and_query_history(tmp_path: Path) -> None:
    """``--introspect-local`` populates DB bullets; missing query_history is non-fatal."""
    db_path = tmp_path / "state.duckdb"
    out_md = tmp_path / "CURRENT.md"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS qa")
        con.execute("CREATE TABLE main.specimen_master_v1 (id INT)")
        con.execute("CREATE TABLE main.specimen_tumor_focus_v1 (id INT)")
        con.execute("CREATE TABLE main.specimen_genomic_assay_v1 (id INT)")
        con.execute("CREATE TABLE main.fhir_bundle_specimen_export_v1 (id INT)")
        con.execute("INSERT INTO main.specimen_master_v1 VALUES (1), (2)")
        con.execute("INSERT INTO main.specimen_tumor_focus_v1 VALUES (10)")
        con.execute("INSERT INTO main.specimen_genomic_assay_v1 VALUES (20)")
        con.execute("INSERT INTO main.fhir_bundle_specimen_export_v1 VALUES (30)")
        con.execute(
            "CREATE TABLE qa.release_manifest ("
            "release_tag VARCHAR, git_sha VARCHAR, created_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO qa.release_manifest VALUES ('v9.9.9', 'abc123def', CURRENT_TIMESTAMP)"
        )
        con.execute(
            "CREATE TABLE qa.manual_review_queue (verification_status VARCHAR)"
        )
        con.execute("INSERT INTO qa.manual_review_queue VALUES (NULL), ('done')")
    finally:
        con.close()

    rc = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "144_md_repo_current_state_summary.py"),
            "--introspect-local",
            "--db-path",
            str(db_path),
            "--output",
            str(out_md),
            "--stale-days",
            "9999",
        ],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    assert rc.returncode == 0
    text = out_md.read_text(encoding="utf-8")
    assert "**Commit SHA:**" in text
    assert "**current_database():**" in text
    assert "**specimen_master_v1:** 2 rows" in text
    assert "**qa.release_manifest (latest 3):**" in text
    assert "v9.9.9" in text
    assert "## Checked-in release manifest (exports/)" in text
    assert "query_history not available" in text or "md_information_schema" in text


def test_144_build_markdown_no_live_section() -> None:
    mod = _load_script_144()
    body = mod.build_markdown(
        now_iso="2026-01-01T00:00:00+00:00",
        sha="fff",
        stale_days=9999,
        md_lines=None,
        telemetry_note="note",
    )
    assert "no live MotherDuck session" in body
    assert "## Checked-in release manifest (exports/)" in body


def _create_stub_db_for_142(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS qa")
        con.execute(
            """
            CREATE TABLE main.specimen_master_v1 (
              specimen_fingerprint_sha256 VARCHAR,
              specimen_id VARCHAR,
              identity_build_run_id VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE main.specimen_genomic_assay_v1 (
              genomic_assay_id VARCHAR,
              specimen_id VARCHAR,
              specimen_focus_id VARCHAR,
              research_id INT,
              linkage_confidence_tier VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE main.fhir_specimen_v1 (
              specimen_id VARCHAR,
              patient_fhir_id VARCHAR,
              resource_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE main.fhir_procedure_collection_v1 (
              specimen_id VARCHAR,
              patient_fhir_id VARCHAR,
              resource_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE main.fhir_encounter_v1 (
              specimen_id VARCHAR,
              patient_fhir_id VARCHAR,
              resource_json VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE main.fhir_episode_of_care_v1 (
              patient_fhir_id VARCHAR,
              fhir_id VARCHAR
            )
            """
        )
        con.execute(
            "CREATE TABLE qa.specimen_genomic_link_review_v1 (review_status VARCHAR)"
        )
    finally:
        con.close()


def test_143_deploy_qa_diag_views_offline(tmp_path: Path) -> None:
    """Script 143 + 142 DDL creates all ``qa.v_diag_*`` views on a file DB."""
    db_path = tmp_path / "diag.duckdb"
    _create_stub_db_for_142(db_path)
    subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "143_md_specimen_fhir_qa_diagnostics_deploy.py"),
            "--db-path",
            str(db_path),
            "--skip-snapshot",
        ],
        cwd=str(ROOT),
        check=True,
    )
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for suffix in EXPECTED_QA_DIAG_VIEWS:
            fq = f"qa.{suffix}"
            con.execute(f"SELECT * FROM {fq} LIMIT 1")
    finally:
        con.close()
