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
            con,
            output_root=out_root,
            limit=0,
            git_sha="test-sha-abc",
            force_reconstruct=False,
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
    assert loaded["export_route"] == "bundle_table"
    assert loaded["motherduck_session_hint"] == mod.DEFAULT_SESSION_HINT
    stm = loaded["source_tables_main"]
    assert stm["fhir_bundle_specimen_export_v1"] == 1
    for tbl in mod.FHIR_TABLES:
        assert tbl in stm
    assert all(stm[t] == 0 for t in mod.FHIR_TABLES if t != "fhir_bundle_specimen_export_v1")
    readme = (out_dir / "README.md").read_text(encoding="utf-8")
    assert "specimen_fhir_export_restore_v1" in readme
    assert "fhir_bundle_specimen_export_v1" in readme


def test_141_reconstruct_path_without_bundle_table(tmp_path: Path) -> None:
    """Reconstruct NDJSON from resource tables when ``fhir_bundle_specimen_export_v1`` absent."""
    mod = _load_script_141()
    db_path = tmp_path / "recon.duckdb"
    out_root = tmp_path / "out2"
    spec_j = json.dumps({"resourceType": "Specimen", "id": "spec01"}, separators=(",", ":"))
    proc_j = json.dumps({"resourceType": "Procedure", "id": "proc01"}, separators=(",", ":"))
    enc_j = json.dumps({"resourceType": "Encounter", "id": "enc01"}, separators=(",", ":"))
    eoc_j = json.dumps({"resourceType": "EpisodeOfCare", "id": "eoc01"}, separators=(",", ":"))
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            "CREATE TABLE main.fhir_specimen_v1 (specimen_id VARCHAR, resource_json JSON)"
        )
        con.execute(
            "CREATE TABLE main.fhir_procedure_collection_v1 "
            "(specimen_id VARCHAR, resource_json JSON)"
        )
        con.execute(
            "CREATE TABLE main.fhir_encounter_v1 "
            "(specimen_id VARCHAR, episode_fhir_id VARCHAR, resource_json JSON)"
        )
        con.execute(
            "CREATE TABLE main.fhir_episode_of_care_v1 "
            "(episode_fhir_id VARCHAR, resource_json JSON)"
        )
        con.execute(
            "INSERT INTO main.fhir_specimen_v1 VALUES ('sp-recon', CAST(? AS JSON))",
            [spec_j],
        )
        con.execute(
            "INSERT INTO main.fhir_procedure_collection_v1 VALUES ('sp-recon', CAST(? AS JSON))",
            [proc_j],
        )
        con.execute(
            "INSERT INTO main.fhir_encounter_v1 VALUES ('sp-recon', 'eocA', CAST(? AS JSON))",
            [enc_j],
        )
        con.execute(
            "INSERT INTO main.fhir_episode_of_care_v1 VALUES ('eocA', CAST(? AS JSON))",
            [eoc_j],
        )
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        _, manifest = mod.run_export(
            con,
            output_root=out_root,
            limit=0,
            git_sha="recon-sha",
            force_reconstruct=False,
        )
    finally:
        con.close()

    assert manifest["export_route"] == "reconstructed_from_resources"
    assert manifest["bundle_row_count"] == 1
    assert manifest["reconstructed_from_tables"] is not None
    nd_dirs = list(out_root.glob("fhir_specimen_*"))
    assert len(nd_dirs) == 1
    line = (nd_dirs[0] / "specimen_bundles.ndjson").read_text(encoding="utf-8").strip()
    obj = json.loads(line)
    assert obj["resourceType"] == "Bundle"
    assert obj["type"] == "collection"
    assert len(obj["entry"]) == 4


def test_141_skips_empty_bundle_json_rows(tmp_path: Path) -> None:
    """Null/blank bundle_json lines are omitted from NDJSON; manifest count matches file."""
    mod = _load_script_141()
    db_path = tmp_path / "emptyrow.duckdb"
    out_root = tmp_path / "out3"
    con = duckdb.connect(str(db_path))
    try:
        for name in mod.FHIR_TABLES:
            con.execute(f"CREATE TABLE main.{name} (stub INT)")
        con.execute("DROP TABLE main.fhir_bundle_specimen_export_v1")
        con.execute(
            "CREATE TABLE main.fhir_bundle_specimen_export_v1 "
            "(specimen_id VARCHAR, bundle_json VARCHAR)"
        )
        con.execute(
            "INSERT INTO main.fhir_bundle_specimen_export_v1 VALUES ('b1', '')"
        )
        con.execute(
            "INSERT INTO main.fhir_bundle_specimen_export_v1 VALUES ('b2', NULL)"
        )
    finally:
        con.close()
    con = duckdb.connect(str(db_path))
    try:
        _, manifest = mod.run_export(
            con,
            output_root=out_root,
            limit=0,
            git_sha="x",
            force_reconstruct=False,
        )
    finally:
        con.close()
    out = list(out_root.glob("fhir_specimen_*"))[0]
    lines = (out / "specimen_bundles.ndjson").read_text(encoding="utf-8").strip()
    assert lines == ""
    assert manifest["bundle_row_count"] == 0


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
            con,
            output_root=tmp_path / "o",
            limit=0,
            git_sha=mod._git_sha(),
            force_reconstruct=False,
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
            CREATE TABLE main.specimen_tumor_focus_v1 (
              focus_fingerprint_sha256 VARCHAR,
              specimen_focus_id VARCHAR,
              specimen_id VARCHAR,
              research_id BIGINT,
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


def test_142_ddl_applies_directly_matches_143_path(tmp_path: Path) -> None:
    """Smoke: same SQL file 143 executes applies cleanly on the offline stub DB."""
    ddl_path = ROOT / "scripts" / "sql" / "142_specimen_fhir_qa_diagnostics_ddl.sql"
    assert ddl_path.is_file() and ddl_path.stat().st_size > 100
    db_path = tmp_path / "ddl_direct.duckdb"
    _create_stub_db_for_142(db_path)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(ddl_path.read_text(encoding="utf-8"))
    finally:
        con.close()
    ro = duckdb.connect(str(db_path), read_only=True)
    try:
        ro.execute("SELECT 1 FROM qa.v_diag_specimen_review_burden_v1 LIMIT 1")
    finally:
        ro.close()


def test_144_collect_live_introspection_graceful_without_md_information_schema(
    tmp_path: Path,
) -> None:
    """`md_information_schema.query_history` / `recent_queries` unavailable → non-fatal note."""
    mod = _load_script_144()
    db_path = tmp_path / "no_md_schema.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS qa")
        con.execute("CREATE TABLE main.specimen_master_v1 (id INT)")
        con.execute("INSERT INTO main.specimen_master_v1 VALUES (1)")
        con.execute(
            "CREATE TABLE qa.release_manifest ("
            "release_tag VARCHAR, git_sha VARCHAR, created_at TIMESTAMP)"
        )
        con.execute(
            "INSERT INTO qa.release_manifest VALUES ('v0.0.1', 'deadbeef', CURRENT_TIMESTAMP)"
        )
        con.execute("CREATE TABLE qa.manual_review_queue (verification_status VARCHAR)")
        con.execute("INSERT INTO qa.manual_review_queue VALUES (NULL)")
        for tbl, ddl in (
            ("specimen_tumor_focus_v1", "CREATE TABLE main.specimen_tumor_focus_v1 (id INT)"),
            ("specimen_genomic_assay_v1", "CREATE TABLE main.specimen_genomic_assay_v1 (id INT)"),
            (
                "fhir_bundle_specimen_export_v1",
                "CREATE TABLE main.fhir_bundle_specimen_export_v1 (id INT)",
            ),
        ):
            con.execute(ddl)
            con.execute(f"INSERT INTO main.{tbl} VALUES (1)")
    finally:
        con.close()

    con = duckdb.connect(str(db_path))
    try:
        md_lines, telemetry_note = mod.collect_live_introspection(con)
    finally:
        con.close()

    body = "\n".join(md_lines)
    assert "**current_database():**" in body
    assert "### Specimen / FHIR layer row counts" in body
    assert "**specimen_master_v1:**" in body and "rows" in body
    assert "**qa.release_manifest (latest 3):**" in body
    assert "v0.0.1" in body
    assert "query_history not available" in telemetry_note or "not available" in telemetry_note


def test_144_build_markdown_includes_release_manifest_heading() -> None:
    mod = _load_script_144()
    text = mod.build_markdown(
        now_iso="2026-04-08T00:00:00+00:00",
        sha="abc",
        stale_days=9999,
        md_lines=["- **current_database():** `ci_stub`"],
        telemetry_note="_(query_history not available: no md_information_schema)_",
    )
    assert "## Checked-in release manifest (exports/)" in text
    assert "## Query-history telemetry (MotherDuck)" in text
    assert "query_history not available" in text


def test_138_orchestrator_dry_run_smoke(tmp_path: Path) -> None:
    """Orchestrator exits 0 on --dry-run without opening MotherDuck (plan only)."""
    study = tmp_path / "study138"
    rc = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "138_md_specimen_fhir_layer.py"),
            "--dry-run",
            "--study-dir",
            str(study),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    assert rc.returncode == 0
    assert "dry-run" in (rc.stdout + rc.stderr).lower()
