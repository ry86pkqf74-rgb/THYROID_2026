#!/usr/bin/env python3
"""
Publication mirror: MotherDuck ``patient_refined_master_clinical_v12`` → BigQuery ``pub_workspace``.

**Source of truth:** ``thyroid_canonical_publication_v1_0.main.patient_refined_master_clinical_v12``
built by the extraction_audit_engine ladder (Phase 11 ``patient_refined_master_clinical_v10`` →
Phase 12 ``v11`` → Phase 13 ``v12``). This driver **does not** rebuild PRM in BigQuery SQL —
only documents a Parquet mirror load (**WRITE_TRUNCATE**), matching the pattern described for
``patient_analysis_resolved_v1`` in ``studies/bq_pub_authoritative_builders_20260514.py``.

**CPM / ASM207:** ``scripts/207_canonical_master_expansion.py`` consumes **128** ``prm.*``
columns from this table — validated against MotherDuck schema before export.

**Prerequisite:** PRM v12 must already exist on MotherDuck publication DB (full ladder absent by
default — deploy phases locally / CI onto publication MD before running this script).

Auth: MotherDuck token via ``motherduck_client`` / ``motherduck.local.toml``; BigQuery via ``bq``
CLI + Application Default Credentials (same pattern as ``bq_replicate_canonical_patient_master.py``).

Usage:
    .venv/bin/python scripts/351_prm_v12_motherduck_to_bq_mirror.py [--dry-run] [--skip-load]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts._md_connect import PUBLICATION_DB, connect_locked  # noqa: E402

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_workspace"
TABLE = "patient_refined_master_clinical_v12"
AUDIT_TABLE = "md_mirror_load_audit_v1"
LOCATION = "us-central1"
OUT_DIR_DEFAULT = _REPO / "exports" / "bq_prm_v12_mirror"
COHORT_EXPECTED = 10871

# Columns referenced as prm.* in scripts/207_canonical_master_expansion.py ASSEMBLY_SQL (128 total).
SCRIPT207_PRM_COLUMNS: tuple[str, ...] = (
    "tirads_best_score_v12",
    "tirads_worst_score_v12",
    "tirads_best_category_v12",
    "tirads_worst_category_v12",
    "tirads_source_v12",
    "tirads_reliability_v12",
    "tirads_n_sources_v12",
    "tirads_n_nodule_records_v12",
    "tirads_concordant_count_v12",
    "tirads_mismatch_count_v12",
    "tirads_has_acr_recalc_v12",
    "tirads_nodule_size_max_mm_v12",
    "imaging_nodule_size_cm_v11",
    "n_fna_episodes",
    "cross_fna_concordance",
    "fna_confidence",
    "worst_bethesda_num",
    "bethesda_final_name",
    "ene_positive",
    "best_ene_grade",
    "ene_grade_v9",
    "ene_levels_v9",
    "ene_deposit_cm",
    "ene_path_synoptic",
    "ene_path_nlp",
    "ene_path_levels",
    "ene_op_intraop",
    "ene_ct",
    "ene_us",
    "ene_pet",
    "ene_rai_scan",
    "ene_n_sources",
    "ene_path_ct_concordance",
    "ene_record_count_v9",
    "molecular_tested_v7",
    "high_risk_molecular_v7",
    "n_molecular_tests_v7",
    "molecular_platforms_v7",
    "alk_positive_v7",
    "ret_positive_v7",
    "ntrk_positive_v7",
    "tp53_positive_v7",
    "eif1ax_positive",
    "pax8_pparg_positive",
    "any_fusion_positive",
    "braf_positive_v7",
    "braf_status_v7",
    "tert_positive_v7",
    "tert_status_v7",
    "ras_positive_v7",
    "ras_positive_v11",
    "nras_positive_v11",
    "hras_positive_v11",
    "kras_positive_v11",
    "ras_primary_subtype_v11",
    "ras_protein_change_v11",
    "ras_allele_freq_v11",
    "braf_recovered_status_v11",
    "braf_recovered_variant_v11",
    "braf_detection_method_v11",
    "ihc_braf_result_v13",
    "ihc_braf_note_type_v13",
    "ihc_braf_confidence_v13",
    "ras_resolved_gene_v13",
    "ras_resolved_variant_v13",
    "ras_resolved_af_v13",
    "ras_resolution_source_v13",
    "ras_resolution_confidence_v13",
    "tert_variant_v9",
    "tert_platforms_v9",
    "tert_test_count_v9",
    "tert_tested",
    "preop_sweep_genes_found_v11",
    "confirmed_rai_episodes",
    "n_rai_episodes",
    "rai_dose_v9",
    "rai_intent_v9",
    "rai_avidity",
    "rai_avid_flag",
    "rai_validation_tier",
    "rai_dose_source",
    "rai_dose_linkage",
    "max_stimulated_tg",
    "rai_stimulated_tg",
    "rai_stimulated_tsh",
    "post_rai_tg_nadir",
    "post_rai_tg_last",
    "post_rai_tg_count",
    "rai_scan_findings_v9",
    "pth_nadir",
    "pth_nadir_30d",
    "pth_nadir_days_postop",
    "calcium_nadir_30d",
    "calcium_nadir_days_postop",
    "capsular_invasion_refined",
    "capsular_invasion_v6",
    "vascular_who_2022_grade",
    "vasc_grade_final_v13",
    "vasc_vessel_count_v13",
    "vasc_source_final_v13",
    "vasc_confidence_final_v13",
    "lvi_grade_final_v13",
    "pni_positive",
    "pni_refined_v6",
    "margin_r_classification",
    "margin_r_class_v10",
    "n_tumors_v10",
    "max_tumor_size_cm_v10",
    "worst_ete_v10",
    "total_ln_positive_v10",
    "voice_outcome_category",
    "has_voice_data",
    "voice_followup_completeness",
    "voice_data_confidence",
    "days_to_first_laryngoscopy",
    "days_to_last_laryngoscopy",
    "lateral_neck_dissected_v10",
    "lateral_detection_method",
    "lateral_levels_v10",
    "lateral_side_v10",
    "lateral_source_v10",
    "completion_reason",
    "completion_reason_confidence",
    "completion_histology_type",
    "completion_t_stage",
    "completion_prior_histology",
    "completion_braf_positive",
    "completion_tert_positive",
)

_PHI_SUBS = (
    "mrn",
    "medical_record",
    "patient_name",
    "first_name",
    "last_name",
    "full_name",
    "ssn",
    "social_security",
    "phone",
    "address",
    "street",
    "email",
    "zip_code",
    "postal",
)
_PHI_EXACT = frozenset(
    {
        "dob",
        "date_of_birth",
        "birth_date",
        "name",
        "patient_first_name",
        "patient_last_name",
        "dob_timestamp",
        "patient_dob",
    }
)


def _phi(name: str) -> bool:
    n = name.strip().lower()
    return n in _PHI_EXACT or any(s in n for s in _PHI_SUBS)


def _prm_table_exists(con) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = 'main' AND table_name = ?
        """,
        [PUBLICATION_DB, TABLE],
    ).fetchone()
    return row is not None and row[0] > 0


def _cols(con, tbl: str) -> tuple[list[str], list[str]]:
    rows = con.execute(
        """
        SELECT column_name, MIN(ordinal_position) AS op
        FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?
        GROUP BY column_name
        ORDER BY op
        """,
        [PUBLICATION_DB, tbl],
    ).fetchall()
    if not rows:
        raise ValueError(f"No columns for {tbl} in {PUBLICATION_DB}.main")
    cols_ordered = [r[0] for r in rows]
    kept = [c for c in cols_ordered if not _phi(c)]
    dropped = [c for c in cols_ordered if _phi(c)]
    return kept, dropped


def _validate_script207(con) -> None:
    kept, _ = _cols(con, TABLE)
    colset = set(kept)
    expected = set(SCRIPT207_PRM_COLUMNS)
    missing = sorted(expected - colset)
    extra_ok = colset >= expected
    if not extra_ok:
        raise SystemExit(
            f"[FAIL] MotherDuck {TABLE} missing {len(missing)} Script-207 prm columns: "
            f"{missing[:40]}{'...' if len(missing) > 40 else ''}"
        )


def _validate_rowcount(con) -> int:
    row = con.execute(
        f'SELECT COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM "{TABLE}"'
    ).fetchone()
    assert row is not None
    n, nd = row
    if n != COHORT_EXPECTED or nd != COHORT_EXPECTED:
        raise SystemExit(
            f"[FAIL] {TABLE} row count / distinct research_id expected {COHORT_EXPECTED}, "
            f"got rows={n} distinct={nd}"
        )
    return int(n)


def export_parquet(con, out_dir: Path, dry_run: bool) -> tuple[int, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pq_path = out_dir / f"{TABLE}.parquet"
    kept, dropped = _cols(con, TABLE)
    if dropped:
        print(
            f"[PHI-drop] {len(dropped)} columns omitted from export: "
            f"{dropped[:8]}{'...' if len(dropped) > 8 else ''}"
        )
    sel = ", ".join(f'"{c}"' for c in kept)
    q = f'SELECT {sel} FROM "{TABLE}"'
    n = con.execute(f"SELECT COUNT(*) FROM ({q}) _").fetchone()[0]
    if not dry_run:
        con.execute(
            f"COPY ({q}) TO '{pq_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    return n, pq_path


def bq_load(pq_path: Path) -> int:
    dest = f"{PROJECT}:{DATASET}.{TABLE}"
    cmd = [
        "bq",
        f"--location={LOCATION}",
        "load",
        "--replace",
        "--source_format=PARQUET",
        "--clustering_fields=research_id",
        dest,
        str(pq_path.resolve()),
    ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)


def ensure_audit_table() -> int:
    ddl = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.{AUDIT_TABLE}` (
  mirror_job_ts TIMESTAMP NOT NULL,
  destination_table STRING NOT NULL,
  source_ssot STRING NOT NULL,
  row_count INT64 NOT NULL,
  mirror_kind STRING NOT NULL,
  builder_script STRING NOT NULL,
  notes STRING
)
"""
    cmd = ["bq", f"--location={LOCATION}", "query", "--use_legacy_sql=false", ddl]
    print("Ensuring audit table:", AUDIT_TABLE)
    return subprocess.call(cmd)


def audit_insert(row_count: int) -> int:
    sql = f"""
INSERT INTO `{PROJECT}.{DATASET}.{AUDIT_TABLE}`
(mirror_job_ts, destination_table, source_ssot, row_count, mirror_kind, builder_script, notes)
VALUES (
  CURRENT_TIMESTAMP(),
  '{TABLE}',
  '{PUBLICATION_DB}.main.{TABLE}',
  {row_count},
  'motherduck_parquet_mirror_documented_not_bq_native',
  'scripts/351_prm_v12_motherduck_to_bq_mirror.py',
  'ASM207 / Script 207 PRM assembly feeder — WRITE_TRUNCATE Parquet load from publication MotherDuck'
)
"""
    cmd = ["bq", f"--location={LOCATION}", "query", "--use_legacy_sql=false", sql]
    return subprocess.call(cmd)


def main() -> None:
    assert len(SCRIPT207_PRM_COLUMNS) == 128, "SCRIPT207_PRM_COLUMNS must stay in sync with 207 SQL"

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="Validate MD only; no Parquet/BQ writes")
    ap.add_argument("--skip-load", action="store_true", help="Export Parquet only; skip bq load")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR_DEFAULT)
    args = ap.parse_args()

    con = connect_locked()

    if not _prm_table_exists(con):
        raise SystemExit(
            f"[FAIL] MotherDuck `{PUBLICATION_DB}.main.{TABLE}` not found. "
            "Deploy the extraction_audit_engine ladder through Phase 13 "
            "(patient_refined_master_clinical_v8→…→v12) onto publication MotherDuck before mirroring."
        )

    _validate_rowcount(con)
    _validate_script207(con)
    print(f"[OK] MotherDuck {TABLE}: {COHORT_EXPECTED} rows; all {len(SCRIPT207_PRM_COLUMNS)} Script-207 prm columns present")

    n, pq_path = export_parquet(con, args.out_dir, args.dry_run)
    print(f"{TABLE}: export rowcount={n} path={pq_path}")

    manifest = {
        "builder_script": "scripts/351_prm_v12_motherduck_to_bq_mirror.py",
        "mirror_kind": "motherduck_parquet_mirror_documented_not_bq_native",
        "source_ssot": f"{PUBLICATION_DB}.main.{TABLE}",
        "destination": f"{PROJECT}.{DATASET}.{TABLE}",
        "row_count_motherduck": n,
        "script207_prm_columns_validated": len(SCRIPT207_PRM_COLUMNS),
        "parquet_uri": str(pq_path.resolve()),
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }

    if args.dry_run:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        mf = args.out_dir / "manifest_dry_run.json"
        mf.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Dry-run manifest: {mf}")
        sys.exit(0)

    mf_path = args.out_dir / "manifest.json"
    mf_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Manifest: {mf_path}")

    if args.skip_load:
        print("--skip-load: Parquet written; skipping BigQuery.")
        sys.exit(0)

    rc = bq_load(pq_path)
    manifest["bq_load_exit_code"] = rc
    mf_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if rc != 0:
        print(f"bq load failed rc={rc}")
        sys.exit(rc)

    rc_audit_schema = ensure_audit_table()
    if rc_audit_schema != 0:
        print(f"[WARN] audit DDL rc={rc_audit_schema}; continuing")

    rc_audit = audit_insert(n)
    manifest["bq_audit_insert_exit_code"] = rc_audit
    mf_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if rc_audit != 0:
        print(f"[WARN] audit INSERT rc={rc_audit}")

    print("Done.")
    sys.exit(0)


if __name__ == "__main__":
    main()
