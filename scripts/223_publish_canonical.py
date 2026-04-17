#!/usr/bin/env python3
"""
THYROID_2026 — Script 223: Ingest Missing Tables + Build Versioned Publication House

PHASE 1 — Ingest raw tables that are in the GitHub repo but not on eras:
  - mri_imaging              (raw/mri_extraction__FINAL_11_20_25.xlsx — 715 rows)
  - nsqip_enrichment         (exports/nsqip/nsqip_enrichment.parquet — 1,275 rows)
  - nsqip_patient_summary    (exports/nsqip/nsqip_patient_summary.parquet — 1,261 rows)
  - patient_completion_oed_path_linkage_v1
                             (exports/patient_completion_oed_path_linkage_v1/ — 11,506 rows)
  - thyroid_weights          (raw/Thyroid_Weight_Data_12_2_25.xlsx — 10,001 rows)
  - thyroid_sizes            (raw/THyroid Sizes, Stanardized_12_2_25.xlsx — 11,675 rows)

  PHI scrubbing:
    - thyroid_weights: drop DOB + duplicated path text columns
    - nsqip_enrichment / nsqip_patient_summary: drop nsqip_dob

PHASE 2 — Build clean versioned publication house via CTAS (full physical isolation).

Usage examples:
    # Build a release candidate for v1_1 (safe — creates _v1_1_rc DB)
    .venv/bin/python scripts/223_publish_canonical.py --version v1_1 --candidate

    # Promote RC to release (after review via Script 224)
    .venv/bin/python scripts/225_promote_canonical_version.py --candidate v1_1_rc --release v1_1

    # Skip ingestion phase (tables already on source DB)
    .venv/bin/python scripts/223_publish_canonical.py --version v1_1 --skip-ingest --candidate

    # Ingest only — load new raw tables into source DB without building a pub house
    .venv/bin/python scripts/223_publish_canonical.py --ingest-only

    # Dry run to preview what would happen
    .venv/bin/python scripts/223_publish_canonical.py --version v1_1 --candidate --dry-run

VERSIONING RULES (enforced by this script):
    - Version format: v1_0, v1_1, v2_0  (underscores, lowercase v prefix)
    - Target DB name: thyroid_canonical_publication_v1_0  (no dots in SQL identifiers)
    - This script refuses to overwrite an existing canonical version.
      Use --allow-overwrite only for development work on unreleased versions,
      and only after confirming with a typed prompt.
    - For production version bumps, always use --candidate first.

PRECONDITION: Script 221c gap fixes already applied (verified 2026-04-16).
ACCOUNT: logan.glosser.eras (TOML token, NOT env var)
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import sys
import time
import warnings
from pathlib import Path

import duckdb
import pandas as pd
import toml

warnings.filterwarnings('ignore')

REPO = Path(__file__).resolve().parent.parent
SCRIPT_TAG = "223_publish_canonical"
SOURCE_DB_NAME = "Thyroid 2026 UPdated"
SOURCE_DB_SQL = '"Thyroid 2026 UPdated".main'
CANONICAL_SRC = "canonical_patient_master_v221"
CANONICAL_TGT = "canonical_patient_master"
EXPECTED_TOTAL_ROWS = 10_871
EXPECTED_MASTER_COLS = 1_377

VERSION_RE = re.compile(r'^v\d+_\d+(_\d+)?(_rc)?$')


def validate_version_arg(version: str) -> str:
    """Normalize and validate the --version argument."""
    v = version.lower().strip()
    if not v.startswith("v"):
        v = "v" + v
    v = v.replace(".", "_")
    if not VERSION_RE.match(v):
        sys.exit(
            f"[{SCRIPT_TAG}] ERROR: invalid --version {version!r}. "
            f"Format must be like v1_0, v1_1, v2_0 (underscores, not dots)."
        )
    return v


# ---------------------------------------------------------------------------
# Ingest spec
# ---------------------------------------------------------------------------

def safe_col(s: str) -> str:
    """'Right Lobe (g)' → 'right_lobe_g' style."""
    s = re.sub(r'\([^)]*\)', '', s).strip()
    s = re.sub(r'[^A-Za-z0-9]+', '_', s).strip('_').lower()
    return s


INGEST_SPEC = [
    {
        "name": "mri_imaging",
        "path": REPO / "raw" / "mri_extraction__FINAL_11_20_25.xlsx",
        "kind": "excel",
        "rid_col": "record_id",
        "drop_cols": [],
    },
    {
        "name": "nsqip_enrichment",
        "path": REPO / "exports" / "nsqip" / "nsqip_enrichment.parquet",
        "kind": "parquet",
        "rid_col": "research_id",
        "drop_cols": ["nsqip_dob"],
    },
    {
        "name": "nsqip_patient_summary",
        "path": REPO / "exports" / "nsqip" / "nsqip_patient_summary.parquet",
        "kind": "parquet",
        "rid_col": "research_id",
        "drop_cols": ["nsqip_dob"],
    },
    {
        "name": "patient_completion_oed_path_linkage_v1",
        "path": REPO / "exports" / "patient_completion_oed_path_linkage_v1" /
                "patient_completion_oed_path_linkage_v1.parquet",
        "kind": "parquet",
        "rid_col": "research_id",
        "drop_cols": [],
    },
    {
        "name": "thyroid_weights",
        "path": REPO / "raw" / "Thyroid_Weight_Data_12_2_25.xlsx",
        "kind": "excel",
        "rid_col": "Research ID",
        "drop_cols": [
            "DOB",
            "Final Diagnosis",
            "Synoptic Diagnosis",
            "Gross Path Description",
            "Microscopic Description",
        ],
    },
    {
        "name": "thyroid_sizes",
        "path": REPO / "raw" / "THyroid Sizes, Stanardized_12_2_25.xlsx",
        "kind": "excel",
        "rid_col": "Research ID number",
        "drop_cols": [],
    },
]


def prepare_df(spec: dict) -> pd.DataFrame:
    if spec["kind"] == "parquet":
        df = pd.read_parquet(spec["path"])
    else:
        df = pd.read_excel(spec["path"], sheet_name=0)

    df.columns = [safe_col(c) for c in df.columns]

    rid_safe = safe_col(spec["rid_col"])
    if rid_safe != "research_id":
        df = df.rename(columns={rid_safe: "research_id"})

    drop_safe = [safe_col(c) for c in spec.get("drop_cols", [])]
    df = df.drop(columns=[c for c in drop_safe if c in df.columns])

    df["research_id"] = df["research_id"].astype(str)

    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).replace({"nan": None, "NaT": None, "None": None})

    return df


# ---------------------------------------------------------------------------
# Publication house inventory
# ---------------------------------------------------------------------------

PATIENT_MASTER = [(CANONICAL_SRC, CANONICAL_TGT)]

PATIENT_SUMMARY = [
    "complication_patient_summary_v1",
    "imaging_patient_summary_v1",
    "tg_timeline_patient_summary_v1",
    "ln_master_rollup_v1",
    "tirads_llm_validation_v2",
    "thyroid_scoring_py_v1",
    "path_outcome_classification_v1",
    "patient_analysis_resolved_v1",
    "canonical_diagnosis_unified_v1",
    "canonical_benign_diagnosis_v1",
    "canonical_malignant_diagnosis_v1",
    "canonical_recurrence_v1",
    "canonical_survival_followup_v1",
    "canonical_molecular_tested_v1",
    "manuscript_cohort_v1",
]

EPISODE_TABLES: dict[str, str] = {
    # IMAGING
    "ultrasound_reports": "All US reports — patient encounters",
    "us_nodules_tirads": "Per-nodule TIRADS from US workbook",
    "imaging_nodule_master_v1": "37,016 nodules — composition, echogenicity, shape, margin",
    "imaging_nodule_long_v2": "19,891 nodules — long format per-nodule features",
    "imaging_exam_master_v1": "13,347 exams — per-exam master across modalities",
    "tirads_llm_extracted_v2": "5,636 LLM-scored ACR TI-RADS records, 1,429 patients",
    "extracted_tirads_validated_v1": "3,439 fully-parsed TIRADS components",
    "ct_imaging": "7,701 CT exams",
    "mri_imaging": "715 MRI exams (ingested in Phase 1 of this script)",
    "nuclear_med": "2,220 nuclear med scans",
    # FNA / CYTOLOGY
    "fna_episode_master_v2": "8,119 FNA episodes — per-biopsy site, laterality, Bethesda, date",
    "fna_cytology": "8,063 FNA cytology records — multi-era Bethesda",
    "fna_history": "8,119 FNA history",
    "extracted_fna_bethesda_v1": "5,249 fully-parsed Bethesda extractions",
    "md_extracted_fna_bethesda_v1": "5,249 MD-validated Bethesda extractions",
    # MOLECULAR / GENETIC
    "molecular_results": "10,862 molecular result records",
    "molecular_testing": "10,862 molecular test records",
    "molecular_test_episode_v2": "10,126 per-test episodes",
    "molecular_variant_long": "1,640 per-variant records — fully parsed gene/variant/AF",
    "molecular_assay_dictionary": "4 assay dictionary",
    "molecular_code_crosswalk": "44 code crosswalk",
    "molecular_ingestion_runs": "1 ingestion provenance",
    "thyroseq_molecular_enrichment": "10,862 ThyroSeq enrichments",
    "specimen_genomic_assay_v1": "10,370 specimen×assay records",
    "extracted_braf_recovery_v1": "730 BRAF status recoveries",
    "extracted_ras_patient_summary_v1": "321 RAS variant summaries",
    "analysis_molecular_subset_v1": "10,025 molecular analysis subset",
    # PATHOLOGY
    "tumor_pathology": "4,290 tumor pathology records",
    "path_synoptics": "11,688 path synoptic reports",
    "synoptic_tumor_long_v1": "11,103 synoptic tumor (long format)",
    "md_synoptic_tumor_long_v1": "11,103 MD-validated synoptic tumor long",
    "tumor_episode_master_v2": "11,691 per-tumor episodes",
    "specimen_master_v1": "10,139 specimen master",
    "specimen_tumor_focus_v1": "11,103 specimen × tumor focus",
    "specimen_source_xref_v1": "11,273 specimen source crosswalk",
    "lesion_analysis_resolved_v1": "11,851 resolved per-lesion analysis",
    "extracted_ete_subgraded_v1": "3,558 fully-graded ETE extractions",
    # LABS
    "longitudinal_lab_canonical_v1": "77,960 labs (TSH/PTH/Ca/VitD time series)",
    "thyroglobulin_lab_canonical_v1": "76,971 Tg values (full time series)",
    "tg_postop_surveillance_windows_v1": "16,184 Tg surveillance windows",
    "extracted_postop_labs_expanded_v1": "1,395 postop PTH/Ca extractions",
    "lab_cross_wave_dedup_map_v1": "21,761 lab dedup linkage",
    # SURGERY / OPERATIVE
    "operative_episode_detail_v2": "9,371 operative episodes — procedure, ETE, RLN, parathyroid",
    "episode_analysis_resolved_v1_dedup": "9,368 resolved episode analysis",
    # RAI / TREATMENT
    "rai_treatment_episode_v2": "RAI treatment episodes",
    # RECURRENCE / OUTCOMES
    "recurrence_event_clean_v1": "1,946 cleaned recurrence events",
    "survival_cohort_enriched": "61,134 survival cohort events",
    # COMPLICATIONS
    "complication_phenotype_v1": "All complication phenotype records",
    "extracted_complications_refined_v5": "358 refined complication extractions",
    "extracted_rln_injury_refined_v2": "92 RLN injury refinements",
    # LYMPH NODES
    "ln_crossval_v1": "LN cross-validation",
    # LINKAGE (validated)
    "imaging_fna_linkage_v3": "Per-imaging-FNA linkage",
    # CLINICAL NOTES
    "clinical_notes_long": "11,050 clinical notes — needed for note-level retrieval",
    "clinical_note_ln_extracted_v1": "Clinical note LN extractions",
    # NSQIP / WEIGHT / SIZE / COMPLETION  (ingested in Phase 1)
    "nsqip_enrichment": "1,275 NSQIP perioperative enrichments (DOB removed)",
    "nsqip_patient_summary": "1,261 NSQIP patient-level summary (DOB removed)",
    "thyroid_weights": "10,001 gland weight records (DOB + duplicated path text removed)",
    "thyroid_sizes": "11,675 standardized gland-size records",
    "patient_completion_oed_path_linkage_v1": "11,506 completion thyroidectomy linkage",
    # TIMELINE
    "patient_cross_domain_timeline_v2": "61,055 cross-domain timeline events",
}

NLP_ENTITY_TABLES: dict[str, str] = {
    "note_entities_llm_tirads_granular": "27,707 TIRADS entities (qwen3:32b)",
    "note_entities_llm_cervical_ln_detail": "36,964 LN detail entities",
    "note_entities_llm_pathology": "29,236 pathology entities",
    "note_entities_llm_recurrence": "Recurrence entities (LLM)",
    "note_entities_llm_survival_followup": "Survival/followup entities",
    "note_entities_llm_rai_detailed": "RAI detail entities",
    "note_entities_llm_tg_kinetics": "Tg kinetics entities",
    "note_entities_llm_imaging": "Imaging entities",
    "note_entities_llm_labs": "Lab entities",
    "note_entities_llm_frozen_section_detail": "Frozen section entities",
    "note_entities_llm_airway_invasion": "Airway invasion entities",
    "note_entities_llm_vascular_invasion": "Vascular invasion entities",
    "note_entities_llm_functional_outcomes": "Functional outcomes",
    "note_entities_llm_parathyroid_detail": "Parathyroid detail",
    "note_entities_llm_dynamic_risk_response": "Dynamic risk response",
    "note_entities_llm_past_medical_hx": "Past medical history",
    "note_entities_llm_past_surgical_hx": "Past surgical history",
    "note_entities_llm_presenting_symptoms": "Presenting symptoms",
    "note_entities_llm_physical_exam": "Physical exam",
    "note_entities_llm_rad_treatment": "Radiation treatment",
    "note_entities_llm_patient_decision_adherence": "Patient decision adherence",
    "note_entities_llm_synoptic_pathology_enrichment": "Synoptic pathology enrichment",
    "note_entities_llm_us_nodule_dynamics": "US nodule dynamics",
    "note_entities_complications": "Complications entities (standard)",
    "note_entities_genetics": "Genetics entities",
    "note_entities_medications": "Medications entities",
    "note_entities_operative_detail": "Operative detail entities",
    "note_entities_problem_list": "Problem list entities",
    "note_entities_procedures": "Procedures entities",
    "note_entities_staging": "Staging entities",
}

DICTIONARY_TABLES = [
    "data_dictionary_v221",
    "data_dictionary_v2",
    "data_dictionary_parquet_v221",
]


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect_eras() -> duckdb.DuckDBPyConnection:
    toml_path = REPO / "motherduck.local.toml"
    if not toml_path.exists():
        sys.exit(f"[{SCRIPT_TAG}] ERROR: {toml_path} not found")
    cfg = toml.load(str(toml_path))
    token = cfg.get("MD_SA_TOKEN") or cfg.get("MOTHERDUCK_TOKEN") or cfg.get("motherduck_token")
    if not token:
        sys.exit(f"[{SCRIPT_TAG}] ERROR: No token in motherduck.local.toml")
    padding = len(token.split(".")[1]) % 4
    payload_b64 = token.split(".")[1] + "=" * (4 - padding if padding else 0)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    email = payload.get("email", "unknown")
    print(f"[{SCRIPT_TAG}] Connected as: {email}")
    if "eras" not in email.lower():
        sys.exit(f"[{SCRIPT_TAG}] ABORT: expected eras account, got {email}")
    return duckdb.connect(f"md:?motherduck_token={token}")


def _safe(s: str) -> str:
    return s.replace('"', '""')


# ---------------------------------------------------------------------------
# Phase 0 — Preconditions
# ---------------------------------------------------------------------------

def phase_0_preconditions(con: duckdb.DuckDBPyConnection) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 0: Preconditions ══")
    r = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE prm_first_fna_date IS NOT NULL) AS fna,
          COUNT(*) FILTER (WHERE first_tg_date IS NOT NULL)      AS tg,
          COUNT(*) FILTER (WHERE followup_years > 0)             AS fu_pos
        FROM {SOURCE_DB_SQL}.{CANONICAL_SRC}
    """).fetchone()
    print(f"  Source canonical: FNA={r[0]}, Tg={r[1]}, fu_pos={r[2]} (expected 5212/2721/4038)")
    if r != (5212, 2721, 4038):
        print("  ⚠ Numbers do not match Script 221c-validated state. Continuing.")
    else:
        print("  ✓ Script 221c gap fixes confirmed")


# ---------------------------------------------------------------------------
# Phase 1 — Ingest missing tables
# ---------------------------------------------------------------------------

def phase_1_ingest(con: duckdb.DuckDBPyConnection, dry_run: bool,
                   out_dir: Path) -> list[tuple]:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 1: Ingest missing tables ══")

    if not dry_run:
        try:
            con.execute(f'DROP TABLE IF EXISTS {SOURCE_DB_SQL}."mri_imaging"')
            print("  Cleaned up any pre-existing empty mri_imaging")
        except Exception as e:
            print(f"  WARN cleanup mri_imaging: {e}")

    out_dir.mkdir(parents=True, exist_ok=True)
    results = []

    for spec in INGEST_SPEC:
        name = spec["name"]
        if not spec["path"].exists():
            print(f"  ✗ {name}: source not found at {spec['path']}")
            results.append((name, "MISSING_SOURCE", str(spec["path"])))
            continue

        df = prepare_df(spec)
        staging = out_dir / f"_ingest_{name}.parquet"
        df.to_parquet(staging, index=False)

        n_rows = len(df)
        n_cols = len(df.columns)
        if dry_run:
            print(f"  [dry-run] {name}: {n_rows:,} rows × {n_cols} cols → {staging.name}")
            results.append((name, "DRY_RUN", f"{n_rows} rows"))
            continue

        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {SOURCE_DB_SQL}."{_safe(name)}" AS
                SELECT * FROM read_parquet('{staging}')
            """)
            verify = con.execute(
                f'SELECT COUNT(*) FROM {SOURCE_DB_SQL}."{_safe(name)}"'
            ).fetchone()[0]
            assert verify == n_rows, f"row count mismatch {verify} vs {n_rows}"

            dropped_str = str(spec.get("drop_cols", [])).replace("'", "''")
            ingested_date = time.strftime("%Y-%m-%d")
            tbl_comment = (
                f"Ingested from {spec['path'].name} on {ingested_date} by Script 223. "
                f"PHI-scrubbed: dropped {dropped_str}."
            )
            con.execute(
                f"COMMENT ON TABLE {SOURCE_DB_SQL}.\"{_safe(name)}\" IS '{tbl_comment}'"
            )
            print(f"  ✓ {name}: {n_rows:,} rows × {n_cols} cols uploaded")
            results.append((name, "OK", f"{n_rows} rows × {n_cols} cols"))
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            results.append((name, "FAIL", str(e)[:120]))

    n_ok = sum(1 for r in results if r[1] == "OK")
    print(f"  Phase 1: {n_ok}/{len(INGEST_SPEC)} ingested successfully")
    fails = [r for r in results if r[1] == "FAIL"]
    if fails and not dry_run:
        sys.exit(f"  ABORT: {len(fails)} ingest failures")
    return results


# ---------------------------------------------------------------------------
# Phase 2 — Create publication DB (version-aware, refuses overwrite)
# ---------------------------------------------------------------------------

def phase_2_create_db(con: duckdb.DuckDBPyConnection, target_db: str,
                      dry_run: bool, allow_overwrite: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 2: Create publication house ══")
    if dry_run:
        print(f"  [dry-run] would CREATE DATABASE {target_db}")
        return

    existing = con.execute(
        "SELECT 1 FROM duckdb_databases() WHERE database_name = ?", [target_db]
    ).fetchone()

    if existing and not allow_overwrite:
        sys.exit(
            f"[{SCRIPT_TAG}] ABORT: {target_db!r} already exists.\n"
            f"  To build a new version: use a different --version argument.\n"
            f"  To force overwrite (dev only): pass --allow-overwrite and confirm."
        )

    if existing and allow_overwrite:
        print(f"  ⚠ --allow-overwrite set. Database {target_db!r} will be replaced.")
        confirm = input("  Type the database name to confirm overwrite: ").strip()
        if confirm != target_db:
            sys.exit("  ABORT: confirmation did not match. No changes made.")
        con.execute(f'DROP DATABASE "{_safe(target_db)}"')
        print(f"  Dropped existing {target_db!r}")

    con.execute(f'CREATE DATABASE "{_safe(target_db)}"')
    print(f"  ✓ {target_db} ready")


# ---------------------------------------------------------------------------
# Phase 3 — Materialize tables
# ---------------------------------------------------------------------------

def materialize(con, src_name, tgt_name, target_db_sql, source_db_name, dry_run=False):
    if tgt_name is None:
        tgt_name = src_name

    exists = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(source_db_name)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
    """).fetchone()[0]
    if not exists:
        return (src_name, "MISSING", "not on eras")

    src_n = con.execute(f'SELECT COUNT(*) FROM {SOURCE_DB_SQL}."{_safe(src_name)}"').fetchone()[0]
    if dry_run:
        return (src_name, "DRY_RUN", f"{src_n} rows → {tgt_name}")

    try:
        con.execute(f'''
            CREATE OR REPLACE TABLE {target_db_sql}."{_safe(tgt_name)}" AS
            SELECT * FROM {SOURCE_DB_SQL}."{_safe(src_name)}"
        ''')
    except Exception as e:
        return (src_name, "FAIL", f"CTAS: {str(e)[:120]}")

    tgt_n = con.execute(f'SELECT COUNT(*) FROM {target_db_sql}."{_safe(tgt_name)}"').fetchone()[0]
    if src_n != tgt_n:
        return (src_name, "ROW_MISMATCH", f"src={src_n} tgt={tgt_n}")

    cmts = con.execute(f"""
        SELECT column_name, comment FROM duckdb_columns()
        WHERE database_name='{_safe(source_db_name)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
          AND comment IS NOT NULL AND TRIM(comment) <> ''
    """).fetchall()
    n_cmts = 0
    for col, cmt in cmts:
        try:
            safe_cmt = cmt.replace("'", "''")
            con.execute(f'''
                COMMENT ON COLUMN {target_db_sql}."{_safe(tgt_name)}"."{_safe(col)}"
                IS '{safe_cmt}'
            ''')
            n_cmts += 1
        except Exception:
            pass

    tbl_cmt = con.execute(f"""
        SELECT comment FROM duckdb_tables()
        WHERE database_name='{_safe(source_db_name)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
    """).fetchone()
    if tbl_cmt and tbl_cmt[0]:
        safe_cmt = tbl_cmt[0].replace("'", "''")
        try:
            con.execute(f'''
                COMMENT ON TABLE {target_db_sql}."{_safe(tgt_name)}" IS '{safe_cmt}'
            ''')
        except Exception:
            pass

    return (src_name, "OK", f"{tgt_n:,} rows, {n_cmts} col-comments")


def phase_3_materialize(con, target_db_sql, include_nlp, dry_run):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 3: Materialize validated tables ══")
    plan = []
    for src, tgt in PATIENT_MASTER:
        plan.append((src, tgt))
    for t in PATIENT_SUMMARY:
        plan.append((t, None))
    for t in EPISODE_TABLES.keys():
        plan.append((t, None))
    for t in DICTIONARY_TABLES:
        plan.append((t, None))
    if include_nlp:
        for t in NLP_ENTITY_TABLES.keys():
            plan.append((t, None))

    print(f"  Tables to materialize: {len(plan)} (include_nlp={include_nlp})")
    results = []
    t0 = time.time()
    for i, (src, tgt) in enumerate(plan, 1):
        eff_tgt = tgt or src
        print(f"  [{i:3d}/{len(plan)}] {src} → {eff_tgt}", end=" ", flush=True)
        result = materialize(con, src, tgt, target_db_sql, SOURCE_DB_NAME, dry_run=dry_run)
        results.append(result)
        sym = {"OK": "✓", "DRY_RUN": "·", "MISSING": "○",
               "FAIL": "✗", "ROW_MISMATCH": "✗"}.get(result[1], "?")
        print(f"{sym} {result[1]}: {result[2]}")
    elapsed = time.time() - t0
    n_ok = sum(1 for r in results if r[1] == "OK")
    n_fail = sum(1 for r in results if r[1] in ("FAIL", "ROW_MISMATCH"))
    n_missing = sum(1 for r in results if r[1] == "MISSING")
    print(f"\n  Phase 3: {n_ok} OK, {n_fail} failed, {n_missing} missing ({elapsed:.1f}s)")
    if n_fail and not dry_run:
        sys.exit(f"  ABORT: {n_fail} table(s) failed in phase 3")
    return results


# ---------------------------------------------------------------------------
# Phase 4 — Validate
# ---------------------------------------------------------------------------

def phase_4_validate(con, target_db_name, target_db_sql, expected_n_tables):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 4: Validate publication house ══")
    all_ok = True

    n = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(target_db_name)}' AND schema_name='main'
          AND table_name <> '__readme'
    """).fetchone()[0]
    print(f"  Tables: {n} (expected {expected_n_tables})")
    if n != expected_n_tables:
        all_ok = False

    r = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL),
               COUNT(*) FILTER (WHERE diagnosis_primary IS NULL)
        FROM {target_db_sql}.{CANONICAL_TGT}
    """).fetchone()
    if r != (EXPECTED_TOTAL_ROWS, EXPECTED_TOTAL_ROWS, 0, 0, 0):
        print(f"  ⚠ Invariants: {r} (expected {EXPECTED_TOTAL_ROWS}×5 clean)")
        all_ok = False
    else:
        print(f"  ✓ Canonical invariants pass: {r}")

    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{_safe(target_db_name)}' AND table_schema='main'
          AND table_name='{CANONICAL_TGT}'
    """).fetchone()[0]
    print(f"  Canonical columns: {n_cols} (expected {EXPECTED_MASTER_COLS})")
    if n_cols != EXPECTED_MASTER_COLS:
        all_ok = False

    n_cmt = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_columns()
        WHERE database_name='{_safe(target_db_name)}' AND schema_name='main'
          AND table_name='{CANONICAL_TGT}'
          AND comment IS NOT NULL AND TRIM(comment) <> ''
    """).fetchone()[0]
    pct = 100 * n_cmt / max(n_cols, 1)
    print(f"  Column comment coverage: {n_cmt}/{n_cols} ({pct:.1f}%)")
    if pct < 95.0:
        all_ok = False

    total = con.execute(f"""
        SELECT SUM(estimated_size) FROM duckdb_tables()
        WHERE database_name='{_safe(target_db_name)}' AND schema_name='main'
    """).fetchone()[0]
    print(f"  ✓ Total rows in publication house: {total:,}")
    return all_ok


# ---------------------------------------------------------------------------
# Phase 5 — __readme inventory
# ---------------------------------------------------------------------------

def phase_5_readme(con, target_db_name, target_db_sql, dry_run):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 5: __readme inventory ══")
    if dry_run:
        return
    con.execute(f"""
        CREATE OR REPLACE TABLE {target_db_sql}."__readme" AS
        SELECT table_name, estimated_size AS rows, comment AS description
        FROM duckdb_tables()
        WHERE database_name='{_safe(target_db_name)}' AND schema_name='main'
          AND table_name <> '__readme'
        ORDER BY estimated_size DESC
    """)
    today = time.strftime('%Y-%m-%d')
    msg = (
        f"Table inventory for the publication-ready canonical thyroid 2026 dataset. "
        f"Built {today} from \"{SOURCE_DB_NAME}\".main on the eras MotherDuck account. "
        f"Includes Tier 0 (patient master), Tier 1 (per-patient summaries), "
        f"Tier 2 (per-episode multi-row tables), Tier 3 (data dictionary)."
    ).replace("'", "''")
    con.execute(f"COMMENT ON TABLE {target_db_sql}.\"__readme\" IS '{msg}'")
    print("  ✓ __readme created")


# ---------------------------------------------------------------------------
# Phase 6 — Provenance
# ---------------------------------------------------------------------------

def phase_6_provenance(con, target_db_sql, version, out_dir,
                       ingest_results, materialize_results, dry_run):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 6: Provenance ══")
    if dry_run:
        return
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "ingest_manifest.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["table", "status", "detail"])
        for row in ingest_results:
            w.writerow(row)

    with open(out_dir / "publication_house_manifest.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src_table", "tgt_table", "status", "detail"])
        for src, status, detail in materialize_results:
            tgt = CANONICAL_TGT if src == CANONICAL_SRC else src
            w.writerow([src, tgt, status, detail])

    parquet_path = out_dir / "canonical_patient_master.parquet"
    con.execute(f"""
        COPY (SELECT * FROM {target_db_sql}.{CANONICAL_TGT})
        TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    sz_mb = parquet_path.stat().st_size / 1_048_576
    print(f"  ✓ Manifests + parquet snapshot saved ({sz_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Build a versioned canonical publication house on MotherDuck."
    )
    parser.add_argument(
        "--version", default=None,
        help="Target canonical version, e.g. v1_1 or v2_0. Required unless --ingest-only."
    )
    parser.add_argument(
        "--candidate", action="store_true",
        help="Build as a release candidate: appends _rc to the DB name (e.g. v1_1_rc). "
             "Candidates are promoted to releases via Script 225."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview all steps without making any changes."
    )
    parser.add_argument(
        "--no-nlp", action="store_true",
        help="Exclude the 30 note_entities_* NLP tables."
    )
    parser.add_argument(
        "--skip-ingest", action="store_true",
        help="Skip Phase 1 (raw table ingestion into source DB). Use when tables are "
             "already present on eras."
    )
    parser.add_argument(
        "--ingest-only", action="store_true",
        help="Run only Phase 1 (ingest raw tables into source DB). Does NOT build a "
             "publication house. --version is not required."
    )
    parser.add_argument(
        "--allow-overwrite", action="store_true",
        help="Allow overwriting an existing canonical DB. Dev use only — requires a "
             "typed confirmation prompt. Never use on released (non-_rc) versions."
    )
    args = parser.parse_args()

    if args.ingest_only and args.version:
        print(f"[{SCRIPT_TAG}] NOTE: --ingest-only ignores --version.")

    if not args.ingest_only and not args.version:
        parser.error("--version is required unless --ingest-only is set.")

    include_nlp = not args.no_nlp

    # Compute target DB name
    if args.ingest_only:
        target_db = None
        target_db_sql = None
        out_dir = REPO / "scripts" / "output" / "publication_house_ingest"
    else:
        version = validate_version_arg(args.version)
        suffix = "_rc" if args.candidate else ""
        target_db = f"thyroid_canonical_publication_{version}{suffix}"
        target_db_sql = f"{target_db}.main"
        out_dir = REPO / "scripts" / "output" / f"publication_house_{version}{suffix}"

    print(f"[{SCRIPT_TAG}] dry_run={args.dry_run} include_nlp={include_nlp} "
          f"skip_ingest={args.skip_ingest} ingest_only={args.ingest_only}")
    if target_db:
        print(f"[{SCRIPT_TAG}] target_db={target_db}")

    con = connect_eras()
    phase_0_preconditions(con)

    if args.skip_ingest or args.ingest_only is False and args.skip_ingest:
        ingest_results = []
        print(f"\n[{SCRIPT_TAG}] (skipping Phase 1 ingest per --skip-ingest)")
    else:
        ingest_results = phase_1_ingest(con, dry_run=args.dry_run, out_dir=out_dir)

    if args.ingest_only:
        print(f"\n[{SCRIPT_TAG}] ══ DONE (ingest only) ══")
        return

    phase_2_create_db(con, target_db, dry_run=args.dry_run,
                      allow_overwrite=args.allow_overwrite)
    materialize_results = phase_3_materialize(
        con, target_db_sql, include_nlp=include_nlp, dry_run=args.dry_run
    )

    expected_n = (
        len(PATIENT_MASTER) + len(PATIENT_SUMMARY) + len(EPISODE_TABLES)
        + len(DICTIONARY_TABLES) + (len(NLP_ENTITY_TABLES) if include_nlp else 0)
    )
    ok = phase_4_validate(con, target_db, target_db_sql, expected_n_tables=expected_n)
    if not ok and not args.dry_run:
        sys.exit(f"[{SCRIPT_TAG}] ⚠ Validation failed — review above")

    phase_5_readme(con, target_db, target_db_sql, dry_run=args.dry_run)
    phase_6_provenance(
        con, target_db_sql, args.version, out_dir,
        ingest_results, materialize_results, dry_run=args.dry_run
    )

    print(f"\n[{SCRIPT_TAG}] ══ DONE ══")
    print(f"  Source DB:         md:\"{SOURCE_DB_NAME}\"  (working/dev house, retained)")
    print(f"  Publication house: md:{target_db}")
    if args.candidate:
        print("\n  This is a RELEASE CANDIDATE. To promote to release:")
        release_ver = validate_version_arg(args.version)
        print(f"    python scripts/224_compare_canonical_versions.py "
              f"--from v1_0 --to {release_ver}_rc")
        print(f"    python scripts/225_promote_canonical_version.py "
              f"--candidate {release_ver}_rc --release {release_ver}")


if __name__ == "__main__":
    main()
