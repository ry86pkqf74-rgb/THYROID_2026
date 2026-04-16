#!/usr/bin/env python3
"""
THYROID_2026 — Script 223: Ingest 4 Missing Tables + Build Publication House

PHASE 1 — Ingest 4 raw tables that are in the GitHub repo but not on eras:
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

PHASE 2 — Build clean publication house thyroid_canonical_publication_v1
          via CTAS (full physical isolation).

PRECONDITION: Script 221c gap fixes already applied (verified 2026-04-16).
ACCOUNT: logan.glosser.eras (TOML token, NOT env var)
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
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
SCRIPT_TAG = "223_ingest_and_publish"
SOURCE_DB_NAME = "Thyroid 2026 UPdated"
SOURCE_DB_SQL = '"Thyroid 2026 UPdated".main'
TARGET_DB_NAME = "thyroid_canonical_publication_v1"
TARGET_DB_SQL = "thyroid_canonical_publication_v1.main"
CANONICAL_SRC = "canonical_patient_master_v221"
CANONICAL_TGT = "canonical_patient_master"
EXPECTED_TOTAL_ROWS = 10_871
EXPECTED_MASTER_COLS = 1_377
OUT_DIR = REPO / "scripts" / "output" / "publication_house_v1"


# ---------------------------------------------------------------------------
# PHASE 1 — Ingest spec
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
        "rid_col": "record_id",   # MRI's record_id IS the research_id (verified)
        "drop_cols": [],
    },
    {
        "name": "nsqip_enrichment",
        "path": REPO / "exports" / "nsqip" / "nsqip_enrichment.parquet",
        "kind": "parquet",
        "rid_col": "research_id",
        "drop_cols": ["nsqip_dob"],   # PHI
    },
    {
        "name": "nsqip_patient_summary",
        "path": REPO / "exports" / "nsqip" / "nsqip_patient_summary.parquet",
        "kind": "parquet",
        "rid_col": "research_id",
        "drop_cols": ["nsqip_dob"],   # PHI
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
            "Final Diagnosis",            # already in tumor_pathology
            "Synoptic Diagnosis",         # already in path_synoptics
            "Gross Path Description",     # already in path_synoptics
            "Microscopic Description",    # already in path_synoptics
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

    # Coerce object columns to string (avoids parquet mixed-type errors)
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).replace({"nan": None, "NaT": None, "None": None})

    return df


# ---------------------------------------------------------------------------
# PHASE 2 — Publication-house inventory
#   (This is the corrected Script 222 inventory with the 4 newly-ingested
#    tables now included.)
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
    payload_b64 = token.split(".")[1] + "==="
    payload = json.loads(base64.urlsafe_b64decode(payload_b64[:len(payload_b64) // 4 * 4]))
    email = payload.get("email", "unknown")
    print(f"[{SCRIPT_TAG}] Connected as: {email}")
    if "eras" not in email.lower():
        sys.exit(f"[{SCRIPT_TAG}] ABORT: expected eras account, got {email}")
    return duckdb.connect(f"md:?motherduck_token={token}")


def _safe(s: str) -> str:
    return s.replace('"', '""')


# ---------------------------------------------------------------------------
# PHASE 0 — Preconditions
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
        print(f"  ⚠ Numbers do not match Script 221c-validated state. Continuing.")
    else:
        print(f"  ✓ Script 221c gap fixes confirmed")


# ---------------------------------------------------------------------------
# PHASE 1 — Ingest 4 missing tables via CTAS from local parquet/excel
# ---------------------------------------------------------------------------

def phase_1_ingest(con: duckdb.DuckDBPyConnection, dry_run: bool) -> list[tuple]:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 1: Ingest missing tables ══")

    # Drop the empty mri_imaging that was left from a prior partial attempt
    if not dry_run:
        try:
            con.execute(f'DROP TABLE IF EXISTS {SOURCE_DB_SQL}."mri_imaging"')
            print(f"  Cleaned up any pre-existing empty mri_imaging")
        except Exception as e:
            print(f"  WARN cleanup mri_imaging: {e}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    for spec in INGEST_SPEC:
        name = spec["name"]
        if not spec["path"].exists():
            print(f"  ✗ {name}: source not found at {spec['path']}")
            results.append((name, "MISSING_SOURCE", str(spec["path"])))
            continue

        # Prepare DataFrame, write to staging parquet
        df = prepare_df(spec)
        staging = OUT_DIR / f"_ingest_{name}.parquet"
        df.to_parquet(staging, index=False)

        n_rows = len(df)
        n_cols = len(df.columns)
        if dry_run:
            print(f"  [dry-run] {name}: {n_rows:,} rows × {n_cols} cols → {staging.name}")
            results.append((name, "DRY_RUN", f"{n_rows} rows"))
            continue

        # Upload via parquet
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {SOURCE_DB_SQL}."{_safe(name)}" AS
                SELECT * FROM read_parquet('{staging}')
            """)
            verify = con.execute(
                f'SELECT COUNT(*) FROM {SOURCE_DB_SQL}."{_safe(name)}"'
            ).fetchone()[0]
            assert verify == n_rows, f"row count mismatch {verify} vs {n_rows}"

            # Comment metadata
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
# PHASE 2 — Build clean publication house
# ---------------------------------------------------------------------------

def phase_2_create_db(con: duckdb.DuckDBPyConnection, dry_run: bool) -> None:
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 2: Create publication house ══")
    if dry_run:
        print(f"  [dry-run] would CREATE DATABASE IF NOT EXISTS {TARGET_DB_NAME}")
        return
    con.execute(f'CREATE DATABASE IF NOT EXISTS "{TARGET_DB_NAME}"')
    print(f"  ✓ {TARGET_DB_NAME} ready")


def materialize(con, src_name, tgt_name=None, dry_run=False):
    if tgt_name is None:
        tgt_name = src_name

    exists = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{_safe(SOURCE_DB_NAME)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
    """).fetchone()[0]
    if not exists:
        return (src_name, "MISSING", "not on eras")

    src_n = con.execute(f'SELECT COUNT(*) FROM {SOURCE_DB_SQL}."{_safe(src_name)}"').fetchone()[0]
    if dry_run:
        return (src_name, "DRY_RUN", f"{src_n} rows → {tgt_name}")

    try:
        con.execute(f'''
            CREATE OR REPLACE TABLE {TARGET_DB_SQL}."{_safe(tgt_name)}" AS
            SELECT * FROM {SOURCE_DB_SQL}."{_safe(src_name)}"
        ''')
    except Exception as e:
        return (src_name, "FAIL", f"CTAS: {str(e)[:120]}")

    tgt_n = con.execute(f'SELECT COUNT(*) FROM {TARGET_DB_SQL}."{_safe(tgt_name)}"').fetchone()[0]
    if src_n != tgt_n:
        return (src_name, "ROW_MISMATCH", f"src={src_n} tgt={tgt_n}")

    # Copy column comments
    cmts = con.execute(f"""
        SELECT column_name, comment FROM duckdb_columns()
        WHERE database_name='{_safe(SOURCE_DB_NAME)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
          AND comment IS NOT NULL AND TRIM(comment) <> ''
    """).fetchall()
    n_cmts = 0
    for col, cmt in cmts:
        try:
            safe_cmt = cmt.replace("'", "''")
            con.execute(f'''
                COMMENT ON COLUMN {TARGET_DB_SQL}."{_safe(tgt_name)}"."{_safe(col)}"
                IS '{safe_cmt}'
            ''')
            n_cmts += 1
        except Exception:
            pass

    # Copy table comment
    tbl_cmt = con.execute(f"""
        SELECT comment FROM duckdb_tables()
        WHERE database_name='{_safe(SOURCE_DB_NAME)}' AND schema_name='main'
          AND table_name='{_safe(src_name)}'
    """).fetchone()
    if tbl_cmt and tbl_cmt[0]:
        safe_cmt = tbl_cmt[0].replace("'", "''")
        try:
            con.execute(f'''
                COMMENT ON TABLE {TARGET_DB_SQL}."{_safe(tgt_name)}" IS '{safe_cmt}'
            ''')
        except Exception:
            pass

    return (src_name, "OK", f"{tgt_n:,} rows, {n_cmts} col-comments")


def phase_3_materialize(con, include_nlp, dry_run):
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
        result = materialize(con, src, tgt, dry_run=dry_run)
        results.append(result)
        sym = {"OK": "✓", "DRY_RUN": "·", "MISSING": "○",
               "FAIL": "✗", "ROW_MISMATCH": "✗"}[result[1]]
        print(f"{sym} {result[1]}: {result[2]}")
    elapsed = time.time() - t0
    n_ok = sum(1 for r in results if r[1] == "OK")
    n_fail = sum(1 for r in results if r[1] in ("FAIL", "ROW_MISMATCH"))
    n_missing = sum(1 for r in results if r[1] == "MISSING")
    print(f"\n  Phase 3: {n_ok} OK, {n_fail} failed, {n_missing} missing ({elapsed:.1f}s)")
    if n_fail and not dry_run:
        sys.exit(f"  ABORT: {n_fail} table(s) failed in phase 3")
    return results


def phase_4_validate(con, expected_n_tables):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 4: Validate publication house ══")
    all_ok = True

    n = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{TARGET_DB_NAME}' AND schema_name='main'
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
        FROM {TARGET_DB_SQL}.{CANONICAL_TGT}
    """).fetchone()
    if r != (EXPECTED_TOTAL_ROWS, EXPECTED_TOTAL_ROWS, 0, 0, 0):
        print(f"  ⚠ Invariants: {r}")
        all_ok = False
    else:
        print(f"  ✓ Canonical invariants pass: {r}")

    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{TARGET_DB_NAME}' AND table_schema='main'
          AND table_name='{CANONICAL_TGT}'
    """).fetchone()[0]
    print(f"  Canonical columns: {n_cols} (expected {EXPECTED_MASTER_COLS})")
    if n_cols != EXPECTED_MASTER_COLS:
        all_ok = False

    n_cmt = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_columns()
        WHERE database_name='{TARGET_DB_NAME}' AND schema_name='main'
          AND table_name='{CANONICAL_TGT}'
          AND comment IS NOT NULL AND TRIM(comment) <> ''
    """).fetchone()[0]
    pct = 100 * n_cmt / max(n_cols, 1)
    print(f"  Column comment coverage: {n_cmt}/{n_cols} ({pct:.1f}%)")
    if pct < 95.0:
        all_ok = False

    total = con.execute(f"""
        SELECT SUM(estimated_size) FROM duckdb_tables()
        WHERE database_name='{TARGET_DB_NAME}' AND schema_name='main'
    """).fetchone()[0]
    print(f"  ✓ Total rows in publication house: {total:,}")
    return all_ok


def phase_5_readme(con, dry_run):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 5: __readme inventory ══")
    if dry_run:
        return
    con.execute(f"""
        CREATE OR REPLACE TABLE {TARGET_DB_SQL}."__readme" AS
        SELECT table_name, estimated_size AS rows, comment AS description
        FROM duckdb_tables()
        WHERE database_name='{TARGET_DB_NAME}' AND schema_name='main'
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
    con.execute(f"COMMENT ON TABLE {TARGET_DB_SQL}.\"__readme\" IS '{msg}'")
    print(f"  ✓ __readme created")


def phase_6_provenance(con, ingest_results, materialize_results, dry_run):
    print(f"\n[{SCRIPT_TAG}] ══ PHASE 6: Provenance ══")
    if dry_run:
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUT_DIR / "ingest_manifest.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["table", "status", "detail"])
        for row in ingest_results:
            w.writerow(row)

    with open(OUT_DIR / "publication_house_manifest.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src_table", "tgt_table", "status", "detail"])
        for src, status, detail in materialize_results:
            tgt = CANONICAL_TGT if src == CANONICAL_SRC else src
            w.writerow([src, tgt, status, detail])

    parquet_path = OUT_DIR / "canonical_patient_master.parquet"
    con.execute(f"""
        COPY (SELECT * FROM {TARGET_DB_SQL}.{CANONICAL_TGT})
        TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    sz_mb = parquet_path.stat().st_size / 1_048_576
    print(f"  ✓ Manifests + parquet snapshot saved ({sz_mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-nlp", action="store_true",
                        help="Exclude 30 note_entities_* tables from publication house")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip Phase 1 (already done)")
    args = parser.parse_args()
    include_nlp = not args.no_nlp

    print(f"[{SCRIPT_TAG}] dry_run={args.dry_run} include_nlp={include_nlp} "
          f"skip_ingest={args.skip_ingest}")

    con = connect_eras()
    phase_0_preconditions(con)

    if args.skip_ingest:
        ingest_results = []
        print(f"\n[{SCRIPT_TAG}] (skipping Phase 1 ingest)")
    else:
        ingest_results = phase_1_ingest(con, dry_run=args.dry_run)

    phase_2_create_db(con, dry_run=args.dry_run)
    materialize_results = phase_3_materialize(
        con, include_nlp=include_nlp, dry_run=args.dry_run
    )

    expected_n = (
        len(PATIENT_MASTER) + len(PATIENT_SUMMARY) + len(EPISODE_TABLES)
        + len(DICTIONARY_TABLES) + (len(NLP_ENTITY_TABLES) if include_nlp else 0)
    )
    ok = phase_4_validate(con, expected_n_tables=expected_n)
    if not ok and not args.dry_run:
        sys.exit(f"[{SCRIPT_TAG}] ⚠ Validation failed — review above")

    phase_5_readme(con, dry_run=args.dry_run)
    phase_6_provenance(con, ingest_results, materialize_results, dry_run=args.dry_run)

    print(f"\n[{SCRIPT_TAG}] ══ DONE ══")
    print(f"  Source DB: md:\"{SOURCE_DB_NAME}\"  (working/dev house, retained)")
    print(f"  Publication house: md:{TARGET_DB_NAME}  (point dashboards/shares here)")


if __name__ == "__main__":
    main()
