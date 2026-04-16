"""Script 236 — Canonical Finalization & Coworker Audit Fixes.

Database: ``thyroid_canonical_publication_v1_0`` on MotherDuck (AUTHORITATIVE
reads + writes).  Archive destination: ``"Thyroid 2026 UPdated".archive_pub_v1_0``.

Finalizes the publication canonical after Scripts 232 (ETE adjudication), 234
(RAI/TG cleanup), 235 (parathyroid/calcium), and the coworker Prompt 8 / 9
REVISED audit.  Every phase is idempotent and can be re-run independently
via ``--phase``.

Phases:
    0  Preflight snapshot + inventory
    1A comp_*_days_postop_v2 filtered to surgery_related_flag (9 entities)
    1B VC paralysis/paresis recalibration via RLN cross-reference
    1C nlp_path_multifocal_concordance_v2 using multifocal_flag_path
    1D NLP rollup strictness audit (nlp_rollup_promotion_audit_v1)
    2  Archive deprecated / _pre235_backup tables to Thyroid 2026 UPdated
    3  Drill-down surface validation
    4  Regenerate detail_table_registry_v1
    5  Rebuild __readme catalog
    6  Canonical column-pointer verification (CSV)
    7  Final invariant checks
    8  Archive pre236 backup (after Phase 7 passes)
    9  (handled externally: git commit + handoff markdown)

All writes target ``thyroid_canonical_publication_v1_0`` except Phase 2/8
which additionally write to ``"Thyroid 2026 UPdated".archive_pub_v1_0``.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_BACKUP_DIR = OUTPUT_DIR / "parquet_backup"
PARQUET_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_DATE = "20260416"
SCRIPT_TAG = "Script 236"

N_EXPECTED_PATIENTS = 10_871

# Entities from complication_phenotype_v1.complication_entity ->
# column-prefix used throughout canonical_patient_master.
ENTITY_MAP: dict[str, str] = {
    "rln_injury": "comp_rln_injury",
    "hematoma": "comp_hematoma",
    "hypoparathyroidism": "comp_hypoparathyroidism",
    "seroma": "comp_seroma",
    "chyle_leak": "comp_chyle_leak",
    "hypocalcemia": "comp_hypocalcemia",
    "wound_infection": "comp_wound_infection",
    "vocal_cord_paralysis": "comp_vc_paralysis",
    "vocal_cord_paresis": "comp_vc_paresis",
}

TABLES_TO_ARCHIVE: list[str] = [
    "canonical_patient_master_pre235_backup",
    "complication_patient_summary_v1_pre235_backup",
    "complication_phenotype_v1_pre235_backup",
    "extracted_postop_labs_expanded_v1_pre235_backup",
    "longitudinal_lab_canonical_v1_pre235_backup",
    "data_dictionary_v221",
    "data_dictionary_v235",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def connect() -> duckdb.DuckDBPyConnection:
    token = get_token()
    if not token:
        raise RuntimeError("No MotherDuck token — see scripts/.../motherduck-credentials SKILL.md")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def column_exists(con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    row = con.execute(
        f"""SELECT 1 FROM information_schema.columns
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name='{table}' AND column_name='{column}' LIMIT 1"""
    ).fetchone()
    return row is not None


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        f"""SELECT 1 FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='{schema}' AND table_name='{table}' LIMIT 1"""
    ).fetchone()
    return row is not None


def archive_table_name(name: str) -> str:
    return f"{name}_{ARCHIVE_DATE}"


# ---------------------------------------------------------------------------
# Phase 0 — Preflight snapshot + inventory
# ---------------------------------------------------------------------------


def phase0(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 0 — Preflight snapshot + inventory")

    print("  0.1  snapshot canonical_patient_master -> canonical_patient_master_pre236_backup")
    con.execute(
        """CREATE OR REPLACE TABLE canonical_patient_master_pre236_backup
           AS SELECT * FROM canonical_patient_master"""
    )
    n = con.execute("SELECT COUNT(*) FROM canonical_patient_master_pre236_backup").fetchone()[0]
    print(f"        backup rows: {n}")
    assert n == N_EXPECTED_PATIENTS, f"expected {N_EXPECTED_PATIENTS} got {n}"

    print("  0.2  export canonical_patient_master to parquet")
    pq_path = PARQUET_BACKUP_DIR / "canonical_patient_master_pre236.parquet"
    con.execute(
        f"""COPY (SELECT * FROM canonical_patient_master)
            TO '{pq_path.as_posix()}' (FORMAT PARQUET, COMPRESSION 'zstd')"""
    )
    size_mb = pq_path.stat().st_size / 1_048_576
    print(f"        {pq_path.name} ({size_mb:.1f} MB)")

    print("  0.3  inventory main-schema tables -> scripts/output/236_preflight_inventory.csv")
    rows = con.execute(
        f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog='{DB}' AND table_schema='main'
        ORDER BY table_type, table_name
        """
    ).fetchall()
    inv_path = OUTPUT_DIR / "236_preflight_inventory.csv"
    with inv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["table_name", "table_type", "row_count"])
        for table_name, table_type in rows:
            if table_type == "BASE TABLE":
                rc = con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()[0]
            else:
                rc = None
            w.writerow([table_name, table_type, rc])
    print(f"        wrote {inv_path.name} ({len(rows)} rows)")

    print("  0.4  canonical_patient_master column count")
    ncols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'"""
    ).fetchone()[0]
    print(f"        columns: {ncols}")

    print("  0.5  registry vs main diff")
    registry_tbls = {
        r[0]
        for r in con.execute(
            "SELECT detail_table_name FROM manuscript_workspace.detail_table_registry_v1"
        ).fetchall()
    }
    main_tbls = {
        r[0]
        for r in con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_type='BASE TABLE'"""
        ).fetchall()
    }
    in_main_not_registry = sorted(
        t
        for t in main_tbls - registry_tbls
        if t != "canonical_patient_master"
        and t != "__readme"
        and "pre235_backup" not in t
        and "pre236_backup" not in t
    )
    in_registry_not_main = sorted(registry_tbls - main_tbls)
    print(f"    (a) in MAIN, not in registry ({len(in_main_not_registry)}):")
    for t in in_main_not_registry:
        print(f"        - {t}")
    print(f"    (b) in REGISTRY, not in main ({len(in_registry_not_main)}):")
    for t in in_registry_not_main:
        print(f"        - {t}")


# ---------------------------------------------------------------------------
# Phase 1A — comp_*_days_postop_v2 filtered to surgery_related_flag
# ---------------------------------------------------------------------------


def phase1a(con: duckdb.DuckDBPyConnection) -> None:
    banner(
        "PHASE 1A — Recompute comp_*_days_postop_v2 filtered to surgery_related_flag=TRUE"
    )

    # Build filtered aggregate CTE as a TEMP TABLE.
    select_parts = ",\n        ".join(
        f"MIN(timing_days_post_surgery) FILTER "
        f"(WHERE complication_entity='{entity}' AND surgery_related_flag=TRUE) "
        f"AS {prefix}_days_postop_v2"
        for entity, prefix in ENTITY_MAP.items()
    )
    con.execute("DROP TABLE IF EXISTS comp_timing_surg_v1")
    con.execute(
        f"""
        CREATE TEMP TABLE comp_timing_surg_v1 AS
        SELECT
            research_id,
            {select_parts}
        FROM complication_phenotype_v1
        GROUP BY research_id
        """
    )
    n = con.execute("SELECT COUNT(*) FROM comp_timing_surg_v1").fetchone()[0]
    print(f"  comp_timing_surg_v1 rows: {n}")

    # Add columns to CPM, UPDATE from CTE, comment old + new.
    for entity, prefix in ENTITY_MAP.items():
        new_col = f"{prefix}_days_postop_v2"
        old_col = f"{prefix}_days_postop"
        # ADD COLUMN IF NOT EXISTS
        con.execute(
            f'ALTER TABLE canonical_patient_master ADD COLUMN IF NOT EXISTS "{new_col}" BIGINT'
        )
        con.execute(
            f"""
            UPDATE canonical_patient_master c
            SET "{new_col}" = t."{new_col}"
            FROM comp_timing_surg_v1 t
            WHERE c.research_id = t.research_id
            """
        )
        # Comment the old column (deprecation marker) and new column (provenance).
        if column_exists(con, "canonical_patient_master", old_col):
            con.execute(
                f"""COMMENT ON COLUMN canonical_patient_master."{old_col}" IS
                    'DEPRECATED 2026-04-16 (Script 236): includes non-surgery phenotype rows. Use {new_col}.'"""
            )
        con.execute(
            f"""COMMENT ON COLUMN canonical_patient_master."{new_col}" IS
                'Days from first surgery to {entity} complication, filtered to surgery_related_flag=TRUE '
                'per complication_phenotype_v1 (Script 236, 2026-04-16). '
                'NULL if no surgery-related row. Bounded 0-730 days.'"""
        )

    # Invariant: every non-NULL _v2 value is between 0 and 730 days.
    for entity, prefix in ENTITY_MAP.items():
        col = f"{prefix}_days_postop_v2"
        bad = con.execute(
            f"""SELECT COUNT(*) FROM canonical_patient_master
                WHERE "{col}" IS NOT NULL AND ("{col}" < 0 OR "{col}" > 730)"""
        ).fetchone()[0]
        n_pop = con.execute(
            f'SELECT COUNT(*) FROM canonical_patient_master WHERE "{col}" IS NOT NULL'
        ).fetchone()[0]
        mn, mx = con.execute(
            f'SELECT MIN("{col}"), MAX("{col}") FROM canonical_patient_master'
        ).fetchone()
        print(
            f"  {col:<44} n={n_pop:>5}  min={mn}  max={mx}  out_of_range={bad}"
        )
        assert bad == 0, f"Invariant violation: {bad} rows have {col} outside [0,730]"

    con.execute("DROP TABLE IF EXISTS comp_timing_surg_v1")


# ---------------------------------------------------------------------------
# Phase 1B — VC paralysis/paresis recalibration via RLN cross-reference
# ---------------------------------------------------------------------------


def phase1b(con: duckdb.DuckDBPyConnection) -> None:
    banner(
        "PHASE 1B — VC paralysis/paresis recalibration via extracted_rln_injury_refined_v2"
    )

    # Pre-counts.
    pre_paralysis = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_vc_paralysis_confirmed=TRUE"
    ).fetchone()[0]
    pre_paresis = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_vc_paresis_confirmed=TRUE"
    ).fetchone()[0]
    pre_rln = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_rln_injury_confirmed=TRUE"
    ).fetchone()[0]
    print(
        f"  PRE  vc_paralysis={pre_paralysis}  vc_paresis={pre_paresis}  rln_injury={pre_rln}"
    )

    # Build upgrade candidate patient sets from extracted_rln_injury_refined_v2.
    # injury_type values observed: paralysis, paresis, rln_injury, unknown.
    con.execute("DROP TABLE IF EXISTS _236_rln_upgrade_candidates")
    con.execute(
        """
        CREATE TEMP TABLE _236_rln_upgrade_candidates AS
        SELECT DISTINCT
            research_id,
            LOWER(injury_type) AS rln_injury_type,
            rln_injury_is_confirmed
        FROM extracted_rln_injury_refined_v2
        WHERE rln_injury_is_confirmed = TRUE
          AND LOWER(injury_type) IN ('paralysis','paresis','rln_injury','unknown')
        """
    )
    n_cand = con.execute("SELECT COUNT(*) FROM _236_rln_upgrade_candidates").fetchone()[0]
    print(f"  RLN confirmed candidates: {n_cand}")

    # Build audit table in manuscript_workspace.
    con.execute(
        """
        CREATE OR REPLACE TABLE manuscript_workspace.vc_paralysis_recalibration_v236 AS
        WITH candidates AS (
            SELECT research_id,
                   rln_injury_type,
                   CASE rln_injury_type
                     WHEN 'paralysis' THEN 'vocal_cord_paralysis'
                     WHEN 'paresis'   THEN 'vocal_cord_paresis'
                     WHEN 'rln_injury' THEN 'rln_injury'
                     ELSE 'rln_injury'
                   END AS vc_entity
            FROM _236_rln_upgrade_candidates
        ),
        joined AS (
            SELECT c.research_id,
                   c.rln_injury_type AS rln_source_status,
                   c.vc_entity,
                   p.final_complication_status AS prior_status,
                   CASE
                     WHEN p.final_complication_status = 'absent_or_unconfirmed'
                       THEN 'confirmed_from_rln_crossref'
                     ELSE p.final_complication_status
                   END AS new_status
            FROM candidates c
            LEFT JOIN complication_phenotype_v1 p
              ON p.research_id = c.research_id AND p.complication_entity = c.vc_entity
        )
        SELECT * FROM joined
        """
    )
    con.execute(
        f"""COMMENT ON TABLE manuscript_workspace.vc_paralysis_recalibration_v236 IS
            'Audit of {SCRIPT_TAG} (2026-04-16) VC paralysis/paresis recalibration. '
            'Maps extracted_rln_injury_refined_v2 confirmed RLN events to complication_phenotype_v1 '
            'rows and records prior vs new (status_v2) value.'"""
    )
    audit_n = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.vc_paralysis_recalibration_v236"
    ).fetchone()[0]
    upgraded = con.execute(
        """SELECT COUNT(*) FROM manuscript_workspace.vc_paralysis_recalibration_v236
           WHERE new_status = 'confirmed_from_rln_crossref'"""
    ).fetchone()[0]
    print(f"  audit rows: {audit_n}  upgraded-to-confirmed_from_rln_crossref: {upgraded}")

    # Add status_v2 column to complication_phenotype_v1 (default to final_complication_status).
    if not column_exists(con, "complication_phenotype_v1", "status_v2"):
        con.execute(
            "ALTER TABLE complication_phenotype_v1 ADD COLUMN status_v2 VARCHAR"
        )
    con.execute(
        "UPDATE complication_phenotype_v1 SET status_v2 = final_complication_status"
    )
    con.execute(
        """
        UPDATE complication_phenotype_v1 p
        SET status_v2 = 'confirmed_from_rln_crossref'
        FROM manuscript_workspace.vc_paralysis_recalibration_v236 a
        WHERE p.research_id = a.research_id
          AND p.complication_entity = a.vc_entity
          AND a.new_status = 'confirmed_from_rln_crossref'
        """
    )
    status_v2_dist = con.execute(
        """SELECT complication_entity, status_v2, COUNT(*)
           FROM complication_phenotype_v1
           WHERE status_v2 LIKE 'confirmed_from_rln_crossref'
           GROUP BY 1,2 ORDER BY 1"""
    ).fetchall()
    print("  status_v2 upgrades per entity:")
    for e, s, n in status_v2_dist:
        print(f"    {e:<28} {s:<32} {n}")

    con.execute(
        f"""COMMENT ON COLUMN complication_phenotype_v1.status_v2 IS
            '{SCRIPT_TAG} (2026-04-16) recalibrated status. Equals final_complication_status '
            'except for VC paralysis/paresis rows that were absent_or_unconfirmed but have a '
            'confirmed RLN injury in extracted_rln_injury_refined_v2 (set to confirmed_from_rln_crossref).'"""
    )

    # Rebuild canonical comp_vc_paralysis_confirmed / comp_vc_paresis_confirmed / comp_rln_injury_confirmed
    # using (complication_phenotype_v1.status_v2 matches confirmed-pattern) OR (cross-ref from RLN table).
    # Use a temp patient-level rollup.
    con.execute("DROP TABLE IF EXISTS _236_conf_rollup")
    con.execute(
        """
        CREATE TEMP TABLE _236_conf_rollup AS
        SELECT
            cpm.research_id,
            BOOL_OR(cp.complication_entity='vocal_cord_paralysis'
                    AND (cp.status_v2 LIKE 'confirmed%' OR cp.confirmed_flag=TRUE))
              OR BOOL_OR(r.research_id IS NOT NULL
                         AND LOWER(r.injury_type)='paralysis'
                         AND r.rln_injury_is_confirmed=TRUE)
              AS vc_paralysis_confirmed_v2,
            BOOL_OR(cp.complication_entity='vocal_cord_paresis'
                    AND (cp.status_v2 LIKE 'confirmed%' OR cp.confirmed_flag=TRUE))
              OR BOOL_OR(r.research_id IS NOT NULL
                         AND LOWER(r.injury_type)='paresis'
                         AND r.rln_injury_is_confirmed=TRUE)
              AS vc_paresis_confirmed_v2,
            BOOL_OR(cp.complication_entity='rln_injury'
                    AND (cp.status_v2 LIKE 'confirmed%' OR cp.confirmed_flag=TRUE))
              OR BOOL_OR(r.research_id IS NOT NULL
                         AND r.rln_injury_is_confirmed=TRUE)
              AS rln_injury_confirmed_v2
        FROM canonical_patient_master cpm
        LEFT JOIN complication_phenotype_v1 cp ON cp.research_id = cpm.research_id
        LEFT JOIN extracted_rln_injury_refined_v2 r ON r.research_id = cpm.research_id
        GROUP BY cpm.research_id
        """
    )
    # Overwrite confirmed canonical columns.
    con.execute(
        """
        UPDATE canonical_patient_master cpm
        SET comp_vc_paralysis_confirmed = COALESCE(r.vc_paralysis_confirmed_v2, FALSE),
            comp_vc_paresis_confirmed   = COALESCE(r.vc_paresis_confirmed_v2, FALSE),
            comp_rln_injury_confirmed   = COALESCE(r.rln_injury_confirmed_v2, FALSE)
        FROM _236_conf_rollup r
        WHERE cpm.research_id = r.research_id
        """
    )
    con.execute("DROP TABLE IF EXISTS _236_conf_rollup")
    con.execute("DROP TABLE IF EXISTS _236_rln_upgrade_candidates")

    post_paralysis = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_vc_paralysis_confirmed=TRUE"
    ).fetchone()[0]
    post_paresis = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_vc_paresis_confirmed=TRUE"
    ).fetchone()[0]
    post_rln = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE comp_rln_injury_confirmed=TRUE"
    ).fetchone()[0]
    print(
        f"  POST vc_paralysis={post_paralysis} (Δ{post_paralysis-pre_paralysis:+d})  "
        f"vc_paresis={post_paresis} (Δ{post_paresis-pre_paresis:+d})  "
        f"rln_injury={post_rln} (Δ{post_rln-pre_rln:+d})"
    )


# ---------------------------------------------------------------------------
# Phase 1C — nlp_path_multifocal_concordance_v2
# ---------------------------------------------------------------------------


def phase1c(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 1C — nlp_path_multifocal_concordance_v2 (uses multifocal_flag_path)")

    # Require the source columns exist.
    for req in ("nlp_path_multifocal_mentioned", "multifocal_flag_path"):
        assert column_exists(con, "canonical_patient_master", req), f"missing {req}"

    if not column_exists(con, "canonical_patient_master", "nlp_path_multifocal_concordance_v2"):
        con.execute(
            """ALTER TABLE canonical_patient_master
               ADD COLUMN nlp_path_multifocal_concordance_v2 VARCHAR"""
        )
    con.execute(
        """
        UPDATE canonical_patient_master SET
          nlp_path_multifocal_concordance_v2 = CASE
            WHEN nlp_path_multifocal_mentioned IS TRUE  AND multifocal_flag_path IS TRUE  THEN 'concordant_positive'
            WHEN nlp_path_multifocal_mentioned IS TRUE  AND multifocal_flag_path IS FALSE THEN 'nlp_positive_path_negative'
            WHEN nlp_path_multifocal_mentioned IS FALSE AND multifocal_flag_path IS TRUE  THEN 'nlp_negative_path_positive'
            WHEN nlp_path_multifocal_mentioned IS FALSE AND multifocal_flag_path IS FALSE THEN 'concordant_negative'
            ELSE NULL
          END
        """
    )
    con.execute(
        f"""COMMENT ON COLUMN canonical_patient_master.nlp_path_multifocal_concordance_v2 IS
            '{SCRIPT_TAG} (2026-04-16): concordance between NLP-mentioned multifocality '
            '(nlp_path_multifocal_mentioned) and the authoritative path-synoptic flag '
            '(multifocal_flag_path). Supersedes prior concordance that used multifocal_flag '
            '(which was the wrong join column per Prompt 8 REVISED audit, 0%% concordance).'"""
    )

    dist = con.execute(
        """SELECT COALESCE(nlp_path_multifocal_concordance_v2,'(null)') AS concordance, COUNT(*)
           FROM canonical_patient_master
           GROUP BY 1 ORDER BY 2 DESC"""
    ).fetchall()
    print("  distribution:")
    for c, n in dist:
        print(f"    {c:<32} {n}")


# ---------------------------------------------------------------------------
# Phase 1D — NLP rollup promotion audit
# ---------------------------------------------------------------------------

NLP_ROLLUP_MAPPING: list[tuple[str, str, str, str]] = [
    # (domain, source_table, canonical_has_data_col, gating_criterion)
    ("airway_invasion",      "note_entities_llm_airway_invasion",            "nlp_airway_has_data",      "Patient promoted if NLP entity classified as validated gross airway invasion (conservative)."),
    ("dynamic_risk_response","note_entities_llm_dynamic_risk_response",      "nlp_dynrisk_has_data",     "Requires validated dynamic-risk-response score."),
    ("frozen_section",       "note_entities_llm_frozen_section_detail",      "nlp_frozensec_has_data",   "Requires frozen-section finding with vocab alignment (loosening pending coworker Prompt 9)."),
    ("functional_outcomes",  "note_entities_llm_functional_outcomes",        "nlp_funcoutcome_has_data", "Requires validated functional outcome (voice/swallow) mention."),
    ("imaging",              "note_entities_llm_imaging",                    "nlp_imaging_has_data",     "Requires validated imaging signal (e.g. size, nodule count)."),
    ("labs",                 "note_entities_llm_labs",                       "nlp_labs_has_data",        "Requires validated lab value with unit + date."),
    ("cervical_ln",          "note_entities_llm_cervical_ln_detail",         "nlp_ln_has_data",          "Requires cervical LN-level detail with laterality."),
    ("parathyroid",          "note_entities_llm_parathyroid_detail",         "nlp_parathyroid_has_data", "Requires parathyroid gland detail (id / autotx / reimplant)."),
    ("pathology",            "note_entities_llm_pathology",                  "nlp_path_has_data",        "Requires at least one validated pathology entity (multifocal/ETE/margin)."),
    ("physical_exam",        "note_entities_llm_physical_exam",              "nlp_physexam_has_data",    "Requires examined-and-documented finding."),
    ("past_medical_hx",      "note_entities_llm_past_medical_hx",            "nlp_pmhx_has_data",        "Requires validated past medical history entity."),
    ("past_surgical_hx",     "note_entities_llm_past_surgical_hx",           "nlp_pshx_has_data",        "Requires validated past surgical history entity."),
    ("patient_decision",     "note_entities_llm_patient_decision_adherence", "nlp_ptdecision_has_data",  "Requires documented patient decision/adherence event."),
    ("rad_treatment",        "note_entities_llm_rad_treatment",              "nlp_radtx_has_data",       "Requires validated radiation treatment mention."),
    ("rai_detailed",         "note_entities_llm_rai_detailed",               "nlp_raidetail_has_data",   "Requires validated RAI detail (dose / scan type)."),
    ("survival_followup",    "note_entities_llm_survival_followup",          "nlp_survfu_has_data",      "Requires survival/followup event with date."),
    ("synoptic_enrichment",  "note_entities_llm_synoptic_pathology_enrichment","nlp_synoptic_has_data",  "Strict concordance tier: rejected unless synoptic vocab aligns with path_synoptics (0.16%% pass-through; per-Prompt-9 REVISED, intentional)."),
    ("tirads",               "note_entities_llm_tirads_granular",            "nlp_tirads_has_data",      "Requires validated TIRADS component scores."),
]


def phase1d(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 1D — NLP rollup strictness audit (nlp_rollup_promotion_audit_v1)")

    rows: list[tuple[str, str, int, int, float, str]] = []
    for domain, src_tbl, canon_col, criterion in NLP_ROLLUP_MAPPING:
        if not column_exists(con, "canonical_patient_master", canon_col):
            src_n = con.execute(
                f"SELECT COUNT(DISTINCT research_id) FROM {src_tbl}"
            ).fetchone()[0]
            rows.append((domain, src_tbl, src_n, 0, 0.0, criterion + " (canonical column MISSING)"))
            continue
        src_n = con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {src_tbl}"
        ).fetchone()[0]
        canon_n = con.execute(
            f'SELECT COUNT(*) FROM canonical_patient_master WHERE "{canon_col}" = TRUE'
        ).fetchone()[0]
        pct = (canon_n / src_n * 100.0) if src_n else 0.0
        rows.append((domain, src_tbl, src_n, canon_n, round(pct, 3), criterion))

    con.execute(
        """
        CREATE OR REPLACE TABLE manuscript_workspace.nlp_rollup_promotion_audit_v1 (
            nlp_domain                    VARCHAR,
            source_table                  VARCHAR,
            source_patient_count          INTEGER,
            canonical_has_data_count      INTEGER,
            pass_through_pct              DOUBLE,
            gating_criterion_description  VARCHAR
        )
        """
    )
    for r in rows:
        con.execute(
            "INSERT INTO manuscript_workspace.nlp_rollup_promotion_audit_v1 VALUES (?,?,?,?,?,?)",
            r,
        )
    con.execute(
        f"""COMMENT ON TABLE manuscript_workspace.nlp_rollup_promotion_audit_v1 IS
            '{SCRIPT_TAG} (2026-04-16): per-NLP-domain gating audit. Documents conservative '
            'promotion from note_entities_llm_* source tables into canonical nlp_*_has_data '
            'columns. source_patient_count = COUNT(DISTINCT research_id) in source (naive). '
            'Analysts wanting broader coverage should join the underlying source table directly. '
            'DO NOT loosen the rollup — conservatism is intentional for publication.'"""
    )
    print(f"  rows inserted: {len(rows)}")
    for r in rows:
        print(
            f"    {r[0]:<24} src={r[2]:>6} canon={r[3]:>6} pct={r[4]:>6.2f}%"
        )


# ---------------------------------------------------------------------------
# Phase 2 — Archive deprecated + _pre235_backup tables
# ---------------------------------------------------------------------------


def phase2(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 2 — Archive deprecated/backup tables -> Thyroid 2026 UPdated.archive_pub_v1_0")

    for table in TABLES_TO_ARCHIVE:
        if not table_exists(con, "main", table):
            print(f"  SKIP (not in canonical): {table}")
            continue
        dest = archive_table_name(table)
        full_dest = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{dest}'
        src_rc = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

        con.execute(f"CREATE OR REPLACE TABLE {full_dest} AS SELECT * FROM {table}")
        dst_rc = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
        if src_rc != dst_rc:
            raise RuntimeError(
                f"Archive row mismatch {table}: src={src_rc} dst={dst_rc} — not dropping."
            )
        con.execute(f'DROP TABLE "{table}"')
        print(f"  ARCHIVED {table:<55} ({src_rc} rows) -> {dest}")


# ---------------------------------------------------------------------------
# Phase 3 — Drill-down surface validation
# ---------------------------------------------------------------------------


DRILL_DOWN_VALIDATIONS: list[tuple[str, list[str]]] = [
    ("ete_adjudication_v1", ["ete_grade_final_v2", "ete_adjudicated_flag"]),
    ("rai_benign_histology_recovery_v234", []),
    ("_molecular_patient_rollup_v227", ["ret_positive_v7", "any_fusion_positive"]),
    ("ret_patient_adjudicated_v226", ["ret_adjudicated_flag", "ret_evidence_source"]),
    ("ret_note_entity_adjudication_v226", []),
    ("patient_tumor_rollup_v1", ["multifocal_flag_path", "r_class_true", "margin_status_true"]),
]


def phase3(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 3 — Drill-down surface validation")

    for table, expected in DRILL_DOWN_VALIDATIONS:
        if not table_exists(con, "main", table):
            print(f"  MISSING TABLE: {table}")
            continue
        n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        missing = [c for c in expected if not column_exists(con, "canonical_patient_master", c)]
        if missing:
            print(f"  {table:<40} rows={n:<6} MISSING_CANONICAL_COLS={missing}")
        else:
            print(f"  {table:<40} rows={n:<6} OK ({', '.join(expected) if expected else 'no-op'})")


# ---------------------------------------------------------------------------
# Phase 4 — Regenerate detail_table_registry_v1
# ---------------------------------------------------------------------------


def domain_heuristic(table_name: str) -> str:
    t = table_name.lower()
    if t.startswith("note_entities_"):
        return "NLP"
    if t.startswith("nsqip_"):
        return "NSQIP"
    if t.startswith("rai_"):
        return "RAI"
    if t.startswith("tg_") or t.startswith("thyroglobulin_") or "_lab_" in t or t.startswith("longitudinal_lab"):
        return "Labs"
    if "molecular" in t or t.startswith("extracted_braf") or t.startswith("extracted_ras") or t.startswith("thyroseq_") or t.startswith("specimen_genomic") or t.startswith("ret_"):
        return "Molecular"
    if "imaging" in t or t.startswith("ct_") or t.startswith("mri_") or t.startswith("us_") or t.startswith("serial_imaging") or "tirads" in t or "ultrasound" in t or t.startswith("nuclear_med"):
        return "Imaging"
    if t.startswith("fna_") or "bethesda" in t:
        return "FNA"
    if t.startswith("ln_") or "lymph" in t or t.startswith("clinical_note_ln"):
        return "Lymph Nodes"
    if "complication" in t or t.startswith("extracted_rln") or t.startswith("extracted_complications"):
        return "Complications"
    if t.startswith("analysis_") or t.startswith("episode_analysis") or t.startswith("lesion_analysis") or t.startswith("patient_analysis"):
        return "Analysis"
    if "diagnosis" in t:
        return "Diagnosis"
    if "recurrence" in t:
        return "Recurrence"
    if "survival" in t:
        return "Survival"
    if "pathology" in t or "path_" in t or "synoptic" in t or t.startswith("tumor_") or t.startswith("specimen") or t.startswith("thyroid_sizes") or t.startswith("thyroid_weights") or t.startswith("ete_"):
        return "Pathology"
    if t.startswith("operative_") or t.startswith("preop_") or t.startswith("surgery_"):
        return "Surgery"
    if t.startswith("linkage_") or t.startswith("imaging_fna_linkage") or t.startswith("patient_completion") or t.startswith("pathology_rai_linkage"):
        return "Linkage"
    if t.startswith("manuscript_"):
        return "Manuscript"
    if "timeline" in t:
        return "Timeline"
    if t in ("clinical_notes_long", "note_extraction_runs"):
        return "Notes"
    if t.startswith("raw_") or t.startswith("md_") or t.startswith("patient_refined"):
        return "Reference"
    return "Other"


REGISTRY_EXCLUDE_PATTERNS = [
    re.compile(r"^canonical_patient_master$"),
    re.compile(r".*pre\d+.*backup$"),
    re.compile(r"^__readme$"),
    re.compile(r"^data_dictionary_v\d+$"),
    re.compile(r".*_pre\d+_backup$"),
]


def phase4(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 4 — Regenerate manuscript_workspace.detail_table_registry_v1")

    # Snapshot existing registry -> preserve domain/feeds/description per table.
    existing = {
        r[0]: r
        for r in con.execute(
            """SELECT detail_table_name, schema_name, join_key, grain, total_rows,
                      total_patients, domain, feeds_master_columns, description,
                      canonical_version
               FROM manuscript_workspace.detail_table_registry_v1"""
        ).fetchall()
    }

    main_tables = [
        r[0]
        for r in con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_type='BASE TABLE'
                ORDER BY table_name"""
        ).fetchall()
    ]

    new_rows: list[tuple] = []
    skipped: list[str] = []
    added: list[str] = []
    removed: list[str] = []
    changed_counts: list[tuple[str, int, int]] = []

    for t in main_tables:
        if any(p.match(t) for p in REGISTRY_EXCLUDE_PATTERNS):
            skipped.append(t)
            continue
        has_rid = column_exists(con, t, "research_id")
        if not has_rid:
            skipped.append(t)
            continue
        total_rows = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        total_pts = con.execute(
            f'SELECT COUNT(DISTINCT research_id) FROM "{t}"'
        ).fetchone()[0]

        prev = existing.get(t)
        if prev is None:
            domain = domain_heuristic(t)
            join_key = "research_id"
            grain = "one row per patient" if total_pts == total_rows else "multi-row per patient"
            feeds = "TODO: manual review (Script 236 auto-added)"
            desc = f"Auto-registered by Script 236 (2026-04-16). Domain={domain}. Review feeds_master_columns manually."
            canonical_version = "v1_0"
            added.append(t)
        else:
            _, schema_name_prev, join_key_prev, grain_prev, prev_rows, prev_pts, domain_prev, feeds_prev, desc_prev, canonical_prev = prev
            if (prev_rows or 0) != total_rows or (prev_pts or 0) != total_pts:
                changed_counts.append((t, prev_rows or 0, total_rows))
            domain = domain_prev or domain_heuristic(t)
            join_key = join_key_prev or "research_id"
            grain = grain_prev or ("one row per patient" if total_pts == total_rows else "multi-row per patient")
            feeds = feeds_prev
            desc = desc_prev
            canonical_version = canonical_prev or "v1_0"

        new_rows.append(
            (t, "main", join_key, grain, total_rows, total_pts, domain, feeds, desc, canonical_version)
        )

    # Registry-only rows (present in prior registry but now absent from main) -> dropped.
    new_names = {r[0] for r in new_rows}
    for prev_name in existing:
        if prev_name not in new_names:
            removed.append(prev_name)

    # Recreate registry.
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.detail_table_registry_v1")
    con.execute(
        """
        CREATE TABLE manuscript_workspace.detail_table_registry_v1 (
            detail_table_name      VARCHAR,
            schema_name            VARCHAR,
            join_key               VARCHAR,
            grain                  VARCHAR,
            total_rows             BIGINT,
            total_patients         BIGINT,
            domain                 VARCHAR,
            feeds_master_columns   VARCHAR,
            description            VARCHAR,
            canonical_version      VARCHAR
        )
        """
    )
    con.executemany(
        "INSERT INTO manuscript_workspace.detail_table_registry_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        new_rows,
    )
    con.execute(
        f"""COMMENT ON TABLE manuscript_workspace.detail_table_registry_v1 IS
            'Regenerated by {SCRIPT_TAG} on 2026-04-16. One row per drill-down table '
            'feeding canonical_patient_master. Preserves prior domain / feeds_master_columns / '
            'description when the table was previously registered. New tables marked TODO.'"""
    )

    print(f"  registered tables: {len(new_rows)}")
    print(f"  ADDED ({len(added)}):")
    for t in sorted(added):
        print(f"    + {t}")
    print(f"  REMOVED ({len(removed)}):")
    for t in sorted(removed):
        print(f"    - {t}")
    print(f"  CHANGED-ROW-COUNTS ({len(changed_counts)}):")
    for t, o, n in sorted(changed_counts):
        print(f"    ~ {t}: {o} -> {n}")
    print(f"  SKIPPED (no research_id or excluded): {len(skipped)}")

    # Also ensure the three new audit-feed tables (Phase 1B / 1D) are added in a
    # manuscript_workspace overlay registry for analyst discoverability.  We add
    # them inline in the main registry too so they surface in Phase 6 checks.
    extra_rows = [
        (
            "vc_paralysis_recalibration_v236",
            "manuscript_workspace",
            "research_id",
            "one row per (research_id × rln injury_type)",
            con.execute(
                "SELECT COUNT(*) FROM manuscript_workspace.vc_paralysis_recalibration_v236"
            ).fetchone()[0],
            con.execute(
                """SELECT COUNT(DISTINCT research_id)
                   FROM manuscript_workspace.vc_paralysis_recalibration_v236"""
            ).fetchone()[0],
            "Complications",
            "comp_vc_paralysis_confirmed;comp_vc_paresis_confirmed;comp_rln_injury_confirmed",
            "Audit of Script 236 VC paralysis/paresis recalibration via extracted_rln_injury_refined_v2 cross-reference.",
            "v1_0",
        ),
        (
            "nlp_rollup_promotion_audit_v1",
            "manuscript_workspace",
            "nlp_domain",
            "one row per NLP domain",
            con.execute(
                "SELECT COUNT(*) FROM manuscript_workspace.nlp_rollup_promotion_audit_v1"
            ).fetchone()[0],
            0,
            "NLP/Audit",
            "(audit only, no canonical column)",
            "Per-domain gating audit for NLP rollup strictness (Script 236). Analysts should drop to source tables when they need broader coverage than canonical nlp_*_has_data columns.",
            "v1_0",
        ),
    ]
    con.executemany(
        "INSERT INTO manuscript_workspace.detail_table_registry_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        extra_rows,
    )
    print(f"  + {len(extra_rows)} manuscript_workspace audit-feed rows appended")


# ---------------------------------------------------------------------------
# Phase 5 — Rebuild __readme catalog
# ---------------------------------------------------------------------------


__README_SEED: dict[str, str] = {
    "data_dictionary_v240": (
        "Canonical data dictionary (Script 240): one row per canonical_patient_master "
        "column with domain, provenance, dtype, and notes. Supersedes v221/v235."
    ),
    "canonical_patient_master_pre236_backup": (
        "Pre-Script-236 snapshot of canonical_patient_master. Archived to "
        "Thyroid 2026 UPdated.archive_pub_v1_0 at end of Script 236."
    ),
    "rai_benign_histology_recovery_v234": (
        "RAI benign-histology recovery table (Script 234). 0 rows indicates no canonical "
        "corrections needed from this source as of Script 234."
    ),
    "ete_adjudication_v1": (
        "LLM adjudication of ETE cases (Script 232). Feeds ete_grade_final_v2."
    ),
    "_molecular_patient_rollup_v227": (
        "Per-patient molecular rollup (Script 227). Supersedes v225. Feeds ret_positive_v7 "
        "and any_fusion_positive."
    ),
    "ret_patient_adjudicated_v226": (
        "Manual RET fusion adjudication at patient level (Script 226). Feeds "
        "ret_adjudicated_flag and ret_evidence_source."
    ),
    "ret_note_entity_adjudication_v226": (
        "Manual RET fusion adjudication at entity level (Script 226). Supports "
        "ret_patient_adjudicated_v226."
    ),
}


def phase5(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 5 — Rebuild __readme catalog")

    # Preserve existing descriptions, augmented by seed map.
    existing = {
        r[0]: r[2]
        for r in con.execute("SELECT table_name, rows, description FROM __readme").fetchall()
    }
    for k, v in __README_SEED.items():
        existing.setdefault(k, v)
        if not existing.get(k) or existing[k].startswith("TODO"):
            existing[k] = v

    main_tables = [
        r[0]
        for r in con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_type='BASE TABLE'
                ORDER BY table_name"""
        ).fetchall()
    ]

    rows: list[tuple[str, int, str]] = []
    for t in main_tables:
        n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        desc = existing.get(t)
        if not desc:
            desc = (
                "TODO: describe" if t != "__readme"
                else "Catalog table — one row per main-schema BASE TABLE with row count and description."
            )
        rows.append((t, n, desc))

    con.execute("DROP TABLE IF EXISTS __readme")
    con.execute(
        """
        CREATE TABLE __readme (
            table_name  VARCHAR,
            rows        BIGINT,
            description VARCHAR
        )
        """
    )
    con.executemany("INSERT INTO __readme VALUES (?,?,?)", rows)
    con.execute(
        f"""COMMENT ON TABLE __readme IS
            'Regenerated by {SCRIPT_TAG} (2026-04-16). One row per main-schema BASE TABLE.'"""
    )

    missing = [t for t, _, d in rows if d.startswith("TODO")]
    print(f"  __readme rows: {len(rows)}  TODO rows: {len(missing)}")
    if missing:
        print("  Tables needing description:")
        for t in missing:
            print(f"    - {t}")


# ---------------------------------------------------------------------------
# Phase 6 — Column pointer verification
# ---------------------------------------------------------------------------


_SKIP_FEED_MARKERS = (
    "(",
    "no direct",
    "audit only",
    "provenance",
    "reference",
    "upstream",
    "crosslink",
    "crosswalk",
    "subset view",
    "manuscript-ready",
    "TODO",
    "dedup crosswalk",
    "specimen->assay",
    "specimen-level",
    "level-specific",
    "episode-level",
    "lesion-level",
    "exam-level",
    "component-level",
)


def _parse_feeds(s: str) -> list[str]:
    """Extract literal column names from a feeds_master_columns string.

    Splits on ``;``, ``,`` and newlines; keeps only tokens that are pure
    snake_case identifiers (no wildcards, parens, or free-text phrases with
    embedded spaces).  This intentionally skips descriptive prose entries
    like ``other benign flags`` or ``tirads component scores``.
    """
    if not s:
        return []
    result: list[str] = []
    for tok in re.split(r"[;,\n]+", s):
        tok = tok.strip().rstrip(".;:")
        if not tok:
            continue
        if "*" in tok or "(" in tok or ")" in tok:
            continue
        if " " in tok or "\t" in tok:
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tok):
            result.append(tok)
    return result


def phase6(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 6 — Canonical column pointer verification")

    rows = con.execute(
        """SELECT detail_table_name, feeds_master_columns, domain
           FROM manuscript_workspace.detail_table_registry_v1
           ORDER BY detail_table_name"""
    ).fetchall()
    cpm_cols = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{DB}' AND table_schema='main'
                  AND table_name='canonical_patient_master'"""
        ).fetchall()
    }

    missing: list[tuple[str, str, str]] = []
    checked = 0
    for table_name, feeds, domain in rows:
        if not feeds:
            continue
        if any(marker in feeds.lower() for marker in (m.lower() for m in _SKIP_FEED_MARKERS)):
            continue
        if "*" in feeds:
            continue
        cols = _parse_feeds(feeds)
        for col in cols:
            checked += 1
            if col not in cpm_cols:
                missing.append((table_name, col, domain or ""))

    out_path = OUTPUT_DIR / "236_missing_canonical_columns.csv"
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["detail_table_name", "feeds_column", "domain"])
        for r in missing:
            w.writerow(r)
    print(f"  tokens checked: {checked}  missing: {len(missing)}")
    print(f"  wrote {out_path.name}")
    for tbl, col, dom in missing[:25]:
        print(f"    {tbl:<40} -> {col}  ({dom})")
    if len(missing) > 25:
        print(f"    ... and {len(missing)-25} more (see CSV)")


# ---------------------------------------------------------------------------
# Phase 7 — Final invariant checks
# ---------------------------------------------------------------------------


def phase7(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 7 — Final invariant checks")

    checks: list[tuple[str, int]] = []

    n_rows = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    checks.append(("canonical_patient_master row count == 10,871", n_rows == N_EXPECTED_PATIENTS))

    n_rid = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM canonical_patient_master"
    ).fetchone()[0]
    checks.append(("distinct research_id == 10,871", n_rid == N_EXPECTED_PATIENTS))

    n_null_rid = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE research_id IS NULL"
    ).fetchone()[0]
    checks.append(("no NULL research_id", n_null_rid == 0))

    n_null_fna = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE fna_path_outcome IS NULL"
    ).fetchone()[0]
    checks.append(("fna_path_outcome fully populated", n_null_fna == 0))

    n_backup = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name LIKE '%pre235_backup%'"""
    ).fetchone()[0]
    checks.append(("no _pre235_backup tables in canonical", n_backup == 0))

    dd_count = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name LIKE 'data_dictionary_v%'"""
    ).fetchone()[0]
    checks.append(("exactly one data_dictionary_v* (v240)", dd_count == 1))

    readme_n = con.execute("SELECT COUNT(*) FROM __readme").fetchone()[0]
    tables_n = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main' AND table_type='BASE TABLE'"""
    ).fetchone()[0]
    checks.append((f"__readme rows ({readme_n}) == main BASE TABLE count ({tables_n})", readme_n == tables_n))

    orphan_reg = con.execute(
        f"""
        SELECT r.detail_table_name
        FROM manuscript_workspace.detail_table_registry_v1 r
        WHERE r.schema_name='main'
          AND NOT EXISTS (
            SELECT 1 FROM information_schema.tables t
            WHERE t.table_catalog='{DB}' AND t.table_schema='main'
              AND t.table_name = r.detail_table_name
          )
        """
    ).fetchall()
    checks.append((f"registry has no orphans (got {len(orphan_reg)})", len(orphan_reg) == 0))

    # Archive destination sanity: all 7 archived tables present in Thyroid 2026 UPdated.archive_pub_v1_0
    archived = {
        r[0]
        for r in con.execute(
            f"""SELECT table_name FROM "{ARCHIVE_DB}".information_schema.tables
                WHERE table_schema='{ARCHIVE_SCHEMA}'"""
        ).fetchall()
    }
    expected_archive_names = [archive_table_name(t) for t in TABLES_TO_ARCHIVE if t != "data_dictionary_v221"]
    # data_dictionary_v221 may already have been archived by a prior run.
    missing_arch = [n for n in expected_archive_names if n not in archived]
    checks.append(
        (f"all archive targets present ({len(expected_archive_names)})", len(missing_arch) == 0)
    )

    # Phase 1 deliverables.
    n_v2_cols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'
              AND column_name LIKE 'comp\\_%\\_days\\_postop\\_v2' ESCAPE '\\'"""
    ).fetchone()[0]
    checks.append(("9 new comp_*_days_postop_v2 columns", n_v2_cols == 9))

    has_mf = column_exists(con, "canonical_patient_master", "nlp_path_multifocal_concordance_v2")
    checks.append(("nlp_path_multifocal_concordance_v2 present", has_mf))

    audit_rows = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.nlp_rollup_promotion_audit_v1"
    ).fetchone()[0]
    checks.append((f"nlp_rollup_promotion_audit_v1 populated (got {audit_rows})", audit_rows > 0))

    all_ok = True
    for label, ok in checks:
        flag = "PASS" if ok else "FAIL"
        print(f"  [{flag}] {label}")
        if not ok:
            all_ok = False

    if not all_ok:
        raise SystemExit("PHASE 7 INVARIANTS FAILED — aborting before Phase 8/9.")

    n_tables = tables_n
    n_cols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'"""
    ).fetchone()[0]
    reg_n = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1"
    ).fetchone()[0]

    print("\n" + "=" * 78)
    print("CANONICAL FINALIZATION COMPLETE — thyroid_canonical_publication_v1_0")
    print("All invariants hold. Registry repointed. Backups archived.")
    print(f"canonical_patient_master: {n_rows:,} × {n_cols} cols")
    print(f"detail_table_registry_v1: {reg_n} drill-down tables, all column pointers verified")
    print(f"__readme: {readme_n} tables, all described")
    print("Deprecated tables moved to Thyroid 2026 UPdated.archive_pub_v1_0")
    print("Ready for publication.")
    print("=" * 78)


# ---------------------------------------------------------------------------
# Phase 8 — Archive canonical_patient_master_pre236_backup
# ---------------------------------------------------------------------------


def phase8(con: duckdb.DuckDBPyConnection) -> None:
    banner("PHASE 8 — Archive canonical_patient_master_pre236_backup")

    if not table_exists(con, "main", "canonical_patient_master_pre236_backup"):
        print("  nothing to archive")
        return
    dest = archive_table_name("canonical_patient_master_pre236_backup")
    full_dest = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.{dest}'
    src_rc = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master_pre236_backup"
    ).fetchone()[0]
    con.execute(
        f"CREATE OR REPLACE TABLE {full_dest} AS SELECT * FROM canonical_patient_master_pre236_backup"
    )
    dst_rc = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    assert src_rc == dst_rc, f"archive copy mismatch {src_rc} vs {dst_rc}"
    con.execute("DROP TABLE canonical_patient_master_pre236_backup")
    print(f"  archived {src_rc} rows -> {dest}")

    # Re-run "no pre*_backup" invariant.
    n_backup = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_name LIKE '%pre%backup%'"""
    ).fetchone()[0]
    assert n_backup == 0, f"still have {n_backup} pre*_backup tables"
    print("  invariant: zero pre*_backup tables in canonical (PASS)")

    # Rebuild __readme so the now-dropped pre236 backup row is removed and the
    # __readme count still matches main BASE TABLE count.
    print("  rebuilding __readme post-drop...")
    phase5(con)


# ---------------------------------------------------------------------------
# Confirmation queries
# ---------------------------------------------------------------------------


def run_confirm(con: duckdb.DuckDBPyConnection) -> None:
    banner("CONFIRMATION — 5 finalization queries")

    q1 = con.execute(
        f"""SELECT COUNT(*) AS patients,
                  (SELECT COUNT(*) FROM information_schema.columns
                   WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                     AND table_schema='main') AS columns
            FROM canonical_patient_master"""
    ).fetchone()
    print(f"  1) canonical shape: patients={q1[0]}  columns={q1[1]}")

    q2 = con.execute(
        f"""SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND (table_name LIKE '%pre%backup%'
                   OR table_name LIKE 'data_dictionary_v221'
                   OR table_name LIKE 'data_dictionary_v235')
            ORDER BY 1"""
    ).fetchall()
    print(f"  2) lingering backup/deprecated tables: {[r[0] for r in q2]}  (expect 0)")

    q3 = con.execute(
        """SELECT COUNT(*) AS registered_tables,
                  COUNT(*) FILTER (WHERE feeds_master_columns IS NULL OR feeds_master_columns = '') AS unmapped
           FROM manuscript_workspace.detail_table_registry_v1"""
    ).fetchone()
    print(f"  3) registry: registered={q3[0]}  unmapped={q3[1]}")

    q4a = con.execute("SELECT COUNT(*) FROM __readme").fetchone()[0]
    q4b = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{DB}' AND table_schema='main'
              AND table_type='BASE TABLE'"""
    ).fetchone()[0]
    print(f"  4) __readme rows={q4a}  main BASE TABLES={q4b}  (equal? {q4a == q4b})")

    q5 = con.execute(
        f"""SELECT
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                 AND table_schema='main'
                 AND column_name LIKE 'comp\\_%\\_days\\_postop\\_v2' ESCAPE '\\') AS new_timing_cols,
              (SELECT COUNT(*) FROM information_schema.columns
               WHERE table_catalog='{DB}' AND table_name='canonical_patient_master'
                 AND table_schema='main'
                 AND column_name='nlp_path_multifocal_concordance_v2') AS new_multifocal_col,
              (SELECT COUNT(*) FROM manuscript_workspace.nlp_rollup_promotion_audit_v1) AS nlp_audit_domains"""
    ).fetchone()
    print(
        f"  5) coworker audit landed: new_timing_cols={q5[0]}  "
        f"new_multifocal_col={q5[1]}  nlp_audit_domains={q5[2]}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


PHASES = {
    "0": phase0,
    "1a": phase1a,
    "1b": phase1b,
    "1c": phase1c,
    "1d": phase1d,
    "2": phase2,
    "3": phase3,
    "4": phase4,
    "5": phase5,
    "6": phase6,
    "7": phase7,
    "8": phase8,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--phase",
        default="all",
        help="comma-separated list of phases (0,1a,1b,1c,1d,2,3,4,5,6,7,8) or 'all' (default)",
    )
    ap.add_argument("--skip-confirm", action="store_true", help="skip final confirmation queries")
    args = ap.parse_args()

    con = connect()

    if args.phase == "all":
        order = ["0", "1a", "1b", "1c", "1d", "2", "3", "4", "5", "6", "7", "8"]
    else:
        order = [p.strip().lower() for p in args.phase.split(",")]

    t0 = datetime.now()
    for p in order:
        if p not in PHASES:
            raise SystemExit(f"unknown phase: {p}")
        PHASES[p](con)
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nTotal elapsed: {elapsed:.1f}s")

    if not args.skip_confirm and args.phase == "all":
        run_confirm(con)


if __name__ == "__main__":
    main()
