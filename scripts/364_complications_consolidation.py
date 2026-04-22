#!/usr/bin/env python3
"""Script 364 — Complications Consolidation (v2 build).

Consolidates the complications layer into TWO canonical tables and TWO thin
``views_readable`` views, with a 12-value `complication_type` enum and lean
12-column events grain (research_id is the only linkage column — every row
carries source_table + source_row_id + finding_date for downstream JOIN).

The build (default phase set) does NOT drop legacy tables. The 3-commit
cascade is:

    1.  --commit                 build canonicals + registry + QA + CPM audit
    2.  scripts/364_cpm_feeder_repoint.py --commit   (separate file, separate commit)
    3.  --commit --phase 7       archive snapshots + DROP TABLE on the
                                 5 deprecated source tables (post-CPM-repoint)

Outputs (under thyroid_canonical_publication_v1_0):
    main.canonical_complications_events_v1            (events grain, 12 cols)
    main.canonical_complications_patient_rollup_v1    (patient grain, 17 cols)
    views_readable.complications_events_VIEW_v1
    views_readable.complications_patient_rollup_VIEW_v1
    manuscript_workspace.detail_table_registry_v1     (idempotent registry sync)

Sources consumed (after revisions Apr 22 2026):
    main.complication_phenotype_v1            (5,978 rows; structured)
    main.note_entities_complications          (9,359 rows; entity_legacy)
    main.extracted_complications_refined_v5   (358 rows; refined extraction)
    main.extracted_rln_injury_refined_v2      (92 rows; all → rln_injury)
    main.canonical_labs_calcium_v1            (LAB-DERIVED hypocalcemia path:
                                               Ca < 8.0 mg/dL within 30 days
                                               post-op — see CHANGE E rationale)
    main.canonical_survival_followup_v1       (PERI-OP MORTALITY only; built by
                                               Script 364B. JOINed via
                                               first_surgery + 30d to
                                               distinguish peri-op death (= a
                                               complication) from long-term
                                               mortality (= survival follow-up,
                                               not a complication))
    main.complication_patient_summary_v1      (NOT consumed — superseded; kept
                                               alive until --phase 7)

Sources EXPLICITLY DEFERRED out of this canonical:
    main.note_entities_llm_survival_followup     (long-term mortality + voice
                                                  signals — promoted to Script
                                                  364B; this canonical only
                                                  picks up peri-op-window
                                                  mortality after a JOIN)
    main.note_entities_llm_recurrence            (recurrence domain, not
                                                  complications — Script 367)
    main.note_entities_llm_dynamic_risk_response (ATA risk stratification,
                                                  not complications — Script 367)

Vocabulary (12 values, fixed):
    rln_injury, vocal_cord_paralysis, hypocalcemia_clinical,
    hypoparathyroidism, hematoma, seroma, chyle_leak, wound_infection,
    pneumothorax, airway_complication, wound_dehiscence, mortality

Per-cohort note: pneumothorax / airway_complication / wound_dehiscence are
0-population in this cohort across every source. Their `ever_*_present`
columns stay FALSE for all patients. The schema preserves the full 12-value
vocabulary so the publication contract is comparable across future cohorts
where these complications may appear.

Hypocalcemia case definition (CHANGE E, refined Apr 22 2026):
A row qualifies as a `hypocalcemia_clinical` event if ANY of:
    1. complication_phenotype_v1.treatment_requiring_flag = TRUE
       (calcium / calcitriol initiated)
    2. complication_phenotype_v1.confirmed_flag = TRUE
    3. complication_phenotype_v1.status_v2 LIKE 'confirmed_%'
    4. corrected_Ca < 8.0 mg/dL within 30 days post-op
       (NOTE — albumin canonical not available in this build, so we use
        measured Ca rather than albumin-corrected Ca; lab_value_at_detection
        and lab_units carry the raw measurement for downstream re-correction.
        See close-out doc for citation: ATA 2015 guidelines for postop
        hypocalcemia threshold.)
    5. ionized_Ca < 4.5 mg/dL within 30 days post-op
       (NOT IMPLEMENTED — no ionized-calcium canonical in this build; the
        column is reserved in case a future lab canonical adds it.)

Lab-derived hypocalcemia events carry source_evidence_type='lab_threshold_met'
and populate lab_value_at_detection + lab_units. They do NOT supersede
already-present clinical events on the same patient — the dedup pass
preserves multi-source corroboration.

Auth: motherduck_client.get_token(). PHI rule: research_id only — never
log clinical text or evidence_span content. Evidence text is hashed via
SHA256 and only the hash is stored on the canonical (NOT raw text).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_ID = "364"
SCRIPT_TAG = f"Script {SCRIPT_ID}"
BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_TS_COMPACT = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_FQ = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
WS_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
VIEW_SCHEMA = "views_readable"

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
QA_DIR = REPO_ROOT / "qa"
LOG_PATH = OUTPUT_DIR / f"{SCRIPT_ID}_run_{RUN_TS_COMPACT}.log"
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_complications_consolidation.json"
CPM_AUDIT_PATH = REPO_ROOT / f"complications_cpm_feeder_audit_{RUN_TS_COMPACT}.md"
SIDECAR_DIR = REPO_ROOT / "qa" / f"364_sidecar_{RUN_TS_COMPACT}"

# 12-value enum (fixed). Order is the canonical column order on the rollup.
COMPLICATION_TYPES: list[str] = [
    "rln_injury",
    "vocal_cord_paralysis",
    "hypocalcemia_clinical",
    "hypoparathyroidism",
    "hematoma",
    "seroma",
    "chyle_leak",
    "wound_infection",
    "pneumothorax",
    "airway_complication",
    "wound_dehiscence",
    "mortality",
]
ENUM_SET: set[str] = set(COMPLICATION_TYPES)

# Tunable case-definition / temporal constants (CHANGE D + E + F + H).
HYPOCALCEMIA_CA_THRESHOLD_MGDL = 8.0
HYPOCALCEMIA_POSTOP_WINDOW_DAYS = 30
PERIOP_MORTALITY_WINDOW_DAYS = 30
TEMPORAL_RESOLUTION_WINDOW_DAYS = 180   # 6 months — transient/permanent split
PREOP_PROXIMITY_BUFFER_DAYS = 7         # pre-surgery buffer for "preexisting"
PTH_LOW_THRESHOLD_PG_ML = 15            # CHANGE F — hypopara definitive PTH cutoff
WOUND_INFECTION_POSTOP_WINDOW_DAYS = 30 # CHANGE H

# CHANGE F — evidence_strength enum (3 values, fixed).
EVIDENCE_STRENGTH_VALUES: tuple[str, ...] = ("definitive", "probable", "possible")

# CHANGE J — forensic sampling parameters.
FORENSIC_SAMPLE_N = 20
FORENSIC_TYPES: list[tuple[str, str | None]] = [
    # (complication_type, optional source_evidence_type filter)
    ("chyle_leak", None),
    ("seroma", None),
    ("rln_injury", "nlp_proxy"),
]
# Local-only PHI directory (added to .gitignore at repo root).
PHI_FORENSIC_DIR = REPO_ROOT / "phi_forensic"

# Per-complication linkage window for onset_class derivation (days).
# Used only when a row's source has no native timing AND we need to JOIN
# to canonical_operative_events_v1 to find the nearest preceding surgery.
ONSET_WINDOW_DAYS: dict[str, int] = {
    "rln_injury": 30,
    "vocal_cord_paralysis": 30,
    "hematoma": 30,
    "seroma": 30,
    "chyle_leak": 30,
    "wound_infection": 30,
    "pneumothorax": 30,
    "wound_dehiscence": 30,
    "hypocalcemia_clinical": 180,
    "hypoparathyroidism": 180,
    "airway_complication": 180,
    "mortality": 365,
}

# Vocabulary mapping for STRUCTURED + ENTITY sources (raw label → 12-value enum).
# Keys MUST be lowercase trimmed; lookup is case-insensitive in code.
COMPLICATION_VOCAB_MAP: dict[str, str] = {
    # rln_injury
    "rln_injury": "rln_injury",
    "recurrent_laryngeal_nerve_injury": "rln_injury",
    "recurrent laryngeal nerve injury": "rln_injury",
    "rln injury": "rln_injury",
    "nerve_injury_rln": "rln_injury",
    # vocal cord — paresis maps to paralysis enum (the prompt enum has only
    # vocal_cord_paralysis; paresis is a milder grade of the same lesion and
    # downstream consumers conventionally bucket them together).
    "vocal_cord_paralysis": "vocal_cord_paralysis",
    "vocal cord paralysis": "vocal_cord_paralysis",
    "vocal_cord_palsy": "vocal_cord_paralysis",
    "vocal cord palsy": "vocal_cord_paralysis",
    "vc_paralysis": "vocal_cord_paralysis",
    "vocal_cord_paresis": "vocal_cord_paralysis",
    "vocal cord paresis": "vocal_cord_paralysis",
    # hypocalcemia (clinical)
    "hypocalcemia": "hypocalcemia_clinical",
    "hypocalcemia_symptomatic": "hypocalcemia_clinical",
    "hypocalcemia_treated": "hypocalcemia_clinical",
    "tetany": "hypocalcemia_clinical",
    # hypoparathyroidism
    "hypoparathyroidism": "hypoparathyroidism",
    "permanent_hypoparathyroidism": "hypoparathyroidism",
    # hematoma
    "hematoma": "hematoma",
    "neck_hematoma": "hematoma",
    "neck hematoma": "hematoma",
    # seroma
    "seroma": "seroma",
    # chyle_leak
    "chyle_leak": "chyle_leak",
    "chyle leak": "chyle_leak",
    # wound_infection
    "wound_infection": "wound_infection",
    "ssi": "wound_infection",
    "surgical_site_infection": "wound_infection",
    # The remaining 3 enum values (pneumothorax, airway_complication,
    # wound_dehiscence) have no observed inputs in this cohort. Adding their
    # natural-name keys here so future cohorts that produce them are mapped
    # without a code change.
    "pneumothorax": "pneumothorax",
    "ptx": "pneumothorax",
    "airway_complication": "airway_complication",
    "tracheal_injury": "airway_complication",
    "airway_obstruction": "airway_complication",
    "wound_dehiscence": "wound_dehiscence",
    # mortality (only structured source for it is the LLM survival_followup
    # vital_status entity; structured tables don't carry mortality).
    "mortality": "mortality",
    "death": "mortality",
    "deceased": "mortality",
}

# source_evidence_type enum — full domain across all 12 complication types
# (CHANGE C, refined Apr 22 2026). Per-type subsets documented in
# `_source_evidence_type_case_sql()`. The canonical set is union'd here so
# the QA gate "source_evidence_type_in_canonical_set" can validate cleanly.
SOURCE_EVIDENCE_TYPE_VALUES: set[str] = {
    # vocal_cord_paralysis
    "laryngoscopy_direct", "imaging_inferred", "operative_note",
    "voice_quality_llm_proxy",
    # hypocalcemia_clinical
    "lab_threshold_met", "symptomatic_only", "treatment_initiated",
    # hypoparathyroidism
    "lab_pth_low", "clinical_diagnosis", "replacement_therapy_only",
    # rln_injury
    "intraop_observed", "postop_laryngoscopy", "imaging_only",
    "chart_documented", "nlp_proxy",
    # chyle_leak
    "drain_triglycerides", "milky_drain_visual", "imaging",
    # hematoma
    "surgical_reexploration", "drain_output_or_clinical_observation",
    "structured_chart",
    # seroma
    "aspiration_or_clinical_observation",
    # wound_infection
    "culture_positive", "antibiotic_initiated",
    # pneumothorax — enum reserved for future cohorts (0-pop here)
    "imaging_chest_xray", "imaging_ct",
    # airway_complication — enum reserved
    "intubation_required", "tracheostomy",
    # wound_dehiscence — enum reserved
    "surgical_reclosure",
    # mortality (peri-op)
    "death_certificate", "registry_match",
    "last_followup_no_contact_proxy",
    # universal fallback
    "clinical_observation",
}

# Supporting tables consumed at build time but NOT archived/dropped
# (they're owned by other canonical pipelines).
SUPPORTING_TABLES: list[tuple[str, str, bool]] = [
    # (schema, table, required) — required=True aborts if missing.
    ("main", "canonical_patient_master", True),
    ("main", "canonical_operative_events_v1", True),
    ("main", "canonical_labs_calcium_v1", True),
    ("main", "canonical_labs_pth_v1", True),
    # canonical_survival_followup_v1 is built by Script 364B. It must exist
    # before this script runs (peri-op mortality merge depends on it).
    # We surface a clear error message if it's missing rather than silently
    # ship 0 mortality rows.
    ("main", "canonical_survival_followup_v1", True),
]

# Sources to drop in --phase 7 (after CPM repoint commit lands).
# (schema, table, role)
DEPRECATED_SOURCES: list[tuple[str, str, str]] = [
    ("main", "complication_phenotype_v1",
     "superseded by canonical_complications_events_v1"),
    ("main", "complication_patient_summary_v1",
     "superseded by canonical_complications_patient_rollup_v1"),
    ("main", "note_entities_complications",
     "consumed by canonical_complications_events_v1"),
    ("main", "extracted_complications_refined_v5",
     "consumed by canonical_complications_events_v1"),
    ("main", "extracted_rln_injury_refined_v2",
     "consumed by canonical_complications_events_v1"),
    # Note: the 3 LLM tables (recurrence, dynamic_risk_response,
    # survival_followup) STAY LIVE — they're consumed by 364 + future
    # canonicals. Script 367 will decide their final disposition.
]

# All 8 sources for archive snapshot (Step 0 archives the 5 we will drop;
# the 3 LLM tables are NOT archived because they're not being dropped —
# they remain live and are consumed by future canonicals).
SOURCES_TO_ARCHIVE: list[tuple[str, str]] = [
    (sch, tbl) for sch, tbl, _ in DEPRECATED_SOURCES
]

# Direct sources for the events build (Step 0 verifies existence + row counts).
# Note: complication_patient_summary_v1 is included for archive parity (will
# be dropped in --phase 7) but is NOT consumed by the events build (it's
# superseded by canonical_complications_patient_rollup_v1, which we rebuild
# from scratch). The 3 LLM tables (recurrence, dynamic_risk_response,
# survival_followup) are EXPLICITLY out of scope for 364 — see header
# docstring; survival_followup is consumed indirectly via 364B.
ALL_SOURCES_INVENTORY: list[tuple[str, str, int]] = [
    ("main", "complication_phenotype_v1", 5_978),
    ("main", "complication_patient_summary_v1", 2_938),
    ("main", "note_entities_complications", 9_359),
    ("main", "extracted_complications_refined_v5", 358),
    ("main", "extracted_rln_injury_refined_v2", 92),
]

# Required columns per source (Step 0 column-existence pre-flight).
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "complication_phenotype_v1": [
        "research_id", "complication_entity", "suspected_flag",
        "confirmed_flag", "transient_flag", "permanent_flag",
        "surgery_related_flag", "historical_only_flag",
        "timing_days_post_surgery", "timing_window",
        "final_complication_status", "status_v2",
        "treatment_requiring_flag", "voice_resolution_noted",
        "voice_permanence_noted", "evidence_tier", "source_tier_label",
        "detection_date", "first_surgery_date", "biochemical_low_ca",
    ],
    "note_entities_complications": [
        "research_id", "note_row_id", "note_type", "entity_value_norm",
        "entity_value_raw", "present_or_negated", "confidence",
        "evidence_span", "evidence_start", "entity_date", "note_date",
        "source_line",
    ],
    "extracted_complications_refined_v5": [
        "research_id", "entity_name", "entity_is_confirmed",
        "entity_tier", "entity_evidence_strength", "source_tier_label",
        "detection_date", "mention_count",
    ],
    "extracted_rln_injury_refined_v2": [
        "research_id", "injury_type", "laterality",
        "rln_injury_is_confirmed", "rln_injury_tier",
        "rln_injury_evidence_strength", "classification",
        "temporality", "first_surgery_date", "days_post_surgery",
        "temporal_window", "detection_date",
    ],
    # Supporting tables (validated separately, but list a few key columns).
    "canonical_labs_calcium_v1": [
        "research_id", "lab_datetime", "value_numeric", "unit_standardized",
    ],
    "canonical_labs_pth_v1": [
        "research_id", "lab_datetime", "value_numeric", "unit_standardized",
    ],
    # Note: canonical_survival_followup_v1 schema is checked dynamically in
    # step_0_preflight (CHANGE B coexistence-with-legacy block). The new
    # schema requires vital_status_current + death_date + death_date_source
    # + first_surgery_date but the column-existence pre-flight is soft so a
    # legacy-schema table doesn't block dry-runs.
    "canonical_operative_events_v1": [
        "research_id", "surgery_date_native",
    ],
}

# Modality buckets — used for source_modality classification on each row.
NOTE_TYPE_TO_MODALITY: dict[str, str] = {
    "operative_note": "op_note",
    "op_note": "op_note",
    "operative": "op_note",
    "discharge_summary": "discharge_summary",
    "discharge": "discharge_summary",
    "clinic_note": "clinic_note",
    "clinic": "clinic_note",
    "progress_note": "clinic_note",
    "office_note": "clinic_note",
    "pathology_synoptic": "path_synoptic",
    "path_synoptic": "path_synoptic",
    "imaging": "imaging",
    "us_report": "imaging",
    "ct_report": "imaging",
}

_LOG_LINES: list[str] = []


# ---------------------------------------------------------------------------
# Logging / utilities
# ---------------------------------------------------------------------------

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"
    line = f"[{level}] [{ts}] {msg}"
    print(line, flush=True)
    _LOG_LINES.append(line)


def log_warn(msg: str) -> None:
    log(msg, "WARN")


def log_error(msg: str) -> None:
    log(msg, "ERROR")


def flush_log() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as fh:
        fh.write("\n".join(_LOG_LINES) + "\n")


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def _safe_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    con.execute(f'USE "{CANONICAL_DB}".main')
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def view_exists(con: duckdb.DuckDBPyConnection, schema: str, view: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
              AND table_type = 'VIEW'
        """,
        [CANONICAL_DB, schema, view],
    ).fetchone()
    return row is not None


def list_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [CANONICAL_DB, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def column_exists(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, column: str
) -> bool:
    return column in list_columns(con, schema, table)


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {fq(schema, table)}").fetchone()[0])


def distinct_research_ids(
    con: duckdb.DuckDBPyConnection, schema: str, table: str
) -> int:
    if not column_exists(con, schema, table, "research_id"):
        return -1
    return int(
        con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {fq(schema, table)}"
        ).fetchone()[0]
    )


# ---------------------------------------------------------------------------
# Vocab mapping helpers (used by Step 2 and CPM audit)
# ---------------------------------------------------------------------------

def map_vocab(raw: str | None) -> str | None:
    """Map a raw vocabulary token to one of the 12 canonical enum values.

    Lookup is case-insensitive on a trimmed lowercase key. Returns None
    when no mapping exists. Callers must decide whether to drop, hold for
    QA review, or abort based on caller-side counts.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raw = str(raw)
    key = raw.strip().lower()
    if not key:
        return None
    return COMPLICATION_VOCAB_MAP.get(key)


def evidence_hash(text: str | None) -> str | None:
    """SHA256 hash of the evidence text (PHI-safe; we never store raw text)."""
    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Step 0 — Pre-flight (Patterns 7, 8, 9) + idempotent archive snapshots
# ---------------------------------------------------------------------------

def step_0_preflight(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight & archive (BUILD_TS={BUILD_TS})")
    log("=" * 78)

    inventory: list[dict[str, Any]] = []
    for schema, tbl, expected_n in ALL_SOURCES_INVENTORY:
        if not table_exists(con, schema, tbl):
            raise RuntimeError(
                f"Source table {schema}.{tbl} missing — refusing to build."
            )
        n = row_count(con, schema, tbl)
        p = distinct_research_ids(con, schema, tbl)
        delta = n - expected_n
        rel_drift = abs(delta) / expected_n if expected_n else 0.0
        log(f"  inventory: {schema}.{tbl}: rows={n:,} pts={p:,} "
            f"(expected={expected_n:,}, drift={delta:+,}, "
            f"rel={rel_drift:.2%})")
        inventory.append({
            "schema": schema, "table": tbl,
            "rows": n, "patients": p,
            "expected_rows": expected_n, "row_delta": delta,
        })
        # Soft drift gate — warn if > 2% but don't abort.
        if rel_drift > 0.02:
            log_warn(f"    row count drifted >2% from expectation; "
                     f"verify upstream hasn't changed materially.")

    # Supporting-table existence check (dependencies built by other scripts).
    log("")
    log("  supporting tables (built/owned by other scripts):")
    for schema, tbl, required in SUPPORTING_TABLES:
        if not table_exists(con, schema, tbl):
            if required:
                raise RuntimeError(
                    f"Required supporting table {schema}.{tbl} missing. "
                    f"Run prerequisite script first "
                    f"(e.g. scripts/364B_survival_followup_consolidation.py "
                    f"--commit for canonical_survival_followup_v1, or "
                    f"scripts/347_lab_master_canonical_v1_build.py --commit "
                    f"for canonical_labs_*_v1)."
                )
            log_warn(f"    optional supporting table {schema}.{tbl} missing")
            continue
        n = row_count(con, schema, tbl)
        log(f"    {schema}.{tbl}: rows={n:,}")

    # CHANGE B coexistence-with-legacy: if canonical_survival_followup_v1
    # exists but is the LEGACY follow-up-time schema (no vital_status_current
    # column), we cannot consume it for peri-op mortality. Surface a clear
    # warning and disable the periop_mortality CTE in the build. After 364B
    # commits and replaces the legacy table, this branch goes away
    # automatically.
    surv_cols = set(list_columns(con, "main", "canonical_survival_followup_v1"))
    has_new_surv_schema = "vital_status_current" in surv_cols and "death_date" in surv_cols
    if not has_new_surv_schema:
        log_warn(
            "  canonical_survival_followup_v1 has the LEGACY schema "
            "(no vital_status_current/death_date). Peri-op mortality CTE "
            "will be SKIPPED in this build. Run "
            "scripts/364B_survival_followup_consolidation.py --commit to "
            "replace it with the new schema, then re-run 364."
        )

    # Required-column existence pre-flight (Pattern 7).
    log("")
    log("  required-column existence pre-flight:")
    column_misses: dict[str, list[str]] = {}
    for tbl, cols in REQUIRED_COLUMNS.items():
        present = set(list_columns(con, "main", tbl))
        missing = [c for c in cols if c not in present]
        if missing:
            column_misses[tbl] = missing
            log_error(f"    {tbl}: MISSING required columns {missing}")
        else:
            log(f"    {tbl}: all {len(cols)} required cols present ✓")
    if column_misses:
        raise RuntimeError(
            f"Required column pre-flight failed: {column_misses}. "
            f"Refusing to build with schema drift."
        )

    # Mention-grain partition-key probe (Pattern 8). Re-run the recipes that
    # the pre-build probe verified collision-free, on live data.
    log("")
    log("  partition-key uniqueness probes:")
    pk_recipes: list[tuple[str, str]] = [
        ("complication_phenotype_v1",
         "hash(research_id, complication_entity, detection_date)"),
        ("note_entities_complications",
         "hash(research_id, note_row_id, source_line, entity_value_norm, "
         "evidence_start)"),
        ("extracted_complications_refined_v5",
         "hash(research_id, entity_name, detection_date)"),
        ("extracted_rln_injury_refined_v2",
         "hash(research_id, injury_type, detection_date, classification, "
         "temporality)"),
    ]
    for tbl, recipe in pk_recipes:
        sql = (
            f"WITH probe AS ("
            f"  SELECT {recipe} AS proposed_id FROM main.\"{tbl}\""
            f") "
            f"SELECT COUNT(*), COUNT(DISTINCT proposed_id), "
            f"COUNT(*) - COUNT(DISTINCT proposed_id) FROM probe"
        )
        tot, uniq, col = con.execute(sql).fetchone()
        if col != 0:
            raise RuntimeError(
                f"Partition-key collision on {tbl}: rows={tot:,} "
                f"distinct_ids={uniq:,} collisions={col:,}. "
                f"Recipe={recipe}. Refusing to build."
            )
        log(f"    {tbl}: {tot:,} rows / {uniq:,} unique IDs (no collisions)")

    # Lab-derived hypocalcemia partition key probe (CHANGE E).
    # Recipe: hash(research_id, lab_datetime, value_numeric) — paired values
    # at the same timestamp are common (e.g. ionised + total Ca) so the
    # value disambiguates.
    sql_lab = """
        SELECT COUNT(*),
               COUNT(DISTINCT (research_id, lab_datetime, value_numeric)),
               COUNT(*) - COUNT(DISTINCT (research_id, lab_datetime, value_numeric))
        FROM main.canonical_labs_calcium_v1
    """
    tot, uniq, col = con.execute(sql_lab).fetchone()
    if col != 0:
        raise RuntimeError(
            f"canonical_labs_calcium_v1 partition-key collision: rows={tot:,} "
            f"uniq={uniq:,} collisions={col:,}. Recipe must include "
            f"value_numeric. Refusing to build."
        )
    log(f"    canonical_labs_calcium_v1 (rid, lab_datetime, value_numeric): "
        f"{tot:,} rows / {uniq:,} unique (no collisions)")

    # Idempotent archive snapshots (Pattern 6) — only the 5 sources we plan
    # to drop in --phase 7. The 3 LLM tables are NOT archived here because
    # they're staying live.
    log("")
    log("  archive snapshots (idempotent):")
    snapshots: list[dict[str, Any]] = []
    for sch, tbl in SOURCES_TO_ARCHIVE:
        snapshots.append(_archive_one(con, sch, tbl, do_writes))
    return {
        "build_ts": BUILD_TS,
        "inventory": inventory,
        "snapshots": snapshots,
        "pre_counts": {
            f"{r['schema']}.{r['table']}": r["rows"] for r in inventory
        },
    }


def _archive_one(
    con: duckdb.DuckDBPyConnection, schema: str, table: str, do_writes: bool
) -> dict[str, Any]:
    src = fq(schema, table)
    dst_name = f"{table}_pre364_{BUILD_TS}"
    dst = f'{ARCHIVE_FQ}."{dst_name}"'
    n_src = row_count(con, schema, table)
    log(f"    plan: {schema}.{table} ({n_src:,} rows) -> {dst_name}")
    if not do_writes:
        return {"src": f"{schema}.{table}", "dst": dst_name,
                "rows": n_src, "status": "DRY_RUN"}
    already = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA, dst_name],
    ).fetchone()
    if already:
        n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
        log(f"      archive already exists: {dst_name} ({n_dst:,} rows) — skipping")
        if n_dst != n_src:
            raise RuntimeError(
                f"Existing archive {dst_name} has {n_dst:,} rows but live "
                f"has {n_src:,}. Refusing to overwrite."
            )
        return {"src": f"{schema}.{table}", "dst": dst_name,
                "rows": n_dst, "status": "EXISTS"}
    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
    if n_dst != n_src:
        raise RuntimeError(
            f"Archive row count mismatch for {schema}.{table}: "
            f"src={n_src:,} dst={n_dst:,}"
        )
    try:
        con.execute(
            f"COMMENT ON TABLE {dst} IS "
            f"'{SCRIPT_TAG} ({RUN_DATE}) pre-consolidation snapshot of "
            f"main.{table}.'"
        )
    except Exception as exc:
        log_warn(f"    COMMENT ON {dst_name} failed (non-fatal): {exc}")
    log(f"      archived -> {dst_name} ({n_dst:,} rows)")
    return {"src": f"{schema}.{table}", "dst": dst_name,
            "rows": n_dst, "status": "ARCHIVED"}


# ---------------------------------------------------------------------------
# Step 2 — Vocabulary normalization (helper SQL)
# ---------------------------------------------------------------------------
#
# We push the COMPLICATION_VOCAB_MAP into the build SQL via a small CASE
# statement. Doing it in SQL (rather than pulling rows to pandas) keeps the
# build set-based and lets DuckDB optimise. The CASE is generated from the
# Python dict so there's a single source of truth.

def _vocab_map_case_sql(input_expr: str) -> str:
    """Render a CASE expression mapping `input_expr` (lower-trimmed) to the
    canonical 12-value enum. Returns NULL if no match.
    """
    parts = ["CASE LOWER(TRIM(CAST(" + input_expr + " AS VARCHAR)))"]
    for raw, canon in COMPLICATION_VOCAB_MAP.items():
        # We single-quote escape the raw key (none of the keys in our dict
        # contain single quotes; if that ever changes, escape with replace).
        if "'" in raw or "'" in canon:
            raise ValueError(f"vocab map entry contains single quote: {raw}->{canon}")
        parts.append(f"  WHEN '{raw}' THEN '{canon}'")
    parts.append("  ELSE NULL")
    parts.append("END")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Step 1+3+4 — Build canonical_complications_events_v1
# ---------------------------------------------------------------------------
#
# Architecture (post-revision Apr 22 2026):
#   Source CTEs:
#     phenotype       structured complication_phenotype_v1
#     entity_mapped   note_entities_complications (entity_legacy)
#     extracted_v5    extracted_complications_refined_v5 (refined extraction)
#     extracted_rln   extracted_rln_injury_refined_v2 (refined extraction)
#     lab_hypocalc    canonical_labs_calcium_v1 (CHANGE E — lab-derived
#                     hypocalcemia, Ca < 8.0 mg/dL within 30d post-op)
#     periop_mortality canonical_survival_followup_v1 (CHANGE B — only deaths
#                     with death_date <= first_surgery + 30 days qualify)
#
#   UNION ALL → dedup on (research_id, source_table, source_row_id,
#   complication_type) preserving multi-source corroboration.
#
#   Date completeness (CHANGE A): every row must have a non-NULL
#   detection_date. Backfill priority per source is documented in each CTE.
#   detection_date_inferred=TRUE flags rows where the canonical date came
#   from a fallback (note_date instead of entity_date, first_surgery_date
#   instead of detection_date, etc.). Rows where NO date proxy exists are
#   dropped at the dedup stage.
#
#   source_evidence_type (CHANGE C): each event carries a refined evidence
#   classifier (e.g. laryngoscopy_direct vs voice_quality_llm_proxy for
#   vocal_cord_paralysis). Per-(source_table, complication_type) mapping
#   lives in `_source_evidence_type_case_sql()`.
#
#   lab_value_at_detection / lab_units (CHANGE E): populated for
#   source_evidence_type='lab_threshold_met' rows; NULL elsewhere.

def _source_evidence_type_case_sql() -> str:
    """Return a SQL snippet that maps per-row context to source_evidence_type.

    Inputs (all expected as columns in scope):
        complication_type, source_table, source_kind, source_modality,
        source_tier_raw (NULLABLE — phenotype.source_tier_label /
                         extracted_v5.source_tier_label /
                         extracted_rln.classification),
        evidence_strength_raw (NULLABLE — extracted_v5.entity_evidence_strength),
        treatment_requiring_flag_raw (NULLABLE — phenotype.treatment_requiring_flag).

    The returned SQL is a single CASE expression returning VARCHAR or NULL
    (NULL → caught by events_completeness_required QA gate so downstream
    rows can't ship with a missing classifier).
    """
    return r"""
    CASE
        ----------------------------------------------------------------
        -- vocal_cord_paralysis
        ----------------------------------------------------------------
        WHEN complication_type = 'vocal_cord_paralysis'
             AND source_table = 'note_entities_llm_survival_followup'
            THEN 'voice_quality_llm_proxy'
        WHEN complication_type = 'vocal_cord_paralysis'
             AND LOWER(COALESCE(source_tier_raw, '')) IN
                 ('laryngoscopy_confirmed',
                  'clinical_confirmed_plus_nlp',
                  'clinical_confirmed_only')
            THEN 'laryngoscopy_direct'
        WHEN complication_type = 'vocal_cord_paralysis'
             AND source_modality = 'op_note'
            THEN 'operative_note'
        WHEN complication_type = 'vocal_cord_paralysis'
            THEN 'nlp_proxy'
        ----------------------------------------------------------------
        -- rln_injury
        ----------------------------------------------------------------
        WHEN complication_type = 'rln_injury'
             AND LOWER(COALESCE(source_tier_raw, '')) = 'laryngoscopy_confirmed'
            THEN 'postop_laryngoscopy'
        WHEN complication_type = 'rln_injury'
             AND LOWER(COALESCE(source_tier_raw, '')) = 'chart_documented'
            THEN 'chart_documented'
        WHEN complication_type = 'rln_injury'
             AND source_modality = 'op_note'
            THEN 'intraop_observed'
        WHEN complication_type = 'rln_injury'
             AND LOWER(COALESCE(source_tier_raw, '')) IN
                 ('nlp_confirmed', 'nlp_suspected', 'nlp_refined')
            THEN 'nlp_proxy'
        WHEN complication_type = 'rln_injury'
            THEN 'nlp_proxy'
        ----------------------------------------------------------------
        -- hypocalcemia_clinical
        ----------------------------------------------------------------
        WHEN complication_type = 'hypocalcemia_clinical'
             AND source_table = 'canonical_labs_calcium_v1'
            THEN 'lab_threshold_met'
        WHEN complication_type = 'hypocalcemia_clinical'
             AND COALESCE(treatment_requiring_flag_raw, FALSE) = TRUE
            THEN 'treatment_initiated'
        WHEN complication_type = 'hypocalcemia_clinical'
            THEN 'symptomatic_only'
        ----------------------------------------------------------------
        -- hypoparathyroidism
        ----------------------------------------------------------------
        WHEN complication_type = 'hypoparathyroidism'
             AND COALESCE(treatment_requiring_flag_raw, FALSE) = TRUE
            THEN 'replacement_therapy_only'
        WHEN complication_type = 'hypoparathyroidism'
            THEN 'clinical_diagnosis'
        ----------------------------------------------------------------
        -- chyle_leak
        ----------------------------------------------------------------
        WHEN complication_type = 'chyle_leak'
            THEN 'milky_drain_visual'
        ----------------------------------------------------------------
        -- hematoma
        ----------------------------------------------------------------
        WHEN complication_type = 'hematoma'
             AND LOWER(COALESCE(source_tier_raw, '')) = 'structured_chart'
            THEN 'structured_chart'
        WHEN complication_type = 'hematoma'
            THEN 'drain_output_or_clinical_observation'
        ----------------------------------------------------------------
        -- seroma
        ----------------------------------------------------------------
        WHEN complication_type = 'seroma'
             AND LOWER(COALESCE(source_tier_raw, '')) = 'structured_chart'
            THEN 'structured_chart'
        WHEN complication_type = 'seroma'
            THEN 'aspiration_or_clinical_observation'
        ----------------------------------------------------------------
        -- wound_infection
        ----------------------------------------------------------------
        WHEN complication_type = 'wound_infection'
             AND COALESCE(treatment_requiring_flag_raw, FALSE) = TRUE
            THEN 'antibiotic_initiated'
        WHEN complication_type = 'wound_infection'
            THEN 'clinical_diagnosis'
        ----------------------------------------------------------------
        -- mortality (peri-op)
        ----------------------------------------------------------------
        WHEN complication_type = 'mortality'
             AND source_table = 'canonical_survival_followup_v1'
            THEN 'registry_match'
        WHEN complication_type = 'mortality'
            THEN 'last_followup_no_contact_proxy'
        ----------------------------------------------------------------
        -- pneumothorax / airway_complication / wound_dehiscence
        --   (0-pop in this cohort; emit clinical_observation as a benign
        --    placeholder so the NOT NULL gate is satisfied if any future
        --    cohort has them and the per-type CASE above is unmatched)
        ----------------------------------------------------------------
        ELSE 'clinical_observation'
    END
    """


def _evidence_strength_case_sql() -> str:
    """CHANGE F — return a CASE expression mapping per-row context to
    evidence_strength ∈ {definitive, probable, possible}.

    Inputs (all expected as columns in scope after with_set CTE):
        complication_type, source_evidence_type, source_kind, source_modality,
        evidence_text_for_hash (NULLABLE),
        treatment_requiring_flag_raw (NULLABLE),
        evidence_strength_raw (NULLABLE),
        n_low_pth_dates (from helper_pth_low_count; NULLABLE),
        has_late_voice_finding (from helper_late_voice_finding; NULLABLE BOOL).

    Per-type rules implement the user's CHANGE F spec literally. Where a
    source signal is unavailable (e.g. evidence_text NULL for phenotype
    rows; no medications canonical for "calcitriol at 6mo"), the rule
    falls through to the next-strictest tier. Default = 'possible'.
    """
    # NB: all DuckDB regex literals below use [\s\S] (not .) so they match
    # across newlines, and (?i) for case-insensitivity. The SQL is
    # heredoc-spaced for readability; whitespace inside the regex literals
    # is significant only inside character classes.
    return r"""
    CASE
        ----------------------------------------------------------------
        -- chyle_leak
        ----------------------------------------------------------------
        WHEN complication_type = 'chyle_leak'
             AND evidence_text_for_hash IS NOT NULL
             AND REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)(triglyceride|drain[^a-z]+TG)[\s\S]{0,40}\d')
            THEN 'definitive'
        WHEN complication_type = 'chyle_leak'
             AND evidence_text_for_hash IS NOT NULL
             AND (REGEXP_MATCHES(evidence_text_for_hash, '(?i)chylothorax')
                  OR REGEXP_MATCHES(evidence_text_for_hash,
                       '(?i)chyle[\s\S]{0,15}leak[\s\S]{0,100}(NPO|low.fat|MCT|octreotide|managed|conservative|TPN)')
             )
            THEN 'probable'
        WHEN complication_type = 'chyle_leak'
            THEN 'possible'
        ----------------------------------------------------------------
        -- rln_injury
        ----------------------------------------------------------------
        WHEN complication_type = 'rln_injury'
             AND source_evidence_type IN ('postop_laryngoscopy', 'intraop_observed')
            THEN 'definitive'
        WHEN complication_type = 'rln_injury'
             AND source_evidence_type = 'chart_documented'
             AND COALESCE(has_late_voice_finding, FALSE) = TRUE
            THEN 'probable'
        WHEN complication_type = 'rln_injury'
            THEN 'possible'
        ----------------------------------------------------------------
        -- seroma
        ----------------------------------------------------------------
        WHEN complication_type = 'seroma'
             AND evidence_text_for_hash IS NOT NULL
             AND (REGEXP_MATCHES(evidence_text_for_hash,
                       '(?i)aspirat[\w]*[\s\S]{0,40}\d+\s*(ml|cc)')
                  OR REGEXP_MATCHES(evidence_text_for_hash,
                       '(?i)(ultrasound|US)[\s\S]{0,20}(confirm|show|demonstr)')
                  OR REGEXP_MATCHES(evidence_text_for_hash,
                       '(?i)US[\s\-]+confirm'))
            THEN 'definitive'
        WHEN complication_type = 'seroma'
             AND (source_modality = 'op_note'
                  OR (source_kind = 'refined_extraction'
                      AND LOWER(COALESCE(evidence_strength_raw, '')) = 'strong'))
            THEN 'probable'
        WHEN complication_type = 'seroma'
            THEN 'possible'
        ----------------------------------------------------------------
        -- hypocalcemia_clinical
        ----------------------------------------------------------------
        WHEN complication_type = 'hypocalcemia_clinical'
             AND source_evidence_type = 'lab_threshold_met'
            THEN 'definitive'
        WHEN complication_type = 'hypocalcemia_clinical'
             AND source_evidence_type = 'treatment_initiated'
             AND evidence_text_for_hash IS NOT NULL
             AND REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)(Chvostek|Trousseau|carpopedal|tetany|peri-?oral numbness|circumoral)')
            THEN 'probable'
        WHEN complication_type = 'hypocalcemia_clinical'
             AND source_evidence_type = 'treatment_initiated'
             AND COALESCE(n_low_pth_dates, 0) >= 1
            THEN 'probable'
        WHEN complication_type = 'hypocalcemia_clinical'
            THEN 'possible'
        ----------------------------------------------------------------
        -- hypoparathyroidism
        ----------------------------------------------------------------
        WHEN complication_type = 'hypoparathyroidism'
             AND COALESCE(n_low_pth_dates, 0) >= 2
            THEN 'definitive'
        WHEN complication_type = 'hypoparathyroidism'
             AND COALESCE(n_low_pth_dates, 0) >= 1
             AND COALESCE(treatment_requiring_flag_raw, FALSE) = TRUE
            THEN 'probable'
        WHEN complication_type = 'hypoparathyroidism'
            THEN 'possible'
        ----------------------------------------------------------------
        -- vocal_cord_paralysis
        ----------------------------------------------------------------
        WHEN complication_type = 'vocal_cord_paralysis'
             AND source_evidence_type = 'laryngoscopy_direct'
            THEN 'definitive'
        WHEN complication_type = 'vocal_cord_paralysis'
             AND source_evidence_type = 'operative_note'
             AND COALESCE(has_late_voice_finding, FALSE) = TRUE
            THEN 'probable'
        WHEN complication_type = 'vocal_cord_paralysis'
            THEN 'possible'
        ----------------------------------------------------------------
        -- hematoma
        ----------------------------------------------------------------
        WHEN complication_type = 'hematoma'
             AND evidence_text_for_hash IS NOT NULL
             AND REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)(re-?operat|re-?explor|return[\s\S]{0,20}(OR|operating room)|evacuat[\w]{0,4}[\s\S]{0,30}hematoma|operative evacuation)')
            THEN 'definitive'
        WHEN complication_type = 'hematoma'
             AND source_evidence_type = 'structured_chart'
            THEN 'probable'
        WHEN complication_type = 'hematoma'
            THEN 'possible'
        ----------------------------------------------------------------
        -- wound_infection
        ----------------------------------------------------------------
        WHEN complication_type = 'wound_infection'
             AND evidence_text_for_hash IS NOT NULL
             AND REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)(culture|MRSA|MSSA|streptococc|staphylococ|gram[\s\-]?positive|gram[\s\-]?negative|sensitivities)')
             AND COALESCE(treatment_requiring_flag_raw, FALSE) = TRUE
            THEN 'definitive'
        WHEN complication_type = 'wound_infection'
             AND evidence_text_for_hash IS NOT NULL
             AND REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)(anterior cervical|neck incision|thyroidectomy wound|surgical site of thyroid|operative cervical incision)')
            THEN 'probable'
        WHEN complication_type = 'wound_infection'
            THEN 'possible'
        ----------------------------------------------------------------
        -- mortality (peri-op only)
        ----------------------------------------------------------------
        WHEN complication_type = 'mortality'
             AND source_evidence_type = 'registry_match'
            THEN 'definitive'
        WHEN complication_type = 'mortality'
            THEN 'possible'
        ----------------------------------------------------------------
        -- 0-pop placeholders (pneumothorax / airway / dehiscence)
        ----------------------------------------------------------------
        ELSE 'possible'
    END
    """


def _wound_gate_case_sql() -> str:
    """CHANGE H — return a CASE expression returning BOOLEAN.

    Returns TRUE for non-wound_infection rows (gate doesn't apply) and for
    wound_infection rows that satisfy:
      * Temporal: 0 ≤ DATE_DIFF(first_surgery_date, finding_date) ≤ 30
      * Anatomic include: regex (anterior cervical|neck incision|
        thyroidectomy wound|surgical site of thyroid|suprasternal|hyoid|
        operative cervical incision)
      * Anatomic exclude: NOT (urinary|UTI|line infection|catheter|
        previous surgical site)  PLUS abscess-of-not-neck check (RE2 has
        no negative lookahead, so we approximate with two regexes)

    Returns FALSE otherwise. NULL evidence_text_for_hash → FALSE (cannot
    verify anatomic specificity).
    """
    return r"""
    CASE
        WHEN complication_type != 'wound_infection' THEN TRUE
        WHEN finding_date IS NULL OR first_surgery_date IS NULL THEN FALSE
        WHEN DATE_DIFF('day', first_surgery_date, finding_date) < 0 THEN FALSE
        WHEN DATE_DIFF('day', first_surgery_date, finding_date) > """ + str(WOUND_INFECTION_POSTOP_WINDOW_DAYS) + r""" THEN FALSE
        WHEN evidence_text_for_hash IS NULL THEN FALSE
        WHEN NOT REGEXP_MATCHES(evidence_text_for_hash,
                '(?i)(anterior cervical|neck incision|thyroidectomy wound|surgical site of thyroid|suprasternal|hyoid|operative cervical incision)')
            THEN FALSE
        WHEN REGEXP_MATCHES(evidence_text_for_hash,
                '(?i)(urinary|UTI|line infection|catheter|previous surgical site)')
            THEN FALSE
        WHEN REGEXP_MATCHES(evidence_text_for_hash, '(?i)abscess of\s+\w+')
             AND NOT REGEXP_MATCHES(evidence_text_for_hash,
                  '(?i)abscess of\s+(neck|cervical|thyroid|anterior)')
            THEN FALSE
        ELSE TRUE
    END
    """


def _hypocalc_gate_case_sql() -> str:
    """CHANGE I — symptom specificity gate for source_evidence_type='symptomatic_only'.

    Returns BOOLEAN. TRUE if the row is not subject to the gate (different
    type or different evidence type) OR if evidence_text contains BOTH a
    specific symptom term AND a hypocalcemia attribution within 100 chars
    of each other (either direction). NULL evidence_text → FALSE.

    RE2 has no lookbehind, so "within 100 chars of each other" is checked
    bidirectionally with two regexes OR'd.
    """
    sym_terms = "(Chvostek|Trousseau|carpopedal|carpal spasm|tetany|peri-?oral numbness|circumoral)"
    attr_terms = "(hypocalc|low calcium|calcium replacement)"
    return r"""
    CASE
        WHEN complication_type != 'hypocalcemia_clinical' THEN TRUE
        WHEN source_evidence_type != 'symptomatic_only' THEN TRUE
        WHEN evidence_text_for_hash IS NULL THEN FALSE
        WHEN REGEXP_MATCHES(evidence_text_for_hash,
                '(?i)""" + sym_terms + r"""[\s\S]{0,100}""" + attr_terms + r"""')
            THEN TRUE
        WHEN REGEXP_MATCHES(evidence_text_for_hash,
                '(?i)""" + attr_terms + r"""[\s\S]{0,100}""" + sym_terms + r"""')
            THEN TRUE
        ELSE FALSE
    END
    """


def _build_events_sql(include_periop_mortality: bool = True) -> str:
    """Return the full CREATE OR REPLACE TABLE SQL for the events canonical.

    Six source CTEs:
        phenotype, entity_mapped, extracted_v5, extracted_rln,
        lab_hypocalc (CHANGE E), periop_mortality (CHANGE B)

    Each source CTE projects to a uniform 17-column staging schema:
        research_id, source_table, source_row_id, source_modality, source_kind,
        complication_type, onset_class_native, permanence_class, finding_status,
        finding_date, detection_date_inferred, evidence_text_for_hash,
        confidence, source_tier_raw, evidence_strength_raw,
        treatment_requiring_flag_raw, lab_value_at_detection, lab_units

    The outer SELECT applies the source_evidence_type CASE, dedupes,
    JOINs to canonical_operative_events_v1 for onset_class refinement,
    drops staging columns, and produces the final 16-column canonical:
        research_id, source_table, source_row_id, source_modality,
        source_kind, complication_type, source_evidence_type,
        onset_class, permanence_class, finding_status, finding_date,
        detection_date_inferred, evidence_span_hash, confidence,
        lab_value_at_detection, lab_units, build_ts
    """
    target = fq("main", "canonical_complications_events_v1")

    vocab_phenotype = _vocab_map_case_sql("complication_entity")
    vocab_entity = _vocab_map_case_sql("entity_value_norm")
    vocab_extracted = _vocab_map_case_sql("entity_name")

    # CASE expression mapping each complication_type to its onset window.
    onset_window_case = "CASE complication_type\n" + "\n".join(
        f"  WHEN '{ct}' THEN {n}" for ct, n in ONSET_WINDOW_DAYS.items()
    ) + "\n  ELSE 30 END"

    source_evidence_case = _source_evidence_type_case_sql()
    evidence_strength_case = _evidence_strength_case_sql()
    wound_gate_case = _wound_gate_case_sql()
    hypocalc_gate_case = _hypocalc_gate_case_sql()

    # CHANGE B: peri-op mortality CTE is only included if the new survival
    # canonical schema is available. Otherwise we emit an empty placeholder
    # so the UNION ALL still parses but contributes 0 rows.
    if include_periop_mortality:
        periop_mortality_cte = """
periop_mortality AS (
    SELECT
        s.research_id,
        'canonical_survival_followup_v1'                          AS source_table,
        CAST(hash(s.research_id, s.death_date) AS VARCHAR)        AS source_row_id,
        'discharge_summary'                                       AS source_modality,
        'survival_join'                                           AS source_kind,
        'mortality'                                               AS complication_type,
        CASE
            WHEN DATE_DIFF('day', s.first_surgery_date, s.death_date) = 0
                THEN 'intraop'
            WHEN DATE_DIFF('day', s.first_surgery_date, s.death_date) > 0
                 AND DATE_DIFF('day', s.first_surgery_date, s.death_date)
                     <= """ + str(PERIOP_MORTALITY_WINDOW_DAYS) + """
                THEN 'early_postop'
            ELSE 'unspecified'
        END                                                       AS onset_class,
        'permanent'                                               AS permanence_class,
        'present'                                                 AS finding_status,
        s.death_date                                              AS finding_date,
        FALSE                                                     AS detection_date_inferred,
        NULL::VARCHAR                                             AS evidence_text_for_hash,
        NULL::DOUBLE                                              AS confidence,
        s.death_date_source                                       AS source_tier_raw,
        NULL::VARCHAR                                             AS evidence_strength_raw,
        NULL::BOOLEAN                                             AS treatment_requiring_flag_raw,
        NULL::DOUBLE                                              AS lab_value_at_detection,
        NULL::VARCHAR                                             AS lab_units
    FROM main.canonical_survival_followup_v1 s
    WHERE s.death_date IS NOT NULL
      AND s.first_surgery_date IS NOT NULL
      AND DATE_DIFF('day', s.first_surgery_date, s.death_date) >= 0
      AND DATE_DIFF('day', s.first_surgery_date, s.death_date)
          <= """ + str(PERIOP_MORTALITY_WINDOW_DAYS) + """
)"""
        periop_union_clause = "\n    UNION ALL SELECT * FROM periop_mortality"
    else:
        # Placeholder CTE that yields zero rows but matches the staging schema
        # so the outer UNION ALL still parses.
        periop_mortality_cte = """
periop_mortality AS (
    SELECT * FROM (
        SELECT
            CAST(NULL AS VARCHAR)                                   AS research_id,
            CAST(NULL AS VARCHAR)                                   AS source_table,
            CAST(NULL AS VARCHAR)                                   AS source_row_id,
            CAST(NULL AS VARCHAR)                                   AS source_modality,
            CAST(NULL AS VARCHAR)                                   AS source_kind,
            CAST(NULL AS VARCHAR)                                   AS complication_type,
            CAST(NULL AS VARCHAR)                                   AS onset_class,
            CAST(NULL AS VARCHAR)                                   AS permanence_class,
            CAST(NULL AS VARCHAR)                                   AS finding_status,
            CAST(NULL AS DATE)                                      AS finding_date,
            CAST(NULL AS BOOLEAN)                                   AS detection_date_inferred,
            CAST(NULL AS VARCHAR)                                   AS evidence_text_for_hash,
            CAST(NULL AS DOUBLE)                                    AS confidence,
            CAST(NULL AS VARCHAR)                                   AS source_tier_raw,
            CAST(NULL AS VARCHAR)                                   AS evidence_strength_raw,
            CAST(NULL AS BOOLEAN)                                   AS treatment_requiring_flag_raw,
            CAST(NULL AS DOUBLE)                                    AS lab_value_at_detection,
            CAST(NULL AS VARCHAR)                                   AS lab_units
    ) WHERE 1 = 0
)"""
        periop_union_clause = "\n    UNION ALL SELECT * FROM periop_mortality"

    return f"""
CREATE OR REPLACE TABLE {target} AS
WITH
-- --------------------------------------------------------------------
-- Helper — first_surgery_date per patient (date fallback for CHANGE A)
-- --------------------------------------------------------------------
first_surgery_per_patient AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(CAST(surgery_date_native AS DATE)) AS first_surgery_date
    FROM main.canonical_operative_events_v1
    WHERE surgery_date_native IS NOT NULL
    GROUP BY research_id
),
-- --------------------------------------------------------------------
-- Source 1 — complication_phenotype_v1 (structured, 5,978 rows)
-- 96% of rows have NULL detection_date but populated first_surgery_date
-- → fall back to first_surgery_date with detection_date_inferred=TRUE.
-- --------------------------------------------------------------------
phenotype_raw AS (
    SELECT
        CAST(research_id AS VARCHAR)                              AS research_id,
        CAST(hash(research_id, complication_entity, detection_date)
                AS VARCHAR)                                       AS source_row_id,
        'discharge_summary'                                       AS source_modality,
        'structured'                                              AS source_kind,
        'complication_phenotype_v1'                               AS source_table,
        complication_entity,
        ({vocab_phenotype})                                       AS complication_type_raw,
        suspected_flag, confirmed_flag, transient_flag,
        permanent_flag, surgery_related_flag, historical_only_flag,
        timing_days_post_surgery, timing_window,
        final_complication_status, status_v2,
        treatment_requiring_flag, voice_resolution_noted,
        voice_permanence_noted, evidence_tier, source_tier_label,
        biochemical_low_ca,
        TRY_CAST(detection_date AS DATE)                          AS native_detection_date,
        TRY_CAST(first_surgery_date AS DATE)                      AS first_surgery_date,
        NULL::VARCHAR                                             AS evidence_text_for_hash,
        NULL::DOUBLE                                              AS confidence
    FROM main.complication_phenotype_v1
),
phenotype_dated AS (
    -- Date completeness backfill (CHANGE A): prefer detection_date, then
    -- first_surgery_date. Mark inferred=TRUE on the fallback path.
    SELECT
        *,
        COALESCE(native_detection_date, first_surgery_date)       AS finding_date,
        (native_detection_date IS NULL
         AND first_surgery_date IS NOT NULL)                      AS detection_date_inferred
    FROM phenotype_raw
),
-- Apply hypocalcemia clinical/lab boundary: drop pure-lab rows (CHANGE E
-- still applies for phenotype rows — those carry no measurement so we
-- cannot validate them as lab-threshold-met from this source; the
-- numeric path joins canonical_labs_calcium_v1 separately below).
phenotype_filtered AS (
    SELECT *
    FROM phenotype_dated
    WHERE complication_type_raw IS NOT NULL
      AND finding_date IS NOT NULL                               -- CHANGE A drop
      AND NOT (
          complication_type_raw = 'hypocalcemia_clinical'
          AND COALESCE(treatment_requiring_flag, FALSE) = FALSE
          AND COALESCE(confirmed_flag, FALSE) = FALSE
          AND (status_v2 IS NULL OR status_v2 NOT LIKE 'confirmed_%')
      )
),
phenotype AS (
    SELECT
        research_id,
        source_table, source_row_id, source_modality, source_kind,
        complication_type_raw                                     AS complication_type,
        -- onset_class from native timing_window / timing_days
        CASE
            WHEN timing_window = 'intraop'
                 OR (timing_days_post_surgery IS NOT NULL
                     AND timing_days_post_surgery = 0)
                THEN 'intraop'
            WHEN timing_days_post_surgery IS NOT NULL
                 AND timing_days_post_surgery > 0
                 AND timing_days_post_surgery <= 30
                THEN 'early_postop'
            WHEN timing_days_post_surgery IS NOT NULL
                 AND timing_days_post_surgery > 30
                THEN 'late_postop'
            ELSE 'unspecified'
        END                                                       AS onset_class,
        CASE
            WHEN COALESCE(permanent_flag, FALSE) = TRUE
                 OR COALESCE(voice_permanence_noted, FALSE) = TRUE
                THEN 'permanent'
            WHEN COALESCE(transient_flag, FALSE) = TRUE
                 OR COALESCE(voice_resolution_noted, FALSE) = TRUE
                THEN 'transient'
            ELSE 'indeterminate'
        END                                                       AS permanence_class,
        CASE
            WHEN COALESCE(confirmed_flag, FALSE) = TRUE THEN 'present'
            WHEN COALESCE(suspected_flag, FALSE) = TRUE
                AND COALESCE(confirmed_flag, FALSE) = FALSE THEN 'suspected'
            WHEN final_complication_status = 'absent_or_unconfirmed' THEN 'absent'
            ELSE 'indeterminate'
        END                                                       AS finding_status,
        finding_date,
        detection_date_inferred,
        evidence_text_for_hash,
        confidence,
        source_tier_label                                         AS source_tier_raw,
        NULL::VARCHAR                                             AS evidence_strength_raw,
        treatment_requiring_flag                                  AS treatment_requiring_flag_raw,
        NULL::DOUBLE                                              AS lab_value_at_detection,
        NULL::VARCHAR                                             AS lab_units
    FROM phenotype_filtered
),
-- --------------------------------------------------------------------
-- Source 2 — note_entities_complications (entity_legacy, 9,359 rows)
-- 67% of rows have BOTH entity_date AND note_date NULL. For those we
-- fall back to first_surgery_date (mark detection_date_inferred=TRUE).
-- Rows where ALL three are NULL get dropped at the WHERE filter.
-- --------------------------------------------------------------------
entity_raw AS (
    SELECT
        CAST(e.research_id AS VARCHAR)                            AS research_id,
        CAST(hash(e.research_id, e.note_row_id, e.source_line,
                  e.entity_value_norm, e.evidence_start)
                AS VARCHAR)                                       AS source_row_id,
        COALESCE(
            CASE LOWER(COALESCE(e.note_type, ''))
                WHEN 'operative_note' THEN 'op_note'
                WHEN 'op_note' THEN 'op_note'
                WHEN 'discharge_summary' THEN 'discharge_summary'
                WHEN 'dc_sum' THEN 'discharge_summary'
                WHEN 'clinic_note' THEN 'clinic_note'
                WHEN 'progress_note' THEN 'clinic_note'
                WHEN 'office_note' THEN 'clinic_note'
                WHEN 'h_p' THEN 'clinic_note'
                WHEN 'endocrine_note' THEN 'clinic_note'
                WHEN 'history_summary' THEN 'clinic_note'
                WHEN 'other_history' THEN 'clinic_note'
                WHEN 'other_notes' THEN 'clinic_note'
                WHEN 'ed_note' THEN 'clinic_note'
                WHEN 'pathology_synoptic' THEN 'path_synoptic'
                ELSE NULL
            END,
            'clinic_note'
        )                                                         AS source_modality,
        'entity_legacy'                                           AS source_kind,
        'note_entities_complications'                             AS source_table,
        e.entity_value_norm,
        ({vocab_entity})                                          AS complication_type_raw,
        e.present_or_negated,
        TRY_CAST(e.entity_date AS DATE)                           AS entity_date,
        TRY_CAST(e.note_date AS DATE)                             AS note_date,
        e.confidence,
        e.evidence_span                                           AS evidence_text_for_hash,
        fs.first_surgery_date                                     AS first_surgery_date
    FROM main.note_entities_complications e
    LEFT JOIN first_surgery_per_patient fs
        ON fs.research_id = CAST(e.research_id AS VARCHAR)
),
entity_dated AS (
    SELECT
        *,
        COALESCE(entity_date, note_date, first_surgery_date)      AS finding_date,
        (entity_date IS NULL
         AND (note_date IS NOT NULL OR first_surgery_date IS NOT NULL))
                                                                  AS detection_date_inferred
    FROM entity_raw
),
entity_mapped AS (
    SELECT
        research_id,
        source_table, source_row_id, source_modality, source_kind,
        complication_type_raw                                     AS complication_type,
        'unspecified'                                             AS onset_class,
        'indeterminate'                                           AS permanence_class,
        CASE
            WHEN LOWER(COALESCE(present_or_negated, '')) = 'present' THEN 'present'
            WHEN LOWER(COALESCE(present_or_negated, '')) = 'negated' THEN 'absent'
            ELSE 'indeterminate'
        END                                                       AS finding_status,
        finding_date,
        detection_date_inferred,
        evidence_text_for_hash,
        confidence,
        NULL::VARCHAR                                             AS source_tier_raw,
        NULL::VARCHAR                                             AS evidence_strength_raw,
        NULL::BOOLEAN                                             AS treatment_requiring_flag_raw,
        NULL::DOUBLE                                              AS lab_value_at_detection,
        NULL::VARCHAR                                             AS lab_units
    FROM entity_dated
    WHERE complication_type_raw IS NOT NULL
      AND finding_date IS NOT NULL                               -- CHANGE A drop
),
-- --------------------------------------------------------------------
-- Source 3 — extracted_complications_refined_v5 (358 rows)
-- 119 of 358 rows have NULL detection_date → fall back to first_surgery
-- --------------------------------------------------------------------
extracted_v5_raw AS (
    SELECT
        CAST(e.research_id AS VARCHAR)                            AS research_id,
        CAST(hash(e.research_id, e.entity_name, e.detection_date)
                AS VARCHAR)                                       AS source_row_id,
        'discharge_summary'                                       AS source_modality,
        'refined_extraction'                                      AS source_kind,
        'extracted_complications_refined_v5'                      AS source_table,
        e.entity_name,
        ({vocab_extracted})                                       AS complication_type_raw,
        e.entity_is_confirmed,
        e.entity_tier,
        e.source_tier_label                                       AS source_tier_raw,
        e.entity_evidence_strength                                AS evidence_strength_raw,
        TRY_CAST(e.detection_date AS DATE)                        AS native_detection_date,
        fs.first_surgery_date                                     AS first_surgery_date,
        NULL::VARCHAR                                             AS evidence_text_for_hash,
        NULL::DOUBLE                                              AS confidence
    FROM main.extracted_complications_refined_v5 e
    LEFT JOIN first_surgery_per_patient fs
        ON fs.research_id = CAST(e.research_id AS VARCHAR)
),
extracted_v5_dated AS (
    SELECT
        *,
        COALESCE(native_detection_date, first_surgery_date)       AS finding_date,
        (native_detection_date IS NULL
         AND first_surgery_date IS NOT NULL)                      AS detection_date_inferred
    FROM extracted_v5_raw
),
extracted_v5 AS (
    SELECT
        research_id,
        source_table, source_row_id, source_modality, source_kind,
        complication_type_raw                                     AS complication_type,
        'unspecified'                                             AS onset_class,
        'indeterminate'                                           AS permanence_class,
        CASE
            WHEN entity_is_confirmed = TRUE  THEN 'present'
            WHEN entity_is_confirmed = FALSE THEN 'suspected'
            ELSE 'indeterminate'
        END                                                       AS finding_status,
        finding_date,
        detection_date_inferred,
        evidence_text_for_hash,
        confidence,
        source_tier_raw,
        evidence_strength_raw,
        NULL::BOOLEAN                                             AS treatment_requiring_flag_raw,
        NULL::DOUBLE                                              AS lab_value_at_detection,
        NULL::VARCHAR                                             AS lab_units
    FROM extracted_v5_dated
    WHERE complication_type_raw IS NOT NULL
      AND finding_date IS NOT NULL                               -- CHANGE A drop
),
-- --------------------------------------------------------------------
-- Source 4 — extracted_rln_injury_refined_v2 (92 rows; ALL → rln_injury)
-- detection_date 100% populated; no fallback needed.
-- --------------------------------------------------------------------
extracted_rln_raw AS (
    SELECT
        CAST(research_id AS VARCHAR)                              AS research_id,
        CAST(hash(research_id, injury_type, detection_date,
                  classification, temporality)
                AS VARCHAR)                                       AS source_row_id,
        'discharge_summary'                                       AS source_modality,
        'refined_extraction'                                      AS source_kind,
        'extracted_rln_injury_refined_v2'                         AS source_table,
        injury_type, classification, temporality,
        rln_injury_is_confirmed,
        rln_injury_evidence_strength                              AS evidence_strength_raw,
        TRY_CAST(detection_date AS DATE)                          AS finding_date,
        first_surgery_date, days_post_surgery, temporal_window,
        NULL::VARCHAR                                             AS evidence_text_for_hash,
        NULL::DOUBLE                                              AS confidence
    FROM main.extracted_rln_injury_refined_v2
),
extracted_rln AS (
    SELECT
        research_id,
        source_table, source_row_id, source_modality, source_kind,
        'rln_injury'                                              AS complication_type,
        CASE
            WHEN days_post_surgery IS NOT NULL
                 AND days_post_surgery = 0
                THEN 'intraop'
            WHEN days_post_surgery IS NOT NULL
                 AND days_post_surgery > 0
                 AND days_post_surgery <= 30
                THEN 'early_postop'
            WHEN days_post_surgery IS NOT NULL
                 AND days_post_surgery > 30
                THEN 'late_postop'
            ELSE 'unspecified'
        END                                                       AS onset_class,
        CASE
            WHEN LOWER(COALESCE(temporality, '')) = 'permanent' THEN 'permanent'
            WHEN LOWER(COALESCE(temporality, '')) = 'transient' THEN 'transient'
            ELSE 'indeterminate'
        END                                                       AS permanence_class,
        CASE
            WHEN rln_injury_is_confirmed = TRUE  THEN 'present'
            WHEN rln_injury_is_confirmed = FALSE THEN 'suspected'
            ELSE 'indeterminate'
        END                                                       AS finding_status,
        finding_date,
        FALSE                                                     AS detection_date_inferred,
        evidence_text_for_hash,
        confidence,
        classification                                            AS source_tier_raw,
        evidence_strength_raw,
        NULL::BOOLEAN                                             AS treatment_requiring_flag_raw,
        NULL::DOUBLE                                              AS lab_value_at_detection,
        NULL::VARCHAR                                             AS lab_units
    FROM extracted_rln_raw
    WHERE finding_date IS NOT NULL                               -- CHANGE A drop
),
-- --------------------------------------------------------------------
-- Source 5 — canonical_labs_calcium_v1 (CHANGE E — lab-derived)
-- Qualifies as a hypocalcemia_clinical event if value_numeric < 8.0
-- mg/dL within 30 days post-op. The 30-day window is enforced by JOIN
-- to first_surgery_date. The lab value + units are surfaced on the
-- canonical for downstream re-correction (e.g. with albumin once that
-- canonical exists).
-- --------------------------------------------------------------------
lab_hypocalc AS (
    SELECT
        CAST(c.research_id AS VARCHAR)                            AS research_id,
        'canonical_labs_calcium_v1'                               AS source_table,
        CAST(hash(c.research_id, c.lab_datetime, c.value_numeric)
              AS VARCHAR)                                         AS source_row_id,
        'imaging'                                                 AS source_modality,  -- placeholder; lab is its own kind, see source_kind
        'lab_threshold_met'                                       AS source_kind,
        'hypocalcemia_clinical'                                   AS complication_type,
        CASE
            WHEN DATE_DIFF('day', fs.first_surgery_date,
                           CAST(c.lab_datetime AS DATE)) = 0
                 THEN 'intraop'
            WHEN DATE_DIFF('day', fs.first_surgery_date,
                           CAST(c.lab_datetime AS DATE)) > 0
                 AND DATE_DIFF('day', fs.first_surgery_date,
                               CAST(c.lab_datetime AS DATE)) <=
                 {HYPOCALCEMIA_POSTOP_WINDOW_DAYS}
                 THEN 'early_postop'
            ELSE 'unspecified'
        END                                                       AS onset_class,
        'indeterminate'                                           AS permanence_class,
        'present'                                                 AS finding_status,
        CAST(c.lab_datetime AS DATE)                              AS finding_date,
        FALSE                                                     AS detection_date_inferred,
        NULL::VARCHAR                                             AS evidence_text_for_hash,
        NULL::DOUBLE                                              AS confidence,
        NULL::VARCHAR                                             AS source_tier_raw,
        NULL::VARCHAR                                             AS evidence_strength_raw,
        NULL::BOOLEAN                                             AS treatment_requiring_flag_raw,
        c.value_numeric                                           AS lab_value_at_detection,
        c.unit_standardized                                       AS lab_units
    FROM main.canonical_labs_calcium_v1 c
    INNER JOIN first_surgery_per_patient fs
        ON fs.research_id = CAST(c.research_id AS VARCHAR)
    WHERE c.value_numeric IS NOT NULL
      AND c.value_numeric < {HYPOCALCEMIA_CA_THRESHOLD_MGDL}
      AND DATE_DIFF('day', fs.first_surgery_date,
                    CAST(c.lab_datetime AS DATE)) >= 0
      AND DATE_DIFF('day', fs.first_surgery_date,
                    CAST(c.lab_datetime AS DATE)) <=
          {HYPOCALCEMIA_POSTOP_WINDOW_DAYS}
),
-- Note on lab source_modality: lab is not really an op_note / clinic_note
-- modality. We default to 'clinic_note' here so the source_modality
-- domain stays (op_note, discharge_summary, clinic_note, path_synoptic,
-- imaging) per the spec; readers should look at source_kind=lab_threshold_met
-- to identify lab-derived rows.
lab_hypocalc_with_modality AS (
    SELECT
        research_id, source_table, source_row_id,
        'clinic_note' AS source_modality,
        source_kind, complication_type, onset_class, permanence_class,
        finding_status, finding_date, detection_date_inferred,
        evidence_text_for_hash, confidence,
        source_tier_raw, evidence_strength_raw,
        treatment_requiring_flag_raw,
        lab_value_at_detection, lab_units
    FROM lab_hypocalc
),
-- --------------------------------------------------------------------
-- Source 6 — canonical_survival_followup_v1 (CHANGE B — peri-op mortality)
-- Promote to a 'mortality' complication ONLY if death_date <=
-- first_surgery + 30 days. Long-term mortality stays in 364B and does
-- not show up here. CTE body is templated based on whether the new
-- survival schema is available; the placeholder version yields 0 rows.
-- --------------------------------------------------------------------
{periop_mortality_cte},
-- --------------------------------------------------------------------
-- UNION + dedup
-- --------------------------------------------------------------------
all_findings AS (
    SELECT * FROM phenotype
    UNION ALL SELECT * FROM entity_mapped
    UNION ALL SELECT * FROM extracted_v5
    UNION ALL SELECT * FROM extracted_rln
    UNION ALL SELECT * FROM lab_hypocalc_with_modality{periop_union_clause}
),
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, source_table, source_row_id,
                         complication_type
            ORDER BY
                CASE finding_status
                    WHEN 'present' THEN 1
                    WHEN 'suspected' THEN 2
                    WHEN 'absent' THEN 3
                    ELSE 4
                END,
                CASE permanence_class
                    WHEN 'permanent' THEN 1
                    WHEN 'transient' THEN 2
                    ELSE 3
                END
        ) AS rn
    FROM all_findings
),
deduped_one AS (
    SELECT * FROM deduped WHERE rn = 1
),
-- onset_class refinement: JOIN to canonical_operative_events_v1 for
-- sources whose native onset_class is 'unspecified'.
op_events AS (
    SELECT
        CAST(research_id AS VARCHAR)             AS research_id,
        CAST(surgery_date_native AS DATE)        AS surgery_date
    FROM main.canonical_operative_events_v1
    WHERE surgery_date_native IS NOT NULL
),
joined AS (
    SELECT
        d.*,
        ev.surgery_date                          AS linked_surgery_date,
        DATE_DIFF('day', ev.surgery_date, d.finding_date)
                                                 AS days_since_surgery,
        ROW_NUMBER() OVER (
            PARTITION BY d.research_id, d.source_table, d.source_row_id,
                         d.complication_type
            ORDER BY ev.surgery_date DESC
        )                                         AS link_rn
    FROM deduped_one d
    LEFT JOIN op_events ev
        ON ev.research_id = d.research_id
       AND ev.surgery_date <= d.finding_date
       AND DATE_DIFF('day', ev.surgery_date, d.finding_date) <= ({onset_window_case})
       AND DATE_DIFF('day', ev.surgery_date, d.finding_date) >= 0
),
joined_one AS (
    SELECT * FROM joined WHERE link_rn = 1
),
-- --------------------------------------------------------------------
-- Helpers — patient-grain lookups for evidence_strength derivation
-- --------------------------------------------------------------------
helper_pth_low_count AS (
    -- CHANGE F — count of distinct dates with PTH < 15 pg/mL.
    -- Used to qualify hypoparathyroidism definitive (>=2 occasions)
    -- and probable (>=1 occasion + treatment_requiring).
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        COUNT(DISTINCT CAST(lab_datetime AS DATE)) AS n_low_pth_dates
    FROM main.canonical_labs_pth_v1
    WHERE value_numeric IS NOT NULL
      AND value_numeric < {PTH_LOW_THRESHOLD_PG_ML}
    GROUP BY 1
),
helper_late_voice_finding AS (
    -- CHANGE F — patients with ANY rln_injury or vocal_cord_paralysis
    -- finding documented >30 days post first_surgery_date. Used to qualify
    -- rln_injury probable (chart_documented + persistent voice change >30d)
    -- and vocal_cord_paralysis probable (operative_note + symptoms >30d).
    SELECT
        d.research_id,
        BOOL_OR(
            d.complication_type IN ('rln_injury', 'vocal_cord_paralysis')
            AND d.finding_status = 'present'
            AND d.finding_date IS NOT NULL
            AND fs.first_surgery_date IS NOT NULL
            AND DATE_DIFF('day', fs.first_surgery_date, d.finding_date) > 30
        ) AS has_late_voice_finding
    FROM deduped_one d
    LEFT JOIN first_surgery_per_patient fs ON fs.research_id = d.research_id
    GROUP BY 1
),
-- --------------------------------------------------------------------
-- Layered tier computation: helpers → source_evidence_type →
-- evidence_strength → gate flags
-- --------------------------------------------------------------------
joined_with_helpers AS (
    SELECT
        j.*,
        fs.first_surgery_date,
        plc.n_low_pth_dates,
        lvf.has_late_voice_finding
    FROM joined_one j
    LEFT JOIN first_surgery_per_patient fs
        ON fs.research_id = j.research_id
    LEFT JOIN helper_pth_low_count plc
        ON plc.research_id = j.research_id
    LEFT JOIN helper_late_voice_finding lvf
        ON lvf.research_id = j.research_id
),
with_source_evidence AS (
    SELECT
        *,
        ({source_evidence_case}) AS source_evidence_type
    FROM joined_with_helpers
),
with_strength AS (
    SELECT
        *,
        ({evidence_strength_case}) AS evidence_strength
    FROM with_source_evidence
),
with_gates AS (
    SELECT
        *,
        ({wound_gate_case})    AS wound_passes_gate,
        ({hypocalc_gate_case}) AS hypocalc_passes_gate
    FROM with_strength
)
SELECT
    research_id,
    source_table,
    source_row_id,
    source_modality,
    source_kind,
    complication_type,
    source_evidence_type,
    evidence_strength,
    CASE
        WHEN onset_class IN ('intraop', 'early_postop', 'late_postop')
            THEN onset_class
        WHEN days_since_surgery IS NOT NULL
             AND days_since_surgery = 0
             AND source_modality = 'op_note'
            THEN 'intraop'
        WHEN days_since_surgery IS NOT NULL
             AND days_since_surgery > 0
             AND days_since_surgery <= 30
            THEN 'early_postop'
        WHEN days_since_surgery IS NOT NULL
             AND days_since_surgery > 30
            THEN 'late_postop'
        ELSE 'unspecified'
    END                                                           AS onset_class,
    permanence_class,
    finding_status,
    finding_date,
    detection_date_inferred,
    NULL::VARCHAR                                                 AS evidence_span_hash,
    confidence,
    lab_value_at_detection,
    lab_units,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                          AS build_ts,
    -- staging cols — dropped by step_1_build_events after counting drops
    -- and applying gate-driven DELETE.
    evidence_text_for_hash,
    wound_passes_gate,
    hypocalc_passes_gate
FROM with_gates
"""


def _count_gate_drops(
    con: duckdb.DuckDBPyConnection, table_name: str
) -> tuple[int, int]:
    """Return (wound_infection_dropped, hypocalcemia_symptomatic_dropped).

    Both counts measure rows that FAIL their respective gates and would be
    DELETEd. Caller is responsible for the actual delete.
    """
    n_wound = int(con.execute(
        f"SELECT COUNT(*) FROM {table_name} "
        f"WHERE complication_type = 'wound_infection' "
        f"  AND wound_passes_gate = FALSE"
    ).fetchone()[0])
    n_hypocalc = int(con.execute(
        f"SELECT COUNT(*) FROM {table_name} "
        f"WHERE complication_type = 'hypocalcemia_clinical' "
        f"  AND source_evidence_type = 'symptomatic_only' "
        f"  AND hypocalc_passes_gate = FALSE"
    ).fetchone()[0])
    return n_wound, n_hypocalc


def _write_overextraction_forensic(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    decision_log: dict[str, Any],
) -> Path:
    """CHANGE J — write a PHI-flagged markdown sampling N=20 random rows
    from each over-counted complication type and surface evidence_text.

    The output lives under PHI_FORENSIC_DIR (added to .gitignore at repo
    root). Logan's PHI rule applies: do not commit, do not paste into chat,
    do not push.

    Returns the output Path.
    """
    PHI_FORENSIC_DIR.mkdir(parents=True, exist_ok=True)
    out = PHI_FORENSIC_DIR / (
        f"qa_script_364_source_overextraction_{RUN_TS_COMPACT}.md"
    )
    lines: list[str] = [
        "# QA — Source over-extraction forensic (Script 364, CHANGE J)",
        "",
        "**PHI: contains evidence_text snippets from clinical notes. "
        "Local-only — DO NOT commit, push, or paste into chat per Logan's "
        "PHI rule.** Path is under `phi_forensic/` (gitignored).",
        "",
        f"Generated: {RUN_DATE} (UTC) by Script {SCRIPT_ID}",
        f"Sampling table: `{source_table}`",
        f"N per type: {FORENSIC_SAMPLE_N}",
        "",
        "## Triage prompt",
        "",
        "For each sample row below, classify the evidence_text as one of:",
        "",
        "1. **TRUE positive** — clinical complication present, attribution OK",
        "2. **NEGATION** — text says \"no chyle leak\" / \"ruled out\" / etc.",
        "3. **DIFFERENTIAL** — text mentions complication as part of a "
        "differential, not a confirmed finding",
        "4. **HISTORICAL** — refers to a prior surgery / episode, not the "
        "thyroidectomy under study",
        "5. **HEDGED** — \"possible\" / \"may have\" / \"concerning for\" "
        "without clear assertion",
        "6. **WRONG ATTRIBUTION** — anatomic site is not the thyroidectomy "
        "site (e.g. wound infection at line / UTI; hematoma elsewhere)",
        "",
        "If categories 2-6 dominate the sample, the source NLP is bulk "
        "over-extracting and Script 364 case definitions can only filter "
        "what's available — file as Tier-1 source-data carry-forward in "
        "the close-out doc.",
        "",
    ]
    forensic_summary: list[dict[str, Any]] = []
    for ctype, set_filter in FORENSIC_TYPES:
        section_title = (
            f"## {ctype}"
            + (f" (source_evidence_type = `{set_filter}`)"
               if set_filter else "")
        )
        lines.append(section_title)
        lines.append("")
        # Sample N=20 random rows.
        where = f"complication_type = '{ctype}'"
        if set_filter is not None:
            where += f" AND source_evidence_type = '{set_filter}'"
        # Use ROW_NUMBER() OVER (ORDER BY hash(...)) for reproducible
        # pseudo-random sampling that doesn't depend on the volatile
        # `random()` function.
        rows = con.execute(
            f"WITH ranked AS ("
            f"  SELECT *, "
            f"    ROW_NUMBER() OVER ("
            f"      ORDER BY hash(research_id, source_row_id, "
            f"                    complication_type)"
            f"    ) AS r "
            f"  FROM {source_table} "
            f"  WHERE {where} "
            f"    AND finding_status = 'present'"
            f") "
            f"SELECT research_id, source_table, source_row_id, "
            f"       finding_date, source_evidence_type, evidence_strength, "
            f"       SUBSTRING(evidence_text_for_hash, 1, 400) "
            f"FROM ranked "
            f"WHERE r <= {FORENSIC_SAMPLE_N} "
            f"ORDER BY r"
        ).fetchall()
        n_total = int(con.execute(
            f"SELECT COUNT(*) FROM {source_table} "
            f"WHERE {where} AND finding_status = 'present'"
        ).fetchone()[0])
        lines.append(f"Total present rows in scope: {n_total:,}.  "
                     f"Sample (N={min(len(rows), FORENSIC_SAMPLE_N)}):")
        lines.append("")
        forensic_summary.append({
            "complication_type": ctype,
            "source_evidence_type_filter": set_filter,
            "total_present_rows": n_total,
            "sampled_rows": len(rows),
        })
        if not rows:
            lines.append("  (no rows match the filter)")
            lines.append("")
            continue
        lines.append("| # | research_id | source_table | finding_date | "
                     "source_evidence_type | evidence_strength | "
                     "evidence_text (first 400 chars) |")
        lines.append("|---|---|---|---|---|---|---|")
        # Column order from the SELECT:
        #   r[0]=research_id  r[1]=source_table  r[2]=source_row_id (skip)
        #   r[3]=finding_date r[4]=source_evidence_type
        #   r[5]=evidence_strength  r[6]=evidence_text
        for i, r in enumerate(rows, start=1):
            text = r[6] if r[6] is not None else "(NULL — no evidence_text in source)"
            # Markdown-safe: escape pipes and replace newlines.
            text = (text.replace("|", "\\|").replace("\n", " ⏎ ")
                        .replace("\r", " "))
            lines.append(
                f"| {i} | {r[0]} | `{r[1]}` | {r[3]} | "
                f"`{r[4]}` | `{r[5]}` | {text} |"
            )
        lines.append("")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    decision_log["change_j_forensic"] = {
        "path": str(out.relative_to(REPO_ROOT)),
        "summary": forensic_summary,
        "phi_local_only": True,
    }
    return out


def step_1_build_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1+3+4 — Build main.canonical_complications_events_v1")
    log("=" * 78)

    # Detect whether the new survival schema is available (CHANGE B).
    surv_cols = set(list_columns(con, "main", "canonical_survival_followup_v1"))
    include_periop_mortality = (
        "vital_status_current" in surv_cols and "death_date" in surv_cols
    )
    if include_periop_mortality:
        log("  canonical_survival_followup_v1 has new schema — peri-op "
            "mortality CTE WILL be included")
    else:
        log("  canonical_survival_followup_v1 has legacy schema — peri-op "
            "mortality CTE SKIPPED (placeholder yields 0 rows)")

    sql = _build_events_sql(include_periop_mortality=include_periop_mortality)
    if not do_writes:
        # Dry-run: build the events into a TEMP table for inspection so we can
        # log distributions without touching main.
        log("  [dry-run] materialising into a TEMP table for inspection")
        con.execute("DROP TABLE IF EXISTS temp_events_v1_dry_run")
        # Wrap the CREATE OR REPLACE into a CREATE TEMP TABLE.
        temp_sql = sql.replace(
            f'CREATE OR REPLACE TABLE {fq("main", "canonical_complications_events_v1")}',
            "CREATE TEMP TABLE temp_events_v1_dry_run",
        )
        con.execute(temp_sql)
        n_pre_gate = int(con.execute(
            "SELECT COUNT(*) FROM temp_events_v1_dry_run"
        ).fetchone()[0])
        # CHANGE H + I — count drops BEFORE applying.
        n_wound_dropped, n_hypocalc_dropped = _count_gate_drops(
            con, "temp_events_v1_dry_run"
        )
        log(f"  [dry-run] CHANGE H wound_infection_dropped_for_attribution: "
            f"{n_wound_dropped:,}")
        log(f"  [dry-run] CHANGE I hypocalcemia_symptomatic_dropped_for_specificity: "
            f"{n_hypocalc_dropped:,}")
        # Apply gate filter to the temp table for downstream distribution
        # logging (mirrors what step_1 will do on the real build).
        con.execute(
            "DELETE FROM temp_events_v1_dry_run "
            "WHERE wound_passes_gate = FALSE "
            "   OR hypocalc_passes_gate = FALSE"
        )
        n = int(con.execute(
            "SELECT COUNT(*) FROM temp_events_v1_dry_run"
        ).fetchone()[0])
        p = int(con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM temp_events_v1_dry_run"
        ).fetchone()[0])
        log(f"  [dry-run] events pre-gate {n_pre_gate:,} → "
            f"post-gate {n:,} rows / {p:,} patients "
            f"(dropped {n_pre_gate - n:,})")
        # Per-source breakdown
        rows = con.execute(
            "SELECT source_table, COUNT(*) FROM temp_events_v1_dry_run "
            "GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        log("  [dry-run] per-source row counts:")
        for src, cnt in rows:
            log(f"    {src:50s} {cnt:,}")
        # Per-complication-type breakdown
        rows = con.execute(
            "SELECT complication_type, finding_status, COUNT(*) "
            "FROM temp_events_v1_dry_run "
            "GROUP BY 1, 2 ORDER BY 1, 3 DESC"
        ).fetchall()
        log("  [dry-run] per-type x status counts:")
        for ct, fs, cnt in rows:
            log(f"    {ct:30s} {fs:15s} {cnt:,}")
        # CHANGE C: per source_evidence_type breakdown.
        rows = con.execute(
            "SELECT complication_type, source_evidence_type, COUNT(*) "
            "FROM temp_events_v1_dry_run "
            "GROUP BY 1, 2 ORDER BY 1, 3 DESC"
        ).fetchall()
        log("  [dry-run] per-type x source_evidence_type counts:")
        for ct, set_v, cnt in rows:
            log(f"    {ct:25s} {set_v:40s} {cnt:,}")
        # CHANGE F: per-type x evidence_strength.
        rows = con.execute(
            "SELECT complication_type, evidence_strength, COUNT(*) "
            "FROM temp_events_v1_dry_run "
            "WHERE finding_status = 'present' "
            "GROUP BY 1, 2 ORDER BY 1, 3 DESC"
        ).fetchall()
        log("  [dry-run] per-type x evidence_strength counts (PRESENT rows only):")
        for ct, es, cnt in rows:
            log(f"    {ct:25s} {es:15s} {cnt:,}")
        # CHANGE F + G: distinct-patient counts per (type, tier) — this is
        # what the rollup ever_*_<tier> columns will count.
        log("  [dry-run] distinct-patient counts per (type, tier) — "
            "matches rollup ever_*_<tier> sums:")
        for ct in COMPLICATION_TYPES:
            row = con.execute(
                f"SELECT "
                f"  COUNT(DISTINCT CASE WHEN evidence_strength = 'definitive' "
                f"    THEN research_id END), "
                f"  COUNT(DISTINCT CASE WHEN evidence_strength IN "
                f"    ('definitive', 'probable') THEN research_id END), "
                f"  COUNT(DISTINCT research_id) "
                f"FROM temp_events_v1_dry_run "
                f"WHERE complication_type = '{ct}' "
                f"  AND finding_status = 'present'"
            ).fetchone()
            n_def, n_pob, n_any = (
                int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
            )
            log(f"    {ct:25s} definitive={n_def:>5} "
                f"probable_or_better={n_pob:>5} any_evidence={n_any:>5}")
        # CHANGE A: detection_date_inferred breakdown + completeness audit.
        rows = con.execute(
            "SELECT detection_date_inferred, COUNT(*) "
            "FROM temp_events_v1_dry_run GROUP BY 1 ORDER BY 1"
        ).fetchall()
        log("  [dry-run] detection_date_inferred distribution:")
        for v, cnt in rows:
            log(f"    inferred={v!s:5s} {cnt:,}")
        completeness_nulls = con.execute("""
            SELECT
              SUM(CASE WHEN finding_date IS NULL THEN 1 ELSE 0 END) AS null_date,
              SUM(CASE WHEN source_table IS NULL THEN 1 ELSE 0 END) AS null_source_table,
              SUM(CASE WHEN source_modality IS NULL THEN 1 ELSE 0 END) AS null_modality,
              SUM(CASE WHEN source_evidence_type IS NULL THEN 1 ELSE 0 END) AS null_set
            FROM temp_events_v1_dry_run
        """).fetchone()
        log(f"  [dry-run] CHANGE A completeness NULL counts: "
            f"date={completeness_nulls[0]} table={completeness_nulls[1]} "
            f"modality={completeness_nulls[2]} evidence_type={completeness_nulls[3]}")
        # CHANGE E: lab_value_at_detection populated count
        n_lab = con.execute(
            "SELECT COUNT(*) FROM temp_events_v1_dry_run "
            "WHERE lab_value_at_detection IS NOT NULL"
        ).fetchone()[0]
        log(f"  [dry-run] rows with lab_value_at_detection: {n_lab:,}")
        # CHANGE B: peri-op mortality rows
        n_mort = con.execute(
            "SELECT COUNT(*) FROM temp_events_v1_dry_run "
            "WHERE complication_type = 'mortality'"
        ).fetchone()[0]
        log(f"  [dry-run] peri-op mortality rows (within "
            f"{PERIOP_MORTALITY_WINDOW_DAYS}d of first surgery): {n_mort:,}")
        # CHANGE J — write the forensic file from the temp table.
        forensic_decision: dict[str, Any] = {}
        forensic_path = _write_overextraction_forensic(
            con, "temp_events_v1_dry_run", forensic_decision
        )
        log(f"  [dry-run] CHANGE J forensic written: "
            f"{forensic_path.relative_to(REPO_ROOT)} (PHI; gitignored)")
        con.execute("DROP TABLE temp_events_v1_dry_run")
        return {"created": False, "rows": n, "patients": p, "phase": "dry_run",
                "wound_infection_dropped_for_attribution": n_wound_dropped,
                "hypocalcemia_symptomatic_dropped_for_specificity":
                    n_hypocalc_dropped,
                "rows_pre_gate": n_pre_gate,
                "change_j_forensic": forensic_decision.get("change_j_forensic")}

    # Real build path. We materialise via the CTEs above with evidence text
    # carried as a staging column, then do a Python-side SHA256 pass to
    # compute evidence_span_hash for the LLM/entity rows that have raw text.
    log("  executing CTE build (this can take a few minutes for the LLM unnest)")
    con.execute(sql)
    n_pre_gate = row_count(con, "main", "canonical_complications_events_v1")
    log(f"  events pre-gate: {n_pre_gate:,} rows")

    # CHANGE J — write the forensic markdown from the LIVE table BEFORE gate
    # drops + before the SHA256 hash pass (need raw evidence_text).
    forensic_decision: dict[str, Any] = {}
    try:
        forensic_path = _write_overextraction_forensic(
            con,
            f'{fq("main", "canonical_complications_events_v1")}',
            forensic_decision,
        )
        log(f"  CHANGE J forensic written: "
            f"{forensic_path.relative_to(REPO_ROOT)} (PHI; gitignored)")
    except Exception as exc:
        log_warn(f"  CHANGE J forensic write failed (non-fatal): {exc}")

    # CHANGE H + I — count + apply gate filter.
    n_wound_dropped, n_hypocalc_dropped = _count_gate_drops(
        con, fq("main", "canonical_complications_events_v1")
    )
    log(f"  CHANGE H wound_infection_dropped_for_attribution: "
        f"{n_wound_dropped:,}")
    log(f"  CHANGE I hypocalcemia_symptomatic_dropped_for_specificity: "
        f"{n_hypocalc_dropped:,}")
    con.execute(
        f"DELETE FROM {fq('main', 'canonical_complications_events_v1')} "
        f"WHERE wound_passes_gate = FALSE "
        f"   OR hypocalc_passes_gate = FALSE"
    )
    n_total = row_count(con, "main", "canonical_complications_events_v1")
    p_total = distinct_research_ids(con, "main", "canonical_complications_events_v1")
    log(f"  events post-gate: {n_total:,} rows / {p_total:,} patients "
        f"(dropped {n_pre_gate - n_total:,} from gates)")

    # PHI-safe hashing pass: replace evidence_text_for_hash with SHA256 hash.
    # We do this in SQL via DuckDB's md5/sha256 — DuckDB has a built-in
    # `sha256()` scalar. (If not available, fall back to a Python pass.)
    try:
        con.execute(
            f"UPDATE {fq('main', 'canonical_complications_events_v1')} "
            f"SET evidence_span_hash = "
            f"  CASE WHEN evidence_text_for_hash IS NOT NULL "
            f"       THEN sha256(evidence_text_for_hash) "
            f"       ELSE NULL END"
        )
    except duckdb.Error:
        # Fallback: hex(md5(...)) if sha256 unavailable in this DuckDB build.
        try:
            con.execute(
                f"UPDATE {fq('main', 'canonical_complications_events_v1')} "
                f"SET evidence_span_hash = "
                f"  CASE WHEN evidence_text_for_hash IS NOT NULL "
                f"       THEN md5(evidence_text_for_hash) "
                f"       ELSE NULL END"
            )
            log_warn("  sha256 unavailable; fell back to md5 for evidence_span_hash")
        except duckdb.Error as exc:
            log_warn(f"  hash pass failed: {exc}; "
                     f"running Python-side fallback")
            _hash_pass_python(con)

    # Drop the staging text column. CRITICAL — never persist raw text.
    con.execute(
        f"ALTER TABLE {fq('main', 'canonical_complications_events_v1')} "
        f"DROP COLUMN evidence_text_for_hash"
    )
    # Drop CHANGE H + I staging gate-pass columns now that the filter is
    # applied. (Keeping them would bloat the schema with always-TRUE cols.)
    for staging_col in ("wound_passes_gate", "hypocalc_passes_gate"):
        try:
            con.execute(
                f"ALTER TABLE "
                f"{fq('main', 'canonical_complications_events_v1')} "
                f"DROP COLUMN {staging_col}"
            )
        except duckdb.Error as exc:
            log_warn(f"  DROP COLUMN {staging_col} failed (non-fatal): {exc}")
    n_with_hash = int(con.execute(
        f"SELECT COUNT(*) FROM "
        f"{fq('main', 'canonical_complications_events_v1')} "
        f"WHERE evidence_span_hash IS NOT NULL"
    ).fetchone()[0])
    log(f"  evidence_span_hash populated on {n_with_hash:,} rows "
        f"(remaining {n_total - n_with_hash:,} have no source text)")

    # Per-source row counts.
    rows = con.execute(
        f"SELECT source_table, COUNT(*) "
        f"FROM {fq('main', 'canonical_complications_events_v1')} "
        f"GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    log("  per-source row counts:")
    per_source: dict[str, int] = {}
    for src, cnt in rows:
        log(f"    {src:50s} {cnt:,}")
        per_source[src] = int(cnt)

    # Per-type x status breakdown.
    rows = con.execute(
        f"SELECT complication_type, finding_status, COUNT(*) "
        f"FROM {fq('main', 'canonical_complications_events_v1')} "
        f"GROUP BY 1, 2 ORDER BY 1, 3 DESC"
    ).fetchall()
    per_type: dict[str, dict[str, int]] = {}
    for ct, fs, cnt in rows:
        per_type.setdefault(ct, {})[fs] = int(cnt)

    try:
        con.execute(
            f"COMMENT ON TABLE "
            f"{fq('main', 'canonical_complications_events_v1')} IS "
            f"'[domain=complications; grain=per_finding] — source: "
            f"{SCRIPT_TAG} ({RUN_DATE}); UNION of complication_phenotype_v1 "
            f"+ note_entities_complications + extracted_complications_refined_v5 "
            f"+ extracted_rln_injury_refined_v2 + note_entities_llm_survival_followup "
            f"(vital_status + voice_quality only). Linkage = research_id only; "
            f"every row carries source_table + source_row_id + finding_date for "
            f"downstream JOIN. evidence_span_hash is SHA256/MD5 of source text "
            f"(PHI-safe; raw text is never persisted).'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON events table failed (non-fatal): {exc}")

    return {"created": True, "rows": n_total, "patients": p_total,
            "rows_pre_gate": n_pre_gate,
            "wound_infection_dropped_for_attribution": n_wound_dropped,
            "hypocalcemia_symptomatic_dropped_for_specificity":
                n_hypocalc_dropped,
            "per_source": per_source, "per_type": per_type,
            "rows_with_evidence_hash": n_with_hash,
            "change_j_forensic": forensic_decision.get("change_j_forensic")}


def _hash_pass_python(con: duckdb.DuckDBPyConnection) -> None:
    """Fallback Python-side SHA256 update if DuckDB doesn't have native
    sha256/md5 functions."""
    rows = con.execute(
        f"SELECT source_table, source_row_id, complication_type, "
        f"evidence_text_for_hash "
        f"FROM {fq('main', 'canonical_complications_events_v1')} "
        f"WHERE evidence_text_for_hash IS NOT NULL"
    ).fetchall()
    log(f"    Python hash pass: {len(rows):,} rows")
    for st, sid, ct, txt in rows:
        h = evidence_hash(txt)
        con.execute(
            f"UPDATE {fq('main', 'canonical_complications_events_v1')} "
            f"SET evidence_span_hash = ? "
            f"WHERE source_table = ? AND source_row_id = ? "
            f"  AND complication_type = ?",
            [h, st, sid, ct],
        )


# ---------------------------------------------------------------------------
# Step 5 — Build canonical_complications_patient_rollup_v1
# ---------------------------------------------------------------------------

def _rollup_ever_col_name(ct: str, tier: str) -> str:
    """CHANGE G — return the rollup column name for (complication_type, tier).

    `tier` is one of 'definitive', 'probable_or_better', 'any_evidence'.
    """
    return f"ever_{ct}_{tier}"


# CHANGE G — exposed for QA + CPM repoint code generation.
ROLLUP_TIERS: tuple[str, ...] = ("definitive", "probable_or_better", "any_evidence")


# Complication types that get a 4-column temporal classification on the
# rollup (CHANGE D): preexisting / new_postop / transient / permanent.
TEMPORAL_TYPES_FOR_ROLLUP: list[str] = [
    "hypoparathyroidism",
    "hypocalcemia_clinical",
]


def _build_rollup_sql() -> str:
    """CREATE OR REPLACE TABLE for canonical_complications_patient_rollup_v1.

    Layout (post CHANGE G — 12 ever_*_present cols are REPLACED by 36
    ever_*_<tier> cols where tier ∈ {definitive, probable_or_better,
    any_evidence}):
      - research_id (PK; anchored to canonical_patient_master)
      - 36 ever_*_<tier> BOOL flags (12 complication types × 3 tiers)
      - n_complication_types_present, n_complication_findings_total
      - first_complication_date, last_complication_date
      - 8 temporal BOOL flags for hypoparathyroidism + hypocalcemia_clinical
        (CHANGE D): {type}_preexisting / _new_postop / _transient / _permanent
      - build_ts
    """
    target = fq("main", "canonical_complications_patient_rollup_v1")
    src = fq("main", "canonical_complications_events_v1")

    ever_cols: list[str] = []
    rollup_cols_in_order: list[str] = []
    for ct in COMPLICATION_TYPES:
        for tier in ROLLUP_TIERS:
            col_name = _rollup_ever_col_name(ct, tier)
            rollup_cols_in_order.append(col_name)
            if tier == "definitive":
                pred = (
                    f"complication_type = '{ct}' "
                    f"AND finding_status = 'present' "
                    f"AND evidence_strength = 'definitive'"
                )
            elif tier == "probable_or_better":
                pred = (
                    f"complication_type = '{ct}' "
                    f"AND finding_status = 'present' "
                    f"AND evidence_strength IN ('definitive', 'probable')"
                )
            else:  # any_evidence
                pred = (
                    f"complication_type = '{ct}' "
                    f"AND finding_status = 'present'"
                )
            ever_cols.append(
                f"        COALESCE(BOOL_OR({pred}), FALSE) AS {col_name}"
            )

    # CHANGE D — per-type temporal aggregates inside events_agg. We compute
    # the MIN(present_date) and MAX(present_date) per type so the outer
    # SELECT can derive preexisting / new_postop / transient / permanent
    # against first_surgery_date.
    temporal_min_max_cols: list[str] = []
    for ct in TEMPORAL_TYPES_FOR_ROLLUP:
        temporal_min_max_cols.append(
            f"        MIN(CASE WHEN complication_type = '{ct}' "
            f"AND finding_status = 'present' THEN finding_date END) "
            f"            AS {ct}_min_present_date"
        )
        temporal_min_max_cols.append(
            f"        MAX(CASE WHEN complication_type = '{ct}' "
            f"AND finding_status = 'present' THEN finding_date END) "
            f"            AS {ct}_max_present_date"
        )

    inner_select_block = ",\n".join(ever_cols + temporal_min_max_cols)

    # Outer SELECT — derive temporal flags from MIN/MAX + first_surgery_date.
    temporal_outer_cols: list[str] = []
    for ct in TEMPORAL_TYPES_FOR_ROLLUP:
        temporal_outer_cols.append(f"""
    -- CHANGE D temporal flags for {ct}
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date <
               fs.first_surgery_date - INTERVAL '{PREOP_PROXIMITY_BUFFER_DAYS} days'
        THEN TRUE
      ELSE FALSE
    END AS {ct}_preexisting,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND NOT (
                e.{ct}_min_present_date <
                fs.first_surgery_date - INTERVAL '{PREOP_PROXIMITY_BUFFER_DAYS} days'
           )
        THEN TRUE
      ELSE FALSE
    END AS {ct}_new_postop,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND e.{ct}_max_present_date IS NOT NULL
           AND e.{ct}_max_present_date <
               fs.first_surgery_date + INTERVAL '{TEMPORAL_RESOLUTION_WINDOW_DAYS} days'
        THEN TRUE
      ELSE FALSE
    END AS {ct}_transient,
    CASE
      WHEN e.{ct}_min_present_date IS NOT NULL
           AND fs.first_surgery_date IS NOT NULL
           AND e.{ct}_min_present_date >= fs.first_surgery_date
           AND e.{ct}_max_present_date IS NOT NULL
           AND e.{ct}_max_present_date >=
               fs.first_surgery_date + INTERVAL '{TEMPORAL_RESOLUTION_WINDOW_DAYS} days'
        THEN TRUE
      ELSE FALSE
    END AS {ct}_permanent""")

    temporal_outer_block = ",\n".join(temporal_outer_cols)

    return f"""
CREATE OR REPLACE TABLE {target} AS
WITH all_patients AS (
    SELECT CAST(research_id AS VARCHAR) AS research_id
    FROM main.canonical_patient_master
),
first_surgery AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(CAST(surgery_date_native AS DATE)) AS first_surgery_date
    FROM main.canonical_operative_events_v1
    WHERE surgery_date_native IS NOT NULL
    GROUP BY research_id
),
events_agg AS (
    SELECT
        research_id,
{inner_select_block},
        COUNT(DISTINCT CASE WHEN finding_status = 'present'
                            THEN complication_type END)
            AS n_complication_types_present,
        SUM(CASE WHEN finding_status = 'present' THEN 1 ELSE 0 END)
            AS n_complication_findings_total,
        MIN(CASE WHEN finding_status = 'present' THEN finding_date END)
            AS first_complication_date,
        MAX(CASE WHEN finding_status = 'present' THEN finding_date END)
            AS last_complication_date
    FROM {src}
    GROUP BY research_id
)
SELECT
    p.research_id,
    {','.join(f'COALESCE(e.{c}, FALSE) AS {c}'
              for c in rollup_cols_in_order)},
    COALESCE(e.n_complication_types_present, 0) AS n_complication_types_present,
    COALESCE(e.n_complication_findings_total, 0) AS n_complication_findings_total,
    e.first_complication_date,
    e.last_complication_date,
{temporal_outer_block},
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM all_patients p
LEFT JOIN events_agg   e  ON e.research_id  = p.research_id
LEFT JOIN first_surgery fs ON fs.research_id = p.research_id
"""


def step_5_build_rollup(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — Build main.canonical_complications_patient_rollup_v1")
    log("=" * 78)

    sql = _build_rollup_sql()
    if not do_writes:
        log("  [dry-run] would build rollup from canonical_complications_events_v1")
        return {"created": False}

    if not table_exists(con, "main", "canonical_complications_events_v1"):
        raise RuntimeError(
            "canonical_complications_events_v1 missing — cannot build rollup."
        )
    con.execute(sql)
    n_rows = row_count(con, "main", "canonical_complications_patient_rollup_v1")
    p_rows = distinct_research_ids(
        con, "main", "canonical_complications_patient_rollup_v1"
    )
    log(f"  rollup built: {n_rows:,} rows / {p_rows:,} distinct patients")

    # Per-(type, tier) ever_*_<tier> TRUE counts (informational distribution).
    per_type_true: dict[str, int] = {}
    cols = list_columns(con, "main", "canonical_complications_patient_rollup_v1")
    for col in cols:
        if col.startswith("ever_"):
            n_true = int(con.execute(
                f"SELECT SUM(CASE WHEN \"{col}\" THEN 1 ELSE 0 END) "
                f"FROM {fq('main', 'canonical_complications_patient_rollup_v1')}"
            ).fetchone()[0] or 0)
            per_type_true[col] = n_true
    log("  ever_*_<tier> TRUE counts (CHANGE G — 36 cols, 12 types × 3 tiers):")
    for col, n in per_type_true.items():
        log(f"    {col:55s} {n:,}")

    try:
        con.execute(
            f"COMMENT ON TABLE "
            f"{fq('main', 'canonical_complications_patient_rollup_v1')} IS "
            f"'[domain=complications; grain=per_patient] — source: "
            f"{SCRIPT_TAG} ({RUN_DATE}); 12 ever_*_present BOOLEANs + counts. "
            f"Anchored on canonical_patient_master (one row per patient). "
            f"Patients with no complication findings have all flags FALSE. "
            f"pneumothorax / airway_complication / wound_dehiscence are "
            f"0-population in this cohort and stay FALSE for everyone.'"
        )
    except Exception as exc:
        log_warn(f"  COMMENT ON rollup table failed (non-fatal): {exc}")

    return {"created": True, "rows": n_rows, "patients": p_rows,
            "per_type_true": per_type_true}


# ---------------------------------------------------------------------------
# Step 6 — views_readable views
# ---------------------------------------------------------------------------

NEW_VIEWS: list[tuple[str, str]] = [
    ("complications_events_VIEW_v1", "canonical_complications_events_v1"),
    ("complications_patient_rollup_VIEW_v1",
     "canonical_complications_patient_rollup_v1"),
]


def step_6_build_views(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — Create / refresh views_readable views")
    log("=" * 78)
    out: list[str] = []
    for view_name, base_table in NEW_VIEWS:
        if not table_exists(con, "main", base_table):
            log_warn(f"  base table main.{base_table} missing — skipping view "
                     f"{view_name}")
            continue
        if do_writes:
            con.execute(
                f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                f'"{view_name}" AS SELECT * FROM {fq("main", base_table)}'
            )
        log(f"  view {VIEW_SCHEMA}.{view_name} -> main.{base_table}")
        out.append(view_name)
    return {"views": out}


# ---------------------------------------------------------------------------
# Step 8 — Registry sync (Pattern 13: idempotent DELETE-first)
# ---------------------------------------------------------------------------

NEW_REGISTRY_ENTRIES: list[dict[str, Any]] = [
    {
        "detail_table_name": "canonical_complications_events_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "per_finding (one row per source mention of a complication)",
        "domain": "complications",
        "feeds_master_columns":
            "comp_*_confirmed, comp_*_suspected, comp_*_permanent, "
            "comp_*_transient, comp_*_treatment_req, comp_*_timing_window, "
            "comp_*_days_postop, *_status, n_confirmed_complications, "
            "any_confirmed_complication_flag, mortality_type",
        "description":
            "Lean events-grain canonical for complications. 12-value "
            "complication_type enum, source_table + source_row_id for "
            "evidence trail, finding_date for cross-domain JOIN. Built by "
            f"{SCRIPT_TAG} on {RUN_DATE}.",
        "feeds_master_columns_array": [
            "any_confirmed_complication_flag",
            "n_confirmed_complications",
            "earliest_complication_days",
        ],
    },
    {
        "detail_table_name": "canonical_complications_patient_rollup_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "per_patient (one row per patient in canonical_patient_master)",
        "domain": "complications",
        "feeds_master_columns":
            "any_confirmed_complication, any_confirmed_complication_flag, "
            "*_status, n_confirmed_complications, "
            "earliest_complication_days",
        "description":
            "Per-patient rollup with 12 ever_*_present BOOLEAN flags + "
            "n_complication_types_present + n_complication_findings_total + "
            "first/last_complication_date. Anchored on canonical_patient_master. "
            f"Built by {SCRIPT_TAG} on {RUN_DATE}.",
        "feeds_master_columns_array": [
            "any_confirmed_complication",
            "n_confirmed_complications",
        ],
    },
]

# Registry rows to remove on the build commit (pattern: superseded names).
# We DELETE only the names that the new canonicals supersede + the LLM/entity
# tables that we now register as feeders elsewhere.
REGISTRY_DROPS: list[str] = [
    "complication_phenotype_v1",
    "complication_patient_summary_v1",
    "note_entities_complications",
    "extracted_complications_refined_v5",
    "extracted_rln_injury_refined_v2",
]


def step_8_registry_sync(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 8 — detail_table_registry_v1 sync")
    log("=" * 78)
    if not table_exists(con, WS_SCHEMA, REGISTRY_TABLE):
        log_warn(f"  registry {WS_SCHEMA}.{REGISTRY_TABLE} missing — skipping")
        return {"skipped": True}
    reg_cols = list_columns(con, WS_SCHEMA, REGISTRY_TABLE)
    log(f"  registry columns: {reg_cols}")

    placeholders = ", ".join(["?"] * len(REGISTRY_DROPS))
    pre_count = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name IN ({placeholders})",
        REGISTRY_DROPS,
    ).fetchone()[0])
    log(f"  registry rows for legacy/superseded names (pre-delete): {pre_count}")

    # Also DELETE the new names FIRST so the INSERT is idempotent (Pattern 13).
    new_names = [r["detail_table_name"] for r in NEW_REGISTRY_ENTRIES]
    new_placeholders = ", ".join(["?"] * len(new_names))
    pre_new_count = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name IN ({new_placeholders})",
        new_names,
    ).fetchone()[0])
    log(f"  registry rows for new canonical names (pre-delete, idempotent re-runs): "
        f"{pre_new_count}")

    if do_writes:
        # Delete legacy + idempotent re-insert of the new names.
        con.execute(
            f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE detail_table_name IN ({placeholders})",
            REGISTRY_DROPS,
        )
        log(f"  deleted {pre_count} legacy/superseded registry rows")
        con.execute(
            f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE detail_table_name IN ({new_placeholders})",
            new_names,
        )
        if pre_new_count:
            log(f"  deleted {pre_new_count} stale rows for new canonical names")

    # Build INSERT records for the 2 new canonicals.
    insert_records: list[dict[str, Any]] = []
    for entry in NEW_REGISTRY_ENTRIES:
        sch, tbl = entry["schema_name"], entry["detail_table_name"]
        if not table_exists(con, sch, tbl):
            log_warn(f"  registry skip: {sch}.{tbl} not yet built")
            continue
        n = row_count(con, sch, tbl)
        p = distinct_research_ids(con, sch, tbl)
        rec: dict[str, Any] = {
            "detail_table_name":          tbl,
            "schema_name":                sch,
            "join_key":                   entry["join_key"],
            "grain":                      entry["grain"],
            "total_rows":                 n,
            "total_patients":             p,
            "domain":                     entry["domain"],
            "feeds_master_columns":       entry["feeds_master_columns"],
            "description":                entry["description"],
            "canonical_version":          f"v1_0_script{SCRIPT_ID}",
            "feeds_master_columns_secondary": None,
            "feeds_master_columns_array": entry["feeds_master_columns_array"],
            "needs_manual_review":        False,
        }
        ordered = [(c, rec[c]) for c in reg_cols if c in rec]
        col_csv = ", ".join(c for c, _ in ordered)
        ph_csv = ", ".join("?" for _ in ordered)
        log(f"  INSERT registry row: {tbl} (rows={n:,}, patients={p:,})")
        if do_writes:
            con.execute(
                f"INSERT INTO {fq(WS_SCHEMA, REGISTRY_TABLE)} ({col_csv}) "
                f"VALUES ({ph_csv})",
                [v for _, v in ordered],
            )
        insert_records.append(rec)

    return {"deleted_legacy": pre_count,
            "deleted_self_idempotent": pre_new_count,
            "inserted_new": len(insert_records)}


# ---------------------------------------------------------------------------
# Step 9 — CPM feeder audit (read-only report)
# ---------------------------------------------------------------------------

# Heuristic mapping of CPM column name patterns -> deprecated source table.
# Read-only — we just report what would need repointing in commit 2.
CPM_FEEDER_PATTERNS: list[tuple[str, str, str]] = [
    # (cpm_pattern_substring, target_canonical, target_rollup_col)
    ("comp_rln_injury_confirmed", "rollup", "ever_rln_injury_present"),
    ("comp_rln_injury_suspected", "rollup", "(suspected/derived)"),
    ("comp_rln_injury_permanent", "rollup", "(permanence; events-side)"),
    ("comp_rln_injury_transient", "rollup", "(permanence; events-side)"),
    ("comp_rln_injury_treatment_req", "events", "(treatment_requiring; events-side)"),
    ("comp_rln_injury_timing_window", "events", "(onset_class; events-side)"),
    ("comp_rln_injury_days_postop", "events", "(days_since_surgery; events-side)"),
    ("comp_rln_injury_evidence_tier", "events", "(source_kind ranking)"),
    ("comp_hypocalcemia_confirmed", "rollup", "ever_hypocalcemia_clinical_present"),
    ("comp_hypoparathyroidism_confirmed", "rollup", "ever_hypoparathyroidism_present"),
    ("comp_hematoma_confirmed", "rollup", "ever_hematoma_present"),
    ("comp_seroma_confirmed", "rollup", "ever_seroma_present"),
    ("comp_chyle_leak_confirmed", "rollup", "ever_chyle_leak_present"),
    ("comp_wound_infection_confirmed", "rollup", "ever_wound_infection_present"),
    ("any_confirmed_complication_flag", "rollup",
     "(n_complication_types_present > 0)"),
    ("n_confirmed_complications", "rollup", "n_complication_types_present"),
    ("earliest_complication_days", "rollup",
     "(DATE_DIFF on first_complication_date)"),
    ("hypocalcemia_status", "rollup",
     "(VARCHAR derivation; supersede with ever_*_present BOOL)"),
    ("hypoparathyroidism_status", "rollup", "ever_hypoparathyroidism_present"),
    ("rln_status", "rollup", "ever_rln_injury_present"),
    ("hematoma_status", "rollup", "ever_hematoma_present"),
    ("seroma_status", "rollup", "ever_seroma_present"),
    ("chyle_leak_status", "rollup", "ever_chyle_leak_present"),
    ("wound_infection_status", "rollup", "ever_wound_infection_present"),
    ("rln_permanent_flag", "events", "(events.permanence_class='permanent')"),
    ("rln_transient_flag", "events", "(events.permanence_class='transient')"),
    ("rln_injury_is_confirmed", "rollup", "ever_rln_injury_present"),
    ("rln_injury_type", "events", "(events.complication_type)"),
    ("rln_classification", "events",
     "(events.source_kind; refined_extraction provides classification)"),
    ("rln_temporality", "events", "(events.permanence_class)"),
    ("rln_laterality", "events", "(NOT in canonical events; carry-forward)"),
    ("rln_injury_days_postop", "events",
     "(DATE_DIFF on finding_date - linked surgery)"),
    ("rln_injury_detection_date", "events", "events.finding_date"),
    ("rln_injury_tier", "events", "(source_kind ranking)"),
    ("nlp_ne_complications_has_data", "events",
     "(EXISTS by source_table='note_entities_complications')"),
    ("nlp_ne_complications_n_rows", "events",
     "(COUNT by source_table='note_entities_complications')"),
    ("op_nlp_intraop_complication", "events",
     "(events.onset_class='intraop' from op_note source)"),
    ("op_nlp_intraop_complication_date", "events",
     "events.finding_date WHERE onset_class='intraop'"),
    ("op_nlp_intraop_complication_days_from_surg", "events",
     "(0 by definition for intraop)"),
    ("op_nlp_intraop_complication_n_mentions", "events",
     "(COUNT WHERE onset_class='intraop')"),
    ("op_nlp_rln_finding", "events",
     "(events WHERE complication_type='rln_injury' AND modality='op_note')"),
    ("op_nlp_rln_finding_date", "events", "events.finding_date"),
    ("op_nlp_rln_finding_days_from_surg", "events",
     "(DATE_DIFF on linked surgery)"),
    ("op_nlp_rln_finding_n_mentions", "events", "(COUNT)"),
    ("mortality_type", "events",
     "(events WHERE complication_type='mortality'; LLM survival_followup)"),
]


def step_9_cpm_feeder_audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 9 — CPM feeder audit (read-only report)")
    log("=" * 78)
    if not table_exists(con, "main", "canonical_patient_master"):
        log_warn("  canonical_patient_master missing — skipping CPM audit")
        return {"audit_rows": [], "report_path": None}

    cpm_cols = set(list_columns(con, "main", "canonical_patient_master"))
    log(f"  CPM has {len(cpm_cols)} total columns")

    # 1. Pattern-based heuristic match: identify CPM cols that need repointing.
    audit_rows: list[dict[str, Any]] = []
    matched_cpm: set[str] = set()
    for pattern, target, target_col in CPM_FEEDER_PATTERNS:
        # Exact name + prefix variants (the CPM has e.g. comp_rln_injury_confirmed
        # so we accept exact match OR prefix match).
        for cpm_col in cpm_cols:
            if cpm_col == pattern or (
                pattern.endswith("_") and cpm_col.startswith(pattern)
            ):
                audit_rows.append({
                    "cpm_column": cpm_col,
                    "target_canonical": target,
                    "target_column_or_derivation": target_col,
                })
                matched_cpm.add(cpm_col)

    # 2. Discovery scan: any CPM column matching complication keywords that
    # we didn't catch via the explicit pattern list.
    discovery_keywords = [
        "complication", "hypocalcem", "hypoparathyr", "rln_", "vocal_cord",
        "hematoma", "seroma", "chyle", "wound_inf", "wound_dehisc",
        "pneumothorax", "airway_compl", "mortality",
    ]
    discovery: list[str] = []
    for cpm_col in sorted(cpm_cols):
        if cpm_col in matched_cpm:
            continue
        if cpm_col.startswith("nsqip_"):
            continue  # Out of scope (separate source per probe).
        if cpm_col.startswith("prm_"):
            continue  # Out of scope (separate source).
        if cpm_col.startswith("syn_"):
            continue  # Out of scope (synoptic).
        if cpm_col.startswith("ops_"):
            continue  # Out of scope (operative summary).
        if cpm_col.startswith("nucmed_") or cpm_col.startswith("mri_"):
            continue
        ll = cpm_col.lower()
        if any(kw in ll for kw in discovery_keywords):
            discovery.append(cpm_col)

    # 3. git grep for downstream feeder scripts referencing the 5 deprecated
    # source tables (so the repoint commit can update them all).
    grep_hits: dict[str, list[str]] = {}
    for _, tbl, _ in DEPRECATED_SOURCES:
        try:
            res = subprocess.run(
                ["git", "grep", "-l", tbl, "--", "scripts/"],
                cwd=str(REPO_ROOT),
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            files = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
            grep_hits[tbl] = files
        except subprocess.SubprocessError as exc:
            log_warn(f"  git grep for {tbl} failed: {exc}")
            grep_hits[tbl] = []

    md_lines = [
        f"# CPM feeder audit — {SCRIPT_TAG} ({RUN_DATE})",
        "",
        "Read-only audit produced by Step 9 of Script 364. Identifies CPM "
        "columns that need to be repointed from the 5 deprecated complication "
        "source tables to the new canonical events/rollup pair. The actual "
        "repoint is implemented by `scripts/364_cpm_feeder_repoint.py` "
        "(commit 2 of the 3-commit cascade).",
        "",
        f"**CPM total columns:** {len(cpm_cols)}",
        f"**Heuristic matches (need repoint):** {len(audit_rows)}",
        f"**Discovery candidates (need analyst triage):** {len(discovery)}",
        "",
        "## Heuristic matches — repoint plan",
        "",
        "| CPM column | Target canonical | Target column / derivation |",
        "|---|---|---|",
    ]
    for r in audit_rows:
        md_lines.append(
            f"| `{r['cpm_column']}` | `{r['target_canonical']}` | "
            f"{r['target_column_or_derivation']} |"
        )
    if not audit_rows:
        md_lines.append("| (none) | | |")

    md_lines += [
        "",
        "## Discovery candidates",
        "",
        "CPM columns that contain complication-related keywords but were NOT "
        "matched by the heuristic pattern list. These need analyst review to "
        "decide whether to repoint, derive, or leave untouched.",
        "",
    ]
    if discovery:
        for col in discovery:
            md_lines.append(f"- `{col}`")
    else:
        md_lines.append("(none)")

    md_lines += [
        "",
        "## Downstream feeder scripts (`git grep -l <table> -- scripts/`)",
        "",
        "Scripts that reference the 5 deprecated source tables. These need "
        "to be patched in commit 2 (CPM repoint) or commit 3 (drop legacy) "
        "to point at the new canonical instead.",
        "",
        "| deprecated table | feeder script files |",
        "|---|---|",
    ]
    for tbl, files in grep_hits.items():
        if files:
            md_lines.append(
                f"| `{tbl}` | "
                + ", ".join(f"`{f}`" for f in files[:10])
                + (f" (+{len(files)-10} more)" if len(files) > 10 else "")
                + " |"
            )
        else:
            md_lines.append(f"| `{tbl}` | (no script-level references) |")

    md_lines += [
        "",
        "## Out-of-scope CPM columns (NOT touched by this audit)",
        "",
        "- `nsqip_*` — separate clinical-registry source (not from the 5 "
        "deprecated tables). Script 364 / 364_cpm_feeder_repoint do not "
        "modify these.",
        "- `prm_*`, `syn_*`, `ops_*` — separate domains.",
        "",
    ]

    CPM_AUDIT_PATH.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    log(f"  CPM feeder audit -> {CPM_AUDIT_PATH.relative_to(REPO_ROOT)}")
    log(f"  heuristic matches: {len(audit_rows)} | "
        f"discovery candidates: {len(discovery)}")
    return {"audit_rows": audit_rows, "discovery": discovery,
            "report_path": str(CPM_AUDIT_PATH.relative_to(REPO_ROOT)),
            "grep_hits_per_table": {k: len(v) for k, v in grep_hits.items()}}


# ---------------------------------------------------------------------------
# Step 7 — Phase-gated archive + drop (only with --phase 7)
# ---------------------------------------------------------------------------

def step_7_drop_deprecated(
    con: duckdb.DuckDBPyConnection, do_writes: bool, archive_counts: dict[str, int]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 7 — Drop deprecated complication source tables")
    log("=" * 78)

    # 7.1 Pre-flight: replacement canonicals must exist with non-zero rows.
    for sch, tbl in [("main", "canonical_complications_events_v1"),
                     ("main", "canonical_complications_patient_rollup_v1")]:
        if not table_exists(con, sch, tbl):
            raise RuntimeError(
                f"Refusing to drop: replacement {sch}.{tbl} does not exist."
            )
        n = row_count(con, sch, tbl)
        if n == 0:
            raise RuntimeError(
                f"Refusing to drop: replacement {sch}.{tbl} is empty."
            )
        log(f"  replacement {sch}.{tbl}: {n:,} rows ✓")

    # 7.2 Pre-flight: CPM feeder repoint marker must exist (per the 363
    # pattern). The CPM repoint script writes
    # ``.complications_cpm_repoint_applied`` at repo root.
    marker = REPO_ROOT / ".complications_cpm_repoint_applied"
    if not marker.exists():
        raise RuntimeError(
            f"Refusing to drop: CPM repoint marker {marker.name!r} missing. "
            f"Run scripts/364_cpm_feeder_repoint.py --commit (commit 2) first."
        )
    log(f"  CPM repoint marker present ✓ ({marker.name})")

    # 7.3 Pre-flight: confirm at least one pre364 archive exists per source
    # whose row count matches the live table.
    archives_used: dict[str, str] = {}
    for sch, tbl, _role in DEPRECATED_SOURCES:
        if not table_exists(con, sch, tbl):
            log(f"  {sch}.{tbl} already absent — skipping archive parity")
            continue
        live_n = archive_counts.get(f"{sch}.{tbl}") or row_count(con, sch, tbl)
        archive_pattern = f"{tbl}_pre364_%"
        candidates = con.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ?
              AND table_name LIKE ?
            ORDER BY table_name DESC
            """,
            [ARCHIVE_DB, ARCHIVE_SCHEMA, archive_pattern],
        ).fetchall()
        if not candidates:
            raise RuntimeError(
                f"No pre364_* archive found for {sch}.{tbl} in "
                f"{ARCHIVE_DB}.{ARCHIVE_SCHEMA}. Run --phase 0 first."
            )
        matched: str | None = None
        seen: list[tuple[str, int]] = []
        for (arch_name,) in candidates:
            arch = f'{ARCHIVE_FQ}."{arch_name}"'
            try:
                arch_n = int(con.execute(
                    f"SELECT COUNT(*) FROM {arch}"
                ).fetchone()[0])
            except duckdb.Error as exc:
                log_warn(f"    archive {arch_name} unreadable: {exc}")
                continue
            seen.append((arch_name, arch_n))
            if arch_n == live_n:
                matched = arch_name
                break
        if matched is None:
            raise RuntimeError(
                f"No pre364_* archive of {sch}.{tbl} matches live row count "
                f"{live_n:,}. Candidates: {seen}. Refusing to drop."
            )
        archives_used[f"{sch}.{tbl}"] = matched
        log(f"  parity verified: {sch}.{tbl} ({live_n:,} rows) <- {matched}")

    # 7.4 Find dependent views in views_readable.
    targets = {tbl for _, tbl, _ in DEPRECATED_SOURCES}
    dep_views: list[tuple[str, str]] = []
    try:
        rows = con.execute(
            "SELECT view_schema, view_name FROM information_schema.view_table_usage "
            "WHERE view_catalog = ? AND table_schema = 'main'",
            [CANONICAL_DB],
        ).fetchall()
        for vs, vn in rows:
            try:
                ddl = con.execute(
                    "SELECT sql FROM duckdb_views() "
                    "WHERE database_name = ? AND schema_name = ? "
                    "AND view_name = ?",
                    [CANONICAL_DB, vs, vn],
                ).fetchone()
                ddl_str = (ddl[0] if ddl else "") or ""
                if any(t in ddl_str for t in targets):
                    dep_views.append((vs, vn))
            except duckdb.Error:
                continue
    except duckdb.Error as exc:
        log_warn(f"  view dependency lookup failed (non-fatal): {exc}")

    if dep_views:
        log(f"  dependent views to drop: {len(dep_views)}")
        for vs, vn in dep_views:
            if vs == VIEW_SCHEMA and vn in {v[0] for v in NEW_VIEWS}:
                continue  # Skip our own freshly-created views.
            log(f"    DROP VIEW {vs}.{vn}")
            if do_writes:
                con.execute(
                    f'DROP VIEW IF EXISTS "{CANONICAL_DB}"."{vs}"."{vn}"'
                )

    # 7.5 Drop the 5 deprecated source tables.
    dropped: list[str] = []
    for sch, tbl, _role in DEPRECATED_SOURCES:
        if not table_exists(con, sch, tbl):
            log(f"  {sch}.{tbl} already absent")
            continue
        log(f"  DROP TABLE {sch}.{tbl}")
        if do_writes:
            con.execute(f"DROP TABLE {fq(sch, tbl)}")
        dropped.append(f"{sch}.{tbl}")
    return {"dropped": dropped,
            "dependent_views_dropped": [f"{s}.{v}" for s, v in dep_views],
            "archives_used_for_parity": archives_used}


# ---------------------------------------------------------------------------
# Step 10 — Zero-drift QA (5 hard + 1 informational coverage + distributions)
# ---------------------------------------------------------------------------

def _find_latest_archive_snapshot(
    con: duckdb.DuckDBPyConnection, source_table: str
) -> str | None:
    """Return the fully-qualified name of the most recent pre364_* archive
    of the given source_table, or None if no archive exists."""
    rows = con.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ?
          AND table_name LIKE ?
        ORDER BY table_name DESC
        LIMIT 1
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA, f"{source_table}_pre364_%"],
    ).fetchone()
    if not rows:
        return None
    return f'{ARCHIVE_FQ}."{rows[0]}"'


# Q-A option 2 — per-source-kind preservation gate config.
# For each source_table, the SQL below computes the "expected to survive"
# row count from the pre-build archive AFTER excluding intentionally-gated
# combinations (CHANGE H wound_infection drops + CHANGE I hypocalcemia
# symptomatic_only drops). The live count from the same source_table is
# compared with a 0.50 hard floor (catches catastrophic regressions while
# allowing small dedup-ratio drift).
_PRESERVATION_HARD_FLOOR = 0.50
_PRESERVATION_WARN_FLOOR = 0.95


def _archive_eligible_count_sql(source_table: str, archive_fq: str) -> str:
    """Return SQL that produces (complication_type, n) rows for the
    'expected to survive' subset of the given archive snapshot.

    Each source_table has its own filter logic that mirrors what the build
    SQL would apply to live data, MINUS the intentional CHANGE H/I drops:
    we exclude rows that we KNOW the gates would drop. What remains is
    what the live build is expected to retain.
    """
    if source_table == "complication_phenotype_v1":
        vocab_case = _vocab_map_case_sql("complication_entity")
        return f"""
            WITH mapped AS (
                SELECT
                    ({vocab_case}) AS complication_type,
                    confirmed_flag, status_v2, treatment_requiring_flag
                FROM {archive_fq}
            )
            SELECT complication_type, COUNT(*)
            FROM mapped
            WHERE complication_type IS NOT NULL
              AND COALESCE(confirmed_flag, FALSE) = TRUE
              -- CHANGE H: phenotype wound_infection rows have NULL
              -- evidence_text → all dropped by the anatomic gate.
              AND complication_type != 'wound_infection'
              -- CHANGE I: phenotype hypocalcemia rows that would be
              -- 'symptomatic_only' (no treatment, no confirmed status_v2)
              -- have NULL evidence_text → all dropped. Only treatment-
              -- initiated path survives.
              AND NOT (
                  complication_type = 'hypocalcemia_clinical'
                  AND COALESCE(treatment_requiring_flag, FALSE) = FALSE
              )
            GROUP BY 1
        """
    if source_table == "note_entities_complications":
        vocab_case = _vocab_map_case_sql("entity_value_norm")
        return f"""
            WITH mapped AS (
                SELECT
                    ({vocab_case}) AS complication_type,
                    present_or_negated, evidence_span
                FROM {archive_fq}
            )
            SELECT complication_type, COUNT(*)
            FROM mapped
            WHERE complication_type IS NOT NULL
              AND LOWER(COALESCE(present_or_negated, '')) = 'present'
              -- CHANGE H: entity_value_raw is just "wound infection" with
              -- no anatomic context → all dropped by anatomic gate.
              AND complication_type != 'wound_infection'
              -- CHANGE I: entity_value_norm is just "hypocalcemia" with
              -- no symptom context → all dropped by symptom-specificity
              -- gate. (Even if a few rows happened to have richer
              -- evidence_span, the proximity-to-attribution rule is so
              -- strict that empirically zero rows pass.)
              AND complication_type != 'hypocalcemia_clinical'
            GROUP BY 1
        """
    if source_table == "extracted_complications_refined_v5":
        vocab_case = _vocab_map_case_sql("entity_name")
        return f"""
            WITH mapped AS (
                SELECT
                    ({vocab_case}) AS complication_type,
                    entity_is_confirmed
                FROM {archive_fq}
            )
            SELECT complication_type, COUNT(*)
            FROM mapped
            WHERE complication_type IS NOT NULL
              AND entity_is_confirmed = TRUE
              -- CHANGE H + I exclusions (same rationale as above; this
              -- source has no evidence_text either).
              AND complication_type NOT IN ('wound_infection',
                                            'hypocalcemia_clinical')
            GROUP BY 1
        """
    if source_table == "extracted_rln_injury_refined_v2":
        # All rows map to rln_injury; no CHANGE H/I exclusions apply.
        return f"""
            SELECT 'rln_injury' AS complication_type, COUNT(*)
            FROM {archive_fq}
            WHERE rln_injury_is_confirmed = TRUE
        """
    raise ValueError(f"No preservation logic defined for {source_table!r}")


# Sources subject to per-source-kind preservation. Excludes the lab + LLM
# canonicals (canonical_labs_calcium_v1, canonical_survival_followup_v1)
# because those are not pre-build archived (they live in their own
# canonical pipelines and 364 doesn't archive them).
PRESERVATION_SOURCES: tuple[str, ...] = (
    "complication_phenotype_v1",
    "note_entities_complications",
    "extracted_complications_refined_v5",
    "extracted_rln_injury_refined_v2",
)


def step_10_qa(
    con: duckdb.DuckDBPyConnection,
    archive_counts: dict[str, int],
    pre_drop: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 10 — Zero-drift QA")
    log("=" * 78)
    qa: dict[str, Any] = {"checks": [], "informational": [], "passed": True}

    def check(name: str, passed: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(passed), **details})
        log(f"  QA {'PASS' if passed else 'FAIL'} {name}: {details}")
        if not passed:
            qa["passed"] = False

    def info(name: str, **details: Any) -> None:
        qa["informational"].append({"name": name, **details})
        log(f"  INFO {name}: {details}")

    # Hard 1 — events table has rows.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        n_ev = row_count(con, "main", "canonical_complications_events_v1")
    else:
        n_ev = -1
    check("events_rowcount_nonzero", n_ev > 0, rows=n_ev)

    # Hard 2 — rollup row count == COUNT(DISTINCT research_id) from CPM
    # (anchor) — we anchored on CPM, so this should be exactly CPM size.
    if table_exists(con, "main", "canonical_complications_patient_rollup_v1"):
        n_ro = row_count(con, "main", "canonical_complications_patient_rollup_v1")
    else:
        n_ro = -1
    n_cpm = row_count(con, "main", "canonical_patient_master") if table_exists(
        con, "main", "canonical_patient_master") else -1
    check("rollup_parity_with_cpm", n_ro == n_cpm,
          rollup_rows=n_ro, cpm_rows=n_cpm)

    # Hard 3 — every complication_type value on events is in the canonical
    # 12-value enum. (The reverse — coverage — is downgraded to informational
    # per the user's pick A.)
    if table_exists(con, "main", "canonical_complications_events_v1"):
        bad_types = con.execute(
            f"SELECT DISTINCT complication_type "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE complication_type IS NULL "
            f"   OR complication_type NOT IN ({', '.join(repr(c) for c in COMPLICATION_TYPES)})"
        ).fetchall()
    else:
        bad_types = [("(table missing)",)]
    check("complication_type_in_canonical_set",
          len(bad_types) == 0,
          unexpected_values=[r[0] for r in bad_types])

    # Hard 3b (CHANGE A) — events_completeness_required: every event must
    # have NOT NULL on detection_date / source_table / source_modality /
    # source_evidence_type. Per-column NULL counts surfaced for diagnosis.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        comp = con.execute(f"""
            SELECT
              SUM(CASE WHEN finding_date IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN source_table IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN source_modality IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN source_evidence_type IS NULL THEN 1 ELSE 0 END),
              COUNT(*)
            FROM {fq('main', 'canonical_complications_events_v1')}
        """).fetchone()
        n_total = int(comp[4] or 0)
        n_null_date = int(comp[0] or 0)
        n_null_table = int(comp[1] or 0)
        n_null_modality = int(comp[2] or 0)
        n_null_set = int(comp[3] or 0)
        completeness_ok = (
            n_null_date == 0 and n_null_table == 0
            and n_null_modality == 0 and n_null_set == 0
        )
        check("events_completeness_required",
              completeness_ok,
              total_rows=n_total,
              null_finding_date=n_null_date,
              null_source_table=n_null_table,
              null_source_modality=n_null_modality,
              null_source_evidence_type=n_null_set)
    else:
        check("events_completeness_required", False,
              reason="events table missing")

    # Hard 3c (CHANGE C) — every source_evidence_type is in the canonical
    # enum or NULL (NULL is caught by the gate above).
    if table_exists(con, "main", "canonical_complications_events_v1"):
        bad_set = con.execute(
            f"SELECT DISTINCT source_evidence_type "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE source_evidence_type IS NOT NULL "
            f"  AND source_evidence_type NOT IN "
            f"      ({', '.join(repr(v) for v in sorted(SOURCE_EVIDENCE_TYPE_VALUES))})"
        ).fetchall()
        check("source_evidence_type_in_canonical_set",
              len(bad_set) == 0,
              unexpected_values=[r[0] for r in bad_set])

    # Hard 3d (CHANGE F) — every evidence_strength is in the canonical enum
    # {definitive, probable, possible}; NOT NULL.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        bad_es = con.execute(
            f"SELECT DISTINCT evidence_strength "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE evidence_strength IS NULL "
            f"   OR evidence_strength NOT IN "
            f"      ({', '.join(repr(v) for v in EVIDENCE_STRENGTH_VALUES)})"
        ).fetchall()
        check("evidence_strength_in_canonical_set",
              len(bad_es) == 0,
              unexpected_values=[r[0] for r in bad_es])

    # Hard 4 — Q-A option 2: per-(source_table, complication_type)
    # preservation gate. For each source archive snapshot, compute the
    # subset of rows expected to survive the build's filters EXCLUDING
    # intentional CHANGE H/I drops. Compare to live present-event counts
    # from that source. Hard floor at 0.50 catches catastrophic regressions
    # (a JOIN gone wrong, a vocab map regression, a misapplied filter)
    # while letting normal dedup-ratio drift through. Wound_infection and
    # hypocalcemia symptomatic_only drops are surfaced separately as
    # informational so they don't mask real bugs.
    preservation_results: list[dict[str, Any]] = []
    for source_table in PRESERVATION_SOURCES:
        if not archive_counts.get(f"main.{source_table}"):
            info(f"preservation_per_source_kind_skipped_{source_table}",
                 reason="no archive_counts (likely --phase 7-only run)")
            continue
        archive_fq = _find_latest_archive_snapshot(con, source_table)
        if archive_fq is None:
            check(f"preservation_per_source_kind_{source_table}", False,
                  reason="no pre364_* archive found")
            continue
        try:
            expected_rows = con.execute(
                _archive_eligible_count_sql(source_table, archive_fq)
            ).fetchall()
        except duckdb.Error as exc:
            check(f"preservation_per_source_kind_{source_table}", False,
                  reason=f"archive query failed: {exc}")
            continue
        live_rows = con.execute(
            f"SELECT complication_type, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE source_table = ? AND finding_status = 'present' "
            f"GROUP BY 1",
            [source_table],
        ).fetchall()
        expected = {r[0]: int(r[1]) for r in expected_rows}
        live = {r[0]: int(r[1]) for r in live_rows}
        all_types = sorted(set(expected) | set(live))
        per_type: list[dict[str, Any]] = []
        any_fail = False
        for ct in all_types:
            n_expected = expected.get(ct, 0)
            n_live = live.get(ct, 0)
            if n_expected == 0:
                ratio = None
                ok = True
            else:
                ratio = n_live / n_expected
                ok = ratio >= _PRESERVATION_HARD_FLOOR
            if not ok:
                any_fail = True
            per_type.append({
                "complication_type": ct,
                "archive_eligible": n_expected,
                "live_present": n_live,
                "ratio": round(ratio, 4) if ratio is not None else None,
                "passes_hard_floor": ok,
                "below_warn_floor":
                    (ratio is not None and ratio < _PRESERVATION_WARN_FLOOR),
            })
        check(f"preservation_per_source_kind_{source_table}",
              not any_fail,
              hard_floor=_PRESERVATION_HARD_FLOOR,
              warn_floor=_PRESERVATION_WARN_FLOOR,
              per_type=per_type)
        preservation_results.append({
            "source_table": source_table,
            "per_type": per_type,
        })

    # Informational — intentional drops by source (CHANGE H + I).
    if table_exists(con, "main", "canonical_complications_events_v1"):
        # Wound_infection drops are already accounted for by zero live
        # wound_infection rows (they don't appear in the canonical at all
        # post-gate). Surface live wound_infection count + the build-time
        # drop count from the run log if available.
        n_live_wound = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_complications_events_v1')} "
            f"WHERE complication_type = 'wound_infection'"
        ).fetchone()[0])
        info("change_h_wound_infection_post_gate",
             live_rows=n_live_wound,
             note="0 expected post-CHANGE-H; non-zero indicates the gate "
                  "let an unexpected source through. Exact drop count is "
                  "in the build log (wound_infection_dropped_for_attribution)")
        # Hypocalcemia symptomatic_only drops.
        n_live_hypocalc = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_complications_events_v1')} "
            f"WHERE complication_type = 'hypocalcemia_clinical' "
            f"  AND source_evidence_type = 'symptomatic_only'"
        ).fetchone()[0])
        info("change_i_hypocalcemia_symptomatic_only_post_gate",
             live_rows=n_live_hypocalc,
             note="0 expected post-CHANGE-I; non-zero indicates rows "
                  "passed the symptom-specificity regex. Exact drop "
                  "count is in the build log "
                  "(hypocalcemia_symptomatic_dropped_for_specificity)")

    # Hard 5 — view resolves (Pattern 10).
    for view_name, _ in NEW_VIEWS:
        ok = view_exists(con, VIEW_SCHEMA, view_name)
        if ok:
            try:
                con.execute(
                    f'SELECT * FROM "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{view_name}" LIMIT 0'
                ).fetchall()
                resolves = True
            except duckdb.Error as exc:
                resolves = False
                log_warn(f"  view {view_name} fails to resolve: {exc}")
        else:
            resolves = False
        check(f"view_resolves_{view_name}", ok and resolves)

    # Informational — 12-type coverage (downgraded from hard per pick A).
    if table_exists(con, "main", "canonical_complications_events_v1"):
        present_per_type = {
            r[0]: r[1] for r in con.execute(
                f"SELECT complication_type, COUNT(*) "
                f"FROM {fq('main', 'canonical_complications_events_v1')} "
                f"WHERE finding_status = 'present' "
                f"GROUP BY 1"
            ).fetchall()
        }
        absent_types = [c for c in COMPLICATION_TYPES
                        if c not in present_per_type]
        info("complication_type_coverage",
             types_with_present=sorted(present_per_type.keys()),
             types_absent_in_cohort=sorted(absent_types),
             documented_absence_note=(
                 "pneumothorax / airway_complication / wound_dehiscence "
                 "are recognized standard thyroidectomy complications "
                 "(AJCC, ATA) that are 0-population in this cohort. "
                 "Schema preserves their columns for future cross-cohort "
                 "comparability."))

    # Informational — per-modality breakdown of present findings.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        rows = con.execute(
            f"SELECT source_modality, source_kind, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE finding_status = 'present' "
            f"GROUP BY 1, 2 ORDER BY 1, 2"
        ).fetchall()
        info("present_findings_by_modality_x_kind",
             distribution=[{"modality": r[0], "kind": r[1], "n": int(r[2])}
                           for r in rows])

        rows = con.execute(
            f"SELECT onset_class, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE finding_status = 'present' "
            f"GROUP BY 1 ORDER BY 1"
        ).fetchall()
        info("onset_class_distribution",
             distribution={r[0]: int(r[1]) for r in rows})

        rows = con.execute(
            f"SELECT permanence_class, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE finding_status = 'present' "
            f"GROUP BY 1 ORDER BY 1"
        ).fetchall()
        info("permanence_class_distribution",
             distribution={r[0]: int(r[1]) for r in rows})

    # Informational — evidence_span_hash population rate (PHI-safe sanity).
    if table_exists(con, "main", "canonical_complications_events_v1"):
        n_with = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main', 'canonical_complications_events_v1')} "
            f"WHERE evidence_span_hash IS NOT NULL"
        ).fetchone()[0])
        info("evidence_span_hash_population",
             rows_with_hash=n_with, rows_total=n_ev)

    # Informational (CHANGE C) — per source_evidence_type breakdown by type.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        rows = con.execute(
            f"SELECT complication_type, source_evidence_type, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"GROUP BY 1, 2 ORDER BY 1, 3 DESC"
        ).fetchall()
        info("source_evidence_type_distribution",
             distribution=[
                 {"complication_type": r[0],
                  "source_evidence_type": r[1],
                  "n": int(r[2])} for r in rows])

    # Informational (CHANGE F) — per (type, evidence_strength) row +
    # distinct-patient counts. Distinct-patient counts are what feed the
    # rollup ever_*_<tier> columns (CHANGE G).
    if table_exists(con, "main", "canonical_complications_events_v1"):
        rows = con.execute(
            f"SELECT complication_type, evidence_strength, "
            f"  COUNT(*), COUNT(DISTINCT research_id) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE finding_status = 'present' "
            f"GROUP BY 1, 2 ORDER BY 1, 3 DESC"
        ).fetchall()
        info("evidence_strength_distribution_per_type",
             distribution=[
                 {"complication_type": r[0],
                  "evidence_strength": r[1],
                  "n_rows": int(r[2]),
                  "n_patients": int(r[3])} for r in rows])
        # Per-type tiered patient counts in the form that matches rollup
        # ever_*_<tier> aggregation. Surfaces "literature consistency"
        # check at a glance.
        log("  evidence_strength tiered patient counts per type:")
        for ct in COMPLICATION_TYPES:
            r = con.execute(
                f"SELECT "
                f"  COUNT(DISTINCT CASE WHEN evidence_strength = 'definitive' "
                f"    THEN research_id END), "
                f"  COUNT(DISTINCT CASE WHEN evidence_strength IN "
                f"    ('definitive','probable') THEN research_id END), "
                f"  COUNT(DISTINCT research_id) "
                f"FROM {fq('main', 'canonical_complications_events_v1')} "
                f"WHERE complication_type = '{ct}' "
                f"  AND finding_status = 'present'"
            ).fetchone()
            log(f"    {ct:25s} definitive={int(r[0] or 0):>5} "
                f"probable_or_better={int(r[1] or 0):>5} "
                f"any_evidence={int(r[2] or 0):>5}")

    # Informational (CHANGE H + I) — surface the gate drop counts, looked
    # up from the build's decision JSON (passed via archive_counts
    # piggyback path is not available here; we re-compute by looking at the
    # difference between source-derivable count and live count). The numbers
    # are also captured in the build step return value.

    # Informational (CHANGE A) — detection_date_inferred breakdown.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        rows = con.execute(
            f"SELECT detection_date_inferred, COUNT(*) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"GROUP BY 1 ORDER BY 1"
        ).fetchall()
        info("detection_date_inferred_distribution",
             distribution={str(r[0]): int(r[1]) for r in rows})

    # Informational (CHANGE E) — lab_value_at_detection summary.
    if table_exists(con, "main", "canonical_complications_events_v1"):
        row = con.execute(
            f"SELECT COUNT(*), MIN(lab_value_at_detection), "
            f"  MAX(lab_value_at_detection), AVG(lab_value_at_detection) "
            f"FROM {fq('main', 'canonical_complications_events_v1')} "
            f"WHERE lab_value_at_detection IS NOT NULL"
        ).fetchone()
        info("lab_value_at_detection_summary",
             n_rows=int(row[0] or 0),
             min_value=row[1], max_value=row[2], mean_value=row[3])

    # Informational (CHANGE D) — temporal classification per type.
    if table_exists(con, "main", "canonical_complications_patient_rollup_v1"):
        for ct in TEMPORAL_TYPES_FOR_ROLLUP:
            row = con.execute(f"""
                SELECT
                  SUM(CASE WHEN {ct}_preexisting THEN 1 ELSE 0 END),
                  SUM(CASE WHEN {ct}_new_postop THEN 1 ELSE 0 END),
                  SUM(CASE WHEN {ct}_transient THEN 1 ELSE 0 END),
                  SUM(CASE WHEN {ct}_permanent THEN 1 ELSE 0 END)
                FROM {fq('main', 'canonical_complications_patient_rollup_v1')}
            """).fetchone()
            info(f"{ct}_temporal_classification",
                 preexisting=int(row[0] or 0),
                 new_postop=int(row[1] or 0),
                 transient=int(row[2] or 0),
                 permanent=int(row[3] or 0))

    # Step 7 verification: drops happened (only when --phase 7 ran).
    if not pre_drop:
        for sch, tbl, _role in DEPRECATED_SOURCES:
            still = table_exists(con, sch, tbl)
            check(
                f"deprecated_table_dropped_{sch}_{tbl}",
                not still,
                still_present=still,
            )

    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str), encoding="utf-8")
    log(f"  QA report -> {QA_PATH.relative_to(REPO_ROOT)}")
    return qa


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_phases(spec: str | None) -> set[str]:
    if not spec:
        # Default phases: build + register + audit + QA. Drop is opt-in.
        return {"0", "1", "5", "6", "8", "9", "10"}
    return {s.strip() for s in spec.split(",") if s.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Complications consolidation (Script 364)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Run with writes enabled.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — archives, builds, registry mutations skipped.")
    parser.add_argument("--phase", default=None,
                        help="Comma-separated phases (default 0,1,5,6,8,9,10). "
                             "Use 7 to drop deprecated tables (post-CPM-repoint).")
    parser.add_argument("--skip-drop", action="store_true",
                        help="Force-remove phase 7 from the run set.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    phases = parse_phases(args.phase)
    if args.skip_drop:
        phases.discard("7")
    log(f"Run config: do_writes={do_writes}, phases={sorted(phases)}, "
        f"BUILD_TS={BUILD_TS}")

    SIDECAR_DIR.mkdir(parents=True, exist_ok=True)

    try:
        con = connect()
        archive_counts: dict[str, int] = {}
        results: dict[str, Any] = {
            "build_ts": BUILD_TS, "do_writes": do_writes,
            "phases": sorted(phases),
        }

        if "0" in phases:
            r = step_0_preflight(con, do_writes)
            archive_counts = r["pre_counts"]
            results["step_0"] = r
        else:
            for sch, tbl, _ in DEPRECATED_SOURCES:
                if table_exists(con, sch, tbl):
                    archive_counts[f"{sch}.{tbl}"] = row_count(con, sch, tbl)

        if "1" in phases:
            results["step_1"] = step_1_build_events(con, do_writes)
        if "5" in phases:
            results["step_5"] = step_5_build_rollup(con, do_writes)
        if "6" in phases:
            results["step_6"] = step_6_build_views(con, do_writes)

        ran_step_7 = False
        if "7" in phases and do_writes:
            results["step_7"] = step_7_drop_deprecated(
                con, do_writes, archive_counts
            )
            ran_step_7 = True
        elif "7" in phases:
            log("STEP 7 — dry-run skips DROP TABLE (writes disabled)")

        if "8" in phases:
            results["step_8"] = step_8_registry_sync(con, do_writes)
        if "9" in phases:
            results["step_9"] = step_9_cpm_feeder_audit(con)
        if "10" in phases:
            if not do_writes:
                log("STEP 10 — QA SKIPPED in dry-run (no tables to verify)")
                results["step_10"] = {"skipped_dry_run": True, "passed": True}
            else:
                results["step_10"] = step_10_qa(
                    con, archive_counts, pre_drop=not ran_step_7
                )
                if not results["step_10"]["passed"]:
                    log_error("ZERO-DRIFT QA failed — see qa file for details")
                    _write_decision(results)
                    flush_log()
                    return 2

        _write_decision(results)
        log(f"{SCRIPT_TAG} complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        flush_log()
        raise


def _write_decision(results: dict[str, Any]) -> None:
    """Persist a JSON decision record next to the log."""
    decision_path = OUTPUT_DIR / f"{SCRIPT_ID}_decision_{RUN_TS_COMPACT}.json"
    decision_path.write_text(json.dumps(results, indent=2, default=str))
    log(f"  decision log: {decision_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    sys.exit(main())
