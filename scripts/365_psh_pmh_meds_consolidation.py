#!/usr/bin/env python3
"""Script 365 — PSH + PMH/Problem List + Medications consolidation
(REMEDIATED v1_0_script365_remediated; CHANGES A-N + Logan's Phase-1 overrides).

Builds SIX patient-state canonicals across three domains:

    main.canonical_psh_events_v1                   (PSH events,    19 cols)
    main.canonical_psh_patient_rollup_v1           (PSH rollup,   ~28 cols)
    main.canonical_pmh_events_v1                   (PMH events,    19 cols)
    main.canonical_pmh_patient_rollup_v1           (PMH rollup,   ~79 cols)
    main.canonical_medications_events_v1           (Meds events,   19 cols)
    main.canonical_medications_patient_rollup_v1   (Meds rollup,  ~28 cols)

Sources (verified directly from MotherDuck 2026-04-22):
    main.note_entities_llm_past_surgical_hx       11,037 rows / 5,641 patients (LLM JSON)
    main.note_entities_llm_past_medical_hx        11,037 rows / 5,641 patients (LLM JSON)
    main.note_entities_problem_list               11,579 rows / 4,037 patients (entity legacy)
    main.note_entities_medications                 7,501 rows / 2,070 patients (entity legacy)

PSH events  = LLM PSH only.
PMH events  = LLM PMH UNION ALL note_entities_problem_list (multi-source preserved).
Meds events = note_entities_medications only (LLM medications source pending).

Background — REMEDIATION
========================
A previous v1_0_script365 build shipped without the Stage-3 dry-run sign-off
and with a lean rollup that could not feed CPM (LEFT JOIN FROM CPM was
missing; events were 10 cols; no temporal/evidence-strength/med-status
classifications). This script REPLACES that build with the full
CHANGES A-N spec and Logan's Phase-1 overrides applied.

CHANGES A-N IMPLEMENTED
-----------------------
A. Source attribution: source_table, source_row_id, mention_note_date,
   llm_confidence (NULL for legacy), extractor_name. Renamed
   source_modality → source_note_type (CHANGE I).
B. Negation-aware finding_status ladder (Pattern 16). SUSPECTED specifics
   evaluated BEFORE INDETERMINATE generics so '%cannot be ruled out%' is
   not eaten by '%cannot%'. CF-2 from Script 363.

   DOCUMENTED ZERO (per dry-run §4): both upstream extractors PRE-CLASSIFY
   into present/negated only at the present_or_negated layer. The ladder's
   evidence_text LIKE branches are subordinate to the EQ-first match path,
   which matches ~100% of source rows. Tier-1 CF: future upstream
   re-extraction with N-character context window may surface
   suspected/indeterminate values.

C. Temporal classification: days_from_first_thyroidectomy +
   is_preexisting + anchor_source.

   ANCHOR — HYBRID per Logan's Phase-1 override:
     anchor_date = COALESCE(strict_thyroidectomy_date, first_surgery_fallback)
     anchor_source ∈ {'strict', 'first_surgery_fallback', NULL}
   - strict_thyroidectomy_date = MIN(surgery_date_native) FROM
     canonical_operative_events_v1 WHERE procedure_normalized
     ILIKE '%thyroidect%' AND surgery_date_native IS NOT NULL
     (8,367 patients).
   - first_surgery_fallback = canonical_patient_master.first_surgery_date
     (recovers the 2,504 patients = 23.03% whose procedure_normalized was
     corrupted upstream — Tier-1 CF filed at
     docs/tier1_cf_procedure_normalized_corruption_20260422.md).
   - is_preexisting = NULL when anchor_date IS NULL (don't claim certainty
     where the anchor is missing).

D. Dedup-grain probe LOGGED before ROW_NUMBER (memory rule). CF-3 SAFE:
   chosen partition key (research_id, source_table, source_row_id,
   finding_text) where source_row_id = note_row_id:idx for LLM and
   note_row_id:source_line:evidence_start for legacy is functionally
   equivalent to the rich key with zero collisions on every source.

E. Evidence-strength tiers: definitive | probable | possible.
     definitive = legacy/structured source AND finding_value_norm IS NOT NULL
     probable   = LLM source AND finding_value_norm IS NOT NULL AND
                  ( evidence_text contains specificity marker
                    OR LENGTH(evidence_text) >= 80 )
     possible   = bare LLM mention OR unmapped legacy entry

   BY DESIGN — DO NOT TUNE WITHOUT 1.5 RETHINK (per Logan's Phase-1
   sign-off):
     * PSH ALWAYS rolls up to evidence_strength='probable'. PSH has no
       structured source that could reach 'definitive', and the LLM-PSH
       evidence_text is universally detailed enough to pass the
       length-≥80 specificity heuristic. Uniform 'probable' is structurally
       correct under CHANGE E's definition; it's not a bug.
     * Meds ALWAYS rolls up to evidence_strength='definitive'. Every row
       in note_entities_medications is a structured pharmacy-list
       extraction, which IS legitimately definitive (CHANGE E rule fires
       on 'legacy AND finding_value_norm IS NOT NULL'). Uniform
       'definitive' here is honest, not a catch-all.
   Phase 1.5 may revisit if cross-source dx-med reconciliation (e.g.,
   "diabetes+metformin → definitive diabetes") becomes useful.

F. Specificity gate for PMH symptoms: bare LLM mention without numeric
   clinical content (no HbA1c/mg/dL/mmHg/etc.) drops to 'possible'.

G. Med dedup by ingredient: finding_value_norm maps brand→generic
   (synthroid→levothyroxine etc.). Rollup uses n_distinct_findings_norm.
   Events keep one row per mention (evidence trail).

H. med_status: active | historical | unknown (meds-only).

   FIXED in Phase 1 per Logan's override: the v0 dry-run had 'active' as
   the catch-all default, producing 100% active / 0% historical / 0%
   unknown — implausible. Inverted defaults: 'unknown' is now the
   catch-all; 'historical' and 'active' BOTH require an explicit marker
   match in evidence_text. New QA gate `med_status_unknown_lt_90pct`
   asserts at least 10% of rows get classified to active or historical.

I. source_modality → source_note_type rename across all 3 events tables.

J. Cohort parity: rollup row count == canonical_patient_master row count
   (10,871). LEFT JOIN FROM CPM. Patients with no findings get NULL dates,
   0 counts, FALSE phenotype BOOLs.

K. Phenotype BOOL triads on rollup. PMH = 22 phenotypes × 3 tiers + 3
   plain BOOLs for smoking_status (Q-smoking=3 — categorical snapshot,
   tiers don't apply). PSH = 6 phenotypes × 3 tiers. Meds = 6 phenotypes
   × 3 tiers. Total phenotype cols = 105.

L. Tier-1 upstream re-extraction CF flagged but case definitions NOT
   loosened. Filed at docs/tier1_cf_procedure_normalized_corruption_*.md
   (NEW for the procedure_normalized corruption discovered during anchor
   probe) and the long-standing CF-1 from 364 (trigger-phrase context
   window). Future upstream re-extraction is recommended for both.

M. Phase 0 (separate prior commit): tier2.past_medical_hx_event_v1
   archived + dropped.

N. Phase 0 (separate prior commit): scripts/365_canonical_us_lymph_node_v2.py
   renamed to 364b_canonical_us_lymph_node_v2.py.

3-COMMIT REMEDIATION CASCADE (post-Phase 0):
    Commit A — this build (Phase 1):
        --commit                build 6 canonicals + 6 views + registry +
                                CPM audit + 29 hard QA gates
    Commit B — sibling script (Phase 2; awaits Logan sign-off):
        scripts/365_cpm_feeder_repoint.py --commit
    Commit C — phase-7 strip (Phase 3):
        --commit --phase 7      archive + DROP TABLE on the 2 legacy
                                entity-row sources only (LLM tables stay
                                live; Script 367 owns those)

CLI::

    python scripts/365_psh_pmh_meds_consolidation.py --dry-run
    python scripts/365_psh_pmh_meds_consolidation.py --commit
    python scripts/365_psh_pmh_meds_consolidation.py --commit --domain psh
    python scripts/365_psh_pmh_meds_consolidation.py --commit --phase 7

Auth: motherduck_client.get_token(). PHI rule: research_id only — never
log clinical text or evidence_span content. Evidence text is hashed via
SHA256 (64-char hex; DuckDB's sha256() returns hex VARCHAR — do NOT wrap
with HEX() or it double-encodes to 128 chars).
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

SCRIPT_ID = "365"
SCRIPT_TAG = f"Script {SCRIPT_ID}"
CANONICAL_VERSION = "v1_0_script365_remediated"
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
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_psh_pmh_meds.json"
CPM_AUDIT_PATH = REPO_ROOT / f"psh_pmh_meds_cpm_feeder_audit_{RUN_TS_COMPACT}.md"

DOMAINS: tuple[str, ...] = ("psh", "pmh", "meds")

# Per-domain source inventory (kind, table, expected_rows, expected_patients).
SOURCE_INVENTORY: dict[str, list[tuple[str, str, int, int]]] = {
    "psh":  [("llm",    "note_entities_llm_past_surgical_hx", 11_037, 5_641)],
    "pmh":  [("llm",    "note_entities_llm_past_medical_hx",  11_037, 5_641),
             ("legacy", "note_entities_problem_list",          11_579, 4_037)],
    "meds": [("legacy", "note_entities_medications",            7_501, 2_070)],
}

# Per-domain canonical table names.
CANONICAL_TABLES: dict[str, dict[str, str]] = {
    "psh": {
        "events": "canonical_psh_events_v1",
        "rollup": "canonical_psh_patient_rollup_v1",
        "events_view": "psh_events_VIEW_v1",
        "rollup_view": "psh_patient_rollup_VIEW_v1",
        "domain_label": "past_surgical_history",
    },
    "pmh": {
        "events": "canonical_pmh_events_v1",
        "rollup": "canonical_pmh_patient_rollup_v1",
        "events_view": "pmh_events_VIEW_v1",
        "rollup_view": "pmh_patient_rollup_VIEW_v1",
        "domain_label": "past_medical_history",
    },
    "meds": {
        "events": "canonical_medications_events_v1",
        "rollup": "canonical_medications_patient_rollup_v1",
        "events_view": "medications_events_VIEW_v1",
        "rollup_view": "medications_patient_rollup_VIEW_v1",
        "domain_label": "medications",
    },
}

# Tables archived + dropped at --phase 7 (Phase 3 of remediation cascade).
DEPRECATED_SOURCES: list[tuple[str, str, str]] = [
    ("main", "note_entities_problem_list",
     "consumed by canonical_pmh_events_v1"),
    ("main", "note_entities_medications",
     "consumed by canonical_medications_events_v1"),
]

# Required columns per source kind.
REQUIRED_COLUMNS_LLM: list[str] = [
    "research_id", "note_row_id", "note_type", "note_date", "result_json",
]
REQUIRED_COLUMNS_LEGACY: list[str] = [
    "research_id", "note_row_id", "note_type",
    "entity_value_norm", "entity_value_raw",
    "present_or_negated", "entity_date", "note_date",
    "source_line", "evidence_start", "evidence_span",
]

# CHANGE I — source_note_type lookup table (renamed from source_modality).
VALID_NOTE_TYPES: tuple[str, ...] = (
    "op_note", "h_p", "endocrine_note", "ed_note", "history_summary",
    "dc_sum", "other_history", "other_notes", "clinic_note",
    "progress_note", "office_note", "discharge_summary",
)

# Note types interpreted as PRE-OPERATIVE for CHANGE C. When a finding has
# NULL finding_date, it counts as preexisting only if it came from one of
# these note types AND the patient has a non-NULL anchor.
PREOP_NOTE_TYPES: tuple[str, ...] = (
    "h_p", "endocrine_note", "history_summary", "other_history",
    "other_notes", "clinic_note", "progress_note", "office_note",
)

# CHANGE B — finding_status ladder (Pattern 16). Order matters.
STATUS_LADDER: list[tuple[str, str, list[str]]] = [
    ("present", "eq",
     ["present", "positive", "yes", "active", "confirmed"]),
    ("absent", "eq",
     ["absent", "negated", "negative", "no", "denies", "denied",
      "ruled out", "ruled_out", "resolved"]),
    ("suspected", "like",
     ["%cannot be ruled out%", "%suspected%", "%possible%", "%likely%"]),
    ("indeterminate", "like",
     ["%cannot%", "%unclear%", "%unknown%", "%indeterminate%"]),
]
VALID_STATUSES = ("present", "absent", "suspected", "indeterminate")
VALID_EVIDENCE_STRENGTHS = ("definitive", "probable", "possible")
VALID_MED_STATUSES = ("active", "historical", "unknown")
VALID_ANCHOR_SOURCES = ("strict", "first_surgery_fallback")  # NULL also allowed

# CHANGE H — INVERTED med_status defaults per Logan's Phase-1 fix.
# 'unknown' is now the catch-all. Both 'historical' and 'active' require
# an explicit marker match in evidence_text. The dry-run's 100% active
# was caused by 'active' being the default branch.
MED_HISTORICAL_MARKERS: tuple[str, ...] = (
    # Past-tense / discontinuation
    "was on", "was taking", "previously took", "previously taking",
    "history of taking", "h/o taking", "h/o on ",
    "discontinued", "stopped taking", "stopped on", "no longer taking",
    "no longer on", "former use of", "past use of",
    "previously prescribed", "d/c'd", "d/c on ", "d/cd",
    # Past dates
    "(in the past)", "(previously)",
    "off ",   # 'patient is off lisinopril' (prefix-bounded by space)
)
MED_ACTIVE_MARKERS: tuple[str, ...] = (
    # Present-tense + dose context (most pharmacy lists carry dose).
    # Many of these only fire when the upstream evidence_span carries a
    # context window around the med name. note_entities_medications today
    # extracts JUST the med name (avg evidence_span length = 11.8 chars,
    # 100% of rows ≤30 chars), so the space-bounded markers below mostly
    # don't fire. The unbounded dose-unit markers further down DO fire
    # because they appear inside extracted dose annotations like
    # "synthroid 150mcg" or "RAI 150mCi".
    #
    # Tier-1 CF: future upstream re-extraction with an N-character context
    # window around the med name will let the present-tense + frequency
    # markers fire too, materially improving the active/unknown split.
    "currently taking", "currently on", "current dose", "current medication",
    "active medication", "continues on", "continuing", "continued",
    "presently taking", "presently on",
    " bid", " tid", " qid", " qd", " qhs", " prn",
    " daily", " twice daily", " three times daily", " once daily",
    " weekly", " every morning", " every evening",
    "tablet", "capsule",
    # Unbounded dose-unit markers (catches "150mcg" / "150mCi" / "100mg"
    # inside dose annotations). Common med names don't contain these
    # letter sequences (no live drug entity_value_norm in the cohort
    # contains 'mg', 'mcg', or 'mci' substrings).
    "mcg", "mci", "mg",
    # Other dose units (kept space-bounded — false-positive risk higher).
    " ml ", " ml/", " units ", " unit ",
)

# CHANGE F — phenotype-specificity markers. If a PMH-mapped LLM row has
# evidence_text containing one of these, it earns 'probable' (else
# 'possible' for bare LLM mentions).
PMH_SPECIFICITY_MARKERS: tuple[str, ...] = (
    "hba1c", "a1c", "icd",
    "mg/dl", "mmol/l", "mmhg", "mm hg",
    "%",  # numeric percentages e.g. "HbA1c 8.2%"
)

# Vocab map per domain (CF-4 + Phase-1 expansion). Keys lowercase trimmed.
FINDING_VALUE_MAPS: dict[str, dict[str, str]] = {
    "psh": {
        # appendectomy
        "appy": "appendectomy",
        "appendectomy": "appendectomy",
        "appendectomy nos": "appendectomy",
        "lap appendectomy": "appendectomy",
        "laparoscopic appendectomy": "appendectomy",
        # cholecystectomy
        "cholecystectomy": "cholecystectomy",
        "lap chole": "cholecystectomy",
        "lap cholecystectomy": "cholecystectomy",
        "laparoscopic cholecystectomy": "cholecystectomy",
        # hysterectomy
        "hysterectomy": "hysterectomy",
        "tah": "hysterectomy",
        "tah/bso": "hysterectomy",
        # tonsillectomy
        "tonsillectomy": "tonsillectomy",
        "t&a": "tonsillectomy_adenoidectomy",
        "tonsillectomy and adenoidectomy": "tonsillectomy_adenoidectomy",
        # cesarean
        "c-section": "cesarean_section",
        "csection": "cesarean_section",
        "cesarean": "cesarean_section",
        "cesarean section": "cesarean_section",
        # joints
        "thr": "total_hip_replacement",
        "total hip replacement": "total_hip_replacement",
        "tkr": "total_knee_replacement",
        "total knee replacement": "total_knee_replacement",
        # CABG
        "cabg": "coronary_artery_bypass_graft",
        "coronary artery bypass": "coronary_artery_bypass_graft",
        # CF-4 — thyroidectomy variants
        "total thyroidectomy": "total_thyroidectomy",
        "thyroidectomy": "thyroidectomy_unspecified",
        "left hemithyroidectomy": "left_hemithyroidectomy",
        "right hemithyroidectomy": "right_hemithyroidectomy",
        "left thyroid lobectomy": "left_hemithyroidectomy",
        "right thyroid lobectomy": "right_hemithyroidectomy",
        "partial thyroidectomy": "partial_thyroidectomy",
        "completion thyroidectomy": "completion_thyroidectomy",
        "subtotal thyroidectomy": "subtotal_thyroidectomy",
        # FNA / RAI
        "thyroid biopsy": "thyroid_biopsy",
        "fna": "fna",
        "fine needle aspiration": "fna",
        "radioactive iodine treatment": "rai_treatment",
        "rai": "rai_treatment",
        "i-131": "rai_treatment",
    },
    "pmh": {
        # diabetes
        "diabetes": "diabetes_mellitus",
        "diabetes mellitus": "diabetes_mellitus",
        "dm": "diabetes_mellitus",
        "type 2 diabetes": "diabetes_type_2",
        "t2dm": "diabetes_type_2",
        "dm2": "diabetes_type_2",
        "type ii diabetes": "diabetes_type_2",
        "diabetes_type2": "diabetes_type_2",
        "type 1 diabetes": "diabetes_type_1",
        "t1dm": "diabetes_type_1",
        "dm1": "diabetes_type_1",
        # hypertension
        "hypertension": "hypertension",
        "htn": "hypertension",
        "high blood pressure": "hypertension",
        # hyperlipidemia
        "hyperlipidemia": "hyperlipidemia",
        "hld": "hyperlipidemia",
        "dyslipidemia": "hyperlipidemia",
        # GERD
        "gerd": "gerd",
        "reflux": "gerd",
        "acid reflux": "gerd",
        # asthma / COPD
        "asthma": "asthma",
        "copd": "copd",
        # CAD
        "cad": "coronary_artery_disease",
        "coronary artery disease": "coronary_artery_disease",
        # afib
        "afib": "atrial_fibrillation",
        "atrial fibrillation": "atrial_fibrillation",
        "atrial_fibrillation": "atrial_fibrillation",
        "a-fib": "atrial_fibrillation",
        # CKD
        "ckd": "chronic_kidney_disease",
        "chronic kidney disease": "chronic_kidney_disease",
        # thyroid-related
        "hypothyroidism": "hypothyroidism",
        "hyperthyroidism": "hyperthyroidism",
        "graves disease": "graves_disease",
        "graves' disease": "graves_disease",
        "grave's disease": "graves_disease",
        "hashimoto's thyroiditis": "hashimoto_thyroiditis",
        "hashimoto thyroiditis": "hashimoto_thyroiditis",
        "hashimotos thyroiditis": "hashimoto_thyroiditis",
        # obesity
        "obesity": "obesity",
        "morbid obesity": "obesity",
        # psych
        "depression": "depression",
        # cancers
        "breast_cancer": "breast_cancer",
        "breast cancer": "breast_cancer",
        "lung_cancer": "lung_cancer",
        "lung cancer": "lung_cancer",
        # smoking statuses (raw → 3-value form)
        "smoking_status": "smoking_status",
        "current smoker": "smoking_current",
        "smoker": "smoking_current",
        "tobacco use": "smoking_current",
        "former smoker": "smoking_former",
        "ex-smoker": "smoking_former",
        "ex smoker": "smoking_former",
        "quit smoking": "smoking_former",
        "never smoker": "smoking_never",
        "non-smoker": "smoking_never",
        "nonsmoker": "smoking_never",
    },
    "meds": {
        # thyroid hormone (T4)
        "levothyroxine": "levothyroxine",
        "synthroid": "levothyroxine",
        "levoxyl": "levothyroxine",
        "tirosint": "levothyroxine",
        "unithroid": "levothyroxine",
        # thyroid hormone (T3) — Phase-1 vocab expansion
        "liothyronine": "liothyronine",
        "cytomel": "liothyronine",
        # antithyroid (Phase-1 vocab expansion)
        "methimazole": "methimazole",
        "tapazole": "methimazole",
        "mmi": "methimazole",
        "carbimazole": "methimazole",
        "ptu": "propylthiouracil",
        "propylthiouracil": "propylthiouracil",
        # statins
        "atorvastatin": "atorvastatin",
        "lipitor": "atorvastatin",
        "simvastatin": "simvastatin",
        "zocor": "simvastatin",
        "rosuvastatin": "rosuvastatin",
        "crestor": "rosuvastatin",
        "pravastatin": "pravastatin",
        # diabetes
        "metformin": "metformin",
        "glucophage": "metformin",
        "insulin": "insulin",
        # antihypertensives
        "lisinopril": "lisinopril",
        "amlodipine": "amlodipine",
        "norvasc": "amlodipine",
        "losartan": "losartan",
        "metoprolol": "metoprolol",
        "lopressor": "metoprolol",
        "toprol": "metoprolol",
        "hydrochlorothiazide": "hydrochlorothiazide",
        "hctz": "hydrochlorothiazide",
        # anticoagulants
        "warfarin": "warfarin",
        "coumadin": "warfarin",
        "apixaban": "apixaban",
        "eliquis": "apixaban",
        "rivaroxaban": "rivaroxaban",
        "xarelto": "rivaroxaban",
        # antiplatelet
        "aspirin": "aspirin",
        "asa": "aspirin",
        "clopidogrel": "clopidogrel",
        "plavix": "clopidogrel",
        # PPI
        "omeprazole": "omeprazole",
        "prilosec": "omeprazole",
        "pantoprazole": "pantoprazole",
        "protonix": "pantoprazole",
        # calcium / vit D
        "calcium carbonate": "calcium_carbonate",
        "tums": "calcium_carbonate",
        "calcium citrate": "calcium_citrate",
        "calcium_supplement": "calcium_supplement",
        "calcium supplement": "calcium_supplement",
        "calcitriol": "calcitriol",
        "rocaltrol": "calcitriol",
        "vitamin d": "vitamin_d",
        "cholecalciferol": "vitamin_d",
        "ergocalciferol": "vitamin_d",
        # CF-4 — RAI
        "rai_dose": "rai_dose",
        "rai dose": "rai_dose",
        "i-131 dose": "rai_dose",
    },
}

# CHANGE K — phenotype roster + Logan's Phase-1 vocab expansion.
PMH_PHENOTYPES: dict[str, dict[str, list[str]]] = {
    "diabetes": {
        "value_norms": ["diabetes_mellitus", "diabetes_type_1",
                        "diabetes_type_2", "diabetes"],
        "text_patterns": ["%diabet%"],
    },
    "hypertension": {
        "value_norms": ["hypertension"],
        "text_patterns": ["%hypertens%", "%htn%"],
    },
    "cad": {
        "value_norms": ["coronary_artery_disease", "cardiovascular"],
        "text_patterns": ["%coronary%artery%disease%",
                          "% cad %", "%cad,%", "%cad.%"],
    },
    "ckd": {
        "value_norms": ["chronic_kidney_disease"],
        "text_patterns": ["%chronic%kidney%", "% ckd %", "%ckd,%", "%ckd.%",
                          "%renal_disease%"],
    },
    "copd": {
        "value_norms": ["copd"],
        "text_patterns": ["%copd%", "%chronic%obstructive%"],
    },
    "depression": {
        "value_norms": ["depression"],
        "text_patterns": ["%depression%", "%depressive%"],
    },
    "afib": {
        "value_norms": ["atrial_fibrillation"],
        "text_patterns": ["%atrial%fibrillation%", "%afib%", "%a-fib%",
                          "%a fib%"],
    },
    "asthma": {
        "value_norms": ["asthma"],
        "text_patterns": ["%asthma%"],
    },
    "gerd": {
        "value_norms": ["gerd"],
        "text_patterns": ["%gerd%", "%reflux%"],
    },
    "obesity": {
        "value_norms": ["obesity"],
        "text_patterns": ["%obesity%", "%obese%"],
    },
    "osteoporosis": {
        "value_norms": ["osteoporosis"],
        "text_patterns": ["%osteoporos%"],
    },
    "hyperthyroidism": {
        "value_norms": ["hyperthyroidism"],
        "text_patterns": ["%hyperthyroid%"],
    },
    "hypothyroidism": {
        "value_norms": ["hypothyroidism"],
        "text_patterns": ["%hypothyroid%"],
    },
    "autoimmune_thyroid_hx": {
        "value_norms": ["graves_disease", "hashimoto_thyroiditis",
                        "autoimmune_thyroid"],
        "text_patterns": ["%graves%", "%hashimoto%", "%autoimmune%thyroid%"],
    },
    "breast_cancer": {
        "value_norms": ["breast_cancer"],
        "text_patterns": ["%breast%cancer%", "%breast%carcinoma%"],
    },
    "lung_cancer": {
        "value_norms": ["lung_cancer"],
        "text_patterns": ["%lung%cancer%", "%lung%carcinoma%"],
    },
    "radiation_exposure": {
        "value_norms": ["radiation_exposure"],
        "text_patterns": ["%radiation%exposure%", "%radiation%treatment%",
                          "%childhood%radiation%", "%head%neck%radiation%"],
    },
    "prior_cancer_hx": {
        "value_norms": ["prior_cancer", "breast_cancer", "lung_cancer"],
        "text_patterns": ["%prior%cancer%", "%history%of%cancer%",
                          "%cancer%history%"],
    },
    "coagulopathy": {
        "value_norms": ["coagulopathy"],
        "text_patterns": ["%coagulopath%", "%bleeding%disorder%",
                          "%clotting%disorder%"],
    },
    "family_hx_cancer": {
        "value_norms": ["family_hx_cancer"],
        "text_patterns": ["%family%history%of%cancer%",
                          "%family%hx%cancer%", "%fhx%cancer%"],
    },
    "family_hx_thyroid": {
        "value_norms": ["family_hx_thyroid"],
        "text_patterns": ["%family%history%of%thyroid%",
                          "%family%hx%thyroid%", "%fhx%thyroid%"],
    },
    "men_syndrome": {
        "value_norms": ["men_syndrome"],
        "text_patterns": ["%men%syndrome%", "%multiple%endocrine%neoplasia%",
                          "%men 1%", "%men 2%", "%men2a%", "%men2b%"],
    },
}
# CHANGE K + Q-smoking=3 + Phase-1 vocab expansion: 3 plain BOOLs, no tiers.
PMH_SMOKING_STATUSES: dict[str, dict[str, list[str]]] = {
    "current": {
        "value_norms": ["smoking_current"],
        "text_patterns": ["%current%smoker%", "%active%smoker%",
                          "% smoker%", "%tobacco%use%", "%cigarette%",
                          "%pack%year%", "%pack-year%"],
    },
    "former": {
        "value_norms": ["smoking_former"],
        "text_patterns": ["%former%smoker%", "%ex-smoker%", "%ex smoker%",
                          "%quit%smoking%", "%previously%smoked%",
                          "%history%of%smoking%"],
    },
    "never": {
        "value_norms": ["smoking_never"],
        "text_patterns": ["%never%smoker%", "%non-smoker%", "%nonsmoker%",
                          "%no%smoking%history%", "%denies%smoking%"],
    },
}
PSH_PHENOTYPES: dict[str, dict[str, list[str]]] = {
    "prior_thyroidectomy": {
        "value_norms": ["total_thyroidectomy", "thyroidectomy_unspecified",
                        "left_hemithyroidectomy", "right_hemithyroidectomy",
                        "partial_thyroidectomy", "completion_thyroidectomy",
                        "subtotal_thyroidectomy", "prior_thyroidectomy"],
        "text_patterns": ["%thyroidect%", "%thyroid%lobect%",
                          "%hemithyroidect%"],
    },
    "prior_neck_surgery": {
        "value_norms": ["prior_neck_surgery"],
        "text_patterns": ["%neck%surgery%", "%neck%dissect%",
                          "%neck%operation%"],
    },
    "prior_parathyroidectomy": {
        "value_norms": ["prior_parathyroidectomy"],
        "text_patterns": ["%parathyroidect%"],
    },
    "prior_rai": {
        "value_norms": ["rai_treatment", "prior_rai"],
        "text_patterns": ["%radioactive%iodine%", "%rai%treatment%",
                          "%i-131%", "% rai %"],
    },
    "prior_fna": {
        "value_norms": ["fna", "prior_fna"],
        "text_patterns": ["% fna%", "%fine%needle%aspirat%",
                          "%thyroid%biopsy%"],
    },
    "prior_neck_dissection": {
        "value_norms": ["prior_neck_dissection"],
        "text_patterns": ["%neck%dissect%", "%lymph%node%dissect%"],
    },
}
MEDS_PHENOTYPES: dict[str, dict[str, list[str]]] = {
    "levothyroxine": {
        "value_norms": ["levothyroxine"],
        "text_patterns": ["%levothyrox%", "%synthroid%", "%levoxyl%",
                          "%tirosint%"],
    },
    "calcium_supplement": {
        "value_norms": ["calcium_carbonate", "calcium_citrate",
                        "calcium_supplement"],
        "text_patterns": ["%calcium%supplement%", "%calcium%carbonate%",
                          "%calcium%citrate%", "% tums %"],
    },
    "calcitriol": {
        "value_norms": ["calcitriol"],
        "text_patterns": ["%calcitriol%", "%rocaltrol%"],
    },
    "rai_dose": {
        "value_norms": ["rai_dose"],
        "text_patterns": ["%rai%dose%", "%i-131%dose%", "%millicur%",
                          "%mci%"],
    },
    # Phase-1 vocab expansion: add MMI and carbimazole brand/abbrev variants.
    "methimazole_or_ptu": {
        "value_norms": ["methimazole", "propylthiouracil"],
        "text_patterns": ["%methimazole%", "%tapazole%", "%propylthiouracil%",
                          "% ptu %", "% mmi %", "%carbimazole%"],
    },
    # Phase-1 vocab expansion: T3 word-bounded so it doesn't collide with
    # tumor T-stage (T3 in TNM staging). DuckDB regex via REGEXP_MATCHES is
    # used in the phenotype-match SQL when a 'regex' key is present.
    "liothyronine": {
        "value_norms": ["liothyronine"],
        "text_patterns": ["%liothyronine%", "%cytomel%"],
        "regex_patterns": [r"(^|\W)t3(\W|$)"],   # word-bounded T3
    },
}

DOMAIN_PHENOTYPES: dict[str, dict[str, dict[str, list[str]]]] = {
    "psh": PSH_PHENOTYPES,
    "pmh": PMH_PHENOTYPES,
    "meds": MEDS_PHENOTYPES,
}

# CPM keyword buckets for the audit (Step 5).
CPM_KEYWORD_BUCKETS: dict[str, list[str]] = {
    "psh": [
        "pshx_nlp_", "pshx_llm_", "prior_thyroidectomy",
        "prior_neck_surgery", "prior_neck_dissection", "prior_parathyroid",
        "prior_rai", "prior_fna", "prior_cancer_hx", "ops_prior_neck",
    ],
    "pmh": [
        "pmhx_nlp_", "pmhx_llm_", "nlp_pmhx_",
        "syn_graves", "syn_hashimoto",
    ],
    "meds": [
        "med_nlp_", "nlp_ne_medications", "ops_anticoagulation_meds",
    ],
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
            f"No MotherDuck RW token (token_mode={token_mode()})."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    con.execute(f'USE "{CANONICAL_DB}".main')
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [CANONICAL_DB, schema, table],
    ).fetchone()
    return row is not None


def view_exists(con: duckdb.DuckDBPyConnection, schema: str, view: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "AND table_type = 'VIEW'",
        [CANONICAL_DB, schema, view],
    ).fetchone()
    return row is not None


def list_columns(con: duckdb.DuckDBPyConnection, schema: str,
                 table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [CANONICAL_DB, schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def column_exists(con: duckdb.DuckDBPyConnection, schema: str,
                  table: str, column: str) -> bool:
    return column in list_columns(con, schema, table)


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> int:
    return int(con.execute(
        f"SELECT COUNT(*) FROM {fq(schema, table)}"
    ).fetchone()[0])


def distinct_research_ids(con: duckdb.DuckDBPyConnection, schema: str,
                          table: str) -> int:
    if not column_exists(con, schema, table, "research_id"):
        return -1
    return int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {fq(schema, table)}"
    ).fetchone()[0])


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


# ---------------------------------------------------------------------------
# SQL fragment generators
# ---------------------------------------------------------------------------

def _finding_value_case_sql(domain: str, input_expr: str) -> str:
    mapping = FINDING_VALUE_MAPS[domain]
    if not mapping:
        return "NULL"
    parts = [f"CASE LOWER(TRIM(CAST({input_expr} AS VARCHAR)))"]
    for raw, canon in mapping.items():
        parts.append(f"  WHEN '{sql_escape(raw)}' THEN '{sql_escape(canon)}'")
    parts.append("  ELSE NULL")
    parts.append("END")
    return "\n        ".join(parts)


def _status_ladder_case_sql(present_or_negated_expr: str,
                             evidence_text_expr: str) -> str:
    """CHANGE B Pattern-16 ladder. Order matters (SUSPECTED before INDETERMINATE)."""
    parts = ["CASE"]
    pn_norm = f"LOWER(TRIM(CAST({present_or_negated_expr} AS VARCHAR)))"
    et_norm = (f"LOWER(COALESCE(CAST({evidence_text_expr} AS VARCHAR), "
               f"CAST({present_or_negated_expr} AS VARCHAR), ''))")
    for canon, kind, patterns in STATUS_LADDER:
        for pat in patterns:
            if kind == "eq":
                parts.append(f"  WHEN {pn_norm} = '{sql_escape(pat)}' "
                             f"THEN '{canon}'")
            else:
                parts.append(f"  WHEN {pn_norm} LIKE '{sql_escape(pat)}' "
                             f"THEN '{canon}'")
                parts.append(f"  WHEN {et_norm} LIKE '{sql_escape(pat)}' "
                             f"THEN '{canon}'")
    parts.append("  ELSE 'indeterminate'")
    parts.append("END")
    return "\n        ".join(parts)


def _evidence_strength_case_sql() -> str:
    """CHANGE E + F. By design: PSH→uniform 'probable'; Meds→uniform 'definitive'.
    See script header for the documented rationale."""
    spec_or = " OR ".join(
        f"LOWER(COALESCE(evidence_text_for_hash, '')) "
        f"LIKE '%{sql_escape(m.lower())}%'"
        for m in PMH_SPECIFICITY_MARKERS
    )
    return f"""
    CASE
        WHEN source_kind = 'legacy' AND finding_value_norm IS NOT NULL
            THEN 'definitive'
        WHEN source_kind = 'llm' AND finding_value_norm IS NOT NULL AND (
                ({spec_or})
                OR LENGTH(COALESCE(evidence_text_for_hash, '')) >= 80
             )
            THEN 'probable'
        WHEN source_kind = 'legacy' AND finding_value_norm IS NULL
            THEN 'possible'
        ELSE 'possible'
    END"""


def _med_status_case_sql() -> str:
    """CHANGE H — INVERTED defaults per Logan's Phase-1 fix.
    Default = 'unknown'; both 'historical' and 'active' require explicit
    marker matches in evidence_text. NULL for non-meds rows.

    Order: historical CHECKED FIRST so 'previously took 100mg' isn't mapped
    to active just because '100 mg ' fires the active marker.
    """
    hist_or = " OR ".join(
        f"LOWER(COALESCE(evidence_text_for_hash, '')) "
        f"LIKE '%{sql_escape(m.lower())}%'"
        for m in MED_HISTORICAL_MARKERS
    )
    active_or = " OR ".join(
        f"LOWER(COALESCE(evidence_text_for_hash, '')) "
        f"LIKE '%{sql_escape(m.lower())}%'"
        for m in MED_ACTIVE_MARKERS
    )
    return f"""
    CASE
        WHEN source_table != 'note_entities_medications' THEN NULL
        WHEN ({hist_or}) THEN 'historical'
        WHEN ({active_or}) THEN 'active'
        ELSE 'unknown'
    END"""


def _is_preexisting_case_sql() -> str:
    """CHANGE C is_preexisting. NULL when anchor_date IS NULL
    (don't claim certainty)."""
    preop_in = ", ".join(f"'{nt}'" for nt in PREOP_NOTE_TYPES)
    return f"""
    CASE
        WHEN anchor_date IS NULL THEN NULL
        WHEN finding_date IS NOT NULL
            THEN finding_date < anchor_date
        WHEN finding_date IS NULL AND source_note_type IN ({preop_in})
            THEN TRUE
        ELSE FALSE
    END"""


# ---------------------------------------------------------------------------
# Source CTE builders
# ---------------------------------------------------------------------------

def _llm_source_cte(table: str, domain: str) -> str:
    finding_value_case = _finding_value_case_sql(
        domain, "json_extract_string(e_json, '$.entity_value')"
    )
    finding_value_norm_case = f"""
        COALESCE(
            ({finding_value_case}),
            LOWER(NULLIF(TRIM(json_extract_string(e_json, '$.entity_type')), ''))
        )"""
    return f"""
_llm_{domain}_src AS (
    SELECT
        CAST(p.research_id AS VARCHAR)                              AS research_id,
        '{table}'                                                   AS source_table,
        'llm'                                                       AS source_kind,
        (CAST(p.note_row_id AS VARCHAR) || ':' || CAST(idx AS VARCHAR))
                                                                    AS source_row_id,
        CAST(p.note_type AS VARCHAR)                                AS source_note_type,
        TRY_CAST(json_extract_string(e_json, '$.confidence') AS DOUBLE)
                                                                    AS llm_confidence,
        '{table}'                                                   AS extractor_name,
        json_extract_string(e_json, '$.entity_value')               AS finding_text,
        ({finding_value_case})                                      AS finding_value,
        ({finding_value_norm_case})                                 AS finding_value_norm,
        TRY_CAST(json_extract_string(e_json, '$.entity_date') AS DATE)
                                                                    AS finding_date,
        TRY_CAST(p.note_date AS DATE)                               AS mention_note_date,
        json_extract_string(e_json, '$.present_or_negated')         AS present_or_negated_raw,
        json_extract_string(e_json, '$.evidence_text')              AS evidence_text_for_hash
    FROM main."{table}" p,
         UNNEST(json_extract(p.result_json, '$.entities')::JSON[])
             WITH ORDINALITY AS t(e_json, idx)
    WHERE p.result_json LIKE '{{"entities":%'
      AND p.result_json NOT LIKE '{{"entities": []}}'
      AND json_extract_string(e_json, '$.entity_value') IS NOT NULL
)"""


def _legacy_source_cte(table: str, domain: str) -> str:
    finding_value_case = _finding_value_case_sql(
        domain, "COALESCE(entity_value_norm, entity_value_raw)"
    )
    finding_value_norm_case = f"""
        COALESCE(
            ({finding_value_case}),
            LOWER(NULLIF(TRIM(entity_value_norm), '')),
            LOWER(NULLIF(TRIM(entity_value_raw), ''))
        )"""
    return f"""
_legacy_{domain}_src AS (
    SELECT
        CAST(research_id AS VARCHAR)                                AS research_id,
        '{table}'                                                   AS source_table,
        'legacy'                                                    AS source_kind,
        (CAST(note_row_id AS VARCHAR) || ':'
         || CAST(COALESCE(source_line, -1) AS VARCHAR) || ':'
         || CAST(COALESCE(evidence_start, -1) AS VARCHAR))           AS source_row_id,
        CAST(note_type AS VARCHAR)                                  AS source_note_type,
        CAST(NULL AS DOUBLE)                                        AS llm_confidence,
        '{table}'                                                   AS extractor_name,
        COALESCE(entity_value_norm, entity_value_raw)               AS finding_text,
        ({finding_value_case})                                      AS finding_value,
        ({finding_value_norm_case})                                 AS finding_value_norm,
        TRY_CAST(entity_date AS DATE)                               AS finding_date,
        TRY_CAST(note_date AS DATE)                                 AS mention_note_date,
        present_or_negated                                          AS present_or_negated_raw,
        evidence_span                                               AS evidence_text_for_hash
    FROM main."{table}"
    WHERE COALESCE(entity_value_norm, entity_value_raw) IS NOT NULL
)"""


def _build_events_sql_for_domain(domain: str) -> str:
    """CREATE OR REPLACE TABLE main.canonical_<domain>_events_v1.

    Final shape (19 cols):
        research_id, source_table, source_row_id, source_note_type,
        llm_confidence, extractor_name,
        finding_text, finding_value, finding_value_norm,
        finding_date, mention_note_date,
        finding_status, evidence_strength,
        days_from_first_thyroidectomy, is_preexisting, anchor_source,
        med_status, evidence_span_hash, build_ts
    """
    target = fq("main", CANONICAL_TABLES[domain]["events"])
    ctes: list[str] = []
    union_parts: list[str] = []
    for kind, tbl, _, _ in SOURCE_INVENTORY[domain]:
        if kind == "llm":
            ctes.append(_llm_source_cte(tbl, domain))
            union_parts.append(f"SELECT * FROM _llm_{domain}_src")
        else:
            ctes.append(_legacy_source_cte(tbl, domain))
            union_parts.append(f"SELECT * FROM _legacy_{domain}_src")
    cte_block = ",\n".join(ctes)
    union_block = "\n    UNION ALL\n    ".join(union_parts)
    status_case = _status_ladder_case_sql(
        "present_or_negated_raw", "evidence_text_for_hash"
    )
    evidence_strength_case = _evidence_strength_case_sql()
    med_status_case = _med_status_case_sql()
    is_preexisting_case = _is_preexisting_case_sql()
    return f"""
CREATE OR REPLACE TABLE {target} AS
WITH
-- CHANGE C — HYBRID anchor (Logan Phase-1 override).
-- Strict anchor: thyroidectomy-only date from canonical_operative_events_v1.
strict_anchor AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(CAST(surgery_date_native AS DATE)) AS strict_thyroidectomy_date
    FROM main.canonical_operative_events_v1
    WHERE procedure_normalized ILIKE '%thyroidect%'
      AND surgery_date_native IS NOT NULL
    GROUP BY research_id
),
-- Fallback: first surgery of any kind from CPM. Recovers ~23.03% of
-- patients whose procedure_normalized was corrupted upstream
-- (Tier-1 CF: docs/tier1_cf_procedure_normalized_corruption_20260422.md).
cpm_fallback AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        TRY_CAST(first_surgery_date AS DATE) AS first_surgery_date_fallback
    FROM main.canonical_patient_master
    WHERE first_surgery_date IS NOT NULL
),
hybrid_anchor AS (
    SELECT
        COALESCE(s.research_id, f.research_id) AS research_id,
        COALESCE(s.strict_thyroidectomy_date,
                 f.first_surgery_date_fallback) AS anchor_date,
        CASE
            WHEN s.strict_thyroidectomy_date IS NOT NULL THEN 'strict'
            WHEN f.first_surgery_date_fallback IS NOT NULL
                THEN 'first_surgery_fallback'
            ELSE NULL
        END AS anchor_source
    FROM strict_anchor s
    FULL OUTER JOIN cpm_fallback f USING (research_id)
),
{cte_block},
unioned AS (
    {union_block}
),
mapped AS (
    SELECT
        u.*,
        ha.anchor_date,
        ha.anchor_source,
        ({status_case})                                             AS finding_status
    FROM unioned u
    LEFT JOIN hybrid_anchor ha USING (research_id)
),
strength AS (
    SELECT
        m.*,
        ({evidence_strength_case})                                  AS evidence_strength,
        ({med_status_case})                                         AS med_status,
        CASE
            WHEN finding_date IS NOT NULL AND anchor_date IS NOT NULL
                THEN DATE_DIFF('day', anchor_date, finding_date)
            ELSE NULL
        END                                                         AS days_from_first_thyroidectomy,
        ({is_preexisting_case})                                     AS is_preexisting
    FROM mapped m
),
deduped AS (
    -- CF-3 SAFE chosen partition key (probed in step_dedup_grain_probe).
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id, source_table, source_row_id,
                            finding_text
               ORDER BY finding_date DESC NULLS LAST,
                        finding_status,
                        finding_value_norm NULLS LAST
           ) AS _rn
    FROM strength
)
SELECT
    research_id,
    source_table,
    source_row_id,
    source_note_type,
    llm_confidence,
    extractor_name,
    finding_text,
    finding_value,
    finding_value_norm,
    finding_date,
    mention_note_date,
    finding_status,
    evidence_strength,
    days_from_first_thyroidectomy,
    is_preexisting,
    anchor_source,
    med_status,
    -- DuckDB sha256() returns 64-char hex VARCHAR; do NOT wrap with HEX().
    CASE
        WHEN evidence_text_for_hash IS NULL OR evidence_text_for_hash = ''
            THEN NULL
        ELSE LOWER(SHA256(evidence_text_for_hash))
    END                                                             AS evidence_span_hash,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                            AS build_ts
FROM deduped
WHERE _rn = 1
"""


# ---------------------------------------------------------------------------
# Rollup builder (CHANGE J — LEFT JOIN FROM CPM = 10,871 rows always)
# ---------------------------------------------------------------------------

def _phenotype_match_sql(spec: dict[str, list[str]]) -> str:
    """SQL boolean expression that fires when an events row matches the
    phenotype. value_norms (preferred) + text_patterns (fallback) +
    optional regex_patterns (CHANGE-K Phase-1 expansion for word-bounded
    tokens like T3)."""
    parts: list[str] = []
    if spec.get("value_norms"):
        norm_csv = ", ".join(f"'{sql_escape(v)}'" for v in spec["value_norms"])
        parts.append(f"finding_value_norm IN ({norm_csv})")
    for pat in spec.get("text_patterns", []):
        parts.append(f"LOWER(finding_text) LIKE '{sql_escape(pat.lower())}'")
    for rx in spec.get("regex_patterns", []):
        parts.append(
            f"REGEXP_MATCHES(LOWER(finding_text), '{sql_escape(rx)}')"
        )
    if not parts:
        return "FALSE"
    return "(" + " OR ".join(parts) + ")"


def _build_rollup_sql_for_domain(domain: str) -> str:
    """CREATE OR REPLACE TABLE main.canonical_<domain>_patient_rollup_v1.

    LEFT JOIN FROM canonical_patient_master = exactly 10,871 rows.
    Phenotype BOOL triads built per CHANGE K. anchor_source carried for
    cohort filtering (per Logan's Phase-1 override).
    """
    events_fq = fq("main", CANONICAL_TABLES[domain]["events"])
    rollup_fq = fq("main", CANONICAL_TABLES[domain]["rollup"])
    phenos = DOMAIN_PHENOTYPES[domain]
    triad_cols: list[str] = []
    for ph_name, ph_spec in phenos.items():
        match_expr = _phenotype_match_sql(ph_spec)
        for tier_name, tier_filter in [
            ("definitive",
             "evidence_strength = 'definitive'"),
            ("probable_or_better",
             "evidence_strength IN ('definitive','probable')"),
            ("any_evidence",
             "evidence_strength IN ('definitive','probable','possible')"),
        ]:
            col = f"{domain}_{ph_name}_{tier_name}"
            triad_cols.append(
                f"COALESCE(BOOL_OR({match_expr} AND finding_status='present' "
                f"AND {tier_filter}), FALSE) AS {col}"
            )
    smoking_cols: list[str] = []
    if domain == "pmh":
        for status, spec in PMH_SMOKING_STATUSES.items():
            match_expr = _phenotype_match_sql(spec)
            col = f"pmh_smoking_status_{status}"
            smoking_cols.append(
                f"COALESCE(BOOL_OR({match_expr} "
                f"AND finding_status='present'), FALSE) AS {col}"
            )

    phenotype_block = (",\n    " + ",\n    ".join(triad_cols + smoking_cols)
                       if (triad_cols or smoking_cols) else "")

    # SELECT-side phenotype col reference list.
    phenotype_select = ""
    if triad_cols or smoking_cols:
        col_names = [c.split(" AS ")[-1].strip()
                     for c in triad_cols + smoking_cols]
        phenotype_select = ",\n    " + ",\n    ".join(
            f"COALESCE(agg.{c}, FALSE) AS {c}" for c in col_names
        )

    return f"""
CREATE OR REPLACE TABLE {rollup_fq} AS
WITH ev AS (
    SELECT
        research_id, finding_status, finding_text, finding_value_norm,
        finding_date, evidence_strength, anchor_source
    FROM {events_fq}
),
agg AS (
    SELECT
        research_id,
        ANY_VALUE(anchor_source) AS anchor_source,
        SUM(CASE WHEN finding_status IN
             ('present','suspected','indeterminate','absent')
             THEN 1 ELSE 0 END)                          AS n_findings_any,
        SUM(CASE WHEN finding_status = 'present' THEN 1 ELSE 0 END)
                                                         AS n_findings_present,
        SUM(CASE WHEN finding_status = 'present'
                  AND evidence_strength = 'definitive' THEN 1 ELSE 0 END)
                                                         AS n_findings_definitive,
        SUM(CASE WHEN finding_status = 'present'
                  AND evidence_strength IN ('definitive','probable')
                  THEN 1 ELSE 0 END)
                                                         AS n_findings_probable_or_better,
        MIN(CASE WHEN finding_status = 'present' THEN finding_date END)
                                                         AS first_finding_date,
        MAX(CASE WHEN finding_status = 'present' THEN finding_date END)
                                                         AS last_finding_date,
        COUNT(DISTINCT CASE WHEN finding_status = 'present'
                              THEN finding_value_norm END)
                                                         AS n_distinct_findings_norm{phenotype_block}
    FROM ev
    GROUP BY research_id
),
-- Hybrid anchor source per patient (NULL when no surgery info at all).
hybrid_anchor_per_pt AS (
    SELECT
        CAST(cpm.research_id AS VARCHAR) AS research_id,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM main.canonical_operative_events_v1 o
                WHERE CAST(o.research_id AS VARCHAR)
                      = CAST(cpm.research_id AS VARCHAR)
                  AND o.procedure_normalized ILIKE '%thyroidect%'
                  AND o.surgery_date_native IS NOT NULL
            ) THEN 'strict'
            WHEN cpm.first_surgery_date IS NOT NULL
                THEN 'first_surgery_fallback'
            ELSE NULL
        END AS anchor_source
    FROM main.canonical_patient_master cpm
)
SELECT
    cpm.research_id,
    -- anchor_source from per-patient lookup (not from agg, so it's
    -- populated for patients with NO findings too).
    ha.anchor_source                                     AS anchor_source,
    COALESCE(agg.n_findings_any, 0)                      AS n_findings_any,
    COALESCE(agg.n_findings_present, 0)                  AS n_findings_present,
    COALESCE(agg.n_findings_definitive, 0)               AS n_findings_definitive,
    COALESCE(agg.n_findings_probable_or_better, 0)       AS n_findings_probable_or_better,
    agg.first_finding_date                               AS first_finding_date,
    agg.last_finding_date                                AS last_finding_date,
    COALESCE(agg.n_distinct_findings_norm, 0)            AS n_distinct_findings_norm{phenotype_select},
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)                 AS build_ts
FROM main.canonical_patient_master cpm
LEFT JOIN agg ON CAST(cpm.research_id AS VARCHAR) = agg.research_id
LEFT JOIN hybrid_anchor_per_pt ha ON CAST(cpm.research_id AS VARCHAR)
                                       = ha.research_id
"""


# ---------------------------------------------------------------------------
# Step 0 — Pre-flight (sources + CPM + anchor probe + dedup-grain probe)
# ---------------------------------------------------------------------------

def step_0_preflight(
    con: duckdb.DuckDBPyConnection, do_writes: bool, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight & archive (BUILD_TS={BUILD_TS}, "
        f"domains={list(domains)})")
    log("=" * 78)

    inventory: list[dict[str, Any]] = []
    archive_counts: dict[str, int] = {}
    seen_tables: set[str] = set()
    for d in domains:
        for kind, tbl, exp_rows, exp_pts in SOURCE_INVENTORY[d]:
            if tbl in seen_tables:
                continue
            seen_tables.add(tbl)
            if not table_exists(con, "main", tbl):
                raise RuntimeError(
                    f"Source main.{tbl} missing — refusing to build."
                )
            n = row_count(con, "main", tbl)
            p = distinct_research_ids(con, "main", tbl)
            archive_counts[f"main.{tbl}"] = n
            delta = n - exp_rows
            rel = abs(delta) / exp_rows if exp_rows else 0.0
            log(f"  inventory: main.{tbl} ({kind}): rows={n:,} pts={p:,} "
                f"(exp_rows={exp_rows:,}, drift={delta:+,}, rel={rel:.2%})")
            if rel > 0.02:
                log_warn(f"    row count drifted >2% — verify upstream")
            inventory.append({
                "domain": d, "kind": kind, "schema": "main", "table": tbl,
                "rows": n, "patients": p,
                "expected_rows": exp_rows, "row_delta": delta,
            })

    log("")
    log("  required-column existence pre-flight:")
    misses: dict[str, list[str]] = {}
    for d in domains:
        for kind, tbl, _, _ in SOURCE_INVENTORY[d]:
            required = (REQUIRED_COLUMNS_LLM if kind == "llm"
                        else REQUIRED_COLUMNS_LEGACY)
            present = set(list_columns(con, "main", tbl))
            missing = [c for c in required if c not in present]
            if missing:
                misses[tbl] = missing
                log_error(f"    {tbl}: MISSING {missing}")
            else:
                log(f"    {tbl}: all {len(required)} required cols present ✓")
    if misses:
        raise RuntimeError(f"Required-column pre-flight failed: {misses}.")

    # CPM + anchor probe (CHANGE C HYBRID).
    log("")
    log("  CPM cohort + anchor probe (CHANGE C hybrid):")
    n_cpm = row_count(con, "main", "canonical_patient_master")
    log(f"    canonical_patient_master rows: {n_cpm:,}")
    n_strict = int(con.execute("""
        SELECT COUNT(DISTINCT research_id)
        FROM main.canonical_operative_events_v1
        WHERE procedure_normalized ILIKE '%thyroidect%'
          AND surgery_date_native IS NOT NULL
    """).fetchone()[0])
    n_fallback = int(con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        WHERE NOT EXISTS (
            SELECT 1 FROM main.canonical_operative_events_v1 op
            WHERE op.research_id = cpm.research_id
              AND op.procedure_normalized ILIKE '%thyroidect%'
              AND op.surgery_date_native IS NOT NULL
        ) AND first_surgery_date IS NOT NULL
    """).fetchone()[0])
    n_null_anchor = n_cpm - n_strict - n_fallback
    log(f"    anchor=strict:                 {n_strict:>6,} "
        f"({100*n_strict/n_cpm:.2f}%)")
    log(f"    anchor=first_surgery_fallback: {n_fallback:>6,} "
        f"({100*n_fallback/n_cpm:.2f}%)")
    log(f"    anchor=NULL (no surgery info): {n_null_anchor:>6,} "
        f"({100*n_null_anchor/n_cpm:.2f}%)")

    # CHANGE D dedup-grain probe (log only).
    log("")
    log("  CHANGE D dedup-grain probe (log only):")
    dedup_probe: dict[str, dict[str, int]] = {}
    for d in domains:
        for kind, tbl, _, _ in SOURCE_INVENTORY[d]:
            if kind == "llm":
                sql = f"""
                    WITH ents AS (
                        SELECT
                            CAST(p.research_id AS VARCHAR) AS rid,
                            p.note_row_id, idx,
                            json_extract_string(e_json, '$.entity_value') AS ev
                        FROM main."{tbl}" p,
                             UNNEST(json_extract(p.result_json, '$.entities')::JSON[])
                                 WITH ORDINALITY AS t(e_json, idx)
                        WHERE p.result_json LIKE '{{"entities":%'
                          AND p.result_json NOT LIKE '{{"entities": []}}'
                    )
                    SELECT COUNT(*),
                           COUNT(DISTINCT (rid, note_row_id, idx, ev))
                    FROM ents
                """
            else:
                sql = f"""
                    SELECT COUNT(*),
                           COUNT(DISTINCT (research_id, note_row_id,
                                           COALESCE(source_line, -1),
                                           COALESCE(evidence_start, -1),
                                           COALESCE(entity_value_norm,
                                                    entity_value_raw)))
                    FROM main."{tbl}"
                """
            r = con.execute(sql).fetchone()
            log(f"    {tbl}: raw={r[0]:,} chosen_key={r[1]:,} "
                f"collisions={r[0]-r[1]:,}")
            dedup_probe[tbl] = {
                "raw": int(r[0]), "chosen_key": int(r[1]),
                "collisions": int(r[0]) - int(r[1]),
            }

    # Idempotent archive snapshots (only for legacy tables we'll drop in
    # phase 7; LLM tables stay live so they're not archived).
    log("")
    log("  archive snapshots (idempotent, legacy entity-row sources only):")
    snapshots: list[dict[str, Any]] = []
    for sch, tbl, _ in DEPRECATED_SOURCES:
        if tbl not in seen_tables:
            log(f"    skip: {sch}.{tbl} not in domain set this run")
            continue
        snapshots.append(_archive_one(con, sch, tbl, do_writes))

    return {
        "build_ts": BUILD_TS,
        "inventory": inventory,
        "snapshots": snapshots,
        "pre_counts": archive_counts,
        "anchor_probe": {
            "n_cpm": n_cpm, "n_strict": n_strict,
            "n_fallback": n_fallback, "n_null_anchor": n_null_anchor,
            "pct_strict": round(100*n_strict/n_cpm, 2),
            "pct_fallback": round(100*n_fallback/n_cpm, 2),
            "pct_null_anchor": round(100*n_null_anchor/n_cpm, 2),
        },
        "dedup_probe": dedup_probe,
    }


def _archive_one(con: duckdb.DuckDBPyConnection, schema: str, table: str,
                  do_writes: bool) -> dict[str, Any]:
    src = fq(schema, table)
    dst_name = f"{table}_pre365_{BUILD_TS}"
    dst = f'{ARCHIVE_FQ}."{dst_name}"'
    n_src = row_count(con, schema, table)
    log(f"    plan: {schema}.{table} ({n_src:,} rows) -> {dst_name}")
    if not do_writes:
        return {"src": f"{schema}.{table}", "dst": dst_name,
                "rows": n_src, "status": "DRY_RUN"}
    already = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [ARCHIVE_DB, ARCHIVE_SCHEMA, dst_name],
    ).fetchone()
    if already:
        n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
        log(f"      archive already exists: {dst_name} ({n_dst:,} rows)")
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
        log_warn(f"    COMMENT failed (non-fatal): {exc}")
    log(f"      archived -> {dst_name} ({n_dst:,} rows)")
    return {"src": f"{schema}.{table}", "dst": dst_name,
            "rows": n_dst, "status": "ARCHIVED"}


# ---------------------------------------------------------------------------
# Step 1 — Build events canonicals
# ---------------------------------------------------------------------------

def step_1_build_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1 — Build events canonicals (CREATE OR REPLACE in main)")
    log("=" * 78)
    out: dict[str, Any] = {}
    for d in domains:
        target = CANONICAL_TABLES[d]["events"]
        sql = _build_events_sql_for_domain(d)
        log(f"  building main.{target} ({d})")
        if not do_writes:
            con.execute("DROP TABLE IF EXISTS _temp_events_dryrun")
            temp_sql = sql.replace(
                f"CREATE OR REPLACE TABLE {fq('main', target)}",
                "CREATE TEMP TABLE _temp_events_dryrun",
            )
            con.execute(temp_sql)
            n = int(con.execute(
                "SELECT COUNT(*) FROM _temp_events_dryrun"
            ).fetchone()[0])
            p = int(con.execute(
                "SELECT COUNT(DISTINCT research_id) FROM _temp_events_dryrun"
            ).fetchone()[0])
            log(f"    [dry-run] would build {n:,} rows / {p:,} patients")
            con.execute("DROP TABLE _temp_events_dryrun")
            out[d] = {"created": False, "rows": n, "patients": p}
            continue
        con.execute(sql)
        n = row_count(con, "main", target)
        p = distinct_research_ids(con, "main", target)
        log(f"    built main.{target}: {n:,} rows / {p:,} patients")
        domain_label = CANONICAL_TABLES[d]["domain_label"]
        try:
            con.execute(
                f"COMMENT ON TABLE {fq('main', target)} IS "
                f"'[domain={domain_label}; grain=per_finding] — REMEDIATED "
                f"by {SCRIPT_TAG} on {RUN_DATE} "
                f"({CANONICAL_VERSION}). 19 cols. CHANGES A-N applied. "
                f"linkage = research_id only; source_table + source_row_id + "
                f"finding_date provide evidence trail. anchor_source ∈ "
                f"(strict|first_surgery_fallback|NULL); is_preexisting NULL "
                f"when anchor_source IS NULL. evidence_span_hash is SHA256 "
                f"(PHI-safe; raw text NEVER stored).'"
            )
        except Exception as exc:
            log_warn(f"    COMMENT failed (non-fatal): {exc}")
        out[d] = {"created": True, "rows": n, "patients": p}
    return out


# ---------------------------------------------------------------------------
# Step 2 — Build patient rollups
# ---------------------------------------------------------------------------

def step_2_build_rollups(
    con: duckdb.DuckDBPyConnection, do_writes: bool, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Build patient rollups (LEFT JOIN FROM CPM = 10,871 each)")
    log("=" * 78)
    out: dict[str, Any] = {}
    for d in domains:
        target = CANONICAL_TABLES[d]["rollup"]
        events_table = CANONICAL_TABLES[d]["events"]
        if not do_writes:
            log(f"  [dry-run] would CREATE {target} from {events_table}")
            out[d] = {"created": False}
            continue
        if not table_exists(con, "main", events_table):
            log_warn(f"  events table main.{events_table} missing — skip")
            out[d] = {"created": False, "reason": "events_missing"}
            continue
        con.execute(_build_rollup_sql_for_domain(d))
        n = row_count(con, "main", target)
        p = distinct_research_ids(con, "main", target)
        n_cols = len(list_columns(con, "main", target))
        log(f"  built main.{target}: {n:,} rows / {p:,} pts / {n_cols} cols")
        domain_label = CANONICAL_TABLES[d]["domain_label"]
        try:
            con.execute(
                f"COMMENT ON TABLE {fq('main', target)} IS "
                f"'[domain={domain_label}; grain=per_patient] — REMEDIATED "
                f"by {SCRIPT_TAG} on {RUN_DATE} "
                f"({CANONICAL_VERSION}). LEFT JOIN FROM canonical_patient_"
                f"master = 10,871 rows. Phenotype BOOL triads "
                f"(definitive/probable_or_better/any_evidence) per CHANGE K; "
                f"PMH smoking_status uses 3 plain BOOLs (Q-smoking=3). "
                f"anchor_source carried for cohort filtering.'"
            )
        except Exception as exc:
            log_warn(f"  COMMENT failed (non-fatal): {exc}")
        out[d] = {"created": True, "rows": n, "patients": p, "n_cols": n_cols}
    return out


# ---------------------------------------------------------------------------
# Step 3 — views_readable views
# ---------------------------------------------------------------------------

def step_3_build_views(
    con: duckdb.DuckDBPyConnection, do_writes: bool, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Create / refresh views_readable views")
    log("=" * 78)
    created: list[str] = []
    for d in domains:
        for view_key, base_key in [("events_view", "events"),
                                    ("rollup_view", "rollup")]:
            view_name = CANONICAL_TABLES[d][view_key]
            base_table = CANONICAL_TABLES[d][base_key]
            if not table_exists(con, "main", base_table):
                log_warn(f"  base main.{base_table} missing — skipping "
                         f"view {view_name}")
                continue
            log(f"  view {VIEW_SCHEMA}.{view_name} -> main.{base_table}")
            if do_writes:
                con.execute(
                    f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{view_name}" AS SELECT * FROM {fq("main", base_table)}'
                )
            created.append(view_name)
    return {"views": created}


# ---------------------------------------------------------------------------
# Step 4 — Registry sync (Pattern 13: idempotent DELETE-first + INSERT)
# ---------------------------------------------------------------------------

def _registry_entries(domains: tuple[str, ...]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for d in domains:
        meta = CANONICAL_TABLES[d]
        domain_label = meta["domain_label"]
        entries.append({
            "detail_table_name": meta["events"],
            "schema_name": "main",
            "join_key": "research_id",
            "grain": (f"per_finding (one row per source-mention of a "
                      f"{domain_label} finding)"),
            "domain": domain_label,
            "feeds_master_columns":
                ("documentation-grain — typically queried directly via "
                 "finding_text/value/status; CPM cols feed via the rollup"),
            "description":
                ("REMEDIATED events-grain canonical for "
                 f"{domain_label}. Built by {SCRIPT_TAG} on {RUN_DATE}. "
                 "19 cols (CHANGES A-N applied). anchor_source HYBRID; "
                 "is_preexisting NULL when anchor missing; evidence_strength "
                 "tiered; med_status default unknown."),
            "feeds_master_columns_array": [],
        })
        entries.append({
            "detail_table_name": meta["rollup"],
            "schema_name": "main",
            "join_key": "research_id",
            "grain": "per_patient (LEFT JOIN FROM canonical_patient_master)",
            "domain": domain_label,
            "feeds_master_columns":
                (f"{domain_label}_<phenotype>_{{definitive,"
                 "probable_or_better,any_evidence}} BOOLs + n_findings_*"),
            "description":
                ("REMEDIATED per-patient rollup for "
                 f"{domain_label}. Built by {SCRIPT_TAG} on {RUN_DATE}. "
                 "LEFT JOIN FROM CPM = 10,871 rows. Phenotype BOOL triads + "
                 "anchor_source for cohort filtering."),
            "feeds_master_columns_array": [
                "n_findings_present", "n_distinct_findings_norm",
                "first_finding_date", "last_finding_date", "anchor_source",
            ],
        })
    return entries


def step_4_registry_sync(
    con: duckdb.DuckDBPyConnection, do_writes: bool, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — detail_table_registry_v1 sync (idempotent)")
    log("=" * 78)
    if not table_exists(con, WS_SCHEMA, REGISTRY_TABLE):
        log_warn(f"  registry {WS_SCHEMA}.{REGISTRY_TABLE} missing — skip")
        return {"skipped": True}
    reg_cols = list_columns(con, WS_SCHEMA, REGISTRY_TABLE)
    log(f"  registry columns: {reg_cols}")

    entries = _registry_entries(domains)
    new_names = [e["detail_table_name"] for e in entries]

    # Idempotent: DELETE the new canonical names (clears prior runs of this
    # script) AND the source-table names (cleared by the original 365 build).
    drop_names: list[str] = list(new_names)
    for d in domains:
        for _kind, tbl, _, _ in SOURCE_INVENTORY[d]:
            drop_names.append(tbl)
    drop_names = sorted(set(drop_names))
    drop_placeholders = ", ".join(["?"] * len(drop_names))

    pre_count = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE detail_table_name IN ({drop_placeholders})",
        drop_names,
    ).fetchone()[0])
    log(f"  registry rows scheduled for DELETE (pre-insert): {pre_count}")
    if do_writes:
        con.execute(
            f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE detail_table_name IN ({drop_placeholders})",
            drop_names,
        )
        log(f"  deleted {pre_count} stale registry rows")

    inserted = 0
    for entry in entries:
        sch, tbl = entry["schema_name"], entry["detail_table_name"]
        if not table_exists(con, sch, tbl):
            log_warn(f"  registry skip: {sch}.{tbl} not yet built")
            continue
        n = row_count(con, sch, tbl)
        p = distinct_research_ids(con, sch, tbl)
        rec: dict[str, Any] = {
            "detail_table_name": tbl,
            "schema_name": sch,
            "join_key": entry["join_key"],
            "grain": entry["grain"],
            "total_rows": n,
            "total_patients": p,
            "domain": entry["domain"],
            "feeds_master_columns": entry["feeds_master_columns"],
            "description": entry["description"],
            "canonical_version": CANONICAL_VERSION,
            "feeds_master_columns_secondary": None,
            "feeds_master_columns_array": entry["feeds_master_columns_array"],
            "needs_manual_review": False,
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
        inserted += 1
    return {"deleted": pre_count, "inserted": inserted}


# ---------------------------------------------------------------------------
# Step 5 — CPM feeder audit (read-only report; Phase 2 follow-up)
# ---------------------------------------------------------------------------

def step_5_cpm_audit(
    con: duckdb.DuckDBPyConnection, domains: tuple[str, ...]
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — CPM feeder audit (read-only report; Phase 2 follow-up)")
    log("=" * 78)
    if not table_exists(con, "main", "canonical_patient_master"):
        log_warn("  canonical_patient_master missing — skipping CPM audit")
        return {"audit_rows": [], "report_path": None}
    cpm_cols = sorted(list_columns(con, "main", "canonical_patient_master"))
    log(f"  CPM has {len(cpm_cols)} total columns")

    bucketed: dict[str, list[str]] = {d: [] for d in domains}
    matched: set[str] = set()
    for d in domains:
        for cpm_col in cpm_cols:
            if cpm_col in matched:
                continue
            for kw in CPM_KEYWORD_BUCKETS[d]:
                if kw in cpm_col:
                    bucketed[d].append(cpm_col)
                    matched.add(cpm_col)
                    break

    grep_hits: dict[str, list[str]] = {}
    seen_sources: set[str] = set()
    for d in domains:
        for _kind, tbl, _, _ in SOURCE_INVENTORY[d]:
            if tbl in seen_sources:
                continue
            seen_sources.add(tbl)
            try:
                res = subprocess.run(
                    ["git", "grep", "-l", tbl, "--", "scripts/"],
                    cwd=str(REPO_ROOT),
                    capture_output=True, text=True, timeout=60, check=False,
                )
                files = [ln.strip() for ln in res.stdout.splitlines()
                         if ln.strip()]
                grep_hits[tbl] = files
            except subprocess.SubprocessError as exc:
                log_warn(f"  git grep for {tbl} failed: {exc}")
                grep_hits[tbl] = []

    md = [
        f"# PSH/PMH/Meds CPM feeder audit — {SCRIPT_TAG} REMEDIATED ({RUN_DATE})",
        "",
        "Read-only audit produced by Step 5 of the Phase-1 rebuild. Identifies "
        "CPM columns that may need repointing to the new rollup canonicals.",
        "",
        f"**CPM total columns:** {len(cpm_cols)}",
        f"**Canonical version:** `{CANONICAL_VERSION}`",
        "",
    ]
    for d in domains:
        md += [
            f"## Domain: {d.upper()} — keyword-bucketed CPM cols "
            f"({len(bucketed[d])} matches)",
            "",
        ]
        if bucketed[d]:
            for col in bucketed[d]:
                md.append(f"- `{col}`")
        else:
            md.append("(no matches)")
        md.append("")

    md += [
        "## Scripts referencing the source tables (`git grep -l`)",
        "",
        "| source table | feeder script files |",
        "|---|---|",
    ]
    for tbl, files in grep_hits.items():
        if files:
            md.append(
                f"| `{tbl}` | "
                + ", ".join(f"`{f}`" for f in files[:10])
                + (f" (+{len(files)-10} more)" if len(files) > 10 else "")
                + " |"
            )
        else:
            md.append(f"| `{tbl}` | (no script-level references) |")

    md += [
        "",
        "## Recommended action (Phase 2)",
        "",
        "1. The 2 LLM source tables stay LIVE (Script 367 owns them).",
        "2. The 2 entity-row legacy tables drop in Phase 3 of the cascade. "
        "Any CPM column sourced from them must be repointed to the new "
        "rollup canonicals BEFORE the drop. The new rollup phenotype BOOL "
        "triads are the natural feeders for `pmhx_nlp_<phenotype>` cols.",
        "3. Phase 2 commit (`scripts/365_cpm_feeder_repoint.py`) materialises "
        "the repoint plan against the rollup `<dom>_<phen>_<tier>` cols.",
        "",
    ]
    CPM_AUDIT_PATH.write_text("\n".join(md) + "\n", encoding="utf-8")
    log(f"  CPM feeder audit -> {CPM_AUDIT_PATH.relative_to(REPO_ROOT)}")
    return {
        "bucketed_cpm_cols": {d: bucketed[d] for d in domains},
        "git_grep_hits_per_table": {k: len(v) for k, v in grep_hits.items()},
        "report_path": str(CPM_AUDIT_PATH.relative_to(REPO_ROOT)),
    }


# ---------------------------------------------------------------------------
# Step 6 — QA gates (29 hard gates: 27 from dry-run + 2 new)
# ---------------------------------------------------------------------------

def step_6_qa(
    con: duckdb.DuckDBPyConnection,
    archive_counts: dict[str, int],
    anchor_probe: dict[str, Any],
    domains: tuple[str, ...],
    pre_drop: bool,
    expected_events_rows: dict[str, int] | None = None,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — QA gates (29 hard + informational)")
    log("=" * 78)
    qa: dict[str, Any] = {"checks": [], "informational": [], "passed": True}

    def check(name: str, ok: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(ok), **details})
        log(f"  QA {'PASS' if ok else 'FAIL'} {name}: {details}")
        if not ok:
            qa["passed"] = False

    def info(name: str, **details: Any) -> None:
        qa["informational"].append({"name": name, **details})

    # Hard 1 — events_rowcount_nonzero per domain
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        n = (row_count(con, "main", ev) if table_exists(con, "main", ev) else -1)
        check(f"events_rowcount_nonzero_{d}", n > 0, rows=n)

    # Hard 1b — events rowcounts match the dry-run (Logan's guardrail).
    if expected_events_rows:
        for d in domains:
            ev = CANONICAL_TABLES[d]["events"]
            if not table_exists(con, "main", ev):
                continue
            actual = row_count(con, "main", ev)
            expected = expected_events_rows[d]
            check(f"events_rowcount_unchanged_{d}",
                  actual == expected,
                  expected=expected, actual=actual,
                  drift=actual - expected)

    # Hard 2 (gate J) — rollup_equals_cpm_rowcount per domain
    n_cpm = anchor_probe["n_cpm"]
    for d in domains:
        ro = CANONICAL_TABLES[d]["rollup"]
        if not table_exists(con, "main", ro):
            check(f"rollup_equals_cpm_rowcount_{d}", False,
                  reason="rollup missing")
            continue
        n_ro = row_count(con, "main", ro)
        check(f"rollup_equals_cpm_rowcount_{d}",
              n_ro == n_cpm, rollup_rows=n_ro, cpm_rows=n_cpm)

    # Hard 3 — finding_status_in_canonical_set
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        rows = con.execute(
            f"SELECT finding_status, COUNT(*) FROM {fq('main', ev)} "
            f"GROUP BY 1 ORDER BY 1"
        ).fetchall()
        dist = {r[0]: int(r[1]) for r in rows}
        bad = [s for s in dist if s not in VALID_STATUSES]
        check(f"finding_status_in_canonical_set_{d}",
              len(bad) == 0, off_domain=bad)
        # Informational: status distribution + documented-zero rationale.
        n_susp_or_ind = dist.get("suspected", 0) + dist.get("indeterminate", 0)
        info(f"negation_ladder_distribution_{d}", distribution=dist,
             n_suspected_or_indeterminate=n_susp_or_ind,
             documented_zero_rationale=(
                 "Upstream extractors emit only present/negated at "
                 "present_or_negated; ladder evidence_text LIKE branches are "
                 "subordinate to the EQ-first path. Tier-1 CF: future "
                 "upstream re-extraction may surface these values."
             ) if n_susp_or_ind == 0 else None)

    # Hard 4 — source_note_type_in_lookup
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        rows = con.execute(
            f"SELECT DISTINCT source_note_type FROM {fq('main', ev)}"
        ).fetchall()
        observed = {r[0] for r in rows if r[0] is not None}
        unknown = sorted(observed - set(VALID_NOTE_TYPES))
        check(f"source_note_type_in_lookup_{d}",
              len(unknown) == 0, unknown_values=unknown,
              observed_count=len(observed))

    # Hard 5 — evidence_strength_in_canonical_set
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        rows = con.execute(
            f"SELECT DISTINCT evidence_strength FROM {fq('main', ev)}"
        ).fetchall()
        observed = {r[0] for r in rows if r[0] is not None}
        bad = sorted(observed - set(VALID_EVIDENCE_STRENGTHS))
        check(f"evidence_strength_in_canonical_set_{d}",
              len(bad) == 0, unexpected=bad, observed=sorted(observed))

    # Hard 6 — med_status set + null/non-null per domain
    if "meds" in domains:
        ev = CANONICAL_TABLES["meds"]["events"]
        if table_exists(con, "main", ev):
            rows = con.execute(
                f"SELECT DISTINCT med_status FROM {fq('main', ev)} "
                f"WHERE med_status IS NOT NULL"
            ).fetchall()
            observed = {r[0] for r in rows}
            bad = sorted(observed - set(VALID_MED_STATUSES))
            check("med_status_in_canonical_set_meds",
                  len(bad) == 0, unexpected=bad, observed=sorted(observed))
            n_null_meds = int(con.execute(
                f"SELECT COUNT(*) FROM {fq('main', ev)} WHERE med_status IS NULL"
            ).fetchone()[0])
            check("med_status_not_null_for_meds",
                  n_null_meds == 0, n_null_in_meds=n_null_meds)
    for d in ("psh", "pmh"):
        if d not in domains:
            continue
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        n_non_null = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main', ev)} WHERE med_status IS NOT NULL"
        ).fetchone()[0])
        check(f"med_status_null_for_{d}",
              n_non_null == 0, n_non_null=n_non_null)

    # Hard 7 (NEW Phase-1 gate per Logan): med_status_unknown < 90%
    if "meds" in domains:
        ev = CANONICAL_TABLES["meds"]["events"]
        if table_exists(con, "main", ev):
            row = con.execute(f"""
                SELECT
                    SUM(CASE WHEN med_status='unknown' THEN 1 ELSE 0 END),
                    COUNT(*)
                FROM {fq('main', ev)}
            """).fetchone()
            n_unknown = int(row[0] or 0)
            n_total = int(row[1] or 0)
            pct_unknown = (100*n_unknown/n_total) if n_total else 0.0
            check("med_status_unknown_lt_90pct",
                  pct_unknown < 90.0,
                  pct_unknown=round(pct_unknown, 2),
                  n_unknown=n_unknown, n_total=n_total)
            # Informational: full meds_status distribution.
            dist_rows = con.execute(
                f"SELECT med_status, COUNT(*) FROM {fq('main', ev)} "
                f"GROUP BY 1 ORDER BY 1"
            ).fetchall()
            info("med_status_distribution_meds",
                 distribution={r[0]: int(r[1]) for r in dist_rows})

    # Hard 8 — meds_ingredient_dedup_compresses (CHANGE G)
    if "meds" in domains:
        ev = CANONICAL_TABLES["meds"]["events"]
        if table_exists(con, "main", ev):
            n_ev = row_count(con, "main", ev)
            n_norm = int(con.execute(f"""
                SELECT SUM(d) FROM (
                    SELECT COUNT(DISTINCT finding_value_norm) AS d
                    FROM {fq('main', ev)} WHERE finding_status='present'
                    GROUP BY research_id
                )
            """).fetchone()[0] or 0)
            check("meds_ingredient_dedup_compresses",
                  n_norm < n_ev,
                  events_rows=n_ev, sum_distinct_norm_per_pt=n_norm)

    # Hard 9 — is_preexisting NULL when anchor_source IS NULL
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        n_violation = int(con.execute(f"""
            SELECT COUNT(*) FROM {fq('main', ev)}
            WHERE is_preexisting IS NOT NULL AND anchor_source IS NULL
        """).fetchone()[0])
        check(f"is_preexisting_null_when_anchor_null_{d}",
              n_violation == 0,
              rows_with_null_anchor_but_set_is_preexisting=n_violation)

    # Hard 10 — anchor_source domain check
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        rows = con.execute(
            f"SELECT DISTINCT anchor_source FROM {fq('main', ev)} "
            f"WHERE anchor_source IS NOT NULL"
        ).fetchall()
        observed = {r[0] for r in rows}
        bad = sorted(observed - set(VALID_ANCHOR_SOURCES))
        check(f"anchor_source_in_canonical_set_{d}",
              len(bad) == 0, unexpected=bad, observed=sorted(observed))

    # Hard 11 — events_completeness_required
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        nulls = con.execute(f"""
            SELECT
                SUM(CASE WHEN research_id IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN source_table IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN source_row_id IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN source_note_type IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN finding_text IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN finding_status IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN evidence_strength IS NULL THEN 1 ELSE 0 END),
                COUNT(*)
            FROM {fq('main', ev)}
        """).fetchone()
        nulls_dict = {
            "research_id": int(nulls[0] or 0),
            "source_table": int(nulls[1] or 0),
            "source_row_id": int(nulls[2] or 0),
            "source_note_type": int(nulls[3] or 0),
            "finding_text": int(nulls[4] or 0),
            "finding_status": int(nulls[5] or 0),
            "evidence_strength": int(nulls[6] or 0),
        }
        ok = all(v == 0 for v in nulls_dict.values())
        check(f"events_completeness_required_{d}", ok,
              null_counts=nulls_dict, total=int(nulls[7] or 0))

    # Hard 12 — view_resolves (Pattern 10)
    for d in domains:
        for view_key in ("events_view", "rollup_view"):
            view_name = CANONICAL_TABLES[d][view_key]
            if not view_exists(con, VIEW_SCHEMA, view_name):
                check(f"view_resolves_{view_name}", False,
                      reason="view missing")
                continue
            try:
                con.execute(
                    f'SELECT * FROM "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                    f'"{view_name}" LIMIT 0'
                ).fetchall()
                ok = True
            except duckdb.Error as exc:
                ok = False
                log_warn(f"  view {view_name} fails to resolve: {exc}")
            check(f"view_resolves_{view_name}", ok)

    # Hard 13 (NEW Phase-1 gate per Logan): anchor_source distribution surface
    # Computed against the rollup (per-patient grain).
    for d in domains:
        ro = CANONICAL_TABLES[d]["rollup"]
        if not table_exists(con, "main", ro):
            continue
        rows = con.execute(
            f"SELECT COALESCE(anchor_source, 'NULL'), COUNT(*) "
            f"FROM {fq('main', ro)} GROUP BY 1 ORDER BY 1"
        ).fetchall()
        dist = {r[0]: int(r[1]) for r in rows}
        n_strict = dist.get("strict", 0)
        n_fallback = dist.get("first_surgery_fallback", 0)
        n_null = dist.get("NULL", 0)
        n_total = sum(dist.values())
        check(f"anchor_source_distribution_surfaced_{d}",
              True,  # informational gate, always pass — surface is the point
              distribution=dist, total=n_total,
              pct_strict=round(100*n_strict/n_total, 2) if n_total else 0,
              pct_fallback=round(100*n_fallback/n_total, 2) if n_total else 0,
              pct_null=round(100*n_null/n_total, 2) if n_total else 0)

    # Per-domain top-finding informational
    for d in domains:
        ev = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev):
            continue
        rows = con.execute(
            f"SELECT finding_text, finding_value_norm, COUNT(*) "
            f"FROM {fq('main', ev)} WHERE finding_status='present' "
            f"GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20"
        ).fetchall()
        info(f"top_finding_text_{d}",
             top20=[{"finding_text": r[0], "finding_value_norm": r[1],
                     "n": int(r[2])} for r in rows])

    # Step 7 verification (only when --phase 7 ran).
    if not pre_drop:
        for sch, tbl, _ in DEPRECATED_SOURCES:
            still = table_exists(con, sch, tbl)
            check(f"deprecated_table_dropped_{sch}_{tbl}",
                  not still, still_present=still)

    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str), encoding="utf-8")
    log(f"  QA report -> {QA_PATH.relative_to(REPO_ROOT)}")
    return qa


# ---------------------------------------------------------------------------
# Step 7 — Phase-gated drop (Phase 3 of the remediation cascade)
# ---------------------------------------------------------------------------

def step_7_drop_deprecated(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
    archive_counts: dict[str, int],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 7 — Drop deprecated entity-row source tables (Phase 3)")
    log("=" * 78)
    for d in ("pmh", "meds"):
        ev_tbl = CANONICAL_TABLES[d]["events"]
        if not table_exists(con, "main", ev_tbl):
            raise RuntimeError(f"Refusing: replacement main.{ev_tbl} missing")
        n = row_count(con, "main", ev_tbl)
        if n == 0:
            raise RuntimeError(f"Refusing: replacement main.{ev_tbl} empty")
        log(f"  replacement main.{ev_tbl}: {n:,} rows ✓")

    archives_used: dict[str, str] = {}
    for sch, tbl, _ in DEPRECATED_SOURCES:
        if not table_exists(con, sch, tbl):
            log(f"  {sch}.{tbl} already absent")
            continue
        live_n = archive_counts.get(f"{sch}.{tbl}") or row_count(con, sch, tbl)
        candidates = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? AND table_name LIKE ? "
            "ORDER BY table_name DESC",
            [ARCHIVE_DB, ARCHIVE_SCHEMA, f"{tbl}_pre365_%"],
        ).fetchall()
        if not candidates:
            raise RuntimeError(
                f"No pre365_* archive found for {sch}.{tbl}. Run --phase 0 first."
            )
        matched: str | None = None
        seen: list[tuple[str, int]] = []
        for (arch_name,) in candidates:
            arch_fq = f'{ARCHIVE_FQ}."{arch_name}"'
            try:
                arch_n = int(con.execute(
                    f"SELECT COUNT(*) FROM {arch_fq}"
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
                f"No matching archive of {sch}.{tbl} ({live_n:,} rows). "
                f"Candidates: {seen}."
            )
        archives_used[f"{sch}.{tbl}"] = matched
        log(f"  parity verified: {sch}.{tbl} <- {matched}")

    dropped: list[str] = []
    for sch, tbl, role in DEPRECATED_SOURCES:
        if not table_exists(con, sch, tbl):
            continue
        log(f"  DROP TABLE {sch}.{tbl} ({role})")
        if do_writes:
            con.execute(f"DROP TABLE {fq(sch, tbl)}")
        dropped.append(f"{sch}.{tbl}")
    return {"dropped": dropped,
            "archives_used_for_parity": archives_used}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_phases(spec: str | None) -> set[str]:
    if not spec:
        return {"0", "1", "2", "3", "4", "5", "6"}
    return {s.strip() for s in spec.split(",") if s.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PSH + PMH/Problem List + Medications consolidation "
                    "(Script 365 REMEDIATED)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Run with writes enabled.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — no writes to main.")
    parser.add_argument("--phase", default=None,
                        help="Comma-separated phases (default 0,1,2,3,4,5,6). "
                             "Use 7 to drop deprecated entity-row tables.")
    parser.add_argument("--skip-drop", action="store_true",
                        help="Force-remove phase 7 from the run set.")
    parser.add_argument("--domain", choices=DOMAINS, default=None,
                        help="Build only one domain (default: all 3).")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    phases = parse_phases(args.phase)
    if args.skip_drop:
        phases.discard("7")
    domains: tuple[str, ...] = ((args.domain,) if args.domain else DOMAINS)
    log(f"Run config: do_writes={do_writes}, phases={sorted(phases)}, "
        f"domains={list(domains)}, BUILD_TS={BUILD_TS}, "
        f"canonical_version={CANONICAL_VERSION}")

    # Guardrail values from dry-run for events rowcounts (Logan's check).
    EXPECTED_EVENTS_ROWS: dict[str, int] = {
        "psh": 3919, "pmh": 12444, "meds": 7501,
    }

    try:
        con = connect()
        archive_counts: dict[str, int] = {}
        anchor_probe: dict[str, Any] = {}
        results: dict[str, Any] = {
            "build_ts": BUILD_TS, "do_writes": do_writes,
            "phases": sorted(phases), "domains": list(domains),
            "canonical_version": CANONICAL_VERSION,
        }

        if "0" in phases:
            r = step_0_preflight(con, do_writes, domains)
            archive_counts = r["pre_counts"]
            anchor_probe = r["anchor_probe"]
            results["step_0"] = r
        else:
            for sch, tbl, _ in DEPRECATED_SOURCES:
                if table_exists(con, sch, tbl):
                    archive_counts[f"{sch}.{tbl}"] = row_count(con, sch, tbl)
            anchor_probe = {"n_cpm": row_count(
                con, "main", "canonical_patient_master")}

        if "1" in phases:
            results["step_1"] = step_1_build_events(con, do_writes, domains)
        if "2" in phases:
            results["step_2"] = step_2_build_rollups(con, do_writes, domains)
        if "3" in phases:
            results["step_3"] = step_3_build_views(con, do_writes, domains)
        if "4" in phases:
            results["step_4"] = step_4_registry_sync(con, do_writes, domains)
        if "5" in phases:
            results["step_5"] = step_5_cpm_audit(con, domains)

        ran_step_7 = False
        if "7" in phases and do_writes:
            results["step_7"] = step_7_drop_deprecated(
                con, do_writes, archive_counts,
            )
            ran_step_7 = True
        elif "7" in phases:
            log("STEP 7 — dry-run skips DROP TABLE (writes disabled)")

        if "6" in phases:
            if not do_writes:
                log("STEP 6 — QA SKIPPED in dry-run (no live tables)")
                results["step_6"] = {"skipped_dry_run": True, "passed": True}
            else:
                results["step_6"] = step_6_qa(
                    con, archive_counts, anchor_probe, domains,
                    pre_drop=not ran_step_7,
                    expected_events_rows=EXPECTED_EVENTS_ROWS,
                )
                if not results["step_6"]["passed"]:
                    log_error("QA failed — see qa file")
                    _write_decision(results)
                    flush_log()
                    return 2

        _write_decision(results)
        log(f"{SCRIPT_TAG} complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        import traceback
        log_error(traceback.format_exc())
        flush_log()
        raise


def _write_decision(results: dict[str, Any]) -> None:
    decision_path = OUTPUT_DIR / f"{SCRIPT_ID}_decision_{RUN_TS_COMPACT}.json"
    decision_path.write_text(json.dumps(results, indent=2, default=str))
    log(f"  decision log: {decision_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    sys.exit(main())
