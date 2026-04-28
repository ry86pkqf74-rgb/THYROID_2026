#!/usr/bin/env python3
"""
Script 363 — Cross-Modal Invasion Findings Canonical (NEW DOMAIN).

Third of four planned consolidations. Builds a single source of truth
for invasion findings (gross ETE, microscopic ETE, tracheal, esophageal,
vascular_microscopic, airway, local) by tracing each finding back to the
modality / report that documented it. Honours Logan's Q3 requirement
("trace back independently across different reports") and Q9 ("just get
all info into one clean table, no redundancy").

Design source: cursor_prompt_script_363_invasion_v2.md (supersedes v1).

Build outputs (all under thyroid_canonical_publication_v1_0)
============================================================
Event-grain table (main):
    canonical_invasion_events_v1            -- one row per
                                               (research_id × invasion_type
                                                × source_modality × source_row_id)
                                               invasion-finding mention.
Patient-grain rollup (main):
    canonical_invasion_patient_rollup_v1    -- one row per research_id
                                               with at least one finding.
Readable views (views_readable, suffix _VIEW_v1):
    invasion_events_VIEW_v1
    invasion_patient_rollup_VIEW_v1

Cascade strip (Step 7, only under --commit without --skip-strip OR
--commit --phase 7):
    Drops 4 invasion BOOLEAN columns from
    main.canonical_operative_events_v1:
        gross_ete_flag, tracheal_involvement_flag,
        esophageal_involvement_flag, local_invasion_flag
    DOES NOT drop strap_muscle_involvement_flag (per Q3 — strap muscle
    stays only on operative).
    DOES NOT touch canonical_path_malignant_events_v1 (no invasion cols
    on it; synoptic-path data sourced from pre361 archive snapshots).

Usage
-----
    python scripts/363_invasion_canonical.py --dry-run
    python scripts/363_invasion_canonical.py --commit --skip-strip
    python scripts/363_invasion_canonical.py --commit --phase 7
    python scripts/363_invasion_canonical.py --commit --phase 7 --force-strip

Phases (idempotent):
    0  pre-flight + dependency check + modality coverage census +
       categorical vocab probe + LLM JSON key probe + date type probes
       + (Step 0.g) pre-flight archive lookup
    1  build canonical_invasion_events_v1
    2  build canonical_invasion_patient_rollup_v1
    3  views (2)
    4  detail_table_registry_v1 sync (idempotent DELETE-first)
    5  CPM feeder audit (read-only report)
    6  zero-drift QA -> qa/qa_script_363_invasion.json
    7  cascade strip (only --phase 7 or --commit without --skip-strip)
    8  close-out summary

Auth: motherduck_client.get_token(). PHI rule: research_id only — never
log clinical text or note narrative contents. Evidence spans are stored
as md5 hash, never raw text.
"""
from __future__ import annotations

import argparse
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

SCRIPT_ID = "363"
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
QA_PATH = QA_DIR / f"qa_script_{SCRIPT_ID}_invasion.json"
CENSUS_PATH = REPO_ROOT / f"invasion_coverage_census_{BUILD_TS}.md"
VOCAB_PATH = REPO_ROOT / f"invasion_categorical_vocab_{BUILD_TS}.md"
JSON_KEYS_PATH = REPO_ROOT / f"invasion_llm_json_keys_{BUILD_TS}.md"
CPM_AUDIT_PATH = REPO_ROOT / "invasion_cpm_feeder_repoint_plan.md"
CLOSEOUT_PATH = REPO_ROOT / f"script_363_closeout_{BUILD_TS}.md"

# 11 invasion types (v3 + mig_95 ETE taxonomy — per Logan rejection of
# v2 'local' bundling and 2026-04-28 gross-vs-micro follow-up.
# bug + V/L aggregation bug). Direct-extension findings only;
# mass-effect entities (tracheal_deviation, substernal_extension,
# esophageal_compression, vascular_encasement, airway_compromise_grade,
# vocal_cord_imaging, mass_effect) are EXCISED — they belong in a
# future mass-effect canonical or in 364 complications, not here.
INVASION_TYPES = [
    "gross_ete", "microscopic_ete", "ete_present_not_further_specified",
    "vascular_microscopic",       # vascular_invasion ONLY (not lymphatic)
    "lymphatic_microscopic",      # NEW v3 — split from vascular
    "capsular",                   # NEW v3 — split from 'local'
    "perineural",                 # NEW v3 — split from 'local'
    "soft_tissue",                # NEW v3 — split from 'local'
    "airway", "tracheal", "esophageal",
]

# Strip targets (Step 7) — operative canonical only.
STRIP_TARGET_TABLE = ("main", "canonical_operative_events_v1")
STRIP_COLUMNS = [
    "gross_ete_flag", "tracheal_involvement_flag",
    "esophageal_involvement_flag", "local_invasion_flag",
]
# Explicit DO-NOT-DROP list (sanity guard).
STRIP_FORBIDDEN_COLUMNS = ["strap_muscle_involvement_flag"]

# New canonicals (for registry sync + Step 7 pre-flight).
NEW_TABLES: list[tuple[str, str, str]] = [
    ("main", "canonical_invasion_events_v1", "per_invasion_mention"),
    ("main", "canonical_invasion_patient_rollup_v1", "per_patient"),
]
NEW_VIEWS = [
    ("invasion_events_VIEW_v1", "canonical_invasion_events_v1"),
    ("invasion_patient_rollup_VIEW_v1", "canonical_invasion_patient_rollup_v1"),
]

# Archive table name patterns. v3 has NO source archives (Pattern 8
# REJECTED per Logan; cross-DB FROM archive_pub_v1_0.* is forbidden).
# This dict is kept empty to surface that fact to anyone reading the
# code; the archive_* CTE-builder branches and Step 0.a archive
# resolution are intentionally vestigial — they no-op when the dict is
# empty. The Step 0.g + Step 7 pre-strip-snapshot helpers still write
# TO archive_pub_v1_0 (that's archiving, not sourcing — different
# semantics, allowed).
ARCHIVE_PATTERNS: dict[str, str] = {}

_LOG_LINES: list[str] = []

# ---------------------------------------------------------------------------
# VARCHAR vocab → finding_status (Pattern 9)
# ---------------------------------------------------------------------------

# Lowercase + trailing-punctuation-stripped lookup. Anything not here →
# 'indeterminate' + log to unmapped_categorical_values.
VARCHAR_TO_FINDING_STATUS: dict[str, str] = {
    # ------------------------------------------------------------------
    # AUTHORITATIVE per Logan (CHECKPOINT 1, 2026-04-22):
    # 'x' / 'X' is the synoptic template's ABSENT placeholder, NOT
    # missing data. Logan validated this against per-field cardinality:
    #   vascular_invasion: 'x' = 3,122 absent placeholder
    #     vs ~672 patients with present-flavored values
    #     → ~6.2% of cohort, clinically realistic.
    # Mapping 'x' → indeterminate would over-report uncertainty;
    # mapping any-non-null → present would massively over-report.
    # ------------------------------------------------------------------
    # ----- ABSENT
    "x": "absent", "no": "absent", "false": "absent", "none": "absent",
    "n/s": "absent", "n/a": "absent", "0": "absent",
    "not identified": "absent",
    # ----- PRESENT — clean keywords
    "present": "present", "yes": "present", "true": "present",
    "identified": "present",
    "extensive": "present", "focal": "present", "minimal": "present",
    "minimally invasive": "present", "widely invasive": "present",
    "infiltrative": "present", "invasive": "present",
    "microscopic": "present", "microscopic extension": "present",
    "minimal extension": "present",
    "multifocal": "present", "multiple foci": "present", "1 focus": "present",
    "single focus": "present", "into but not through": "present",
    "yes (minimal)": "present", "yes (focal)": "present",
    "yes, minimal": "present", "yes, extensive": "present",
    "yes (extensive)": "present",
    "prominent": "present", "limited": "present", "s": "present",
    "yes;minimal": "present",
    "present, minimal": "present", "present (minimal)": "present",
    # ----- PRESENT — typos (all map to present per Logan)
    "preesent": "present", "preent": "present", "presnt": "present",
    "preseent": "present", "prewent": "present", "preewnt": "present",
    "miinimally invasive": "present", "minimallyinvasive": "present",
    "minimally invasvie": "present", "minimally invasivre": "present",
    "widely invasvie": "present", "widely invasivre": "present",
    "extensivre": "present", "extensiver": "present",
    "estensive": "present", "extrensive": "present", "extesive": "present",
    "foacl": "present", "minimal (1 focus)": "present",
    "minimal microscopic": "present", "minimal into fat": "present",
    "microscopiic": "present",
    # ----- PRESENT — descriptive narratives (raw text captured into
    # evidence_qualifier column for later clinical review per Logan)
    "focal early extension into perithyroidal fat": "present",
    "focal right side": "present", "right side focal": "present",
    "present, widely invasive": "present",
    "multifocal invasion": "present",
    "x\n(single microscopic focus of extension)": "present",
    "yes;capsular invasion into but not through capsule": "present",
    "yes;capsular invasion into but not through capsule;": "present",
    "present (perithyroidal fibroadipose tissue involved)": "present",
    "present (microscopic perithyroidal soft tissue only with no "
    "clinical or macroscopic evidence of invasion)": "present",
    # ----- SUSPECTED — hedged language (Logan: 'Infiltrative?' is
    # suspected, not present — the question mark matters)
    "suspicious": "suspected", "infiltrative?": "suspected",
    # ----- INDETERMINATE
    "indeterminate": "indeterminate", "equivocal": "indeterminate",
    "c/a": "indeterminate",
    "cannot be assessed": "indeterminate",
    "cannot be determined": "indeterminate",
    "cannot be determined: focal interstitial psammomatoid calcification "
    "present": "indeterminate",
    "indeeterminate": "indeterminate", "indetermiante": "indeterminate",
    "indeterminent": "indeterminate", "none?": "indeterminate",
    # ----- INDETERMINATE — synoptic margin annotation placeholders
    # (kept for completeness; clearly margin cross-references, not
    # findings)
    "*": "indeterminate", "* (see margin comment)": "indeterminate",
    "`x": "indeterminate", "classical": "indeterminate", "m": "indeterminate",
}

# gross_ete is BIGINT: only value 1 ever observed in the archive
# (1,571 rows). Per Logan: 1 → present; 0 → absent in source schema
# (NULL → unknown / not asked).
GROSS_ETE_BIGINT_MAP: dict[int, str] = {1: "present", 0: "absent"}

# ETE subtype mapping (used only when source column is
# extrathyroidal_extension). Generic present/yes values are NOT gross;
# they are ETE present, not further specified. Only explicit gross /
# extensive / macroscopic-flavored evidence maps to gross_ete.
EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE: dict[str, str] = {
    # ------------------------------------------------------------------
    # Subtype only applies to PRESENT findings. Values that map to
    # 'absent' / 'indeterminate' / 'suspected' in
    # VARCHAR_TO_FINDING_STATUS are NOT in this dict. The Step 1 SQL
    # only consults this dict when finding_status='present'; if the
    # value isn't found here, default invasion_type is
    # ete_present_not_further_specified.
    # 'Infiltrative?' is suspected (not present), so it doesn't appear
    # here — the suspected row gets invasion_type='gross_ete' by
    # default since suspicion of ETE without a microscopic qualifier
    # is treated as gross-suspicious.
    # ------------------------------------------------------------------
    # ----- MICROSCOPIC_ETE (minimal/microscopic/focal qualifiers)
    "minimal": "microscopic_ete", "microscopic": "microscopic_ete",
    "yes (minimal)": "microscopic_ete", "yes, minimal": "microscopic_ete",
    "minimally invasive": "microscopic_ete",
    "minimally invasivre": "microscopic_ete",
    "minimally invasvie": "microscopic_ete",
    "minimallyinvasive": "microscopic_ete",
    "miinimally invasive": "microscopic_ete",
    "minimal extension": "microscopic_ete",
    "microscopic extension": "microscopic_ete",
    "microscopiic": "microscopic_ete",
    "focal": "microscopic_ete", "focal right side": "microscopic_ete",
    "focal early extension into perithyroidal fat": "microscopic_ete",
    "multifocal": "microscopic_ete", "yes (focal)": "microscopic_ete",
    "minimal microscopic": "microscopic_ete",
    "minimal into fat": "microscopic_ete",
    "minimal (1 focus)": "microscopic_ete",
    "single focus": "microscopic_ete",
    "x\n(single microscopic focus of extension)": "microscopic_ete",
    "yes;minimal": "microscopic_ete",
    "present, minimal": "microscopic_ete",
    "present (minimal)": "microscopic_ete",
    "present (microscopic perithyroidal soft tissue only with no "
    "clinical or macroscopic evidence of invasion)": "microscopic_ete",
    "into but not through": "microscopic_ete",
    # ----- GROSS_ETE (explicit extensive/wide/gross/macroscopic qualifiers)
    "extensive": "gross_ete", "widely invasive": "gross_ete",
    "yes, extensive": "gross_ete", "yes (extensive)": "gross_ete",
    "extensiver": "gross_ete", "extensivre": "gross_ete",
    "extrensive": "gross_ete", "estensive": "gross_ete",
    "extesive": "gross_ete",
    "widely invasivre": "gross_ete", "widely invasvie": "gross_ete",
    "infiltrative": "gross_ete", "invasive": "gross_ete",
    "present, widely invasive": "gross_ete",
    "multifocal invasion": "gross_ete",
    "multiple foci": "gross_ete",
    # ----- PRESENT, NOT FURTHER SPECIFIED
    "yes": "ete_present_not_further_specified",
    "true": "ete_present_not_further_specified",
    "present": "ete_present_not_further_specified",
    "preesent": "ete_present_not_further_specified",
    "presnt": "ete_present_not_further_specified",
    "preent": "ete_present_not_further_specified",
    "preseent": "ete_present_not_further_specified",
    "prewent": "ete_present_not_further_specified",
    "preewnt": "ete_present_not_further_specified",
    "present (perithyroidal fibroadipose tissue involved)": (
        "ete_present_not_further_specified"
    ),
}

# ---------------------------------------------------------------------------
# LLM entity_type → invasion_type (Pattern 10)
# ---------------------------------------------------------------------------

ENTITY_TYPE_TO_INVASION_TYPE: dict[str, str] = {
    # ==================================================================
    # v3 mapping per Logan's rejection findings (CHECKPOINT 1
    # follow-up). The v2 'local' bundling bug is fixed by:
    #   1. Splitting vascular vs lymphatic (clinically distinct AJCC
    #      descriptors).
    #   2. Routing capsular / perineural / soft_tissue to their own
    #      invasion_types instead of dumping them into 'local'.
    #   3. EXCISING mass-effect entity_types entirely (return NULL via
    #      "not in dict" → CTE filter drops the row). They route to a
    #      future mass-effect canonical or 364 complications scope.
    #
    # Excised entity_types (do NOT add to this dict — they are NOT
    # invasion findings):
    #   * tracheal_deviation, tracheal_displacement, tracheal_narrowing
    #     (compression, not invasion)
    #   * substernal_extension (anatomic extension, not invasion)
    #   * esophageal_compression (compression, not invasion)
    #   * vascular_encasement (tumor-around-vessel, not vessel-wall
    #     invasion — different pathophysiology)
    #   * mass_effect (compression/displacement)
    #   * airway_compromise_grade (severity descriptor)
    #   * vocal_cord_imaging (finding category, not invasion)
    #   * vascular_invasion_type, vessel_count, mitotic_rate, necrosis,
    #     ptnm_stage, dedifferentiation (not invasion findings at all)
    # ==================================================================
    # ----- airway_invasion table (DIRECT-INVASION entities only)
    "airway_invasion": "airway",
    "laryngeal_invasion": "airway",
    "hypopharyngeal_invasion": "airway",
    "tracheal_invasion": "tracheal",
    "tracheal_involvement": "tracheal",
    "esophageal_invasion": "esophageal",
    "esophageal_involvement": "esophageal",
    # ETE entities — disambiguated via entity_value modifier in CTE.
    # The dict-level default remains gross_ete only for legacy callers that
    # do not pass entity_value; live SQL callers pass entity_value and return
    # microscopic, gross, or present_not_further_specified.
    "ete_on_imaging": "gross_ete",
    "extrathyroidal_extension": "gross_ete",
    "extrathyroidal_extension_present": "gross_ete",
    "extranodal_extension": "gross_ete",
    "strap_muscle_invasion": "gross_ete",
    # ----- vascular_invasion table (V/L SPLIT per v3)
    "vascular_invasion": "vascular_microscopic",
    "vascular_invasion_extensive": "vascular_microscopic",
    "vascular_invasion_focal": "vascular_microscopic",
    "angioinvasion": "vascular_microscopic",
    "lymphatic_invasion": "lymphatic_microscopic",
    "lymphovascular_invasion": "vascular_microscopic",  # rare composite
    # capsular / perineural / soft_tissue — split from v2 'local'
    "capsular_invasion": "capsular",
    "perineural_invasion": "perineural",
    "perineural_invasion_detailed": "perineural",
    "soft_tissue_invasion": "soft_tissue",
}

# ---------------------------------------------------------------------------
# Modality plan — config-driven CTE builder for Step 1.
# ---------------------------------------------------------------------------
# kind: 'structured_bool' | 'live_varchar' | 'live_varchar_ete'
#       | 'live_bigint' | 'archive_varchar' | 'archive_varchar_ete'
#       | 'archive_bigint' | 'llm_json_unnest'
# All entries also carry source_kind ∈ {'structured', 'llm'} which is
# emitted as a column on canonical_invasion_events_v1 (Pattern 13:
# orthogonal source_modality vs source_kind). source_modality is "what
# kind of document?", source_kind is "how was the evidence extracted?".
#
# Per Q1=C (hybrid):
#   synoptic_path → LIVE main.canonical_path_malignant_events_v1
#                   (361's deduped output; same patient-level coverage
#                    as archive but row count reduced from 11,106 → 6,689
#                    after dedup)
#   narrative_path → archives synoptic_tumor_long_v1_pre361_* +
#                    tumor_episode_master_v2_pre361_* (no live equivalent
#                    — Pattern 8: archive as permanent source dependency)
MODALITY_PLAN: list[dict[str, Any]] = [
    # ===== op_note (structured) — 4 BOOL flags from operative canonical
    {"modality": "op_note", "source_kind": "structured",
     "invasion_type": "gross_ete", "kind": "structured_bool",
     "source_schema": "main", "source_table": "canonical_operative_events_v1",
     "flag_col": "gross_ete_flag", "row_id_col": "surgery_episode_id",
     "date_col": "surgery_date_native", "rid_col": "research_id",
     "links_directly": True},
    {"modality": "op_note", "source_kind": "structured",
     "invasion_type": "tracheal", "kind": "structured_bool",
     "source_schema": "main", "source_table": "canonical_operative_events_v1",
     "flag_col": "tracheal_involvement_flag", "row_id_col": "surgery_episode_id",
     "date_col": "surgery_date_native", "rid_col": "research_id",
     "links_directly": True},
    {"modality": "op_note", "source_kind": "structured",
     "invasion_type": "esophageal", "kind": "structured_bool",
     "source_schema": "main", "source_table": "canonical_operative_events_v1",
     "flag_col": "esophageal_involvement_flag", "row_id_col": "surgery_episode_id",
     "date_col": "surgery_date_native", "rid_col": "research_id",
     "links_directly": True},
    # v3: local_invasion_flag → soft_tissue (intra-op surgeon-noted
    # direct extension into adjacent tissue; clinically the gross
    # equivalent of pathology's soft_tissue_invasion — the surgeon
    # doesn't differentiate capsular vs perineural at gross
    # inspection). Was 'local' in v2; removed from vocabulary.
    {"modality": "op_note", "source_kind": "structured",
     "invasion_type": "soft_tissue", "kind": "structured_bool",
     "source_schema": "main", "source_table": "canonical_operative_events_v1",
     "flag_col": "local_invasion_flag", "row_id_col": "surgery_episode_id",
     "date_col": "surgery_date_native", "rid_col": "research_id",
     "links_directly": True},
    # ===== op_note (LLM) — 2 source tables × OPNOTE note_type
    {"modality": "op_note", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_airway_invasion",
     "note_type_filter": "OPNOTE"},
    {"modality": "op_note", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_vascular_invasion",
     "note_type_filter": "OPNOTE",
     "table_suffix": "vasc"},
    # ===== synoptic_path (structured) — LIVE
    #       main.canonical_path_malignant_events_v1
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "ete_split", "kind": "live_varchar_ete",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "extrathyroidal_extension",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "gross_ete", "kind": "live_bigint",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "gross_ete",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "vascular_microscopic", "kind": "live_varchar",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "vascular_invasion",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "lymphatic_microscopic", "kind": "live_varchar",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "lymphatic_invasion",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "perineural", "kind": "live_varchar",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "perineural_invasion",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    {"modality": "synoptic_path", "source_kind": "structured",
     "invasion_type": "capsular", "kind": "live_varchar",
     "source_schema": "main", "source_table": "canonical_path_malignant_events_v1",
     "value_col": "capsular_invasion",
     "row_id_col": "path_surgery_id", "date_col": "surgery_date",
     "rid_col": "research_id"},
    # ===== synoptic_path (LLM) — note_entities_llm_*_invasion path_synoptics
    {"modality": "synoptic_path", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_airway_invasion",
     "note_type_filter": "path_synoptics"},
    {"modality": "synoptic_path", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_vascular_invasion",
     "note_type_filter": "path_synoptics",
     "table_suffix": "vasc"},
    # ===== narrative_path REMOVED in v3 (Logan rejection):
    # cross-DB sourcing from archive_pub_v1_0.* is forbidden. The 48
    # patients with gross_ete only in narrative_path archives (0.44%
    # cohort coverage loss) all have alternate gross_ete coverage from
    # other modalities. Acceptable per Logan's CHECKPOINT 1 follow-up.
    # ===== ct + mri (LLM only, from airway_invasion table)
    {"modality": "ct", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_airway_invasion",
     "note_type_filter": "ct_imaging"},
    {"modality": "mri", "source_kind": "llm",
     "kind": "llm_json_unnest",
     "source_schema": "main",
     "source_table": "note_entities_llm_airway_invasion",
     "note_type_filter": "mri_imaging"},
]

# ---------------------------------------------------------------------------
# Logging / utilities (mirrors Script 362)
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


def fq_archive(name: str) -> str:
    return f'{ARCHIVE_FQ}."{name}"'


def _validate_sql_identifier(name: str) -> str:
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


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str,
                 db: str = CANONICAL_DB) -> bool:
    if db == CANONICAL_DB:
        sql = ("SELECT 1 FROM information_schema.tables "
               "WHERE table_catalog=? AND table_schema=? AND table_name=?")
    else:
        # Archive DB needs a USE switch since duckdb can't qualify
        # information_schema across databases inline.
        con.execute(f'USE "{db}"')
        sql = ("SELECT 1 FROM information_schema.tables "
               "WHERE table_catalog=? AND table_schema=? AND table_name=?")
    row = con.execute(sql, [db, schema, table]).fetchone()
    if db != CANONICAL_DB:
        con.execute(f'USE "{CANONICAL_DB}"')
    return row is not None


def view_exists(con: duckdb.DuckDBPyConnection, schema: str, view: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "AND table_type='VIEW'",
        [CANONICAL_DB, schema, view],
    ).fetchone()
    return row is not None


def list_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str,
                 db: str = CANONICAL_DB) -> list[str]:
    if db != CANONICAL_DB:
        con.execute(f'USE "{db}"')
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "ORDER BY ordinal_position",
        [db, schema, table],
    ).fetchall()
    if db != CANONICAL_DB:
        con.execute(f'USE "{CANONICAL_DB}"')
    return [r[0] for r in rows]


def column_dtype(con: duckdb.DuckDBPyConnection, schema: str,
                 table: str, column: str, db: str = CANONICAL_DB) -> str | None:
    if db != CANONICAL_DB:
        con.execute(f'USE "{db}"')
    row = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "AND column_name=?",
        [db, schema, table, column],
    ).fetchone()
    if db != CANONICAL_DB:
        con.execute(f'USE "{CANONICAL_DB}"')
    return row[0] if row else None


def row_count(con: duckdb.DuckDBPyConnection, schema: str, table: str,
              db: str = CANONICAL_DB) -> int:
    if db == CANONICAL_DB:
        target = fq(schema, table)
    else:
        target = f'"{db}"."{schema}"."{table}"'
    return int(con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0])


def distinct_research_ids(con: duckdb.DuckDBPyConnection,
                          schema: str, table: str,
                          db: str = CANONICAL_DB) -> int:
    if "research_id" not in list_columns(con, schema, table, db):
        return -1
    if db == CANONICAL_DB:
        target = fq(schema, table)
    else:
        target = f'"{db}"."{schema}"."{table}"'
    return int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {target}"
    ).fetchone()[0])


def resolve_archive(con: duckdb.DuckDBPyConnection, pattern: str) -> str | None:
    """Pick the most recent archive table matching the LIKE pattern."""
    con.execute(f'USE "{ARCHIVE_DB}"')
    row = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema=? AND table_name LIKE ? "
        "ORDER BY table_name DESC LIMIT 1",
        [ARCHIVE_SCHEMA, pattern],
    ).fetchone()
    con.execute(f'USE "{CANONICAL_DB}"')
    return row[0] if row else None


# ---------------------------------------------------------------------------
# SQL fragment builders
# ---------------------------------------------------------------------------

def _norm_value_sql(col_expr: str) -> str:
    """Lowercase + trim trailing ';' and '.' for VARCHAR vocab matching."""
    return (
        f"LOWER(TRIM(BOTH ' ' FROM "
        f"REGEXP_REPLACE({col_expr}, '[;.]+$', '')))"
    )


def _status_case_sql(norm_expr: str) -> str:
    """Build a CASE WHEN ladder for VARCHAR_TO_FINDING_STATUS."""
    by_status: dict[str, list[str]] = {}
    for k, v in VARCHAR_TO_FINDING_STATUS.items():
        by_status.setdefault(v, []).append(k.replace("'", "''"))
    parts = ["CASE"]
    for status in ("absent", "indeterminate", "suspected", "present"):
        if status not in by_status:
            continue
        in_list = ", ".join(f"'{v}'" for v in by_status[status])
        parts.append(f"  WHEN {norm_expr} IN ({in_list}) THEN '{status}'")
    parts.append("  ELSE 'indeterminate'")
    parts.append("END")
    return "\n".join(parts)


def _ete_subtype_case_sql(norm_expr: str) -> str:
    """Build CASE WHEN ladder for EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE."""
    by_subtype: dict[str, list[str]] = {}
    for k, v in EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE.items():
        by_subtype.setdefault(v, []).append(k.replace("'", "''"))
    parts = ["CASE"]
    for subtype in (
        "microscopic_ete",
        "gross_ete",
        "ete_present_not_further_specified",
    ):
        if subtype not in by_subtype:
            continue
        in_list = ", ".join(f"'{v}'" for v in by_subtype[subtype])
        parts.append(f"  WHEN {norm_expr} IN ({in_list}) THEN '{subtype}'")
    parts.append("  ELSE 'ete_present_not_further_specified'")
    parts.append("END")
    return "\n".join(parts)


def _entity_invasion_case_sql(et_expr: str,
                              ev_expr: str | None = None) -> str:
    """Build CASE WHEN ladder for ENTITY_TYPE_TO_INVASION_TYPE.

    When `ev_expr` (entity_value SQL expression) is provided, ETE
    entity_types disambiguate to microscopic_ete, gross_ete, or
    ete_present_not_further_specified via the entity_value modifier.
    Generic yes/present ETE is not treated as gross.
    """
    ete_entities = [k for k, v in ENTITY_TYPE_TO_INVASION_TYPE.items()
                    if v == "gross_ete"]
    by_invtype: dict[str, list[str]] = {}
    for k, v in ENTITY_TYPE_TO_INVASION_TYPE.items():
        by_invtype.setdefault(v, []).append(k.replace("'", "''"))
    parts = ["CASE"]
    if ev_expr is not None and ete_entities:
        ete_in = ", ".join(f"'{v}'" for v in ete_entities)
        parts.append(
            f"  WHEN {et_expr} IN ({ete_in}) THEN CASE"
        )
        parts.append(
            f"    WHEN LOWER({ev_expr}) LIKE '%microscopic%' "
            f"OR LOWER({ev_expr}) LIKE '%minimal%' "
            f"OR LOWER({ev_expr}) LIKE '%focal%' "
            f"THEN 'microscopic_ete' "
            f"    WHEN LOWER({ev_expr}) LIKE '%gross%' "
            f"OR LOWER({ev_expr}) LIKE '%macroscopic%' "
            f"OR LOWER({ev_expr}) LIKE '%extensive%' "
            f"OR LOWER({ev_expr}) LIKE '%strap%' "
            f"OR LOWER({ev_expr}) LIKE '%trache%' "
            f"OR LOWER({ev_expr}) LIKE '%esophag%' "
            f"OR LOWER({ev_expr}) LIKE '%laryn%' "
            f"OR LOWER({ev_expr}) LIKE '%cricoid%' "
            f"OR LOWER({ev_expr}) LIKE '%cartilage%' "
            f"OR LOWER({ev_expr}) LIKE '%recurrent laryngeal%' "
            f"THEN 'gross_ete' "
            f"ELSE 'ete_present_not_further_specified' END"
        )
    for invtype in INVASION_TYPES:
        if invtype not in by_invtype:
            continue
        if invtype == "gross_ete" and ev_expr is not None:
            continue  # already handled above
        in_list = ", ".join(f"'{v}'" for v in by_invtype[invtype])
        parts.append(f"  WHEN {et_expr} IN ({in_list}) THEN '{invtype}'")
    parts.append("  ELSE NULL")
    parts.append("END")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Step 0 — Pre-flight, census, vocab + JSON probes
# ---------------------------------------------------------------------------

DEPENDENCY_TABLES = [
    ("main", "canonical_path_malignant_events_v1"),
    ("main", "canonical_path_benign_events_v1"),
    ("main", "canonical_operative_events_v1"),
    ("main", "canonical_frozen_section_events_v1"),
    ("main", "note_entities_llm_airway_invasion"),
    ("main", "note_entities_llm_vascular_invasion"),
]


def step_0_preflight(
    con: duckdb.DuckDBPyConnection, do_writes: bool, run_strip: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log(f"STEP 0 — Pre-flight + census + vocab/JSON probes (BUILD_TS={BUILD_TS})")
    log("=" * 78)

    # 0.a Dependency check.
    log("STEP 0.a — Dependency check")
    missing = []
    for s, t in DEPENDENCY_TABLES:
        if not table_exists(con, s, t):
            missing.append((s, t))
        else:
            n = row_count(con, s, t)
            log(f"  OK {s}.{t} ({n:,} rows)")
    if missing:
        raise SystemExit(f"Missing dependencies: {missing}")

    # v3: ARCHIVE_PATTERNS is intentionally empty (Pattern 8 REJECTED).
    # No source archive resolution needed.
    archive_resolved: dict[str, str | None] = {}
    if ARCHIVE_PATTERNS:
        for key, pattern in ARCHIVE_PATTERNS.items():
            name = resolve_archive(con, pattern)
            archive_resolved[key] = name
            if name is None:
                raise SystemExit(
                    f"No archive matched LIKE {pattern!r}"
                )
            n = row_count(con, ARCHIVE_SCHEMA, name, db=ARCHIVE_DB)
            log(f"  resolved archive {key}: {name} ({n:,} rows)")
    else:
        log("  ARCHIVE_PATTERNS empty — no source archives "
            "(v3: Pattern 8 rejected; cross-DB sourcing forbidden)")

    # 0.b Modality coverage census.
    log("STEP 0.b — Modality coverage census")
    census: list[dict[str, Any]] = []
    placeholder_modalities: list[str] = []

    # op_note / structured — count per invasion flag
    for col in ["gross_ete_flag", "tracheal_involvement_flag",
                "esophageal_involvement_flag", "local_invasion_flag"]:
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main','canonical_operative_events_v1')} "
            f"WHERE {col} IS NOT NULL"
        ).fetchone()[0])
        p = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM "
            f"{fq('main','canonical_operative_events_v1')} "
            f"WHERE {col} IS NOT NULL"
        ).fetchone()[0])
        census.append({"modality": "op_note", "source_kind": "structured",
                       "source": f"canonical_operative_events_v1.{col}",
                       "n_mentions": n, "n_patients": p})
        log(f"  op_note/structured / {col}: {n:,} non-null / {p:,} patients")

    # synoptic_path / structured — LIVE main.canonical_path_malignant_events_v1
    # (Q1=C: hybrid sourcing — live for synoptic_path, archive for
    # narrative_path)
    pm_invasion_cols = ["extrathyroidal_extension", "gross_ete",
                        "vascular_invasion", "lymphatic_invasion",
                        "perineural_invasion", "capsular_invasion"]
    pm_present_cols = list_columns(con, "main",
                                   "canonical_path_malignant_events_v1")
    for col in pm_invasion_cols:
        if col not in pm_present_cols:
            log(f"  LIVE main.canonical_path_malignant_events_v1.{col} "
                f"missing — skipping in census")
            continue
        n = int(con.execute(
            f'SELECT COUNT(*) FROM '
            f'{fq("main", "canonical_path_malignant_events_v1")} '
            f'WHERE "{col}" IS NOT NULL'
        ).fetchone()[0])
        p = int(con.execute(
            f'SELECT COUNT(DISTINCT research_id) FROM '
            f'{fq("main", "canonical_path_malignant_events_v1")} '
            f'WHERE "{col}" IS NOT NULL'
        ).fetchone()[0])
        census.append({"modality": "synoptic_path", "source_kind": "structured",
                       "source": f"LIVE.canonical_path_malignant_events_v1.{col}",
                       "n_mentions": n, "n_patients": p})
        log(f"  synoptic_path/structured / "
            f"LIVE.canonical_path_malignant_events_v1.{col}: "
            f"{n:,} non-null / {p:,} patients")

    # op_note + synoptic_path + ct + mri / llm — LLM unnested
    llm_modality_map = [
        ("op_note", "llm", "note_entities_llm_airway_invasion", "OPNOTE"),
        ("op_note", "llm", "note_entities_llm_vascular_invasion", "OPNOTE"),
        ("synoptic_path", "llm", "note_entities_llm_airway_invasion", "path_synoptics"),
        ("synoptic_path", "llm", "note_entities_llm_vascular_invasion", "path_synoptics"),
        ("ct", "llm", "note_entities_llm_airway_invasion", "ct_imaging"),
        ("mri", "llm", "note_entities_llm_airway_invasion", "mri_imaging"),
    ]
    llm_coverage_per_modality: dict[str, int] = {}
    for modality, source_kind, tbl, note_type in llm_modality_map:
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main', tbl)} "
            f"WHERE note_type=? AND LENGTH(result_json) > 100",
            [note_type],
        ).fetchone()[0])
        p = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {fq('main', tbl)} "
            f"WHERE note_type=? AND LENGTH(result_json) > 100",
            [note_type],
        ).fetchone()[0])
        census.append({"modality": modality, "source_kind": source_kind,
                       "source": f"{tbl} ({note_type})",
                       "n_mentions": n, "n_patients": p})
        log(f"  {modality}/{source_kind} / {tbl}({note_type}): "
            f"{n:,} substantive / {p:,} patients")
        llm_coverage_per_modality[modality] = (
            llm_coverage_per_modality.get(modality, 0) + p
        )

    # v3: narrative_path REMOVED. archive_value_cols dict kept empty
    # so the categorical vocab probe loop in 0.e doesn't reference
    # narrative archives. Pattern 8 rejected; cross-DB sourcing
    # forbidden. The 48 patients with gross_ete only in narrative
    # archives (0.44% cohort) have alternate gross_ete coverage from
    # other modalities — acceptable per Logan CHECKPOINT 1 follow-up.
    archive_value_cols: dict[str, list[str]] = {}

    # frozen_section probe
    fs_cols = list_columns(con, "main", "canonical_frozen_section_events_v1")
    fs_invasion_cols = [c for c in fs_cols if any(
        k in c.lower() for k in ("ete", "invas", "involv", "extrathy")
    )]
    for col in fs_invasion_cols:
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main','canonical_frozen_section_events_v1')} "
            f"WHERE {col} IS NOT NULL"
        ).fetchone()[0])
        p = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM "
            f"{fq('main','canonical_frozen_section_events_v1')} "
            f"WHERE {col} IS NOT NULL"
        ).fetchone()[0])
        census.append({"modality": "frozen_section",
                       "source": f"canonical_frozen_section_events_v1.{col}",
                       "n_mentions": n, "n_patients": p})
        log(f"  frozen_section / {col}: {n:,} non-null / {p:,} patients")
    if not fs_invasion_cols:
        census.append({"modality": "frozen_section",
                       "source": "(no invasion columns on canonical_frozen_section_events_v1)",
                       "n_mentions": 0, "n_patients": 0})
        log("  frozen_section: no native invasion columns found")
        placeholder_modalities.append("frozen_section")

    # Confirm zero-coverage modalities
    for zero_modality in ("ultrasound", "pet_ct", "nucmed"):
        census.append({"modality": zero_modality,
                       "source": "(no LLM extractor coverage)",
                       "n_mentions": 0, "n_patients": 0})
        if zero_modality not in placeholder_modalities:
            placeholder_modalities.append(zero_modality)
        log(f"  {zero_modality}: confirmed 0 coverage (per Q10 gap)")

    # Write census.md
    _write_census_md(census, placeholder_modalities, archive_resolved)

    # 0.c Strip target column existence check.
    log("STEP 0.c — Strip target column existence check")
    op_cols = list_columns(con, *STRIP_TARGET_TABLE)
    strip_cols_present = [c for c in STRIP_COLUMNS if c in op_cols]
    strip_cols_missing = [c for c in STRIP_COLUMNS if c not in op_cols]
    log(f"  strip-target cols present on {STRIP_TARGET_TABLE[1]}: "
        f"{strip_cols_present}")
    if strip_cols_missing:
        log_warn(f"  strip-target cols MISSING (already dropped?): "
                 f"{strip_cols_missing}")
    for forbidden in STRIP_FORBIDDEN_COLUMNS:
        if forbidden not in op_cols:
            log_warn(f"  forbidden-from-drop col {forbidden!r} NOT present "
                     f"on {STRIP_TARGET_TABLE[1]} — investigate "
                     f"(strap muscle should be permanent)")
        else:
            log(f"  forbidden-from-drop col {forbidden!r} present "
                f"(will not be dropped)")

    # 0.d Date column type probes.
    log("STEP 0.d — Date column type probes")
    date_probes = {
        "main.canonical_operative_events_v1.surgery_date_native": column_dtype(
            con, "main", "canonical_operative_events_v1",
            "surgery_date_native"),
        "main.canonical_path_malignant_events_v1.surg_date": column_dtype(
            con, "main", "canonical_path_malignant_events_v1", "surg_date"),
        "main.note_entities_llm_airway_invasion.note_date": column_dtype(
            con, "main", "note_entities_llm_airway_invasion", "note_date"),
        "main.note_entities_llm_vascular_invasion.note_date": column_dtype(
            con, "main", "note_entities_llm_vascular_invasion", "note_date"),
    }
    for k, v in date_probes.items():
        log(f"  date dtype probe {k}: {v}")

    # 0.e Categorical vocab probe.
    log("STEP 0.e — Categorical vocab probe")
    unmapped_categorical: dict[str, list[tuple[str, int]]] = {}
    vocab_md_lines = [
        f"# Invasion categorical vocab probe — {SCRIPT_TAG} ({RUN_DATE})",
        f"BUILD_TS: `{BUILD_TS}`",
        "",
        "Probes distinct VARCHAR values on each invasion source column "
        "(LIVE main.canonical_path_malignant_events_v1 for synoptic_path; "
        "ARCHIVE pre361 snapshots for narrative_path) and cross-checks "
        "against `VARCHAR_TO_FINDING_STATUS` and "
        "`EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE` defined at the top of "
        "`scripts/363_invasion_canonical.py`. Unmapped values are listed "
        "as carry-forward.",
        "",
    ]

    # Build a unified probe list: (label, db, schema, table, cols, modality)
    probe_targets: list[tuple[str, str, str, str, list[str], str]] = []
    pm_invasion_cols_local = ["extrathyroidal_extension",
                              "vascular_invasion", "lymphatic_invasion",
                              "perineural_invasion", "capsular_invasion"]
    probe_targets.append((
        "LIVE main.canonical_path_malignant_events_v1 (synoptic_path/structured)",
        CANONICAL_DB, "main", "canonical_path_malignant_events_v1",
        pm_invasion_cols_local, "synoptic_path",
    ))
    for key, cols_list in archive_value_cols.items():
        archive_name = archive_resolved.get(key)
        if not archive_name:
            continue
        probe_targets.append((
            f"ARCHIVE {archive_name} (narrative_path/structured)",
            ARCHIVE_DB, ARCHIVE_SCHEMA, archive_name,
            cols_list, "narrative_path",
        ))

    for label, db, schema, table, cols, _modality in probe_targets:
        vocab_md_lines.append(f"## {label}")
        vocab_md_lines.append("")
        present_cols = list_columns(con, schema, table, db=db)
        for col in cols:
            if col not in present_cols:
                continue
            dt = column_dtype(con, schema, table, col, db=db)
            if dt and dt.upper() not in ("VARCHAR", "TEXT", "STRING"):
                vocab_md_lines.append(f"### `{col}` :: {dt} (skip — non-VARCHAR)")
                vocab_md_lines.append("")
                continue
            if db != CANONICAL_DB:
                con.execute(f'USE "{db}"')
                fq_label = f'"{schema}"."{table}"'
            else:
                fq_label = fq(schema, table)
            rows = con.execute(
                f'SELECT "{col}" AS v, COUNT(*) AS n FROM '
                f'{fq_label} '
                f'WHERE "{col}" IS NOT NULL '
                f'GROUP BY "{col}" ORDER BY n DESC LIMIT 50'
            ).fetchall()
            if db != CANONICAL_DB:
                con.execute(f'USE "{CANONICAL_DB}"')
            vocab_md_lines.append(f"### `{col}` :: {dt}")
            vocab_md_lines.append("| n | value | mapped_status | mapped_subtype |")
            vocab_md_lines.append("|---|---|---|---|")
            unmapped_for_col: list[tuple[str, int]] = []
            for v, n in rows:
                v_norm = v.lower().strip().rstrip(";.").strip()
                status = VARCHAR_TO_FINDING_STATUS.get(v_norm)
                subtype = (EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE.get(v_norm)
                           if col == "extrathyroidal_extension" else None)
                if status is None:
                    unmapped_for_col.append((v, n))
                    status_disp = "**UNMAPPED→indeterminate**"
                else:
                    status_disp = f"`{status}`"
                subtype_disp = f"`{subtype}`" if subtype else "—"
                v_disp = (v[:60] + "…") if len(v) > 60 else v
                vocab_md_lines.append(
                    f"| {n:,} | `{v_disp}` | {status_disp} | {subtype_disp} |"
                )
            vocab_md_lines.append("")
            if unmapped_for_col:
                unmapped_categorical[f"{table}.{col}"] = unmapped_for_col

    if unmapped_categorical:
        vocab_md_lines.append("## ⚠️ Unmapped values summary (carry-forward)")
        vocab_md_lines.append("")
        for src, items in unmapped_categorical.items():
            vocab_md_lines.append(f"- **{src}**: " + ", ".join(
                f"`{v!r}`×{n}" for v, n in items
            ))

    VOCAB_PATH.write_text("\n".join(vocab_md_lines) + "\n", encoding="utf-8")
    log(f"  categorical vocab report -> {VOCAB_PATH}")
    log(f"  unmapped categorical sources: {len(unmapped_categorical)}")

    # 0.f result_json key probe.
    log("STEP 0.f — result_json key probe")
    json_md_lines = [
        f"# LLM result_json key probe — {SCRIPT_TAG} ({RUN_DATE})",
        f"BUILD_TS: `{BUILD_TS}`",
        "",
        "Samples ~30 substantive `result_json` rows per "
        "(table × note_type) combo and enumerates the distinct "
        "`entity_type` values + `entity_value` shapes. Unmapped "
        "`entity_type`s are listed as carry-forward (mapped to NULL "
        "invasion_type, then dropped from CTEs).",
        "",
    ]
    unmapped_entity_types: dict[str, list[tuple[str, int]]] = {}
    for tbl in ("note_entities_llm_airway_invasion",
                "note_entities_llm_vascular_invasion"):
        json_md_lines.append(f"## `{tbl}`")
        json_md_lines.append("")
        note_types = [r[0] for r in con.execute(
            f"SELECT DISTINCT note_type FROM {fq('main', tbl)} "
            f"WHERE note_type IS NOT NULL ORDER BY note_type"
        ).fetchall()]
        for nt in note_types:
            json_md_lines.append(f"### note_type=`{nt}`")
            json_md_lines.append("")
            samples = con.execute(
                f"SELECT result_json FROM {fq('main', tbl)} "
                f"WHERE note_type=? AND LENGTH(result_json) > 100 "
                f"LIMIT 30",
                [nt],
            ).fetchall()
            entity_type_counts: dict[str, int] = {}
            for (rj,) in samples:
                try:
                    parsed = json.loads(rj)
                    for ent in parsed.get("entities", []):
                        if isinstance(ent, dict):
                            et = ent.get("entity_type")
                            if et:
                                entity_type_counts[et] = (
                                    entity_type_counts.get(et, 0) + 1
                                )
                except Exception:
                    continue
            json_md_lines.append("| entity_type | n in sample | mapped_invasion_type |")
            json_md_lines.append("|---|---|---|")
            unmapped_for_table: list[tuple[str, int]] = []
            for et, n in sorted(entity_type_counts.items(),
                                key=lambda x: -x[1]):
                inv = ENTITY_TYPE_TO_INVASION_TYPE.get(et)
                if inv is None:
                    unmapped_for_table.append((et, n))
                    inv_disp = "**UNMAPPED→dropped**"
                else:
                    inv_disp = f"`{inv}`"
                json_md_lines.append(f"| `{et}` | {n} | {inv_disp} |")
            json_md_lines.append("")
            if unmapped_for_table:
                unmapped_entity_types[f"{tbl}.{nt}"] = unmapped_for_table

    if unmapped_entity_types:
        json_md_lines.append("## ⚠️ Unmapped entity_types (carry-forward)")
        json_md_lines.append("")
        for src, items in unmapped_entity_types.items():
            json_md_lines.append(f"- **{src}**: " + ", ".join(
                f"`{v}`×{n}" for v, n in items
            ))

    # v3 CHECKPOINT 1.G — count rows that WILL be excised by the v3
    # entity_type filter (mass-effect entities + non-invasion findings).
    # Probes the LLM tables directly with full json_extract to get
    # actual counts (not just sample counts).
    log("STEP 0.f.2 — Excised entity_type row counts (v3)")
    excised_targets = [
        "tracheal_deviation", "tracheal_displacement",
        "tracheal_compression", "tracheal_narrowing",
        "substernal_extension", "esophageal_compression",
        "vascular_encasement", "mass_effect",
        "airway_compromise_grade", "vocal_cord_imaging",
        "rln_involvement", "vascular_invasion_type", "vessel_count",
        "necrosis", "mitotic_rate", "ptnm_stage", "dedifferentiation",
    ]
    excised_counts: dict[str, dict[str, int]] = {}
    json_md_lines.extend([
        "",
        "## v3 EXCISED entity_type row counts (Logan CHECKPOINT 1.G)",
        "",
        "These entity_types are intentionally dropped from CTEs in v3 — "
        "they describe mass-effect / compression / staging / general "
        "histology, NOT invasion findings. Per Logan's rejection: "
        "tracheal_deviation, substernal_extension, esophageal_compression "
        "etc. belong in a future mass-effect canonical or 364 "
        "complications scope, not here.",
        "",
        "| source_table | entity_type | n_rows | n_patients |",
        "|---|---|---:|---:|",
    ])
    total_excised_rows = 0
    total_excised_patients_sources: dict[str, set[int]] = {}
    for tbl in ("note_entities_llm_airway_invasion",
                "note_entities_llm_vascular_invasion"):
        for et in excised_targets:
            try:
                row = con.execute(
                    f"""
                    WITH unnested AS (
                        SELECT TRY_CAST(research_id AS BIGINT) AS rid,
                               UNNEST(json_extract(result_json,
                                                   '$.entities')::JSON[])
                                 AS entity_json
                        FROM {fq('main', tbl)}
                        WHERE result_json LIKE '{{"entities":%'
                          AND LENGTH(result_json) > 100
                    )
                    SELECT COUNT(*) AS n_rows,
                           COUNT(DISTINCT rid) AS n_patients
                    FROM unnested
                    WHERE json_extract_string(entity_json,
                                              '$.entity_type') = ?
                    """, [et]
                ).fetchone()
            except duckdb.Error as exc:
                log_warn(f"  excised count probe failed for {tbl}.{et}: {exc}")
                continue
            n_rows, n_pats = row[0] or 0, row[1] or 0
            if n_rows == 0:
                continue
            excised_counts.setdefault(tbl, {})[et] = n_rows
            total_excised_rows += n_rows
            total_excised_patients_sources.setdefault(et, set())
            json_md_lines.append(
                f"| `{tbl}` | `{et}` | {n_rows:,} | {n_pats:,} |"
            )
            log(f"  EXCISED {tbl}.{et}: {n_rows:,} rows / "
                f"{n_pats:,} patients")
    json_md_lines.extend([
        "",
        f"**Total excised rows: {total_excised_rows:,}**",
    ])

    JSON_KEYS_PATH.write_text("\n".join(json_md_lines) + "\n",
                              encoding="utf-8")
    log(f"  LLM JSON key report -> {JSON_KEYS_PATH}")
    log(f"  unmapped entity_type sources: {len(unmapped_entity_types)}")
    log(f"  v3 excised entity_type total rows: {total_excised_rows:,}")

    # 0.g Pre-flight archive (only when Step 7 is in scope).
    archive_snapshot: str | None = None
    if run_strip:
        log("STEP 0.g — Pre-flight archive (Step 7 in scope)")
        archive_snapshot = _pre_strip_archive(con, do_writes)
    else:
        log("STEP 0.g — skipped (Step 7 not in scope this invocation)")

    return {
        "build_ts": BUILD_TS,
        "archive_resolved": archive_resolved,
        "census": census,
        "placeholder_modalities": placeholder_modalities,
        "strip_cols_present": strip_cols_present,
        "strip_cols_missing": strip_cols_missing,
        "date_probes": date_probes,
        "unmapped_categorical_count": len(unmapped_categorical),
        "unmapped_entity_type_count": len(unmapped_entity_types),
        "fs_invasion_cols": fs_invasion_cols,
        "archive_snapshot": archive_snapshot,
    }


def _write_census_md(census: list[dict[str, Any]],
                     placeholders: list[str],
                     archive_resolved: dict[str, str | None]) -> None:
    lines = [
        f"# Invasion coverage census — {SCRIPT_TAG} ({RUN_DATE})",
        f"BUILD_TS: `{BUILD_TS}`",
        "",
        "## Resolved archive snapshots",
        "",
    ]
    for k, v in archive_resolved.items():
        lines.append(f"- `{k}` → `{v}`")
    lines.extend([
        "",
        "## Coverage matrix (Pattern 13: source_modality × source_kind)",
        "",
        "| modality | source_kind | source | n_mentions | n_patients |",
        "|---|---|---|---:|---:|",
    ])
    for c in census:
        kind = c.get("source_kind", "—")
        lines.append(
            f"| `{c['modality']}` | `{kind}` | `{c['source']}` | "
            f"{c['n_mentions']:,} | {c['n_patients']:,} |"
        )
    lines.extend([
        "",
        f"## Placeholder modalities (n_patients=0): {placeholders or '(none)'}",
        "",
        "Placeholder modalities are dropped from the build per Pattern 11 "
        "(modality coverage census → placeholder). Their absence is "
        "documented as a carry-forward gap; downstream NLP work would be "
        "needed to populate.",
    ])
    CENSUS_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"  census report -> {CENSUS_PATH}")


def _pre_strip_archive(con: duckdb.DuckDBPyConnection,
                       do_writes: bool) -> str | None:
    schema, table = STRIP_TARGET_TABLE
    archive_name = f"{table}_pre363strip_{BUILD_TS}"
    n_live = row_count(con, schema, table)
    log(f"  pre-strip archive plan: {schema}.{table} ({n_live:,} rows) "
        f"-> {archive_name}")
    if not do_writes:
        return None
    con.execute(f'USE "{ARCHIVE_DB}"')
    candidates = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema=? AND table_name LIKE ? "
        "ORDER BY table_name DESC",
        [ARCHIVE_SCHEMA, f"{table}_pre363strip_%"],
    ).fetchall()
    matched = None
    for (cand,) in candidates:
        cnt = int(con.execute(
            f'SELECT COUNT(*) FROM "{ARCHIVE_SCHEMA}"."{cand}"'
        ).fetchone()[0])
        if cnt == n_live:
            matched = cand
            break
    con.execute(f'USE "{CANONICAL_DB}"')
    if matched:
        log(f"  pre-strip archive already exists w/ matching row count: "
            f"{matched} — reusing")
        return matched
    src = fq(schema, table)
    dst = fq_archive(archive_name)
    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
    if n_dst != n_live:
        raise RuntimeError(
            f"Pre-strip archive mismatch: live={n_live} dst={n_dst}"
        )
    try:
        con.execute(
            f"COMMENT ON TABLE {dst} IS "
            f"'{SCRIPT_TAG} ({RUN_DATE}) pre-strip snapshot of "
            f"main.{table} — captures the 4 invasion BOOL flags before "
            f"Step 7 cascade strip.'"
        )
    except duckdb.Error as exc:
        log_warn(f"  COMMENT ON {archive_name} failed (non-fatal): {exc}")
    log(f"  pre-strip archive created: {archive_name} ({n_dst:,} rows)")
    return archive_name


# ---------------------------------------------------------------------------
# Step 1 — Build canonical_invasion_events_v1
# ---------------------------------------------------------------------------

def _build_cte_structured_bool(plan: dict[str, Any]) -> str:
    """CTE for structured BOOLEAN flag sources (op_note from
    canonical_operative_events_v1)."""
    schema = plan["source_schema"]
    table = plan["source_table"]
    flag_col = _validate_sql_identifier(plan["flag_col"])
    row_id_col = _validate_sql_identifier(plan["row_id_col"])
    date_col = _validate_sql_identifier(plan["date_col"])
    rid_col = _validate_sql_identifier(plan["rid_col"])
    invasion_type = plan["invasion_type"]
    cte_name = f"cte_{plan['modality']}_{plan['source_kind']}_{invasion_type}"
    return f"""
{cte_name} AS (
    SELECT
        '{invasion_type}' AS invasion_type,
        CASE
            WHEN {flag_col} = TRUE  THEN 'present'
            WHEN {flag_col} = FALSE THEN 'absent'
            ELSE 'indeterminate'
        END AS finding_status,
        '{plan["modality"]}' AS source_modality,
        '{plan["source_kind"]}' AS source_kind,
        '{schema}.{table}' AS source_table,
        CAST({row_id_col} AS VARCHAR) AS source_row_id,
        TRY_CAST({rid_col} AS BIGINT) AS research_id,
        TRY_CAST({date_col} AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        NULL::VARCHAR AS evidence_qualifier,
        NULL::VARCHAR AS extraction_run_id,
        TRY_CAST({row_id_col} AS BIGINT) AS exact_linked_episode_id
    FROM {fq(schema, table)}
    WHERE {flag_col} IS NOT NULL
)"""


def _live_or_archive_qualifier(plan: dict[str, Any],
                                archive_name: str | None) -> tuple[str, str]:
    """Resolve (table_fq, source_table_label) for live vs archive plans."""
    kind = plan["kind"]
    if kind.startswith("live_"):
        schema = plan["source_schema"]
        table = plan["source_table"]
        return fq(schema, table), f"{schema}.{table}"
    if kind.startswith("archive_"):
        if not archive_name:
            raise ValueError("archive plan missing resolved name")
        return fq_archive(archive_name), f"archive_pub_v1_0.{archive_name}"
    raise ValueError(f"unsupported kind: {kind}")


def _build_cte_varchar_generic(plan: dict[str, Any],
                                archive_name: str | None) -> str:
    """Generic VARCHAR mapping CTE — handles both live_varchar and
    archive_varchar."""
    value_col = _validate_sql_identifier(plan["value_col"])
    row_id_col = _validate_sql_identifier(plan["row_id_col"])
    date_col = _validate_sql_identifier(plan["date_col"])
    rid_col = _validate_sql_identifier(plan["rid_col"])
    invasion_type = plan["invasion_type"]
    suffix = plan.get("value_suffix", "")
    cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                f"{invasion_type}_{value_col}"
                f"{('_' + suffix) if suffix else ''}")
    norm_expr = _norm_value_sql(f'"{value_col}"')
    status_case = _status_case_sql(norm_expr)
    table_fq, table_label = _live_or_archive_qualifier(plan, archive_name)
    return f"""
{cte_name} AS (
    SELECT
        '{invasion_type}' AS invasion_type,
        {status_case} AS finding_status,
        '{plan["modality"]}' AS source_modality,
        '{plan["source_kind"]}' AS source_kind,
        '{table_label}' AS source_table,
        '{value_col}' || '|' || CAST("{row_id_col}" AS VARCHAR) AS source_row_id,
        TRY_CAST("{rid_col}" AS BIGINT) AS research_id,
        TRY_CAST("{date_col}" AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        "{value_col}" AS evidence_qualifier,
        NULL::VARCHAR AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM {table_fq}
    WHERE "{value_col}" IS NOT NULL
)"""


def _build_cte_varchar_ete_generic(plan: dict[str, Any],
                                    archive_name: str | None) -> str:
    """ETE-split CTE — handles live_varchar_ete and archive_varchar_ete."""
    value_col = _validate_sql_identifier(plan["value_col"])
    row_id_col = _validate_sql_identifier(plan["row_id_col"])
    date_col = _validate_sql_identifier(plan["date_col"])
    rid_col = _validate_sql_identifier(plan["rid_col"])
    suffix = plan.get("value_suffix", "")
    cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_ete_"
                f"{value_col}{('_' + suffix) if suffix else ''}")
    norm_expr = _norm_value_sql(f'"{value_col}"')
    status_case = _status_case_sql(norm_expr)
    subtype_case = _ete_subtype_case_sql(norm_expr)
    table_fq, table_label = _live_or_archive_qualifier(plan, archive_name)
    return f"""
{cte_name} AS (
    SELECT
        {subtype_case} AS invasion_type,
        {status_case} AS finding_status,
        '{plan["modality"]}' AS source_modality,
        '{plan["source_kind"]}' AS source_kind,
        '{table_label}' AS source_table,
        '{value_col}' || '|' || CAST("{row_id_col}" AS VARCHAR) AS source_row_id,
        TRY_CAST("{rid_col}" AS BIGINT) AS research_id,
        TRY_CAST("{date_col}" AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        "{value_col}" AS evidence_qualifier,
        NULL::VARCHAR AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM {table_fq}
    WHERE "{value_col}" IS NOT NULL
)"""


def _build_cte_bigint_generic(plan: dict[str, Any],
                               archive_name: str | None) -> str:
    """BIGINT presence flag — handles live_bigint and archive_bigint."""
    value_col = _validate_sql_identifier(plan["value_col"])
    row_id_col = _validate_sql_identifier(plan["row_id_col"])
    date_col = _validate_sql_identifier(plan["date_col"])
    rid_col = _validate_sql_identifier(plan["rid_col"])
    invasion_type = plan["invasion_type"]
    suffix = plan.get("value_suffix", "")
    cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                f"{invasion_type}_{value_col}"
                f"{('_' + suffix) if suffix else ''}")
    table_fq, table_label = _live_or_archive_qualifier(plan, archive_name)
    return f"""
{cte_name} AS (
    SELECT
        '{invasion_type}' AS invasion_type,
        CASE
            WHEN "{value_col}" >= 1 THEN 'present'
            WHEN "{value_col}" = 0  THEN 'absent'
            ELSE 'indeterminate'
        END AS finding_status,
        '{plan["modality"]}' AS source_modality,
        '{plan["source_kind"]}' AS source_kind,
        '{table_label}' AS source_table,
        '{value_col}' || '|' || CAST("{row_id_col}" AS VARCHAR) AS source_row_id,
        TRY_CAST("{rid_col}" AS BIGINT) AS research_id,
        TRY_CAST("{date_col}" AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        CAST("{value_col}" AS VARCHAR) AS evidence_qualifier,
        NULL::VARCHAR AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM {table_fq}
    WHERE "{value_col}" IS NOT NULL
)"""


def _build_cte_llm_json(plan: dict[str, Any]) -> str:
    """Pattern 14 (LLM result_json UNNEST template).

    Reusable across 363 / 364 / 365 / 366 / 367 — every
    note_entities_llm_* table follows the same JSON shape:
        {"entities": [{
            "entity_type", "entity_value", "entity_date",
            "date_confidence", "date_source_keyword",
            "present_or_negated", "confidence",
            "evidence_text", "source_line"
        }]}

    Some rows contain {"error": "..."} instead of {"entities": [...]}
    (extraction failures). The `result_json LIKE '{"entities":%'`
    filter skips them so UNNEST doesn't blow up on null arrays.
    """
    schema = plan["source_schema"]
    table = plan["source_table"]
    note_type = plan["note_type_filter"]
    suffix = plan.get("table_suffix", "")
    cte_unnest = (f"unnest_{plan['modality']}_{plan['source_kind']}_"
                  f"{table}{('_' + suffix) if suffix else ''}")
    cte_final = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                 f"{table}{('_' + suffix) if suffix else ''}")
    invasion_case = _entity_invasion_case_sql(
        "json_extract_string(entity_json, '$.entity_type')",
        "json_extract_string(entity_json, '$.entity_value')",
    )
    return f"""
{cte_unnest} AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        note_row_id,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        extracted_at,
        llm_model,
        UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS entity_json
    FROM {fq(schema, table)}
    WHERE note_type = '{note_type}'
      -- Pattern 14: skip {{"error":...}} rows so UNNEST doesn't break
      AND result_json LIKE '{{"entities":%'
      AND LENGTH(result_json) > 100
),
{cte_final} AS (
    SELECT
        {invasion_case} AS invasion_type,
        -- v3 LLM finding_status ladder (CHECKPOINT 1 actuals iter 2):
        -- Logan caught a bucketing bug in v3-iter-1 where the LLM CTE
        -- fell through to present_or_negated='present' for any
        -- entity_value not in the explicit absent/present/suspicious
        -- whitelist, mis-classifying "cannot be assessed" / "possible"
        -- / "equivocal" / "pending" / "cannot be ruled out" etc. as
        -- present. Inflated capsular by ~330 patients, vascular by
        -- ~95, perineural by ~10. Corrected ladder per Logan: SPECIFIC
        -- suspected-cannot-be-ruled/excluded patterns BEFORE generic
        -- %cannot% indeterminate catch-all.
        CASE
            -- 1. Exact-match ABSENT (most specific)
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                IN ('absent','negated','no','none',
                    'not identified','negative') THEN 'absent'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE 'no %' THEN 'absent'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE 'not %' THEN 'absent'

            -- 2. SUSPECTED — specific "cannot be ruled/excluded" FIRST
            --    (must come before generic %cannot% -> indeterminate)
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot be ruled%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot be entirely%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot be excluded%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot_be_ruled%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot_be_excluded%'
                THEN 'suspected'

            -- 3. SUSPECTED — other hedged-positive language
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%possibl%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%suspici%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%suspect%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%suggest%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%probabl%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%question%'
                THEN 'suspected'

            -- 4. INDETERMINATE — generic %cannot% catch-all + others
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%cannot_be%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%not assess%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%undetermined%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%pending%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%uncertain%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE '%difficult to assess%'
              OR LOWER(json_extract_string(entity_json, '$.entity_value'))
                IN ('indeterminate','equivocal','ambiguous',
                    'unclear','n/s','n/a','indefinite')
                THEN 'indeterminate'

            -- 5. Explicit PRESENT keywords (Logan dict)
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                IN ('present','yes','true','identified','positive',
                    'extensive','focal','minimal','minimally invasive',
                    'widely invasive','multifocal','infiltrative',
                    'invasive','microscopic','partial') THEN 'present'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE 'yes%' THEN 'present'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE 'present, %' THEN 'present'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                LIKE 'present (%' THEN 'present'

            -- 6. Fallback to present_or_negated ONLY for empty values
            WHEN COALESCE(json_extract_string(
                          entity_json, '$.entity_value'), '') = '' THEN
                CASE WHEN json_extract_string(entity_json,
                                              '$.present_or_negated')
                              = 'present' THEN 'present'
                     WHEN json_extract_string(entity_json,
                                              '$.present_or_negated')
                              = 'negated' THEN 'absent'
                     ELSE 'indeterminate' END

            -- 7. Non-empty entity_value with no pattern match → INDETERMINATE
            ELSE 'indeterminate'
        END AS finding_status,
        '{plan["modality"]}' AS source_modality,
        '{plan["source_kind"]}' AS source_kind,
        'main.{table}' AS source_table,
        note_row_id || '|' || COALESCE(
            json_extract_string(entity_json, '$.source_line'), ''
        ) || '|' || COALESCE(
            json_extract_string(entity_json, '$.entity_type'), ''
        ) AS source_row_id,
        research_id,
        COALESCE(
            TRY_CAST(json_extract_string(entity_json, '$.entity_date') AS DATE),
            note_date_parsed
        ) AS finding_date,
        TRY_CAST(json_extract_string(entity_json, '$.confidence') AS DOUBLE)
            AS confidence,
        md5(COALESCE(
            json_extract_string(entity_json, '$.evidence_text'), ''
        )) AS evidence_span_hash,
        json_extract_string(entity_json, '$.entity_value')
            AS evidence_qualifier,
        COALESCE(extracted_at, '') || '|' || COALESCE(llm_model, '')
            AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM {cte_unnest}
    WHERE {invasion_case} IS NOT NULL
)"""


def _build_step_1_sql(con: duckdb.DuckDBPyConnection,
                      archive_resolved: dict[str, str | None],
                      placeholder_modalities: list[str]
                      ) -> tuple[str, list[str], list[str]]:
    """Returns (full SQL, list of CTE names, list of skipped plans)."""
    ctes: list[str] = []
    cte_names: list[str] = []
    skipped: list[str] = []
    for plan in MODALITY_PLAN:
        if plan["modality"] in placeholder_modalities:
            tag = plan.get("invasion_type",
                           plan.get("note_type_filter", "?"))
            skipped.append(f"{plan['modality']}/{plan['source_kind']}/{tag} "
                           f"(modality placeholder)")
            continue
        kind = plan["kind"]
        suffix = plan.get("value_suffix", "")
        try:
            if kind == "structured_bool":
                cols = list_columns(con, plan["source_schema"],
                                    plan["source_table"])
                if plan["flag_col"] not in cols:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['invasion_type']} "
                        f"(missing col {plan['flag_col']})")
                    continue
                cte_sql = _build_cte_structured_bool(plan)
                cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                            f"{plan['invasion_type']}")
            elif kind in ("live_varchar", "archive_varchar"):
                archive_name = (archive_resolved.get(plan["archive_table_key"])
                                if kind == "archive_varchar" else None)
                if kind == "archive_varchar" and not archive_name:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['invasion_type']} "
                        f"(no archive for {plan['archive_table_key']})")
                    continue
                if kind == "live_varchar":
                    cols = list_columns(con, plan["source_schema"],
                                        plan["source_table"])
                else:
                    cols = list_columns(con, ARCHIVE_SCHEMA, archive_name,
                                        db=ARCHIVE_DB)
                if plan["value_col"] not in cols:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['invasion_type']} "
                        f"(missing col {plan['value_col']})")
                    continue
                cte_sql = _build_cte_varchar_generic(plan, archive_name)
                cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                            f"{plan['invasion_type']}_{plan['value_col']}"
                            f"{('_' + suffix) if suffix else ''}")
            elif kind in ("live_varchar_ete", "archive_varchar_ete"):
                archive_name = (archive_resolved.get(plan["archive_table_key"])
                                if kind == "archive_varchar_ete" else None)
                if kind == "archive_varchar_ete" and not archive_name:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/ete_split "
                        f"(no archive for {plan['archive_table_key']})")
                    continue
                if kind == "live_varchar_ete":
                    cols = list_columns(con, plan["source_schema"],
                                        plan["source_table"])
                else:
                    cols = list_columns(con, ARCHIVE_SCHEMA, archive_name,
                                        db=ARCHIVE_DB)
                if plan["value_col"] not in cols:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/ete_split "
                        f"(missing col {plan['value_col']})")
                    continue
                cte_sql = _build_cte_varchar_ete_generic(plan, archive_name)
                cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                            f"ete_{plan['value_col']}"
                            f"{('_' + suffix) if suffix else ''}")
            elif kind in ("live_bigint", "archive_bigint"):
                archive_name = (archive_resolved.get(plan["archive_table_key"])
                                if kind == "archive_bigint" else None)
                if kind == "archive_bigint" and not archive_name:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['invasion_type']} "
                        f"(no archive for {plan['archive_table_key']})")
                    continue
                if kind == "live_bigint":
                    cols = list_columns(con, plan["source_schema"],
                                        plan["source_table"])
                else:
                    cols = list_columns(con, ARCHIVE_SCHEMA, archive_name,
                                        db=ARCHIVE_DB)
                if plan["value_col"] not in cols:
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['invasion_type']} "
                        f"(missing col {plan['value_col']})")
                    continue
                cte_sql = _build_cte_bigint_generic(plan, archive_name)
                cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                            f"{plan['invasion_type']}_{plan['value_col']}"
                            f"{('_' + suffix) if suffix else ''}")
            elif kind == "llm_json_unnest":
                if not table_exists(con, plan["source_schema"],
                                    plan["source_table"]):
                    skipped.append(
                        f"{plan['modality']}/{plan['source_kind']}/"
                        f"{plan['source_table']} (missing table)")
                    continue
                cte_sql = _build_cte_llm_json(plan)
                tsuffix = plan.get("table_suffix", "")
                cte_name = (f"cte_{plan['modality']}_{plan['source_kind']}_"
                            f"{plan['source_table']}"
                            f"{('_' + tsuffix) if tsuffix else ''}")
            else:
                skipped.append(f"{plan['modality']}/{plan['source_kind']} "
                               f"(unknown kind={kind})")
                continue
            ctes.append(cte_sql)
            cte_names.append(cte_name)
        except Exception as exc:
            log_warn(f"  CTE build failed for {plan}: {exc}")
            skipped.append(f"{plan['modality']}/{plan['source_kind']} "
                           f"(build failed: {exc})")

    if not ctes:
        raise RuntimeError("No CTEs produced — abort")

    cte_block = "WITH\n" + ",\n".join(ctes)
    union_block = "\nUNION ALL\n".join(
        f"SELECT * FROM {n}" for n in cte_names
    )

    final_sql = f"""
CREATE OR REPLACE TABLE {fq('main','canonical_invasion_events_v1')} AS
{cte_block},
all_findings AS (
{union_block}
),
linked AS (
    SELECT
        f.*,
        COALESCE(
            f.exact_linked_episode_id,
            (SELECT MIN(oe.surgery_episode_id)
               FROM {fq('main','canonical_operative_events_v1')} oe
              WHERE TRY_CAST(oe.research_id AS BIGINT) = f.research_id
                AND ABS(DATE_DIFF('day',
                        TRY_CAST(oe.surgery_date_native AS DATE),
                        f.finding_date)) <= 90)
        ) AS linked_surgery_episode_id,
        (SELECT MIN(pm.path_surgery_id)
            FROM {fq('main','canonical_path_malignant_events_v1')} pm
           WHERE pm.research_id = f.research_id
             AND TRY_CAST(pm.surgery_date AS DATE) = f.finding_date
        ) AS linked_path_malignant_event_id,
        COUNT(*) OVER (PARTITION BY f.research_id, f.finding_date)
            AS n_candidate_episodes_window
    FROM all_findings f
)
SELECT
    md5(
        CAST(research_id AS VARCHAR) || '|' ||
        source_modality || '|' ||
        source_kind || '|' ||
        source_table || '|' ||
        source_row_id || '|' ||
        invasion_type
    ) AS invasion_event_id,
    research_id,
    invasion_type,
    finding_status,
    source_modality,
    source_kind,
    source_table,
    source_row_id,
    finding_date,
    linked_surgery_episode_id,
    linked_path_malignant_event_id,
    CASE
        WHEN exact_linked_episode_id IS NOT NULL
            THEN 'na_source_is_surgical'
        WHEN linked_surgery_episode_id IS NULL THEN 'unlinked'
        WHEN n_candidate_episodes_window > 1 THEN 'temporal_90d_ambiguous'
        ELSE 'temporal_90d'
    END AS linkage_method,
    CAST(n_candidate_episodes_window AS INTEGER) AS n_candidate_episodes,
    (n_candidate_episodes_window > 1
     AND exact_linked_episode_id IS NULL) AS linkage_ambiguous_multi_finding,
    confidence,
    evidence_span_hash,
    evidence_qualifier,
    extraction_run_id,
    '363'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM linked
WHERE research_id IS NOT NULL
  AND invasion_type IS NOT NULL
"""
    return final_sql, cte_names, skipped


def step_1_build_events(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
    archive_resolved: dict[str, str | None],
    placeholder_modalities: list[str],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1 — Build main.canonical_invasion_events_v1")
    log("=" * 78)
    sql, cte_names, skipped = _build_step_1_sql(
        con, archive_resolved, placeholder_modalities)
    log(f"  SQL built — {len(cte_names)} CTEs, {len(skipped)} skipped")
    if skipped:
        for s in skipped:
            log(f"    skipped: {s}")
    if not do_writes:
        log("  [dry-run] would CREATE OR REPLACE canonical_invasion_events_v1")
        log("  SQL preview (first 600 chars):")
        log(f"    {sql[:600]} …")
        return {"created": False, "rows": -1, "patients": -1,
                "ctes": cte_names, "skipped_ctes": skipped}
    con.execute(sql)
    n = row_count(con, "main", "canonical_invasion_events_v1")
    p = distinct_research_ids(con, "main", "canonical_invasion_events_v1")
    log(f"  built canonical_invasion_events_v1: {n:,} rows / {p:,} patients")
    try:
        con.execute(
            f"COMMENT ON TABLE {fq('main','canonical_invasion_events_v1')} IS "
            f"'[domain=invasion_findings; grain=per_invasion_mention] — "
            f"source: {SCRIPT_TAG} ({RUN_DATE}). Cross-modal canonical "
            f"unifying invasion findings from op_note + LLM extractions + "
            f"pre361 path archive snapshots. Pattern 8 (archive as "
            f"permanent source dependency).'"
        )
    except duckdb.Error as exc:
        log_warn(f"  COMMENT ON canonical_invasion_events_v1 failed: {exc}")
    return {"created": True, "rows": n, "patients": p,
            "ctes": cte_names, "skipped_ctes": skipped}


# ---------------------------------------------------------------------------
# Step 2 — Build canonical_invasion_patient_rollup_v1
# ---------------------------------------------------------------------------

def step_2_build_rollup(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
    placeholder_modalities: list[str],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Build main.canonical_invasion_patient_rollup_v1")
    log("=" * 78)

    # Determine live (modality, source_kind) combos from MODALITY_PLAN.
    # Per Q2=C the rollup aggregates by modality only — consumers wanting
    # per-source_kind breakdown go to the events table.
    live_modalities = sorted({
        p["modality"] for p in MODALITY_PLAN
        if p["modality"] not in placeholder_modalities
    })
    log(f"  live modalities: {live_modalities}")
    live_source_kinds = sorted({
        (p["modality"], p["source_kind"]) for p in MODALITY_PLAN
        if p["modality"] not in placeholder_modalities
    })
    log(f"  live (modality, source_kind) combos: {live_source_kinds}")

    # Per Logan's CHECKPOINT 1 trim: rollup is cross-modal BOOL flags
    # ONLY. Drop per-(type,modality) cols + drop earliest/latest dates +
    # drop n_modalities_with_<type>. Consumers needing those breakdowns
    # query the events table directly. Keeps the rollup at ~21 cols.
    op_or_path_modalities = [
        m for m in ["op_note", "synoptic_path", "narrative_path",
                    "frozen_section"]
        if m in live_modalities
    ]
    imaging_modalities = [
        m for m in ["ct", "mri", "ultrasound", "pet_ct", "nucmed"]
        if m in live_modalities
    ]

    cross_modal_clauses: list[str] = []
    for inv in INVASION_TYPES:
        cross_modal_clauses.append(
            f"BOOL_OR(invasion_type='{inv}' AND finding_status='present') "
            f"AS any_{inv}_anywhere"
        )
        if op_or_path_modalities:
            mods_in = ", ".join(f"'{m}'" for m in op_or_path_modalities)
            cross_modal_clauses.append(
                f"BOOL_OR(invasion_type='{inv}' AND finding_status='present' "
                f"AND source_modality IN ({mods_in})) "
                f"AS any_{inv}_in_op_or_path"
            )
        else:
            cross_modal_clauses.append(
                f"FALSE::BOOLEAN AS any_{inv}_in_op_or_path"
            )
        if imaging_modalities:
            mods_in = ", ".join(f"'{m}'" for m in imaging_modalities)
            cross_modal_clauses.append(
                f"BOOL_OR(invasion_type='{inv}' AND finding_status='present' "
                f"AND source_modality IN ({mods_in})) "
                f"AS any_{inv}_in_imaging"
            )
        else:
            cross_modal_clauses.append(
                f"FALSE::BOOLEAN AS any_{inv}_in_imaging"
            )

    ete_union = (
        "('gross_ete', 'microscopic_ete', "
        "'ete_present_not_further_specified', 'soft_tissue')"
    )
    cross_modal_clauses.extend([
        "BOOL_OR(invasion_type IN "
        f"{ete_union} AND finding_status='present') AS any_ete_anywhere",
    ])
    if op_or_path_modalities:
        mods_in = ", ".join(f"'{m}'" for m in op_or_path_modalities)
        cross_modal_clauses.append(
            "BOOL_OR(invasion_type IN "
            f"{ete_union} AND finding_status='present' "
            f"AND source_modality IN ({mods_in})) AS any_ete_in_op_or_path"
        )
    else:
        cross_modal_clauses.append("FALSE::BOOLEAN AS any_ete_in_op_or_path")
    if imaging_modalities:
        mods_in = ", ".join(f"'{m}'" for m in imaging_modalities)
        cross_modal_clauses.append(
            "BOOL_OR(invasion_type IN "
            f"{ete_union} AND finding_status='present' "
            f"AND source_modality IN ({mods_in})) AS any_ete_in_imaging"
        )
    else:
        cross_modal_clauses.append("FALSE::BOOLEAN AS any_ete_in_imaging")

    select_csv = ",\n        ".join(cross_modal_clauses)

    sql = f"""
CREATE OR REPLACE TABLE {fq('main','canonical_invasion_patient_rollup_v1')} AS
SELECT
    research_id,
    {select_csv},
    '363'::VARCHAR AS build_script,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS build_ts
FROM {fq('main','canonical_invasion_events_v1')}
GROUP BY research_id
"""
    log(f"  SQL built ({len(cross_modal_clauses)} cross-modal "
        f"BOOL flag columns)")
    if not do_writes:
        log("  [dry-run] would CREATE OR REPLACE "
            "canonical_invasion_patient_rollup_v1")
        return {"created": False, "rows": -1,
                "n_per_combo_cols": 0,
                "n_cross_modal_cols": len(cross_modal_clauses)}
    con.execute(sql)
    n = row_count(con, "main", "canonical_invasion_patient_rollup_v1")
    log(f"  built canonical_invasion_patient_rollup_v1: {n:,} rows")
    try:
        con.execute(
            f"COMMENT ON TABLE "
            f"{fq('main','canonical_invasion_patient_rollup_v1')} IS "
            f"'[domain=invasion_findings; grain=per_patient] — source: "
            f"{SCRIPT_TAG} ({RUN_DATE}). Patient-level invasion finding "
            f"rollup with cross-modal aggregates and per-(type,modality) "
            f"booleans.'"
        )
    except duckdb.Error as exc:
        log_warn(f"  COMMENT failed: {exc}")
    return {"created": True, "rows": n,
            "n_per_combo_cols": 0,
            "n_cross_modal_cols": len(cross_modal_clauses)}


# ---------------------------------------------------------------------------
# Step 3 — Views (2)
# ---------------------------------------------------------------------------

def step_3_build_views(con: duckdb.DuckDBPyConnection,
                       do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Create / refresh views_readable views")
    log("=" * 78)
    out: list[str] = []
    for view_name, base_table in NEW_VIEWS:
        if not table_exists(con, "main", base_table):
            log_warn(f"  base table main.{base_table} missing — skip view "
                     f"{view_name}")
            continue
        sql = (f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
               f'"{view_name}" AS SELECT * FROM {fq("main", base_table)}')
        if do_writes:
            con.execute(sql)
        log(f"  view {VIEW_SCHEMA}.{view_name} -> main.{base_table}")
        out.append(view_name)
    return {"views": out}


# ---------------------------------------------------------------------------
# Step 4 — Registry sync (Pattern 12: idempotent DELETE-first)
# ---------------------------------------------------------------------------

def step_4_registry_sync(con: duckdb.DuckDBPyConnection,
                         do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — detail_table_registry_v1 sync (DELETE-first idempotent)")
    log("=" * 78)
    if not table_exists(con, WS_SCHEMA, REGISTRY_TABLE):
        raise RuntimeError(f"Registry {WS_SCHEMA}.{REGISTRY_TABLE} missing.")
    reg_cols = list_columns(con, WS_SCHEMA, REGISTRY_TABLE)
    log(f"  registry columns: {reg_cols}")

    target_version = f"v1_0_script{SCRIPT_ID}"
    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE canonical_version = ?", [target_version]
    ).fetchone()[0])
    log(f"  pre-state rows for {target_version}: {n_pre}")

    if do_writes:
        con.execute(
            f"DELETE FROM {fq(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE canonical_version = ?", [target_version],
        )
        log(f"  deleted {n_pre} pre-existing {target_version} rows (Pattern 12)")

    inserts: list[dict[str, Any]] = []
    for sch, tbl, grain in NEW_TABLES:
        n = row_count(con, sch, tbl) if table_exists(con, sch, tbl) else 0
        p = (distinct_research_ids(con, sch, tbl)
             if table_exists(con, sch, tbl) else 0)
        rec: dict[str, Any] = {
            "detail_table_name":          tbl,
            "schema_name":                sch,
            "join_key":                   "research_id",
            "grain":                      grain,
            "total_rows":                 n,
            "total_patients":             p,
            "domain":                     "invasion_findings",
            "feeds_master_columns":       None,
            "description": (
                f"[domain=invasion_findings; grain={grain}] — source: "
                f"{SCRIPT_TAG} ({RUN_DATE}). Cross-modal invasion finding "
                f"canonical. Rows={n}, patients={p}."
            ),
            "canonical_version":          target_version,
            "feeds_master_columns_secondary": None,
            "feeds_master_columns_array": None,
            "needs_manual_review":        False,
        }
        ordered = [(c, rec[c]) for c in reg_cols if c in rec]
        col_csv = ", ".join(c for c, _ in ordered)
        ph_csv = ", ".join("?" for _ in ordered)
        log(f"  INSERT registry row: {tbl} (rows={n}, patients={p})")
        if do_writes:
            con.execute(
                f"INSERT INTO {fq(WS_SCHEMA, REGISTRY_TABLE)} ({col_csv}) "
                f"VALUES ({ph_csv})", [v for _, v in ordered],
            )
        inserts.append(rec)
    return {"deleted": n_pre, "inserted": len(inserts)}


# ---------------------------------------------------------------------------
# Step 5 — CPM feeder audit (read-only, writes plan file)
# ---------------------------------------------------------------------------

def step_5_cpm_feeder_audit(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — CPM feeder audit (report only)")
    log("=" * 78)
    if not table_exists(con, "main", "canonical_patient_master"):
        log_warn("  canonical_patient_master missing — skipping audit")
        return {"audit_rows": [], "report_path": None}

    cpm_cols = list_columns(con, "main", "canonical_patient_master")
    candidate_cpm = [c for c in cpm_cols if any(
        c.startswith(p) for p in ("nlp_", "op_", "operative_")
    ) and any(k in c.lower() for k in ("ete", "trach", "esophag", "invas",
                                       "involv", "vascul", "airway"))]
    log(f"  CPM has {len(cpm_cols)} cols; "
        f"{len(candidate_cpm)} invasion-flavored")

    grep_hits: dict[str, list[str]] = {}
    for col in STRIP_COLUMNS:
        try:
            res = subprocess.run(
                ["git", "grep", "-l", col, "--", "scripts/"],
                cwd=str(REPO_ROOT), capture_output=True, text=True,
                timeout=60, check=False,
            )
            files = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
            grep_hits[col] = files
        except subprocess.SubprocessError as exc:
            log_warn(f"  git grep for {col} failed: {exc}")
            grep_hits[col] = []

    audit_rows: list[dict[str, Any]] = []
    flag_to_target_rollup_col: dict[str, str] = {
        "gross_ete_flag": "any_gross_ete_anywhere",
        "tracheal_involvement_flag": "any_tracheal_anywhere",
        "esophageal_involvement_flag": "any_esophageal_anywhere",
        "local_invasion_flag": "any_local_anywhere",
    }

    for cc in candidate_cpm:
        cc_lower = cc.lower()
        for src_flag, target_col in flag_to_target_rollup_col.items():
            base = src_flag.replace("_flag", "")
            if base in cc_lower or any(t in cc_lower for t in base.split("_")):
                audit_rows.append({
                    "cpm_column": cc,
                    "deprecated_source_flag": src_flag,
                    "target_rollup_column":
                        f"canonical_invasion_patient_rollup_v1.{target_col}",
                })
                break

    md_lines = [
        f"# CPM feeder repoint plan — {SCRIPT_TAG} ({RUN_DATE})",
        f"BUILD_TS: `{BUILD_TS}`",
        "",
        "Generated by Step 5 of Script 363. Identifies CPM "
        "(canonical_patient_master) columns that are likely sourced from "
        "the 4 invasion BOOLEAN flags currently on "
        "`canonical_operative_events_v1`. These flags will be ALTER-DROPPED "
        "in Step 7. **A repointing script must run between Step 5 and Step "
        "7** OR Step 7 must be invoked with `--force-strip` (with explicit "
        "user acknowledgement of feeder breakage).",
        "",
        "## Per-flag scripts/ grep hits (`git grep -l <flag> -- scripts/`)",
        "",
        "| deprecated source flag | feeder script files |",
        "|---|---|",
    ]
    for col, files in grep_hits.items():
        if files:
            md_lines.append(
                f"| `{col}` | "
                + ", ".join(f"`{f}`" for f in files[:10])
                + (f" (+{len(files) - 10} more)" if len(files) > 10 else "")
                + " |"
            )
        else:
            md_lines.append(f"| `{col}` | (no hits) |")

    md_lines += [
        "",
        "## CPM column → invasion rollup column repoint plan",
        "",
        "| CPM column | deprecated source flag | target rollup column |",
        "|---|---|---|",
    ]
    if audit_rows:
        for r in audit_rows:
            md_lines.append(
                f"| `{r['cpm_column']}` | `{r['deprecated_source_flag']}` "
                f"| `{r['target_rollup_column']}` |"
            )
    else:
        md_lines.append("| (none — no CPM columns matched the heuristic) | | |")

    md_lines += [
        "",
        "## Apply this plan",
        "",
        "Write `scripts/363_cpm_feeder_repoint.py` that ALTERs the CPM "
        "columns above to read from `canonical_invasion_patient_rollup_v1` "
        "instead of `canonical_operative_events_v1`. Commit + push as the "
        "interim commit between the build and strip commits.",
    ]

    CPM_AUDIT_PATH.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    log(f"  CPM feeder repoint plan -> {CPM_AUDIT_PATH}")
    log(f"  audit_rows: {len(audit_rows)}; grep hits per col: "
        f"{ {k: len(v) for k, v in grep_hits.items()} }")
    return {"audit_rows": audit_rows, "report_path": str(CPM_AUDIT_PATH),
            "grep_hits_per_col": {k: len(v) for k, v in grep_hits.items()}}


# ---------------------------------------------------------------------------
# Step 6 — Zero-drift QA
# ---------------------------------------------------------------------------

def step_6_qa(con: duckdb.DuckDBPyConnection,
              step_0_result: dict[str, Any]) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — Zero-drift QA")
    log("=" * 78)
    qa: dict[str, Any] = {"build_ts": BUILD_TS, "checks": [],
                          "informational": [], "passed": True}

    def gate(name: str, passed: bool, **details: Any) -> None:
        qa["checks"].append({"name": name, "passed": bool(passed), **details})
        log(f"  GATE {'PASS' if passed else 'FAIL'} {name}: {details}")
        if not passed:
            qa["passed"] = False

    def info(name: str, **details: Any) -> None:
        qa["informational"].append({"name": name, **details})
        log(f"  INFO {name}: {details}")

    # 1. events_rowcount_nonzero
    events_table_exists = table_exists(con, "main",
                                       "canonical_invasion_events_v1")
    n_events = (row_count(con, "main", "canonical_invasion_events_v1")
                if events_table_exists else 0)
    gate("events_rowcount_nonzero", n_events > 0,
         events_count=n_events)

    # 2. rollup_parity_with_events
    rollup_exists = table_exists(con, "main",
                                 "canonical_invasion_patient_rollup_v1")
    n_rollup = (row_count(con, "main", "canonical_invasion_patient_rollup_v1")
                if rollup_exists else 0)
    n_events_dist_rid = (distinct_research_ids(
        con, "main", "canonical_invasion_events_v1"
    ) if events_table_exists else 0)
    gate("rollup_parity_with_events", n_rollup == n_events_dist_rid,
         rollup_count=n_rollup, events_distinct_research_id=n_events_dist_rid)

    if not events_table_exists:
        log_warn("  events table missing — remaining gates skipped")
        return qa

    # 3. backbone_modalities_present (v3: narrative_path REMOVED)
    backbone = ["op_note", "synoptic_path"]
    rows = con.execute(
        f"SELECT source_modality, COUNT(*) FROM "
        f"{fq('main','canonical_invasion_events_v1')} "
        f"WHERE source_modality IN ({', '.join(['?'] * len(backbone))}) "
        f"GROUP BY 1", backbone
    ).fetchall()
    seen_backbone = {r[0] for r in rows}
    gate("backbone_modalities_present",
         seen_backbone == set(backbone),
         seen_backbone=sorted(seen_backbone),
         missing_backbone=sorted(set(backbone) - seen_backbone))

    # 4. invasion_type_coverage (v3: 10 types incl. lymphatic_microscopic
    # / capsular / perineural / soft_tissue; 'local' REMOVED)
    rows = con.execute(
        f"SELECT invasion_type, COUNT(*) FROM "
        f"{fq('main','canonical_invasion_events_v1')} GROUP BY 1"
    ).fetchall()
    seen_types = {r[0] for r in rows}
    missing_types = set(INVASION_TYPES) - seen_types
    gate("invasion_type_coverage",
         not missing_types,
         seen_types=sorted(seen_types),
         missing_types=sorted(missing_types))
    # 4b. v3 local-invasion-type EXTINCTION gate
    gate("local_invasion_type_extinct",
         "local" not in seen_types,
         seen_types_includes_local=("local" in seen_types))

    # 4c. v3 NO cross-DB sourcing gate (Pattern 8 rejected)
    n_archive_src = int(con.execute(
        f"SELECT COUNT(*) FROM "
        f"{fq('main','canonical_invasion_events_v1')} "
        f"WHERE source_table LIKE 'archive_pub_v1_0.%'"
    ).fetchone()[0])
    gate("no_cross_db_archive_sourcing",
         n_archive_src == 0,
         archive_source_row_count=n_archive_src)

    # 4d. v3 vascular vs lymphatic patient-count gates per Logan
    # forecast (vascular_microscopic ≥ 682 / 6.27%; lymphatic_microscopic
    # ≥ 783 / 7.20%; both ≥ 293).
    n_vasc = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM "
        f"{fq('main','canonical_invasion_events_v1')} "
        f"WHERE invasion_type='vascular_microscopic' "
        f"AND finding_status='present'"
    ).fetchone()[0])
    n_lymph = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM "
        f"{fq('main','canonical_invasion_events_v1')} "
        f"WHERE invasion_type='lymphatic_microscopic' "
        f"AND finding_status='present'"
    ).fetchone()[0])
    n_both = int(con.execute(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT research_id FROM "
        f"  {fq('main','canonical_invasion_events_v1')} "
        f"  WHERE invasion_type='vascular_microscopic' "
        f"  AND finding_status='present' "
        f"  INTERSECT "
        f"  SELECT research_id FROM "
        f"  {fq('main','canonical_invasion_events_v1')} "
        f"  WHERE invasion_type='lymphatic_microscopic' "
        f"  AND finding_status='present'"
        f")"
    ).fetchone()[0])
    gate("vl_split_vascular_min", n_vasc >= 682,
         n_patients=n_vasc, forecast_min=682)
    # v3-iter-2: ratcheted 783 -> 780 per Logan. Original 783 was Logan
    # forecast precision (~7.20% × 10,871 = 782.71 round-up); actual
    # under authoritative dict is 780 / 7.18%. Internal LIVE re-probe
    # delta = 0.
    gate("vl_split_lymphatic_min", n_lymph >= 780,
         n_patients=n_lymph, forecast_min=780)
    gate("vl_split_intersection_min", n_both >= 293,
         n_patients=n_both, forecast_min=293)

    # 4e. v3-iter-2 finding_status distribution sanity (Logan).
    # Hard-fail if SUSPECTED count is 0 across all invasion_types
    # (would mean the LLM CTE's hedged-language ladder is mis-ordered).
    rows = con.execute(
        f"SELECT invasion_type, finding_status, COUNT(*), "
        f"COUNT(DISTINCT research_id) "
        f"FROM {fq('main','canonical_invasion_events_v1')} "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    ).fetchall()
    distrib: dict[str, dict[str, tuple[int, int]]] = {}
    for inv, status, n, p in rows:
        distrib.setdefault(inv, {})[status] = (n, p)
    n_suspected_total = sum(
        v.get("suspected", (0, 0))[0] for v in distrib.values()
    )
    gate("finding_status_distribution_sanity",
         n_suspected_total > 0,
         total_suspected_mentions=n_suspected_total,
         per_type_status_counts={
             inv: {s: f"{n}/{p}" for s, (n, p) in sorted(d.items())}
             for inv, d in sorted(distrib.items())
         })

    # 5. preservation_op_note (per flag)
    # v3: dropped local_invasion_flag → 'local' mapping (no longer in
    # vocabulary). The structured op_note flag still captures local
    # invasion mentions, but in v3 they route to soft_tissue if the
    # surgeon recorded soft-tissue language. For preservation, we
    # check that the op_note structured BOOL flag count matches the
    # union of soft_tissue+capsular+perineural+gross_ete events from
    # op_note/structured (since local_invasion_flag in operative
    # canonical is a generic flag that could be any of these).
    # Conservative gate: just verify counts for the 3 type-specific
    # flags (gross_ete, tracheal, esophageal); local_invasion_flag is
    # logged informationally.
    for col, inv_type in [
        ("gross_ete_flag", "gross_ete"),
        ("tracheal_involvement_flag", "tracheal"),
        ("esophageal_involvement_flag", "esophageal"),
    ]:
        if col not in step_0_result.get("strip_cols_present", []):
            info(f"preservation_op_note_{inv_type}_skipped",
                 reason=f"strip-target col {col} not present (already dropped?)")
            continue
        n_src_true = int(con.execute(
            f"SELECT COUNT(*) FROM {fq('main','canonical_operative_events_v1')} "
            f"WHERE {col} = TRUE"
        ).fetchone()[0])
        n_events_present = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id || '|' || source_row_id) "
            f"FROM {fq('main','canonical_invasion_events_v1')} "
            f"WHERE invasion_type=? AND source_modality='op_note' "
            f"AND source_kind='structured' "
            f"AND finding_status='present'", [inv_type]
        ).fetchone()[0])
        gate(f"preservation_op_note_{inv_type}",
             n_src_true == n_events_present,
             source_true_count=n_src_true,
             events_present_count=n_events_present)

    # 5b. v3 — local_invasion_flag now routed to soft_tissue
    if "local_invasion_flag" in step_0_result.get("strip_cols_present", []):
        n_src_true = int(con.execute(
            f"SELECT COUNT(*) FROM "
            f"{fq('main','canonical_operative_events_v1')} "
            f"WHERE local_invasion_flag = TRUE"
        ).fetchone()[0])
        n_events_present = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id || '|' || source_row_id) "
            f"FROM {fq('main','canonical_invasion_events_v1')} "
            f"WHERE invasion_type='soft_tissue' "
            f"AND source_modality='op_note' "
            f"AND source_kind='structured' "
            f"AND finding_status='present'"
        ).fetchone()[0])
        gate("preservation_op_note_local_routed_to_soft_tissue",
             n_src_true == n_events_present,
             source_true_count=n_src_true,
             events_present_count=n_events_present,
             routing_note="v3: local_invasion_flag → soft_tissue")

    # 6. view_resolves
    for view_name, _ in NEW_VIEWS:
        try:
            con.execute(
                f'SELECT 1 FROM "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                f'"{view_name}" LIMIT 1'
            ).fetchone()
            gate(f"view_resolves_{view_name}", True)
        except duckdb.Error as exc:
            gate(f"view_resolves_{view_name}", False, error=str(exc))

    # Informational metrics
    rows = con.execute(
        f"SELECT linkage_method, COUNT(*) FROM "
        f"{fq('main','canonical_invasion_events_v1')} GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    info("linkage_method_distribution",
         distribution={r[0]: r[1] for r in rows})

    rows = con.execute(
        f"SELECT source_modality, source_kind, invasion_type, "
        f"finding_status, COUNT(*) "
        f"FROM {fq('main','canonical_invasion_events_v1')} "
        f"GROUP BY 1,2,3,4 ORDER BY 1,2,3,4"
    ).fetchall()
    info("counts_per_modality_kind_type_status",
         counts=[{"modality": r[0], "source_kind": r[1],
                  "invasion_type": r[2], "finding_status": r[3],
                  "n": r[4]} for r in rows])

    info("placeholder_modalities",
         items=step_0_result.get("placeholder_modalities", []))

    # Sidecar QA breakdown — full per-(modality, kind, type, status)
    # counts for clinical sanity check (per Logan CHECKPOINT 1: confirm
    # 'present' counts match clinical realism, e.g. ~6.2% vascular,
    # ~7.2% lymphatic, ~5.4% ETE).
    breakdown_path = QA_DIR / f"qa_script_363_invasion_breakdown_{BUILD_TS}.md"
    cohort_size = int(con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM "
        f"{fq('main','canonical_patient_master')}"
    ).fetchone()[0]) if table_exists(con, "main",
                                     "canonical_patient_master") else 0
    rows = con.execute(
        f"SELECT source_modality, source_kind, invasion_type, "
        f"finding_status, COUNT(*) AS n_mentions, "
        f"COUNT(DISTINCT research_id) AS n_patients "
        f"FROM {fq('main','canonical_invasion_events_v1')} "
        f"GROUP BY 1,2,3,4 ORDER BY 1,2,3,4"
    ).fetchall()
    bd = [
        f"# Script 363 invasion breakdown — {RUN_DATE}",
        f"BUILD_TS: `{BUILD_TS}`  cohort_size (canonical_patient_master): "
        f"{cohort_size:,}",
        "",
        "Per-(source_modality × source_kind × invasion_type × "
        "finding_status) counts. **Compare 'present' percentages against "
        "Logan's clinical realism table** (~6.2% vascular, ~7.2% "
        "lymphatic, ~5.4% ETE, ~0.9% perineural) before signing off on "
        "the cascade strip.",
        "",
        "| modality | source_kind | invasion_type | finding_status | "
        "n_mentions | n_patients | % cohort |",
        "|---|---|---|---|---:|---:|---:|",
    ]
    for mod, kind, inv, status, nm, np_ in rows:
        pct = (np_ / cohort_size * 100.0) if cohort_size else 0.0
        bd.append(
            f"| `{mod}` | `{kind}` | `{inv}` | `{status}` | {nm:,} | "
            f"{np_:,} | {pct:.2f}% |"
        )
    # Cross-modal anywhere summary per invasion_type
    bd.extend([
        "",
        "## Cross-modal `present anywhere` per invasion_type",
        "(consumed by canonical_invasion_patient_rollup_v1.any_<type>_anywhere)",
        "",
        "| invasion_type | n_patients_present_anywhere | % cohort |",
        "|---|---:|---:|",
    ])
    cm_rows = con.execute(
        f"SELECT invasion_type, "
        f"COUNT(DISTINCT research_id) AS n_patients "
        f"FROM {fq('main','canonical_invasion_events_v1')} "
        f"WHERE finding_status='present' "
        f"GROUP BY 1 ORDER BY 1"
    ).fetchall()
    for inv, np_ in cm_rows:
        pct = (np_ / cohort_size * 100.0) if cohort_size else 0.0
        bd.append(f"| `{inv}` | {np_:,} | {pct:.2f}% |")
    breakdown_path.write_text("\n".join(bd) + "\n", encoding="utf-8")
    log(f"  Sidecar breakdown report -> {breakdown_path}")

    # Save QA
    QA_DIR.mkdir(parents=True, exist_ok=True)
    QA_PATH.write_text(json.dumps(qa, indent=2, default=str) + "\n",
                       encoding="utf-8")
    log(f"  QA report written -> {QA_PATH}")
    return qa


# ---------------------------------------------------------------------------
# Step 7 — Cascade strip (operative canonical only)
# ---------------------------------------------------------------------------

def step_7_cascade_strip(
    con: duckdb.DuckDBPyConnection, do_writes: bool, force: bool,
    step_0_result: dict[str, Any],
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 7 — Cascade strip (canonical_operative_events_v1 only)")
    log("=" * 78)

    schema, table = STRIP_TARGET_TABLE

    # 7.1 Verify pre-strip archive (created in Step 0.g for --phase 7)
    archive_snapshot = step_0_result.get("archive_snapshot")
    if not archive_snapshot:
        archive_snapshot = _pre_strip_archive(con, do_writes)
    log(f"  pre-strip archive: {archive_snapshot}")

    # 7.2 Safety gates
    n_events = (row_count(con, "main", "canonical_invasion_events_v1")
                if table_exists(con, "main", "canonical_invasion_events_v1")
                else 0)
    n_rollup = (row_count(con, "main", "canonical_invasion_patient_rollup_v1")
                if table_exists(con, "main",
                                "canonical_invasion_patient_rollup_v1")
                else 0)
    if n_events == 0 or n_rollup == 0:
        raise RuntimeError(
            f"Refusing to strip: invasion canonicals empty "
            f"(events={n_events}, rollup={n_rollup})"
        )
    log(f"  invasion canonicals OK: events={n_events:,}, rollup={n_rollup:,}")

    # 7.2.3 QA preservation gates passed?
    if QA_PATH.exists():
        qa = json.loads(QA_PATH.read_text(encoding="utf-8"))
        preservation_failed = [
            c for c in qa.get("checks", [])
            if c["name"].startswith("preservation_op_note_") and not c["passed"]
        ]
        if preservation_failed:
            raise RuntimeError(
                f"Refusing to strip: preservation gates failed: "
                f"{preservation_failed}"
            )
        log("  preservation gates: all passed (per QA JSON)")
    else:
        log_warn("  no QA JSON found — assuming preservation gates "
                 "passed in current invocation")

    # 7.2.4 CPM feeder repoint plan exists + applied?
    if not CPM_AUDIT_PATH.exists():
        raise RuntimeError(
            f"Refusing to strip: CPM feeder repoint plan missing at "
            f"{CPM_AUDIT_PATH}. Run Step 5 first."
        )
    repoint_marker = REPO_ROOT / ".invasion_cpm_repoint_applied"
    if not repoint_marker.exists() and not force:
        raise RuntimeError(
            f"Refusing to strip: CPM feeder repoint not applied (marker "
            f"file {repoint_marker} missing). Run "
            f"scripts/363_cpm_feeder_repoint.py and create the marker, "
            f"OR pass --force-strip to override (BLAST RADIUS WARNING)."
        )
    if force and not repoint_marker.exists():
        log_warn("  --force-strip ON: CPM feeder repoint NOT applied. "
                 "Downstream CPM feeders that read invasion flags from "
                 "canonical_operative_events_v1 will break.")

    # 7.2.5 Forbidden columns present
    op_cols = list_columns(con, schema, table)
    for forbidden in STRIP_FORBIDDEN_COLUMNS:
        if forbidden in STRIP_COLUMNS:
            raise RuntimeError(
                f"SAFETY VIOLATION: forbidden column {forbidden!r} appears "
                f"in STRIP_COLUMNS list. Refusing to proceed."
            )

    # 7.2.6 Dependent view scan
    view_ddls = con.execute(
        "SELECT view_name, sql FROM duckdb_views() "
        "WHERE database_name=? AND schema_name=?",
        [CANONICAL_DB, VIEW_SCHEMA],
    ).fetchall()
    affected_views: list[str] = []
    for view_name, ddl in view_ddls:
        if not ddl:
            continue
        if any(c in ddl for c in STRIP_COLUMNS):
            affected_views.append(view_name)
    if affected_views:
        log_warn(f"  views in {VIEW_SCHEMA} reference about-to-drop cols: "
                 f"{affected_views} — will refresh after strip")

    # 7.3 ALTER DROP COLUMN one at a time
    dropped: list[str] = []
    for col in STRIP_COLUMNS:
        if col not in op_cols:
            log(f"  {col} already absent on {table} — skip")
            continue
        if col in STRIP_FORBIDDEN_COLUMNS:
            raise RuntimeError(
                f"SAFETY VIOLATION: {col!r} is in STRIP_FORBIDDEN_COLUMNS"
            )
        log(f"  ALTER TABLE main.{table} DROP COLUMN {col}")
        if do_writes:
            con.execute(f"ALTER TABLE {fq(schema, table)} DROP COLUMN {col}")
        dropped.append(col)

    # 7.3b Refresh views (recreate the 363 views to be safe + repoint the
    # 362 Surgery_Episode_Detail view if it exists).
    if do_writes:
        for view_name, base_table in NEW_VIEWS:
            con.execute(
                f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                f'"{view_name}" AS SELECT * FROM {fq("main", base_table)}'
            )
            log(f"  refreshed view {VIEW_SCHEMA}.{view_name}")
        if view_exists(con, VIEW_SCHEMA, "Surgery_Episode_Detail"):
            con.execute(
                f'CREATE OR REPLACE VIEW "{CANONICAL_DB}"."{VIEW_SCHEMA}".'
                f'"Surgery_Episode_Detail" AS SELECT * FROM '
                f'{fq("main", "canonical_operative_events_v1")}'
            )
            log("  refreshed Surgery_Episode_Detail view")

    # 7.4 Post-strip verification
    op_cols_post = list_columns(con, schema, table) if do_writes else op_cols
    still_present = [c for c in dropped if c in op_cols_post]
    if still_present:
        raise RuntimeError(
            f"Post-strip: dropped cols still present: {still_present}"
        )
    n_post = row_count(con, schema, table)
    log(f"  post-strip {table}: {n_post:,} rows; dropped {len(dropped)} cols")

    return {"archive": archive_snapshot, "dropped": dropped,
            "affected_views": affected_views}


# ---------------------------------------------------------------------------
# Step 8 — Close-out summary
# ---------------------------------------------------------------------------

def step_8_closeout(
    results: dict[str, Any], do_writes: bool,
) -> None:
    log("=" * 78)
    log("STEP 8 — Close-out summary")
    log("=" * 78)
    lines = [
        f"# Script 363 close-out — {RUN_DATE}",
        f"BUILD_TS: `{BUILD_TS}`",
        f"do_writes: `{do_writes}`",
        "",
        "## New canonicals",
    ]
    s1 = results.get("step_1", {})
    s2 = results.get("step_2", {})
    if s1.get("created"):
        lines.append(f"- `main.canonical_invasion_events_v1`: "
                     f"{s1['rows']:,} rows / {s1['patients']:,} patients")
        lines.append(f"  CTEs: {len(s1.get('ctes', []))}; "
                     f"skipped: {len(s1.get('skipped_ctes', []))}")
    if s2.get("created"):
        lines.append(f"- `main.canonical_invasion_patient_rollup_v1`: "
                     f"{s2['rows']:,} rows "
                     f"({s2.get('n_per_combo_cols')} per-combo + "
                     f"{s2.get('n_cross_modal_cols')} cross-modal cols)")
    lines.extend([
        "",
        "## Coverage census",
        f"See `{CENSUS_PATH.name}` for full matrix.",
        "",
        "## Categorical vocab + LLM JSON probes",
        f"- Vocab report: `{VOCAB_PATH.name}` "
        f"(unmapped sources: {results.get('step_0', {}).get('unmapped_categorical_count', 0)})",
        f"- JSON keys report: `{JSON_KEYS_PATH.name}` "
        f"(unmapped sources: {results.get('step_0', {}).get('unmapped_entity_type_count', 0)})",
        "",
        "## QA result",
    ])
    qa = results.get("step_6", {})
    if qa:
        lines.append(f"- Hard gates: "
                     f"{sum(1 for c in qa.get('checks', []) if c['passed'])}"
                     f" pass / "
                     f"{sum(1 for c in qa.get('checks', []) if not c['passed'])}"
                     f" fail")
        lines.append(f"- Overall: {'PASS' if qa.get('passed') else 'FAIL'}")
        lines.append(f"- Full report: `qa/{QA_PATH.name}`")

    s7 = results.get("step_7")
    if s7:
        lines.extend([
            "",
            "## Cascade strip outcome",
            f"- Archive: `{s7.get('archive')}`",
            f"- Dropped columns from `canonical_operative_events_v1`: "
            f"{s7.get('dropped')}",
            f"- Affected views (refreshed): {s7.get('affected_views')}",
        ])

    s5 = results.get("step_5", {})
    if s5:
        lines.extend([
            "",
            "## CPM feeder repoint plan",
            f"- Plan file: `{CPM_AUDIT_PATH.name}`",
            f"- Audit rows (CPM cols → invasion rollup target): "
            f"{len(s5.get('audit_rows', []))}",
        ])

    lines.extend([
        "",
        "## New patterns introduced (for AGENTS.md / project memory)",
        "- ~~**Pattern 8**: archive_pub_v1_0 as permanent source "
        "dependency~~ — REJECTED in v3 per Logan's "
        "`feedback_no_cross_db_canonical_sourcing.md`. "
        "Master canonicals are standalone live objects in `main`; "
        "no `FROM archive_pub_v1_0.*` allowed in build scripts.",
        "- **Pattern 9**: VARCHAR vocab → finding_status normalisation. "
        "v3 fix: `'x'` → absent (synoptic placeholder, not missing); "
        "build_ts must be `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` not "
        "TIMESTAMPTZ (per `reference_duckdb_timestamp_tz.md`).",
        "- **Pattern 10**: result_json UNNEST + json_extract_string "
        "design.",
        "- **Pattern 11**: modality coverage census → placeholder.",
        "- **Pattern 12**: orthogonal source_modality × source_kind "
        "(structured / llm) on cross-modal canonicals.",
        "- **Pattern 13**: idempotent registry "
        "DELETE-WHERE-canonical_version before INSERT.",
        "- **Pattern 14**: LLM `result_json` UNNEST template with "
        "`WHERE result_json LIKE '{\"entities\":%'` filter for error "
        "rows. Reusable for 364 / 365 / 366 / 367 — all "
        "`note_entities_llm_*` tables share the same JSON shape.",
        "- **Pattern 15** (NEW v3): EXCISE non-invasion entity_types "
        "from invasion canonical. Mass-effect / compression / staging "
        "/ general-histology entities (tracheal_deviation, "
        "substernal_extension, esophageal_compression, "
        "vascular_encasement, mass_effect, airway_compromise_grade, "
        "vocal_cord_imaging, vascular_invasion_type, vessel_count, "
        "necrosis, mitotic_rate, ptnm_stage, dedifferentiation) get "
        "row-counted into the JSON keys probe report and explicitly "
        "DROPPED from CTEs (NULL invasion_type → CTE filter excludes). "
        "Logan's CHECKPOINT 1.G requirement.",
    ])

    body = "\n".join(lines) + "\n"
    CLOSEOUT_PATH.write_text(body, encoding="utf-8")
    log(f"  close-out -> {CLOSEOUT_PATH}")
    print(body)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def parse_phases(spec: str | None) -> set[str]:
    if not spec:
        return {"0", "1", "2", "3", "4", "5", "6", "7", "8"}
    return {s.strip() for s in spec.split(",") if s.strip()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cross-modal invasion findings canonical (Script 363)"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Run with writes enabled.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — no archives, builds, or strips.")
    parser.add_argument("--phase", default=None,
                        help="Comma-separated phases to run (default all).")
    parser.add_argument("--skip-strip", action="store_true",
                        help="Skip Step 7 cascade strip.")
    parser.add_argument("--force-strip", action="store_true",
                        help="Bypass Step 7 CPM-feeder-repoint safety gate.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    phases = parse_phases(args.phase)
    if args.skip_strip:
        phases.discard("7")
    run_strip = "7" in phases and do_writes
    log(f"Run config: do_writes={do_writes}, phases={sorted(phases)}, "
        f"run_strip={run_strip}, force_strip={args.force_strip}, "
        f"BUILD_TS={BUILD_TS}")

    try:
        con = connect()
        results: dict[str, Any] = {"build_ts": BUILD_TS,
                                   "do_writes": do_writes,
                                   "phases": sorted(phases)}

        step_0_result: dict[str, Any] = {}
        if "0" in phases:
            step_0_result = step_0_preflight(con, do_writes, run_strip)
            results["step_0"] = step_0_result
        else:
            # Re-derive minimal info needed by later phases.
            archive_resolved = {}
            for key, pattern in ARCHIVE_PATTERNS.items():
                archive_resolved[key] = resolve_archive(con, pattern)
            step_0_result = {
                "archive_resolved": archive_resolved,
                "placeholder_modalities": [],
                "strip_cols_present": [
                    c for c in STRIP_COLUMNS
                    if c in list_columns(con, *STRIP_TARGET_TABLE)
                ],
            }

        if "1" in phases:
            results["step_1"] = step_1_build_events(
                con, do_writes,
                step_0_result["archive_resolved"],
                step_0_result.get("placeholder_modalities", []),
            )
        if "2" in phases:
            results["step_2"] = step_2_build_rollup(
                con, do_writes,
                step_0_result.get("placeholder_modalities", []),
            )
        if "3" in phases:
            results["step_3"] = step_3_build_views(con, do_writes)
        if "4" in phases:
            results["step_4"] = step_4_registry_sync(con, do_writes)
        if "5" in phases:
            results["step_5"] = step_5_cpm_feeder_audit(con)
        if "6" in phases:
            results["step_6"] = step_6_qa(con, step_0_result)
            if not results["step_6"].get("passed", False):
                log_error("ZERO-DRIFT QA failed — see qa file for details")
                step_8_closeout(results, do_writes)
                flush_log()
                return 2
        if "7" in phases:
            if not do_writes:
                log("STEP 7 — dry-run skips cascade strip (writes disabled)")
            else:
                results["step_7"] = step_7_cascade_strip(
                    con, do_writes, args.force_strip, step_0_result
                )
        if "8" in phases:
            step_8_closeout(results, do_writes)

        log("Script 363 complete.")
        flush_log()
        return 0
    except Exception as exc:
        log_error(f"FATAL: {exc!r}")
        flush_log()
        raise


if __name__ == "__main__":
    sys.exit(main())
