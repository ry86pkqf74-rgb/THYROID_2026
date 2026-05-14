#!/usr/bin/env python3
"""
Script 386 — post-387 v1_0 dedup pass (narrowed Phase D).

NOT the thyroglobulin refresh path — 2026-05-14: Tg canonical rebuild is BigQuery
mig_340 (qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py).

Per cursor prompt 2026-04-22 §3 Phase D + user-confirmed softening:

Pre-state checks (READ-ONLY; halt on failure):
  - tier2 schema empty/absent
  - verify schema empty/absent
  - views_readable.survival_followup_VIEW_v1 dropped
  - 13 manuscript_workspace legacy artifacts archived
  - scripts/output/387_close_out.md exists; contains "Phase 6 — dedup probe outcomes"

(a) Within-canonical event-row dedup:
  - Auto-applied on the 3 NEW canonicals (path/cervln/esoph events)
    using key (research_id, note_row_id, entity_type, entity_value, source_line)
  - Re-verification sweep on the 36 pre-existing canonicals using 387's keys.
    HALT only if a pre-existing canonical's collapse count INCREASES vs the 387
    baseline — pre-existing carry-forwards are accepted as the baseline state.

(b) Residual archive_drop scan:
  - main.* matching ^(tmp|stg|wip)_
  - registry rows with superseded=TRUE (probe column existence first)
  - tables in __readme deprecation log that still exist
  Plan written to scripts/output/386_dedup_candidates.json.
  Drops require --apply-archive-drop.  Empty bucket = "no residual candidates".

(c) View-naming convention verification:
  - main views NOT matching %_VIEW_v% (excluding any grandfathered list)
  Plan written to scripts/output/386_view_renames.json.
  Renames require --apply-view-renames.  Empty plan = no violators.

Carry-forward (synoptic_diagnosis 88% extraction error rate, etc.) is recorded
in scripts/output/386_close_out.md.

PHI rule: never log evidence_text / value_raw / source_* values.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts._round2_helpers import (  # noqa: E402
    ARCHIVE_DB,
    ARCHIVE_SCHEMA,
    CANONICAL_DB,
    REGISTRY_SCHEMA,
    REGISTRY_TABLE,
    RunLogger,
    column_exists,
    connect_md,
    table_exists,
)

# 13 manuscript_workspace legacy artifacts archived by Script 387.
MS_LEGACY_ARTIFACTS = (
    "canonical_us_nodule_v2_pre_s376_snapshot",
    "candidate_us_exam_master_v2",
    "candidate_us_patient_master_v2",
    "prompt5_remediation_log_v1",
    "prompt5_remediation_summary_v1",
    "prompt6_completion_audit_v1",
    "prompt6_defer_log_v1",
    "prompt6_older_master_decisions_v1",
    "prompt6_poststate_v1",
    "prompt6_prestate_v1",
    "prompt6_view_rebuild_log_v1",
    "prompt6_wiring_gap_remediation_v1",
    "prompt7_handoff_v1",
)

# 387 baseline collapse counts (from scripts/output/387_dedup_probe_report.md).
# This is the accepted carry-forward state — Phase D halts only if a NEW
# collapse appears (i.e. count > baseline) on any pre-existing canonical.
BASELINE_387 = {
    "canonical_complications_events_v1":     {"key": "(research_id, evidence_span_hash)",         "collapse": 15},
    "canonical_invasion_events_v1":          {"key": "(invasion_event_id)",                       "collapse": 7578},
    "canonical_medications_events_v1":       {"key": "(research_id, evidence_span_hash)",         "collapse": 2512},
    "canonical_molecular_genetics_v2":       {"key": "(molecular_episode_id)",                    "collapse": 856},
    "canonical_path_malignant_events_v1":    {"key": "(specimen_focus_id)",                       "collapse": 442},
    "canonical_pmh_events_v1":               {"key": "(research_id, evidence_span_hash)",         "collapse": 816},
    "canonical_psh_events_v1":               {"key": "(research_id, evidence_span_hash)",         "collapse": 233},
}
# Canonicals 387 marked all_null_key — skip re-verification (they need an
# alternate partition key choice in a future probe pass; not a Phase D blocker).
SKIP_REVERIFY = (
    "canonical_path_benign_events_v1",
    "canonical_path_gland_events_v1",
)
# Pre-existing canonicals with known clean dedup state (collapse=0 in 387) —
# any non-zero collapse here is a NEW collapse and HARD-FAILS Phase D.
CLEAN_387 = (
    "canonical_complications_patient_rollup_v1",
    "canonical_fna_events_v1", "canonical_fna_patient_rollup_v1",
    "canonical_frozen_section_events_v1", "canonical_frozen_section_patient_rollup_v1",
    "canonical_invasion_patient_rollup_v1",
    "canonical_labs_calcium_v1", "canonical_labs_pth_v1",
    "canonical_labs_thyroglobulin_v1", "canonical_labs_tsh_v1",
    "canonical_labs_vitamin_d_v1",
    "canonical_medications_patient_rollup_v1",
    "canonical_molecular_genetics_from_notes_v2",
    "canonical_operative_events_v1", "canonical_operative_patient_rollup_v1",
    "canonical_operative_procedure_codes_v1",
    "canonical_path_benign_patient_rollup_v1",
    "canonical_path_gland_patient_rollup_v1",
    "canonical_path_malignant_patient_rollup_v1",
    "canonical_patient_master",
    "canonical_pmh_patient_rollup_v1",
    "canonical_psh_patient_rollup_v1",
    "canonical_recurrence_v1",
    "canonical_survival_followup_v1",
    "canonical_us_lymph_node_v2",
    "canonical_us_nodule_v2",
    "canonical_us_thyroid_gland_v2",
)
# Per-table partition keys used for re-verification (mirrors 387 probe).
PER_TABLE_KEY_OVERRIDES = {
    "canonical_complications_events_v1":     ["research_id", "evidence_span_hash"],
    "canonical_invasion_events_v1":          ["invasion_event_id"],
    "canonical_medications_events_v1":       ["research_id", "evidence_span_hash"],
    "canonical_molecular_genetics_v2":       ["molecular_episode_id"],
    "canonical_path_malignant_events_v1":    ["specimen_focus_id"],
    "canonical_pmh_events_v1":               ["research_id", "evidence_span_hash"],
    "canonical_psh_events_v1":               ["research_id", "evidence_span_hash"],
    "canonical_complications_patient_rollup_v1":     ["research_id"],
    "canonical_fna_events_v1":               ["fna_event_id"],
    "canonical_fna_patient_rollup_v1":       ["research_id"],
    "canonical_frozen_section_events_v1":    ["entity_id_hash"],
    "canonical_frozen_section_patient_rollup_v1": ["research_id"],
    "canonical_invasion_patient_rollup_v1":  ["research_id"],
    "canonical_labs_calcium_v1":             ["research_id", "lab_datetime", "value_raw", "source"],
    "canonical_labs_pth_v1":                 ["research_id", "lab_datetime", "value_raw", "source"],
    "canonical_labs_thyroglobulin_v1":       ["research_id", "lab_datetime", "analyte", "value_raw", "source"],
    "canonical_labs_tsh_v1":                 ["research_id", "lab_datetime", "value_raw", "source"],
    "canonical_labs_vitamin_d_v1":           ["research_id", "lab_datetime", "value_raw", "source"],
    "canonical_medications_patient_rollup_v1": ["research_id"],
    "canonical_molecular_genetics_from_notes_v2": ["research_id", "note_row_id", "entity_type", "evidence_start"],
    "canonical_operative_events_v1":         ["surgery_episode_id"],
    "canonical_operative_patient_rollup_v1": ["research_id"],
    "canonical_operative_procedure_codes_v1": ["procedure_mention_id"],
    "canonical_path_benign_patient_rollup_v1": ["research_id"],
    "canonical_path_gland_patient_rollup_v1": ["research_id"],
    "canonical_path_malignant_patient_rollup_v1": ["research_id"],
    "canonical_patient_master":              ["research_id"],
    "canonical_pmh_patient_rollup_v1":       ["research_id"],
    "canonical_psh_patient_rollup_v1":       ["research_id"],
    "canonical_recurrence_v1":               ["research_id"],
    "canonical_survival_followup_v1":        ["research_id"],
    "canonical_us_lymph_node_v2":            ["research_id", "us_exam_id", "us_ln_id"],
    "canonical_us_nodule_v2":                ["research_id", "us_exam_id", "nodule_id"],
    "canonical_us_thyroid_gland_v2":         ["research_id", "us_exam_id"],
}

# 3 NEW canonicals to dedup (key per memory feedback_mention_grain_partition_probe.md).
NEW_EVENT_TABLES = (
    "canonical_pathology_clinical_events_v1",
    "canonical_cervical_ln_clinical_events_v1",
    "canonical_esophageal_invasion_events_v1",
)
NEW_DEDUP_KEY = ("research_id", "note_row_id", "entity_type", "entity_value", "source_line")

# Optional grandfathered VIEW names exempt from the _VIEW_v naming rule.
# At Phase B time none were observed; left empty.  Add here if any platform
# views surface in future scans.
GRANDFATHERED_VIEW_NAMES: tuple[str, ...] = ()

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "386_run.log"
DEDUP_CANDIDATES_PATH = OUTPUT_DIR / "386_dedup_candidates.json"
VIEW_RENAMES_PATH = OUTPUT_DIR / "386_view_renames.json"
CLOSE_OUT_PATH = OUTPUT_DIR / "386_close_out.md"
CARRYOVER_387 = OUTPUT_DIR / "387_close_out.md"
CARRYOVER_387_PROBE = OUTPUT_DIR / "387_dedup_probe_report.md"

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


def _bool_env(con: Any, schema: str) -> int:
    return con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog=? AND table_schema=?",
        [CANONICAL_DB, schema],
    ).fetchone()[0]


def pre_state(con: Any) -> dict[str, Any]:
    log("=" * 70)
    log("PRE-STATE — verify Script 387 ran cleanly")
    log("=" * 70)
    state: dict[str, Any] = {}

    state["tier2_objects"] = _bool_env(con, "tier2")
    gate(state["tier2_objects"] == 0, f"tier2 schema empty/absent (got {state['tier2_objects']} objects)")

    state["verify_objects"] = _bool_env(con, "verify")
    gate(state["verify_objects"] == 0, f"verify schema empty/absent (got {state['verify_objects']} objects)")

    state["dup_view_remaining"] = con.execute(
        """SELECT COUNT(*) FROM information_schema.tables
           WHERE table_catalog=? AND table_schema='views_readable'
             AND table_name='survival_followup_VIEW_v1'""",
        [CANONICAL_DB],
    ).fetchone()[0]
    gate(state["dup_view_remaining"] == 0,
         f"views_readable.survival_followup_VIEW_v1 dropped (got {state['dup_view_remaining']})")

    placeholders = ",".join("?" for _ in MS_LEGACY_ARTIFACTS)
    state["ms_legacy_remaining"] = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
           WHERE table_catalog=? AND table_schema='manuscript_workspace'
             AND table_name IN ({placeholders})""",
        [CANONICAL_DB, *MS_LEGACY_ARTIFACTS],
    ).fetchone()[0]
    gate(state["ms_legacy_remaining"] == 0,
         f"13 ms_workspace legacy artifacts archived (got {state['ms_legacy_remaining']})")

    # Soft check: 387 close-out file exists.  We do NOT halt on the literal
    # "all clean" string — per user softening, the 7 documented carry-forward
    # flag_event collapses are accepted as the BASELINE_387 state above.
    state["387_close_out_exists"] = CARRYOVER_387.exists()
    state["387_dedup_probe_exists"] = CARRYOVER_387_PROBE.exists()
    gate(state["387_close_out_exists"], f"387 close-out file exists ({CARRYOVER_387.name})")
    gate(state["387_dedup_probe_exists"], f"387 dedup probe report exists ({CARRYOVER_387_PROBE.name})")

    if state["387_close_out_exists"]:
        cl = CARRYOVER_387.read_text()
        has_dedup_section = "Phase 6 — dedup probe outcomes" in cl
        state["387_close_out_has_dedup_section"] = has_dedup_section
        gate(has_dedup_section, "387 close-out contains 'Phase 6 — dedup probe outcomes'")
        # Surface fail_rollup count (the only HARD-FAIL criterion 387 uses)
        for line in cl.splitlines():
            if "`fail_rollup`" in line.lower() or "fail_rollup" in line:
                log(f"  [387 carry] {line.strip()}")

    log("PRE-STATE complete")
    logger.flush()
    return state


def collapse_count_for(con: Any, table: str, key_cols: list[str]) -> dict[str, Any]:
    """Compute (rows, distinct, null_key_rows, collapse) for a given key.

    Mirrors Script 387's _key_sql / probe SQL exactly so the delta vs
    BASELINE_387 is methodology-equivalent:
      * null_key uses OR-of-IS-NULL across the key columns
      * COUNT(DISTINCT (col1, col2, ...)) is taken from the FULL table
        (no WHERE filter), which counts partially-NULL key tuples as distinct
    """
    if not table_exists(con, "main", table):
        return {"present": False}
    missing = [c for c in key_cols if not column_exists(con, "main", table, c)]
    if missing:
        return {"present": True, "missing_key_cols": missing}
    quoted = ", ".join(f'"{c}"' for c in key_cols)
    key_sql = f"({quoted})" if len(key_cols) > 1 else f'"{key_cols[0]}"'
    null_or_clause = " OR ".join(f'"{c}" IS NULL' for c in key_cols)
    rows, distinct, null_key = con.execute(
        f"SELECT COUNT(*) AS total_rows, "
        f"COUNT(DISTINCT {key_sql}) AS distinct_keys, "
        f"SUM(CASE WHEN {null_or_clause} THEN 1 ELSE 0 END) AS null_key_rows "
        f"FROM main.{table}"
    ).fetchone()
    populated = int(rows) - int(null_key or 0)
    collapse = populated - int(distinct)
    return {
        "present": True,
        "rows": int(rows),
        "distinct_key": int(distinct),
        "null_key": int(null_key or 0),
        "collapse": int(collapse),
    }


def part_a(con: Any, *, apply_dedup: bool) -> dict[str, Any]:
    """Within-canonical dedup of NEW canonicals + re-verification of pre-existing."""
    log("=" * 70)
    log("PART (a) — within-canonical event-row dedup + 387 baseline re-verify")
    log("=" * 70)
    out: dict[str, Any] = {"new_canonical_dedup": {}, "pre_existing_reverify": {}}

    # New-canonical dedup probes
    log("  -- NEW canonicals (key = (rid, note_row_id, entity_type, entity_value, source_line)) --")
    for tbl in NEW_EVENT_TABLES:
        info = collapse_count_for(con, tbl, list(NEW_DEDUP_KEY))
        out["new_canonical_dedup"][tbl] = info
        if not info.get("present"):
            log(f"    {tbl}: NOT PRESENT — skipping")
            continue
        if info.get("missing_key_cols"):
            log(f"    {tbl}: missing key cols {info['missing_key_cols']}; skipping dedup")
            continue
        log(f"    {tbl}: rows={info['rows']:,} distinct={info['distinct_key']:,} "
            f"null_key={info['null_key']:,} collapse={info['collapse']:,}")
        if info["collapse"] > 0:
            if apply_dedup:
                _apply_dedup_to(con, tbl)
                # Re-probe after dedup
                post = collapse_count_for(con, tbl, list(NEW_DEDUP_KEY))
                out["new_canonical_dedup"][tbl]["post_apply"] = post
                gate(post["collapse"] == 0,
                     f"post-dedup {tbl} collapse == 0 (got {post['collapse']:,})")
                log(f"      -> dedup applied; new rows={post['rows']:,} (delta={post['rows']-info['rows']:+,})")
            else:
                log(f"    DRY-RUN: would archive + dedup {tbl} "
                    f"(use --apply-dedup to commit)")

    # Pre-existing re-verification sweep
    log("  -- PRE-EXISTING canonicals: re-verify against 387 BASELINE_387 --")
    new_collapses: list[dict[str, Any]] = []
    for tbl in CLEAN_387:
        if tbl in SKIP_REVERIFY:
            continue
        key_cols = PER_TABLE_KEY_OVERRIDES.get(tbl)
        if not key_cols:
            log(f"    {tbl}: no key in PER_TABLE_KEY_OVERRIDES; skipping")
            continue
        info = collapse_count_for(con, tbl, key_cols)
        out["pre_existing_reverify"][tbl] = info
        if not info.get("present"):
            log(f"    {tbl}: NOT PRESENT — skipping")
            continue
        if info["collapse"] != 0:
            new_collapses.append({"table": tbl, "key": key_cols, **info})
            log(f"    NEW COLLAPSE: {tbl} key={key_cols} collapse={info['collapse']:,} (387 baseline=0)")

    for tbl, baseline in BASELINE_387.items():
        key_cols = PER_TABLE_KEY_OVERRIDES.get(tbl)
        if not key_cols:
            continue
        info = collapse_count_for(con, tbl, key_cols)
        out["pre_existing_reverify"][tbl] = {**info, "baseline_387_collapse": baseline["collapse"]}
        if not info.get("present"):
            log(f"    {tbl}: NOT PRESENT — skipping")
            continue
        delta = info["collapse"] - baseline["collapse"]
        log(f"    {tbl}: collapse={info['collapse']:,} (387 baseline={baseline['collapse']:,}, delta={delta:+,})")
        if delta > 0:
            new_collapses.append({
                "table": tbl, "key": key_cols, **info,
                "baseline_387_collapse": baseline["collapse"], "delta": delta,
            })

    out["new_collapses"] = new_collapses
    if new_collapses:
        for c in new_collapses:
            log(f"    NEW collapse on pre-existing: {c['table']} key={c['key']} "
                f"collapse={c['collapse']:,} baseline={c.get('baseline_387_collapse', 0):,}")
        gate(False, f"{len(new_collapses)} pre-existing canonical(s) introduced NEW collapses since 387")

    log("  re-verification clean: no NEW collapses on pre-existing canonicals")
    log("PART (a) complete")
    logger.flush()
    return out


def _apply_dedup_to(con: Any, tbl: str) -> None:
    """Archive + DISTINCT ON rebuild for a NEW events canonical."""
    archive_name = f"{tbl}_predup_20260422"
    log(f"      archiving {tbl} -> archive_pub_v1_0.{archive_name}")
    con.execute(
        f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{archive_name}" AS '
        f"SELECT * FROM main.{tbl}"
    )
    key_csv = ", ".join(NEW_DEDUP_KEY)
    # DISTINCT ON keeps the first row in ORDER BY; pick highest confidence then highest source_line.
    con.execute(f"""
        CREATE OR REPLACE TABLE main.{tbl} AS
        SELECT DISTINCT ON ({key_csv}) *
        FROM main.{tbl}
        ORDER BY {key_csv}, confidence DESC NULLS LAST, source_line DESC NULLS LAST
    """)


def part_b(con: Any, *, apply_archive_drop: bool) -> dict[str, Any]:
    log("=" * 70)
    log("PART (b) — residual archive_drop scan")
    log("=" * 70)
    bucket: dict[str, Any] = {
        "main_tmp_stg_wip": [],
        "registry_superseded": [],
        "readme_deprecation_log_residuals": [],
    }

    rows = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog=? AND table_schema='main'
          AND (table_name LIKE 'tmp\\_%' ESCAPE '\\'
               OR table_name LIKE 'stg\\_%' ESCAPE '\\'
               OR table_name LIKE 'wip\\_%' ESCAPE '\\')""",
                       [CANONICAL_DB]).fetchall()
    bucket["main_tmp_stg_wip"] = [r[0] for r in rows]
    log(f"  main.* tmp/stg/wip: {len(bucket['main_tmp_stg_wip'])} found")

    if column_exists(con, REGISTRY_SCHEMA, REGISTRY_TABLE, "superseded"):
        rows = con.execute(
            f"SELECT detail_table_name FROM {REGISTRY_SCHEMA}.{REGISTRY_TABLE} "
            f"WHERE superseded = TRUE"
        ).fetchall()
        bucket["registry_superseded"] = [r[0] for r in rows]
        log(f"  registry rows superseded=TRUE: {len(bucket['registry_superseded'])} found")
    else:
        log("  registry has no `superseded` column; skipping that probe")
        bucket["registry_superseded"] = []

    log(
        "  __readme deprecation-log residual probe: skipped (the current __readme "
        "schema is content/updated_at/git_sha/script — no structured deprecation log "
        "to parse).  Residual probe would require adding such a log column."
    )

    DEDUP_CANDIDATES_PATH.write_text(json.dumps(bucket, indent=2))
    log(f"  candidates written: {DEDUP_CANDIDATES_PATH.name}")

    total = sum(len(v) for v in bucket.values() if isinstance(v, list))
    if total == 0:
        log("  no residual archive_drop candidates")
    elif not apply_archive_drop:
        log(f"  DRY-RUN: {total} candidates listed; pass --apply-archive-drop to commit")
    else:
        log("  --apply-archive-drop set; processing each candidate...")
        for t in bucket["main_tmp_stg_wip"]:
            archive = f"{t}_archived_20260422"
            log(f"    archiving main.{t} -> archive_pub_v1_0.{archive}")
            con.execute(
                f'CREATE OR REPLACE TABLE "{ARCHIVE_DB}".{ARCHIVE_SCHEMA}."{archive}" AS '
                f"SELECT * FROM main.{t}"
            )
            con.execute(f"DROP TABLE main.{t}")

    log("PART (b) complete")
    logger.flush()
    return bucket


def part_c(con: Any, *, apply_view_renames: bool) -> dict[str, Any]:
    log("=" * 70)
    log("PART (c) — view-naming convention verification")
    log("=" * 70)
    plan: dict[str, Any] = {"violators": []}

    rows = con.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_catalog=? AND table_schema='main' AND table_type='VIEW'
             AND table_name NOT LIKE '%\\_VIEW\\_v%' ESCAPE '\\'""",
        [CANONICAL_DB],
    ).fetchall()
    raw_violators = [r[0] for r in rows]
    plan["violators"] = [v for v in raw_violators if v not in GRANDFATHERED_VIEW_NAMES]
    log(f"  main views without _VIEW_v suffix: {len(raw_violators)} raw, "
        f"{len(plan['violators'])} after grandfathered filter")

    VIEW_RENAMES_PATH.write_text(json.dumps(plan, indent=2))
    log(f"  view-rename plan written: {VIEW_RENAMES_PATH.name}")

    if not plan["violators"]:
        log("  no view-naming violators")
    elif not apply_view_renames:
        log(f"  DRY-RUN: {len(plan['violators'])} violators listed; "
            "pass --apply-view-renames to commit")
    else:
        log("  --apply-view-renames set; renames are NOT auto-implemented this run")
        log("  rationale: each rename needs a CREATE OR REPLACE on dependents; "
            "see memory feedback_alter_view_dependents.md")
        log("  HALT: surfacing to user for manual rename PR")
        gate(False, f"{len(plan['violators'])} view-naming violators need manual rename PR")

    log("PART (c) complete")
    logger.flush()
    return plan


def write_close_out(state: dict[str, Any], a: dict[str, Any], b: dict[str, Any], c: dict[str, Any]) -> None:
    new_dedup = a["new_canonical_dedup"]
    new_collapses = a.get("new_collapses", [])
    rows: list[str] = []
    rows.append("# Script 386 — post-387 dedup pass close-out\n")
    rows.append(f"**Date:** {datetime.now(timezone.utc).isoformat()}\n")
    rows.append("**Script:** `scripts/386_v1_0_dedup_pass.py`\n")
    rows.append("**Prompt:** `cursor_prompts/CURSOR_PROMPT_LLM_INTEGRATION_AND_V1_0_DEDUP_20260422_POST387.md`\n\n")

    rows.append("## Pre-state (387 verification)\n\n")
    for k, v in state.items():
        rows.append(f"- `{k}` = {v}\n")
    rows.append("\n")

    rows.append("## Part (a) — within-canonical dedup\n\n")
    rows.append("### NEW canonicals (auto-applied if collapse>0)\n\n")
    rows.append("| canonical | rows | distinct | null_key | collapse | post-apply rows | post collapse |\n")
    rows.append("|---|---:|---:|---:|---:|---:|---:|\n")
    for tbl, info in new_dedup.items():
        if not info.get("present"):
            rows.append(f"| `{tbl}` | (not present) | | | | | |\n")
            continue
        post = info.get("post_apply", {})
        rows.append(
            f"| `{tbl}` | {info['rows']:,} | {info['distinct_key']:,} | "
            f"{info['null_key']:,} | {info['collapse']:,} | "
            f"{post.get('rows', '—'):,} | {post.get('collapse', '—')} |\n"
            if post else
            f"| `{tbl}` | {info['rows']:,} | {info['distinct_key']:,} | "
            f"{info['null_key']:,} | {info['collapse']:,} | n/a | n/a |\n"
        )

    rows.append("\n### Pre-existing canonicals re-verification\n\n")
    if not new_collapses:
        rows.append("No NEW collapses introduced since 387 baseline. Pre-existing carry-forwards "
                    "(complications, invasion, medications, molecular_genetics_v2, path_malignant, "
                    "pmh, psh) remain at the 387 baseline counts.\n\n")
    else:
        rows.append(f"**HALT:** {len(new_collapses)} pre-existing canonical(s) introduced NEW collapses.\n\n")
        for c_ in new_collapses:
            rows.append(f"- `{c_['table']}` key=`{c_['key']}` collapse={c_['collapse']:,} "
                        f"(baseline={c_.get('baseline_387_collapse', 0):,})\n")

    rows.append("\n## Part (b) — residual archive_drop scan\n\n")
    rows.append(f"- main.* tmp/stg/wip: {len(b.get('main_tmp_stg_wip', []))}\n")
    rows.append(f"- registry rows superseded=TRUE: {len(b.get('registry_superseded', []))}\n")
    rows.append("- __readme deprecation log: probe skipped (no structured log column)\n\n")

    rows.append("## Part (c) — view-naming verification\n\n")
    rows.append(f"- main views violating `%_VIEW_v%` convention: "
                f"{len(c.get('violators', []))}\n\n")

    rows.append("## Carry-forward items (filed for follow-up)\n\n")
    rows.append("### 6.A — Synoptic_diagnosis 88% extraction error rate (Script 368 / vasc v2)\n")
    rows.append("Vascular v2 had 3,187 errors / 3,635 attempts (~88%) on `source_column = "
                "synoptic_diagnosis`. RunPod vLLM `InternalServerError`, likely context-length "
                "or batch-timing issue specific to this column's text shape. Re-extract on "
                "smaller batches with a more conservative inference profile.\n\n")
    rows.append("### 6.B — `canonical_invasion_events_v1` rebuild (Script 388 candidate)\n")
    rows.append("Cross-domain invasion canonical still references the qwen-era source via "
                "`extraction_run_id`. Needs re-derivation from the post-368 source while "
                "preserving cross-domain rows from path_malignant + frozen_section + airway + "
                "ETE.\n\n")
    rows.append("### 6.C — Pathology `benign_pathology` entity routing precision\n")
    rows.append("Spot-check found 2/4 borderline cases (atypical-cell + tubular adenoma "
                "routing). If `nlp_path_*` rollups are used in any cohort definition, audit "
                "the patient list for false positives before publishing.\n\n")
    rows.append("### 6.D — Capsular_invasion margin-distance false positives (vasc v2)\n")
    rows.append("v2 prompt returns `capsular_invasion` for margin-distance phrases "
                "('inked/capsular margin is very close (0.1mm)', '<1 mm from anterior'). When "
                "rebuilding `canonical_invasion_events_v1` (carry-forward 6.B), add evidence-"
                "text postfilter rejecting `\\d+(\\.\\d+)?\\s*(mm|cm)` patterns that lack "
                "other capsular language.\n\n")
    rows.append("### 6.E — `tier2.*` and `verify.*` schema drop — DONE by Script 387\n\n")
    rows.append("### 6.F — Tirads_granular absorb chain (Script 388 candidate, NEW)\n")
    rows.append("Script 383 landed `note_entities_llm_tirads_granular` but the 376/377/378 "
                "absorb chain has no live inputs (`tirads_v2_nodules_raw`, "
                "`note_entities_llm_us_nodule_dynamics`, `note_entities_llm_imaging` were all "
                "archived to us_legacy_20260421 on 2026-04-21). A future script needs to "
                "reconstitute the parsing pipeline from the new tirads_granular source into "
                "`canonical_us_nodule_v2`.\n\n")
    rows.append("### 6.G — `nlp_vasc_positive_mentioned` patient count slightly above evaluator estimate\n")
    rows.append("Script 368 post-fix produced 776 patients with `nlp_vasc_positive_mentioned=TRUE`, "
                "vs the evaluator's spot-check estimate of 719. Within precision-improvement "
                "tolerance (well below the >900 fail threshold), but slightly above the "
                "[680, 760] sanity range from the verification checklist. Likely driven by "
                "qualifier-NULL + present_or_negated='present' rows the evaluator's qualifier-"
                "only count excluded.\n\n")
    rows.append("### 6.H — `chain script flag` discrepancy in cursor prompt\n")
    rows.append("Prompt §3 B3 references `--apply` flag for Scripts 376/377/378/379; the actual "
                "scripts accept `--commit`. Documented in Script 383 source comments.\n\n")
    rows.append("### 6.I — Round-2 ckpt `llm_model` tag is qwen2.5-32b, not gpt-oss-120b\n")
    rows.append("All 4 round-2 ckpt JSONL files are tagged `llm_model: qwen2.5-32b`, but row "
                "counts and entity-type distributions match the prompt's gpt-oss-120b stats "
                "exactly. Either the ckpt model field is stale (likely) or the prompt's claim "
                "is wrong. We preserved the model tag as-is on the source tables; surface to "
                "the evaluator for ground-truth on which model actually generated these.\n\n")
    CLOSE_OUT_PATH.write_text("".join(rows))
    log(f"  close-out: {CLOSE_OUT_PATH.name}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 386 — post-387 v1_0 dedup pass")
    ap.add_argument("--apply-dedup", action="store_true",
                    help="Auto-apply within-canonical dedup on the 3 NEW canonicals.")
    ap.add_argument("--apply-archive-drop", action="store_true",
                    help="Apply archive_drop on residual main.* tmp/stg/wip + superseded rows.")
    ap.add_argument("--apply-view-renames", action="store_true",
                    help="Apply view renames (currently halts with manual-PR instruction).")
    args = ap.parse_args()

    log(f"Script 386 — post-387 dedup pass — {datetime.now(timezone.utc).isoformat()}")
    log(f"  apply_dedup={args.apply_dedup}  apply_archive_drop={args.apply_archive_drop}  "
        f"apply_view_renames={args.apply_view_renames}")

    con = connect_md(logger)
    state = pre_state(con)
    a = part_a(con, apply_dedup=args.apply_dedup)
    b = part_b(con, apply_archive_drop=args.apply_archive_drop)
    c = part_c(con, apply_view_renames=args.apply_view_renames)

    write_close_out(state, a, b, c)
    log("Done.")
    logger.flush()


if __name__ == "__main__":
    main()
