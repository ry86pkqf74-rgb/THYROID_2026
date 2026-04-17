#!/usr/bin/env python3
"""Script 270b — Phase A Step 2: registry closure (Bucket-B + Bucket-C).

This is the substantive Phase A work. Modes:

  --dry-run (DEFAULT)
      Read-only. Runs the 4-stage matcher on every Bucket-C row, auto-
      assigns Bucket-B to build_pipeline, and emits decision CSVs +
      verbose match log + summary JSON. Human reviews
      270b_bucket_c_ambiguous.csv (fills in resolution_feeder column),
      then re-runs in --execute mode.

  --execute (NOT IMPLEMENTED IN THIS COMMIT)
      Will read ambiguous.csv resolutions, ALTER triage table to add
      resolution metadata columns, INSERT build_pipeline registry row,
      UPDATE detail_table_registry_v1.feeds_master_columns_normalized
      for every closure, register main_schema_keep_list_v1 for
      ete_adjudication_v1 + path_size_adjudication_v241, write audit
      rows, and emit execute summary.

      The dry-run output is the gate: do NOT run --execute until the
      ambiguous.csv has been reviewed and committed (or confirmed empty).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OBVIOUS_CSV = OUT_DIR / "270b_bucket_c_obvious_matches.csv"
AMBIGUOUS_CSV = OUT_DIR / "270b_bucket_c_ambiguous.csv"
BUILD_PIPELINE_CSV = OUT_DIR / "270b_bucket_c_build_pipeline.csv"
MATCH_LOG_JSONL = OUT_DIR / "270b_step_2_match_log.jsonl"
DRY_RUN_SUMMARY = OUT_DIR / "270b_step_2_dry_run_summary.json"
DRY_RUN_LOG = OUT_DIR / "270b_step_2_dry_run.log"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
TRIAGE_FQ = f"{WS}.cpm_unmapped_triage_v265"
REGISTRY_FQ = f"{WS}.detail_table_registry_v1"

# =========================================================================
# Stage-2 suffix-stripping list (extended per Q1 amendment)
# Order matters — strip longer suffixes first so '_inferred_negative' wins
# over '_negative' (if we ever add it). The _v\d+ pattern is regex-based
# and applied separately.
# =========================================================================
STAGE2_SUFFIXES_LITERAL = (
    "_inferred_negative",
    "_adjudicated",
    "_reconciled",
    "_corrected",
    "_raw_str",  # before _raw / _str
    "_final",
    "_method",
    "_source",
    "_name",
    "_code",
    "_num",
    "_nlp",
    "_raw",
)
STAGE2_VERSION_RE = re.compile(r"_v\d+$")


def strip_one_suffix(col: str) -> list[str]:
    """Return all single-strip variants of `col` for stage 2."""
    out: list[str] = []
    for suf in STAGE2_SUFFIXES_LITERAL:
        if col.endswith(suf) and len(col) > len(suf):
            out.append(col[: -len(suf)])
    m = STAGE2_VERSION_RE.search(col)
    if m and m.start() > 0:
        out.append(col[: m.start()])
    return out


# =========================================================================
# Stage-3 domain prefix map (per Q1 spec)
# When a prefix maps to multiple feeders, multi-candidate routing applies
# (registry-prefer → coverage-prefer → ambiguous).
# =========================================================================
DOMAIN_PREFIX_MAP: dict[str, list[str]] = {
    "mol_": [
        "canonical_molecular_tested_v1",
        "molecular_test_episode_v2",
        "thyroseq_molecular_enrichment",
        "_molecular_patient_rollup_v227",
    ],
    "us_": [
        "canonical_us_nodule_characteristics_v1",
        "us_nodules_tirads",
        "imaging_nodule_master_v1",
    ],
    "tirads_": [
        "canonical_us_nodule_characteristics_v1",
        "extracted_tirads_validated_v1",
    ],
    "ct_": ["ct_imaging"],
    "mri_": ["mri_imaging"],
    "nuc_": ["nuclear_med"],
    "ln_": [
        "canonical_tumor_characteristics_v1",
        "clinical_note_ln_extracted_v1",
    ],
    "tg_": [
        "thyroglobulin_lab_canonical_v1",
        "tg_timeline_patient_summary_v1",
    ],
    "rai_": ["rai_treatment_episode_v2"],
    "fna_": [
        "fna_cytology",
        "fna_episode_master_v2",
        "extracted_fna_bethesda_v1",
        "fna_history",
    ],
    "bethesda_": [
        "fna_cytology",
        "extracted_fna_bethesda_v1",
    ],
    "ete_": [
        "canonical_tumor_characteristics_v1",
        "ete_adjudication_v1",
        "extracted_ete_subgraded_v1",
    ],
    "path_": [
        "path_synoptics",
        "canonical_tumor_characteristics_v1",
    ],
    "syn_": [
        "synoptic_tumor_long_v1",
        "path_synoptics",
    ],
    "tumor_": [
        "canonical_tumor_characteristics_v1",
        "tumor_episode_master_v2",
    ],
    "op_": ["operative_episode_detail_v2"],
    "recur_": [
        "canonical_recurrence_v1",
        "recurrence_event_clean_v1",
    ],
    "complication_": [
        "complication_patient_summary_v1",
        "complication_phenotype_v1",
        "extracted_complications_refined_v5",
    ],
    "nsqip_": ["nsqip_patient_summary"],
    # nlp_ is a soft prefix for NLP-derived booleans; route to
    # build_pipeline rather than guessing among 6 note_entities_* tables.
    "nlp_": [],
}


# =========================================================================
# Match decision dataclass-equivalent (kept as dict for JSONL serialization)
# =========================================================================

def _new_decision(col: str) -> dict:
    return {
        "cpm_col": col,
        "stage": None,                # 1, 2, 3, 4
        "match_method": None,         # 'stage1_exact' | 'stage2_suffix_<suf>' | 'stage3_domain_registry_preferred' | 'stage3_domain_coverage_preferred' | 'stage3_domain_ambiguous' | 'stage4_build_pipeline'
        "resolved_feeder": None,      # winning feeder OR 'build_pipeline'
        "category": None,             # 'obvious' | 'ambiguous' | 'build_pipeline'
        "candidates": [],             # all candidates considered at the winning stage
        "alternatives_rejected": [],  # candidates dropped by tie-break with reason
        "matched_source_col": None,   # exact source column (for stage 1/2)
        "stage_attempts": [],         # per-stage attempt log
        "decided_at": datetime.now(timezone.utc).isoformat(),
    }


# =========================================================================
# Pre-fetch helpers
# =========================================================================

def fetch_registry(con) -> dict[str, dict]:
    """Return {detail_table_name: {schema_name, total_patients, normalized_set, normalized_str, has_normalized}}."""
    out: dict[str, dict] = {}
    rows = con.execute(f"""
        SELECT detail_table_name, schema_name, total_patients,
               feeds_master_columns_normalized
        FROM {REGISTRY_FQ}
    """).fetchall()
    for name, schema, tp, norm in rows:
        norm_str = norm or ""
        has_norm = bool(norm_str.strip()) and "TODO" not in norm_str.upper()
        norm_set = {tok.strip() for tok in norm_str.split(";") if tok.strip()}
        out[name] = {
            "schema_name": schema,
            "total_patients": int(tp) if tp is not None else 0,
            "normalized_str": norm_str,
            "normalized_set": norm_set,
            "has_normalized": has_norm,
        }
    return out


def fetch_table_columns(con, registry: dict[str, dict]) -> dict[str, set[str]]:
    """For every registered detail_table_name, return its column set
    via information_schema. Tables without an information_schema entry
    (catalogs, ghosts) are skipped silently."""
    if not registry:
        return {}
    name_list = list(registry.keys())
    placeholders = ", ".join(repr(n) for n in name_list)
    rows = con.execute(f"""
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_name IN ({placeholders})
    """).fetchall()
    out: dict[str, set[str]] = {n: set() for n in name_list}
    for sch, tbl, col in rows:
        # Prefer schema match if registry has a schema_name set
        wanted_schema = registry.get(tbl, {}).get("schema_name")
        if wanted_schema and sch != wanted_schema:
            continue
        out[tbl].add(col)
    return out


def fetch_triage(con, bucket: str) -> list[str]:
    rows = con.execute(f"""
        SELECT column_name FROM {TRIAGE_FQ}
        WHERE triage_bucket = '{bucket}'
        ORDER BY column_name
    """).fetchall()
    return [r[0] for r in rows]


# =========================================================================
# Tie-break logic (per Q1 spec)
# =========================================================================

def tie_break(
    candidates: list[str],
    registry: dict[str, dict],
    stage_label: str,
) -> tuple[str | None, str, list[dict]]:
    """Returns (winner_or_None_if_ambiguous, match_method, rejected_log).

    Rules (per Q1):
      - if exactly one candidate is in registry with non-empty normalized,
        pick it (registry_preferred)
      - if 2+ candidates are in registry (with non-empty normalized),
        return ambiguous
      - if 0 in registry, pick highest total_patients (coverage_preferred)
      - never alphabetical
    """
    # De-dup while preserving order
    seen = set()
    deduped: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            deduped.append(c)
    if not deduped:
        return None, f"{stage_label}_no_candidates", []

    if len(deduped) == 1:
        return deduped[0], f"{stage_label}_single_candidate", []

    in_registry_with_norm = [c for c in deduped if registry.get(c, {}).get("has_normalized")]

    rejected: list[dict] = []
    if len(in_registry_with_norm) == 1:
        winner = in_registry_with_norm[0]
        for c in deduped:
            if c != winner:
                rejected.append({
                    "candidate": c,
                    "reason": (
                        "in_registry_no_normalized" if c in registry
                        and not registry[c].get("has_normalized")
                        else "not_in_registry"
                    ),
                })
        return winner, f"{stage_label}_registry_preferred", rejected

    if len(in_registry_with_norm) >= 2:
        return None, f"{stage_label}_ambiguous_multi_registry", []

    # 0 in registry: coverage heuristic
    in_registry = [c for c in deduped if c in registry]
    if len(in_registry) == 0:
        return None, f"{stage_label}_ambiguous_none_in_registry", []
    # Sort by total_patients DESC, fall back on len(detail_table_name)?
    # Per Q1: "highest total_patients". If tied on coverage → ambiguous.
    by_cov = sorted(in_registry,
                    key=lambda c: (-registry[c]["total_patients"], c))
    top_cov = registry[by_cov[0]]["total_patients"]
    cov_winners = [c for c in in_registry if registry[c]["total_patients"] == top_cov]
    if len(cov_winners) > 1:
        return None, f"{stage_label}_ambiguous_tied_coverage", []
    winner = cov_winners[0]
    for c in deduped:
        if c != winner:
            rejected.append({
                "candidate": c,
                "reason": (
                    f"coverage_lower (winner_total_patients={top_cov})"
                    if c in registry
                    else "not_in_registry"
                ),
            })
    return winner, f"{stage_label}_coverage_preferred", rejected


# =========================================================================
# 4-stage matcher
# =========================================================================

def match_one(
    col: str,
    registry: dict[str, dict],
    feeder_cols: dict[str, set[str]],
) -> dict:
    decision = _new_decision(col)

    # ---- Stage 1: exact name in any feeder column set ----
    s1_candidates = [t for t, cols in feeder_cols.items() if col in cols]
    decision["stage_attempts"].append({
        "stage": 1, "method": "exact",
        "n_candidates": len(s1_candidates),
        "candidates": s1_candidates,
    })
    if s1_candidates:
        winner, method, rejected = tie_break(s1_candidates, registry, "stage1_exact")
        if winner:
            decision.update({
                "stage": 1,
                "match_method": method,
                "resolved_feeder": winner,
                "category": "obvious",
                "candidates": s1_candidates,
                "alternatives_rejected": rejected,
                "matched_source_col": col,
            })
            return decision
        decision["category_provisional"] = "ambiguous_at_stage_1"
        decision["candidates"] = s1_candidates
        decision["match_method"] = method
        decision["category"] = "ambiguous"
        return decision

    # ---- Stage 2: suffix-stripped match ----
    s2_attempts: list[dict] = []
    s2_candidates: list[tuple[str, str, str]] = []  # (feeder, source_col, suffix_used)
    for stripped in strip_one_suffix(col):
        for t, cols in feeder_cols.items():
            if stripped in cols:
                # Detect which suffix produced this stripped form
                if STAGE2_VERSION_RE.search(col[len(stripped):]):
                    suf = "_v\\d+"
                else:
                    suf = col[len(stripped):]
                s2_candidates.append((t, stripped, suf))
        s2_attempts.append({"stripped_to": stripped, "n_hits": sum(
            1 for c in s2_candidates if c[1] == stripped)})
    decision["stage_attempts"].append({
        "stage": 2, "method": "suffix_strip",
        "attempts": s2_attempts,
        "n_candidates": len(s2_candidates),
    })
    if s2_candidates:
        feeder_only = [c[0] for c in s2_candidates]
        winner, method, rejected = tie_break(feeder_only, registry, "stage2_suffix")
        if winner:
            # Find the (source_col, suffix) for the winner
            chosen = next(c for c in s2_candidates if c[0] == winner)
            decision.update({
                "stage": 2,
                "match_method": f"{method}_via_{chosen[2]}",
                "resolved_feeder": winner,
                "category": "obvious",
                "candidates": list(set(feeder_only)),
                "alternatives_rejected": rejected,
                "matched_source_col": chosen[1],
            })
            return decision
        decision.update({
            "stage": 2,
            "match_method": method,
            "category": "ambiguous",
            "candidates": list(set(feeder_only)),
        })
        return decision

    # ---- Stage 3: domain prefix map ----
    s3_candidates: list[str] = []
    matched_prefix: str | None = None
    for prefix, feeders in DOMAIN_PREFIX_MAP.items():
        if col.startswith(prefix):
            matched_prefix = prefix
            s3_candidates = [f for f in feeders if f in registry or f in feeder_cols]
            break
    decision["stage_attempts"].append({
        "stage": 3, "method": "domain_prefix",
        "matched_prefix": matched_prefix,
        "raw_candidates": (DOMAIN_PREFIX_MAP.get(matched_prefix, []) if matched_prefix else []),
        "filtered_candidates": s3_candidates,
    })
    if s3_candidates:
        winner, method, rejected = tie_break(s3_candidates, registry, "stage3_domain")
        if winner:
            decision.update({
                "stage": 3,
                "match_method": method,
                "resolved_feeder": winner,
                "category": "obvious",
                "candidates": s3_candidates,
                "alternatives_rejected": rejected,
                "matched_source_col": None,
            })
            return decision
        decision.update({
            "stage": 3,
            "match_method": method,
            "category": "ambiguous",
            "candidates": s3_candidates,
        })
        return decision

    # ---- Stage 4: build_pipeline fallback ----
    decision.update({
        "stage": 4,
        "match_method": "stage4_build_pipeline",
        "resolved_feeder": "build_pipeline",
        "category": "build_pipeline",
        "candidates": [],
    })
    return decision


# =========================================================================
# Dry-run driver
# =========================================================================

def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270b_phase_a_step_2_registry.py --dry-run",
            "generated_at", datetime.now(timezone.utc).isoformat(),
        ])
        w.writerow(header)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])


def main_dry_run() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log(f"=== START 270b Step 2 (DRY-RUN) ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    log("\n--- pre-fetch ---")
    registry = fetch_registry(con)
    log(f"  detail_table_registry_v1: {len(registry)} rows")
    feeder_cols = fetch_table_columns(con, registry)
    feeder_cols_nonempty = {k: v for k, v in feeder_cols.items() if v}
    log(f"  feeders with discoverable columns: {len(feeder_cols_nonempty)}/{len(registry)}")

    bucket_c_cols = fetch_triage(con, "C_missing_feeder")
    bucket_b_cols = fetch_triage(con, "B_computed_score")
    log(f"  Bucket-C rows: {len(bucket_c_cols)}")
    log(f"  Bucket-B rows: {len(bucket_b_cols)}")

    # ---- Bucket-C matching ----
    log("\n--- Bucket-C 4-stage matching ---")
    decisions: list[dict] = []
    for col in bucket_c_cols:
        d = match_one(col, registry, feeder_cols)
        decisions.append(d)

    obvious = [d for d in decisions if d["category"] == "obvious"]
    ambiguous = [d for d in decisions if d["category"] == "ambiguous"]
    build_pipe = [d for d in decisions if d["category"] == "build_pipeline"]
    log(f"  obvious matches:  {len(obvious)}")
    log(f"  ambiguous:        {len(ambiguous)}")
    log(f"  build_pipeline:   {len(build_pipe)}")

    # Stage breakdown for obvious
    by_stage = {}
    for d in obvious:
        by_stage.setdefault(d["stage"], 0)
        by_stage[d["stage"]] += 1
    log(f"  obvious stage breakdown: {by_stage}")

    # ---- Bucket-B (auto-assign build_pipeline) ----
    log("\n--- Bucket-B auto-assignment ---")
    bucket_b_decisions: list[dict] = []
    for col in bucket_b_cols:
        d = _new_decision(col)
        d.update({
            "stage": 0,
            "match_method": "bucket_b_auto_build_pipeline",
            "resolved_feeder": "build_pipeline",
            "category": "build_pipeline",
            "candidates": [],
        })
        bucket_b_decisions.append(d)
    log(f"  Bucket-B → build_pipeline: {len(bucket_b_decisions)}")

    # ---- Write CSVs ----
    log("\n--- emit CSVs ---")

    # Obvious
    obvious_rows = []
    for d in obvious:
        obvious_rows.append([
            d["cpm_col"],
            d["resolved_feeder"],
            d["match_method"],
            d["matched_source_col"] or "",
            json.dumps(d["alternatives_rejected"]) if d["alternatives_rejected"] else "",
        ])
    write_csv(OBVIOUS_CSV, [
        "cpm_col", "feeder", "match_method", "matched_source_col",
        "alternatives_rejected_json",
    ], obvious_rows)
    log(f"  wrote {OBVIOUS_CSV} ({len(obvious_rows)} rows)")

    # Ambiguous (resolution_feeder column blank for human fill)
    amb_rows = []
    for d in ambiguous:
        amb_rows.append([
            d["cpm_col"],
            json.dumps(sorted(set(d["candidates"]))),
            d["match_method"],
            d["stage"],
            "",  # resolution_feeder (human fills this)
            "",  # resolution_notes (human optional)
        ])
    write_csv(AMBIGUOUS_CSV, [
        "cpm_col", "candidate_feeders_json", "why_ambiguous",
        "stage_at_ambiguity", "resolution_feeder", "resolution_notes",
    ], amb_rows)
    log(f"  wrote {AMBIGUOUS_CSV} ({len(amb_rows)} rows)")

    # Build pipeline (Bucket-C fallthrough only — Bucket-B has its own file in --execute)
    bp_rows = []
    for d in build_pipe:
        bp_rows.append([
            d["cpm_col"],
            "no_feeder_candidate_in_4_stages",
            json.dumps(d["stage_attempts"]),
        ])
    write_csv(BUILD_PIPELINE_CSV, [
        "cpm_col", "reason_no_feeder", "stage_attempts_json",
    ], bp_rows)
    log(f"  wrote {BUILD_PIPELINE_CSV} ({len(bp_rows)} rows)")

    # Match log (full provenance: every match decision for all 174 + 49)
    with MATCH_LOG_JSONL.open("w") as f:
        for d in decisions:
            f.write(json.dumps({"source_set": "C_missing_feeder", **d}) + "\n")
        for d in bucket_b_decisions:
            f.write(json.dumps({"source_set": "B_computed_score", **d}) + "\n")
    log(f"  wrote {MATCH_LOG_JSONL} ({len(decisions) + len(bucket_b_decisions)} entries)")

    # Summary JSON
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "publication_db": PUBLICATION_DB,
        "mode": "dry-run",
        "registry_rows": len(registry),
        "feeders_with_discoverable_columns": len(feeder_cols_nonempty),
        "bucket_c_input": len(bucket_c_cols),
        "bucket_b_input": len(bucket_b_cols),
        "bucket_c_obvious_matches": len(obvious),
        "bucket_c_ambiguous": len(ambiguous),
        "bucket_c_build_pipeline_fallthrough": len(build_pipe),
        "bucket_b_auto_build_pipeline": len(bucket_b_decisions),
        "obvious_stage_breakdown": by_stage,
        "outputs": {
            "obvious_csv": str(OBVIOUS_CSV),
            "ambiguous_csv": str(AMBIGUOUS_CSV),
            "build_pipeline_csv": str(BUILD_PIPELINE_CSV),
            "match_log_jsonl": str(MATCH_LOG_JSONL),
        },
        "stage_2_suffix_list_literal": list(STAGE2_SUFFIXES_LITERAL),
        "stage_2_version_regex": "_v\\d+$",
        "stage_3_domain_prefix_map": {
            k: v for k, v in DOMAIN_PREFIX_MAP.items()
        },
        "next_action": (
            "Human review of 270b_bucket_c_ambiguous.csv (fill resolution_feeder "
            "column for each row), commit, then re-run with --execute."
            if amb_rows else
            "ambiguous.csv is empty; --execute can run without further review."
        ),
    }
    DRY_RUN_SUMMARY.write_text(json.dumps(summary, indent=2, default=str))
    DRY_RUN_LOG.write_text("".join(log_lines))
    log(f"\n  wrote {DRY_RUN_SUMMARY}")
    log(f"  wrote {DRY_RUN_LOG}")

    log(f"\n=== END 270b Step 2 (DRY-RUN) ===")
    DRY_RUN_LOG.write_text("".join(log_lines))
    return 0


def main_execute() -> int:
    print(
        "ERROR: --execute mode is intentionally NOT YET IMPLEMENTED in this commit.\n"
        "Workflow:\n"
        "  1. Run --dry-run (default).\n"
        "  2. Review scripts/output/270b_bucket_c_ambiguous.csv; fill the\n"
        "     resolution_feeder column for each row (or confirm empty).\n"
        "  3. Commit the reviewed ambiguous.csv.\n"
        "  4. A subsequent commit will add --execute mode to this file.\n"
        "Refusing to proceed.",
        file=sys.stderr,
    )
    return 2


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", default=True,
                   help="Read-only: emit decision CSVs (default).")
    p.add_argument("--execute", action="store_true", default=False,
                   help="Apply registry closures + audit writes (NOT YET WIRED).")
    args = p.parse_args()
    if args.execute:
        return main_execute()
    return main_dry_run()


if __name__ == "__main__":
    sys.exit(main())
