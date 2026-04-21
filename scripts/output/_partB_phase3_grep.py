#!/usr/bin/env python3
"""Phase 3 grep: enumerate every script-level reader of CPM TIRADS columns
using the amended regex from the pre-B prompt's Amendments section.

Excludes:
  - scripts/frozen/  (will hold the frozen writer scripts)
  - scripts/output/  (scratch + this script itself)
  - **/.venv/**, **/__pycache__/**
  - any file whose basename starts with "_" (scratch helpers, including pre-B
    scripts and Part A audit scripts that legitimately reference these names
    in MAPPING tables — they don't READ from CPM, they just enumerate names)

Excludes archive_*/ paths (frozen-by-design references).
"""
from __future__ import annotations

import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent

# Authoritative legacy column list — drawn from pre-B coverage table
# (drop_no_replacement + retired_redesign + everything mapped to cupm_v2 via
# rename, AS WELL AS the 8 gap_other_v2_table cols Logan flagged for inline
# JOINs at the consumer).
LEGACY_COLS = [
    # _v12 family (12 cols)
    "tirads_best_category_v12", "tirads_worst_category_v12",
    "tirads_best_score_v12", "tirads_worst_score_v12",
    "tirads_n_nodule_records_v12", "tirads_nodule_size_max_mm_v12",
    "tirads_concordant_count_v12", "tirads_mismatch_count_v12",
    "tirads_has_acr_recalc_v12", "tirads_n_sources_v12",
    "tirads_reliability_v12", "tirads_source_v12",
    # _v271 family (3 cols)
    "tirads_worst_points_v271", "tirads_best_points_v271",
    "tirads_source_system_v271",
    # _v271b laterality (3 cols)
    "tumor_pathology_laterality_v271b", "imaging_laterality_rollup_v271b",
    "pathology_vs_imaging_laterality_concordant_v271b",
    # combined (3 cols)
    "tirads_best_combined", "tirads_worst_combined", "tirads_nodules_scored_combined",
    # un-suffixed legacy + obvious pairs (10 cols)
    "max_tirads_ever", "imaging_tirads_best", "imaging_tirads_worst",
    "worst_tirads_category", "preop_tirads_best", "preop_tirads_worst",
    "preop_tirads_category", "imaging_updated_tirads_category_cpm_v1",
    "imaging_tirads_source", "imaging_laterality_rollup",
    "pathology_vs_imaging_laterality_concordant",
    # _v2 / tirads_v2_* family on CPM (16 cols — all to be dropped from CPM)
    "imaging_tirads_best_v2", "imaging_tirads_worst_v2",
    "imaging_updated_tirads_category_cpm_v2", "imaging_laterality_rollup_v2",
    "max_tirads_ever_v2", "preop_tirads_best_v2", "preop_tirads_category_v2",
    "tirads_v2_n_nodules_scored", "tirads_v2_worst_category",
    "tirads_v2_max_points", "tirads_v2_largest_nodule_cm",
    "tirads_v2_any_ete_on_us", "tirads_v2_any_interval_growth",
    "tirads_v2_any_fna_recommended", "tirads_v2_n_reports",
    "tirads_v2_any_suspicious_ln_on_us", "tirads_v2_shortest_followup_months",
    "tirads_v2_worst_rank", "tirads_v2_worst_rank_source",
    "tirads_v2_any_fna_recommended_report",
    "tirads_v2_any_fna_recommended_report_source",
]

SEARCH_DIRS = ["scripts", "sql", "manuscripts", "studies", "lakehouse", "utils", "app", "notebooks"]

EXCLUDE_GLOBS = [
    "!scripts/output/**",
    "!scripts/frozen/**",
    "!scripts/archive/**",
    "!scripts/preB_*.py",                # pre-B scripts contain MAPPING tables
    "!scripts/cpm_tirads_partB_*.py",    # Part B own scripts
    "!**/_partB_*.py",
    "!**/_cpm_tirads_*.py",
    "!**/.venv/**",
    "!**/__pycache__/**",
    "!**/*.parquet",
    "!**/*.json",
    "!**/*.log",
    "!**/*.csv",
    "!**/*.md",                          # cursor prompts + documentation
    "!**/*.before.sql",                  # archived view defs
    "!**/*.after.sql",                   # new view defs
    "!archive*/**",
    "!exports/_archive_*/**",
]

pattern = "|".join(rf"\b{re.escape(c)}\b" for c in LEGACY_COLS)

cmd = [
    "rg", "--no-heading", "--with-filename", "--line-number",
    "-e", pattern,
    *SEARCH_DIRS,
]
for g in EXCLUDE_GLOBS:
    cmd += ["--glob", g]

proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
if proc.returncode not in (0, 1):
    raise SystemExit(f"rg failed: {proc.stderr}")

# Parse hits
hits_per_file: dict[str, list[tuple[int, str, set[str]]]] = defaultdict(list)
col_rgx = {c: re.compile(rf"\b{re.escape(c)}\b") for c in LEGACY_COLS}

for raw in proc.stdout.splitlines():
    m = re.match(r"^([^:]+):(\d+):(.*)$", raw)
    if not m:
        continue
    path, ln, line = m.group(1), int(m.group(2)), m.group(3)
    if line.strip().startswith(("#", "--")):
        continue  # comments don't count
    matched_cols = {c for c, rgx in col_rgx.items() if rgx.search(line)}
    if matched_cols:
        hits_per_file[path].append((ln, line.rstrip(), matched_cols))

# Summary
report: dict[str, dict] = {}
for path, hits in sorted(hits_per_file.items()):
    cols_seen: set[str] = set()
    for _, _, cols in hits:
        cols_seen |= cols
    report[path] = {
        "n_hits": len(hits),
        "distinct_cols": sorted(cols_seen),
        "first_3_examples": [
            f"L{ln}: {line[:140]}" for ln, line, _ in hits[:3]
        ],
    }

(OUT / "_partB_phase3_grep.json").write_text(json.dumps(report, indent=2))

print(f"Total files with reader hits: {len(report)}")
print(f"Total hits: {sum(r['n_hits'] for r in report.values())}")
print()
for path, r in sorted(report.items(), key=lambda kv: -kv[1]["n_hits"]):
    print(f"  {path}  ({r['n_hits']} hits, {len(r['distinct_cols'])} distinct cols)")
    for c in r["distinct_cols"][:8]:
        print(f"    - {c}")
    if len(r["distinct_cols"]) > 8:
        print(f"    - ... +{len(r['distinct_cols']) - 8} more")
