#!/usr/bin/env python3
"""
Part B / Phase 4: freeze 33 scripts that touch CPM TIRADS columns.

Per Logan's Path Z directive (publication-stable lockdown):
  - 16 original writer freezes (Logan's adjudication)
  - 2 newly-found writers (caught by f-string-tolerant regex in my Phase 3 recon)
  - 2 Cat B aliasing scripts (own CTAS uses TIRADS-shaped names; not real readers)
  - 8 Cat C one-shot audit/verification scripts
  - 5 Cat A one-shot metadata generators

Per Logan: "The freeze isn't a punishment — it's a marker that says 'this tool
belongs to a pre-publication schema state; don't run it against the post-Part-B
CPM.'"

Mechanics per script:
  1. git mv scripts/<name>.py -> scripts/frozen/<name>.py
  2. Prepend FROZEN header block (after shebang line, before docstring)
  3. Special headers:
       - 221, 221b   → include NEW TARGET ON REFRESH line per Logan's directive
       - 48, 50      → include CAT B note per Logan's directive

Updates scripts/frozen/README.md with one line per frozen script.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
FROZEN = SCRIPTS / "frozen"
OUT = SCRIPTS / "output"


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Map source path → (one-line summary for README, optional special-header lines)
FREEZE_LIST: list[tuple[str, str, list[str]]] = [
    # ── Original 16 (Logan's 6 + 10 added in Path Z) ──
    ("scripts/207_canonical_master_expansion.py",
     "expanded canonical_patient_master_v1 with _v12 / _v271 / preop / max_tirads* TIRADS columns",
     []),
    ("scripts/265_canonical_finalization.py",
     "finalized CPM with legacy TIRADS columns (best/worst/_v12/combined)",
     []),
    ("scripts/271_tirads_imaging_finalization.py",
     "wrote tirads_*_points_v271 and laterality rollups to CPM",
     []),
    ("scripts/271a_fix_concordance_three_valued.py",
     "wrote pathology_vs_imaging_laterality_concordant 3-valued patch to CPM",
     []),
    ("scripts/271b_laterality_normalization.py",
     "wrote *_v271b laterality rollups to CPM",
     []),
    ("scripts/273_registry_curation.py",
     "registry curation that read/wrote CPM TIRADS columns",
     []),
    ("scripts/204_canonical_master_assembly.py",
     "wrote preop_tirads_best/worst/category to CPM during master assembly",
     []),
    ("scripts/205_canonical_consolidation.py",
     "wrote preop_tirads_* + tirads_*_combined to CPM during consolidation",
     []),
    ("scripts/221_tirads_v2_integration.py",
     "main producer of the tirads_v2_* family on CPM (13 cols)",
     [
         "# NEW TARGET ON REFRESH: main.cupm_v2_canonical_backfill_v1",
         "#   (column rename required — see 'Migration notes' in",
         "#    CPM_tirads_preB_canonical_backfill_cursor_prompt_20260421.md;",
         "#    several output cols were DROPPED in Part B and have no canonical home,",
         "#    e.g. tirads_v2_n_reports, _shortest_followup_months, _any_ete_on_us)",
     ]),
    ("scripts/221b_suspicious_ln_reextraction.py",
     "wrote tirads_v2_any_fna_recommended_report* + suspicious_ln_on_us to CPM",
     [
         "# NEW TARGET ON REFRESH: main.cupm_v2_canonical_backfill_v1",
         "#   (column rename required — see 'Migration notes' in",
         "#    CPM_tirads_preB_canonical_backfill_cursor_prompt_20260421.md;",
         "#    legacy cols any_fna_recommended_report / worst_rank ported as ",
         "#    any_fna_recommended_report_ever / tirads_worst_rank_ever; the rest",
         "#    were dropped without replacement)",
     ]),
    ("scripts/221c_rollup_threevalue_patch.py",
     "patched 3-valued tirads_v2_any_* rollups on CPM",
     []),
    ("scripts/252_recompute_max_tirads.py",
     "recomputed max_tirads_ever / imaging_tirads_worst / preop_tirads_best on CPM",
     []),
    ("scripts/301_canonical_us_patient_master_v1.py",
     "old US v1 patient-master writer; superseded by canonical_us_patient_master_VIEW_v2 pipeline",
     []),
    ("scripts/328_tirads_v2_gap_a_cast_fix.py",
     "Gap-A patch on tirads_v2_* family on CPM",
     []),
    ("scripts/329_tirads_v2_gap_b_report_reroll.py",
     "Gap-B patch on tirads_v2_any_fna_recommended_report on CPM",
     []),
    ("scripts/368_cpm_us_cutover_to_v2.py",
     "the literal CPM US v2 cutover script that materialized the 6 *_v2 cols on CPM",
     []),
    ("scripts/prompt6_348_older_masters.py",
     "writer of tirads_v2_worst_rank + any_fna_recommended_report from older rollup tables (caught by Part B Phase 3 grep, missed by Logan's initial 6-script list)",
     []),
    ("scripts/375_cpm_column_cleanup_and_audit.py",
     "ALTER TABLE RENAME COLUMN of imaging_updated_tirads_category_cpm_v2_v2 -> imaging_updated_tirads_category_cpm_v2 (already executed; CPM-mutating, frozen for safety)",
     []),

    # ── Cat B aliasing (2): own CTAS uses TIRADS-shaped names ──
    ("scripts/48_build_analysis_resolved_layer.py",
     "built patient_analysis_resolved_v1 with TIRADS-shaped column aliases sourced from extracted_tirads_validated_v1",
     [
         "# CAT B: this script uses TIRADS-shaped names (imaging_tirads_best, ",
         "#         imaging_tirads_worst, imaging_tirads_source) as LOCAL CTAS aliases ",
         "#         on its own derived view — they are NOT reads of CPM. Schema migration ",
         "#         requires renaming these internal aliases (substantial rewrite), not ",
         "#         a simple column substitution. Frozen rather than migrated for that reason.",
     ]),
    ("scripts/50_multinodule_imaging.py",
     "built multi_exam CTE with max_tirads_ever / worst_tirads_category as LOCAL aliases on a derived rollup",
     [
         "# CAT B: this script uses TIRADS-shaped names (max_tirads_ever, ",
         "#         worst_tirads_category) as LOCAL CTAS aliases on its own derived ",
         "#         rollup — they are NOT reads of CPM. Schema migration requires ",
         "#         renaming these internal aliases (substantial rewrite), not a ",
         "#         simple column substitution. Frozen rather than migrated for that reason.",
     ]),

    # ── Cat C one-shot audit/verification (8) ──
    ("scripts/259_final_verification_lock.py",
     "one-shot final verification audit (max_tirads_ever undercount check); already executed",
     []),
    ("scripts/264_final_acceptance_addendum.py",
     "one-shot final acceptance audit; already executed",
     []),
    ("scripts/277_canonical_cleanup_phase7_verification.py",
     "one-shot Phase 7 cleanup verification; already executed",
     []),
    ("scripts/336_final_main_audit.py",
     "one-shot final main audit (per-column NULL counts on CPM); already executed",
     []),
    ("studies/canonical_cleanup_20260417/phase1_dryrun_probe.py",
     "one-shot Phase 1 dry-run sizing probe; already executed",
     []),
    ("studies/canonical_cleanup_20260417/preflight.py",
     "one-shot canonical-cleanup preflight; already executed",
     []),
    ("studies/canonical_cleanup_20260417/schema_recon.py",
     "one-shot canonical-cleanup schema reconnaissance; already executed",
     []),
    ("studies/proposal_multimodal_prediction_20260318/run_feasibility.py",
     "one-shot multimodal-prediction feasibility study; already executed",
     []),

    # ── Cat A one-shot metadata generators (5) ──
    ("scripts/228_registry_backfill.py",
     "registry backfill metadata script with TIRADS column refs in source-table comments",
     []),
    ("scripts/246_canonical_us_nodule_characteristics.py",
     "canonical US nodule characteristics builder with TIRADS column refs in docstring",
     []),
    ("scripts/369_us_v2_views_and_registry.py",
     "US v2 views/registry builder with TIRADS column refs in view definitions",
     []),
    ("scripts/prompt6_353_completion_audit.py",
     "Prompt 6 completion audit with TIRADS column refs in audit metadata",
     []),
    ("scripts/prompt6_353_repoint_orphan_view.py",
     "Prompt 6 orphan-view repoint with TIRADS column refs in header",
     []),
]


FROZEN_HEADER_TEMPLATE = """\
# =====================================================================
# FROZEN — {date} — {script_name}
# =====================================================================
# Reason: CPM TIRADS columns dropped per Option C-soft (CPM TIRADS Part B,
# Logan's adjudication 2026-04-21). This script touched
# main.canonical_patient_master TIRADS / laterality columns that no longer exist.
#
# Replacement: canonical TIRADS values live on
#     main.canonical_us_patient_master_VIEW_v2  (patient grain — 28 cols post pre-B)
#     main.canonical_us_exam_master_VIEW_v2     (per-exam grain)
#     main.canonical_us_nodule_v2          (per-nodule grain)
# Rebuild via the US v2 pipeline; do NOT re-enable this script without a new
# column plan and CPM-schema decision.
#
# Pre-Part-B CPM snapshot (for archive restore if needed):
#     "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB
#
# Per-script note: {summary}
{extra_lines}# =====================================================================

"""


def build_header(script_name: str, summary: str, extras: list[str]) -> str:
    extra_block = ""
    if extras:
        extra_block = "#\n" + "\n".join(extras) + "\n"
    return FROZEN_HEADER_TEMPLATE.format(
        date="2026-04-21",
        script_name=script_name,
        summary=summary,
        extra_lines=extra_block,
    )


def freeze_one(rel_src: str, summary: str, extras: list[str]) -> dict:
    src = REPO / rel_src
    if not src.exists():
        return {"src": rel_src, "status": "MISSING"}

    name = src.name
    dst = FROZEN / name
    if dst.exists():
        return {"src": rel_src, "status": "ALREADY_FROZEN", "dst": str(dst.relative_to(REPO))}

    # Read original content
    content = src.read_text()

    # Build header
    header = build_header(name, summary, extras)

    # Inject header: keep shebang line if present, then header, then rest
    lines = content.splitlines(keepends=True)
    if lines and lines[0].startswith("#!"):
        new_content = lines[0] + header + "".join(lines[1:])
    else:
        new_content = header + content

    # git mv (this preserves history)
    proc = subprocess.run(
        ["git", "mv", rel_src, str(dst.relative_to(REPO))],
        cwd=REPO, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return {"src": rel_src, "status": "GIT_MV_FAILED", "stderr": proc.stderr}

    # Now write the new content (with FROZEN header) at the destination
    dst.write_text(new_content)

    return {
        "src": rel_src,
        "dst": str(dst.relative_to(REPO)),
        "status": "FROZEN",
        "summary": summary,
        "extras_n": len(extras),
    }


def write_readme(results: list[dict]) -> Path:
    md = [
        "# `scripts/frozen/` — frozen scripts catalog",
        "",
        "Scripts in this directory are intentionally inert. They reference",
        "schema state that no longer exists on the live database (typically",
        "columns that were dropped during a publication-stable cleanup).",
        "",
        "Each script carries a FROZEN header at the top documenting:",
        "  - the freeze date and the cleanup operation",
        "  - the canonical replacement (where applicable)",
        "  - the archive location for restoration if needed",
        "",
        "**Do NOT run scripts in this directory against the live database.**",
        "If a use case resurrects, unfreezing requires:",
        "  1. Reviewing the FROZEN header for replacement guidance.",
        "  2. Confirming the upstream schema still supports the script's logic.",
        "  3. Updating the script to use canonical post-cleanup tables.",
        "  4. `git mv` back to `scripts/`, removing the FROZEN header.",
        "",
        "---",
        "",
        "## CPM TIRADS Part B (2026-04-21) — 33 scripts",
        "",
        "Architecture: Option C-soft. canonical_patient_master no longer carries TIRADS",
        "columns. Canonical TIRADS lives on canonical_us_*_v2 surface (cupm_v2 patient,",
        "cuem_v2 exam, cunc_v2 nodule grain).",
        "",
    ]
    for r in results:
        if r["status"] != "FROZEN":
            continue
        md.append(
            f"- `{Path(r['dst']).name}` — frozen 2026-04-21 — CPM TIRADS Part B — {r['summary']}"
        )
    return md


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    FROZEN.mkdir(parents=True, exist_ok=True)

    log: dict = {"phase": 4, "started_at_utc": utc_iso(), "results": []}
    for rel, summary, extras in FREEZE_LIST:
        result = freeze_one(rel, summary, extras)
        log["results"].append(result)

    # Write the README catalog
    md_lines = write_readme(log["results"])
    readme = FROZEN / "README.md"
    if readme.exists():
        # Append to existing (preserve any prior frozen-script entries)
        existing = readme.read_text()
        readme.write_text(existing.rstrip() + "\n\n" + "\n".join(md_lines) + "\n")
    else:
        readme.write_text("\n".join(md_lines) + "\n")

    # Summary
    n_frozen = sum(1 for r in log["results"] if r["status"] == "FROZEN")
    n_missing = sum(1 for r in log["results"] if r["status"] == "MISSING")
    n_already = sum(1 for r in log["results"] if r["status"] == "ALREADY_FROZEN")
    n_failed  = sum(1 for r in log["results"] if r["status"] == "GIT_MV_FAILED")

    log["finished_at_utc"] = utc_iso()
    log["counts"] = {
        "frozen": n_frozen, "missing": n_missing,
        "already_frozen": n_already, "git_mv_failed": n_failed,
    }
    out_path = OUT / "partB_phase4_freeze.json"
    out_path.write_text(json.dumps(log, indent=2, default=str))

    print(f"Phase 4 done. Report: {out_path.relative_to(REPO)}")
    print(f"  frozen:        {n_frozen}")
    print(f"  missing:       {n_missing}")
    print(f"  already frozen: {n_already}")
    print(f"  git mv failed: {n_failed}")
    if n_failed:
        for r in log["results"]:
            if r["status"] == "GIT_MV_FAILED":
                print(f"  FAIL: {r['src']} — {r.get('stderr', '')}")


if __name__ == "__main__":
    main()
