#!/usr/bin/env python3
"""
mig_249: re-score manuscript_feasibility_v1 post-mig_248 (cohort parent aliases).

Emits qc_framework_v1/migrations/249_feasibility_re_refresh_20260501.sql

  .venv/bin/python scripts/_mig249_build_feasibility_re_refresh.py
  .venv/bin/python scripts/_mig249_build_feasibility_re_refresh.py --apply

Closes CF-MIG_247-RERUN stale flags resolved by mig_248 column aliases on
cohort_descriptive_full_cohort_v1.
"""
from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import PUBLICATION_DB, connect_locked  # noqa: E402


TOTAL_CPM = 10871

RENAME_DOC = {
    "syn_isthmus_size_cm": "syn_isthmus_size_cm_legacy_raw (+ syn_isthmus_size_parse_status)",
    "tumor_size_cm": "path_tumor_size_cm or tumor_size_cm_max (verify manuscript intent)",
    "tirads_best_category_v12": "CPM lacks v12 alias; NLP rollup: nlp_tirads_max_category; cohort views alias cupm.max_tirads_category_ever",
    "tirads_worst_category_v12": "see tirads_best_category_v12 / cupm rollup",
    "tirads_nodules_scored_combined": "no direct CPM column; use canonical_us / imaging rollup",
}


def sq_str(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def sq_dbl_arr(vals: list) -> str:
    parts = []
    for v in vals:
        if v is None:
            parts.append("NULL::DOUBLE")
        else:
            parts.append(str(float(v)))
    return "[" + ", ".join(parts) + "]::DOUBLE[]"


def color_rank(c: str) -> int:
    return {"RED": 0, "YELLOW": 1, "GREEN": 2}.get(c.upper(), -1)


def strip_stale_gating(text: str | None) -> str:
    """Remove mig_248-invalidated blocker clauses from prior gating text."""
    if not text or not str(text).strip():
        return ""
    raw = str(text).strip().replace("; ", ";").replace(";.", ".")
    fragments = [p.strip() for p in re.split(r"[;\n]", raw) if p.strip()]
    if len(fragments) <= 1 and ". " in raw:
        fragments = [p.strip() for p in raw.split(". ") if p.strip()]
    kept: list[str] = []
    for f in fragments:
        fl = f.lower()
        if "cohort select failed" in fl:
            continue
        if "tumor_size_cm missing" in fl:
            continue
        if "tirads_best_category_v12 missing" in fl:
            continue
        if "tirads_worst_category_v12 missing" in fl:
            continue
        if "syn_isthmus_size_cm" in fl and "binder" in fl:
            continue
        kept.append(f)
    return ". ".join(kept).strip()


def compute_feas_color(
    *,
    candidate_n: int,
    coverages: list[float | None],
    any_missing_col: bool,
) -> str:
    if any_missing_col:
        return "RED"
    usable = [c for c in coverages if c is not None]
    if not usable:
        return "RED"
    if candidate_n < 50:
        return "RED"
    if any(c < 30 for c in usable):
        return "RED"
    if any(30 <= c < 80 for c in usable):
        return "YELLOW"
    if all(c >= 80 for c in usable) and candidate_n >= 100:
        return "GREEN"
    return "YELLOW"


def main() -> None:
    apply_md = "--apply" in sys.argv
    out_sql = REPO / "qc_framework_v1/migrations/249_feasibility_re_refresh_20260501.sql"
    con = connect_locked()
    db = PUBLICATION_DB

    cpm_cols = {
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = 'main'
              AND table_name = 'canonical_patient_master'
            """,
            [db],
        ).fetchall()
    }

    cohort_cols = {
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = ? AND table_schema = 'manuscript_workspace'
              AND table_name = 'cohort_descriptive_full_cohort_v1'
            """,
            [db],
        ).fetchall()
    }

    dive = {
        mid: vn
        for mid, vn in con.execute(
            f"""
            SELECT manuscript_id, cohort_view_name
            FROM {db}.manuscript_workspace.manuscript_dive_map_v1
            """
        ).fetchall()
    }

    dive_counts: dict[str, int] = {}
    try:
        for vw, cn in con.execute(
            f"""
            SELECT cohort_view_name, current_row_count
            FROM {db}.manuscript_workspace.dive_cohort_size_v1
            """
        ).fetchall():
            dive_counts[str(vw)] = int(cn)
    except Exception:
        dive_counts = {}

    rows = con.execute(
        f"""
        SELECT manuscript_id, title, status, candidate_n, key_variables,
               variable_coverage_pct, feasibility_color, gating_issues,
               recommended_next_step, v1_1_upgrade_prediction
        FROM {db}.manuscript_workspace.manuscript_feasibility_v1
        ORDER BY manuscript_id
        """
    ).fetchall()

    resolve_cache: dict[str, tuple[str, int]] = {}
    """var -> ('cpm'|'cohort'|'missing', non_null_count)."""

    def resolve_var(v: str) -> tuple[str, int]:
        if v in resolve_cache:
            return resolve_cache[v]
        if v in cpm_cols:
            cnt = con.execute(
                f"""
                SELECT SUM(CASE WHEN "{v}" IS NOT NULL THEN 1 ELSE 0 END)
                FROM {db}.main.canonical_patient_master
                """
            ).fetchone()[0]
            resolve_cache[v] = ("cpm", int(cnt))
            return resolve_cache[v]
        if v in cohort_cols:
            cnt = con.execute(
                f"""
                SELECT SUM(CASE WHEN "{v}" IS NOT NULL THEN 1 ELSE 0 END)
                FROM {db}.manuscript_workspace.cohort_descriptive_full_cohort_v1
                """
            ).fetchone()[0]
            resolve_cache[v] = ("cohort", int(cnt))
            return resolve_cache[v]
        resolve_cache[v] = ("missing", 0)
        return resolve_cache[v]

    for (
        mid,
        _title,
        _status,
        _cn,
        kv,
        _ov,
        _fc,
        _gi,
        _rn,
        _vu,
    ) in rows:
        if kv:
            for v in kv:
                resolve_var(v)

    transitions: dict[str, int] = defaultdict(int)
    gained_green: list[int] = []
    lost_feas: list[int] = []
    broken_cohorts: list[tuple[int, str, str]] = []
    lines: list[str] = []

    lines.extend(
        [
            "-- =============================================================================",
            "-- mig_249 — manuscript_workspace.manuscript_feasibility_v1 re-refresh",
            "-- Date:    2026-05-01",
            "-- DB:      thyroid_canonical_publication_v1_0",
            "--",
            "-- Post-mig_248: resolves key_variables via cohort_descriptive_full_cohort_v1",
            "-- aliases (tumor_size_cm, tirads_*_v12, syn_*_legacy_raw, rai_received_flag).",
            "-- Generated by scripts/_mig249_build_feasibility_re_refresh.py",
            "-- =============================================================================",
            "",
            f'USE "{db}";',
            "USE main;",
            "",
        ]
    )

    for (
        mid,
        title,
        status,
        old_cn,
        kv,
        old_cov,
        old_color,
        old_gating,
        old_rec,
        v11_pred,
    ) in rows:
        kv_list = list(kv or [])
        coverages: list[float | None] = []
        miss_detail: list[str] = []

        for v in kv_list:
            src, cnt = resolve_var(v)
            if src == "missing":
                coverages.append(None)
                rn = RENAME_DOC.get(v)
                if rn:
                    miss_detail.append(f"{v} MISSING (rename hint: {rn})")
                else:
                    miss_detail.append(f"{v} MISSING from canonical_patient_master and cohort_descriptive_full_cohort_v1")
            else:
                pct = 100.0 * cnt / TOTAL_CPM
                coverages.append(round(pct, 2))

        any_missing = any(resolve_var(v)[0] == "missing" for v in kv_list)

        cohort_view = dive.get(mid)
        cohort_n = int(old_cn)
        cohort_ok = False
        cohort_err = ""

        if cohort_view:
            if cohort_view in dive_counts:
                cohort_n = dive_counts[cohort_view]
                cohort_ok = True
            else:
                fq = f'{db}.manuscript_workspace."{cohort_view}"'
                try:
                    cohort_n = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
                    cohort_ok = True
                except Exception as e:
                    cohort_err = str(e).replace("\n", " ")[:320]
                    broken_cohorts.append((mid, cohort_view, cohort_err))
                    wheres = []
                    for v in kv_list:
                        if resolve_var(v)[0] != "missing":
                            wheres.append(f'("{v}" IS NOT NULL)')
                    if wheres:
                        cohort_n = int(
                            con.execute(
                                "SELECT COUNT(*) FROM "
                                + f'{db}.main.canonical_patient_master WHERE '
                                + " AND ".join(wheres)
                            ).fetchone()[0]
                        )
                    else:
                        cohort_n = TOTAL_CPM

        new_color = compute_feas_color(
            candidate_n=int(cohort_n),
            coverages=coverages,
            any_missing_col=any_missing,
        )

        tr = f"{old_color}->{new_color}"
        transitions[tr] += 1
        if new_color == "GREEN" and old_color != "GREEN":
            gained_green.append(mid)
        if color_rank(new_color) < color_rank(old_color):
            lost_feas.append(mid)

        cleaned = strip_stale_gating(old_gating)
        post_bits: list[str] = []

        if not cohort_ok and cohort_view and cohort_err:
            post_bits.append(
                f"cohort regression: SELECT failed for {cohort_view} ({cohort_err}). "
                "STOP — investigate before relying on candidate_n fallback."
            )
        if miss_detail:
            post_bits.append(
                "Schema/coverage gaps: "
                + "; ".join(miss_detail[:6])
                + ("; …" if len(miss_detail) > 6 else "")
            )

        mig248_note = ""
        if (
            old_color == "RED"
            and new_color == "GREEN"
            and cohort_ok
            and not any_missing
        ):
            mig248_note = (
                "post-mig_248: column-rename drift resolved "
                "(e.g., tirads_best_category_v12 / tumor_size_cm aliases active); RED→GREEN."
            )
        elif new_color == "GREEN" and old_color == "GREEN":
            mig248_note = "Refreshed post-mig_248."
        elif old_color != new_color:
            post_bits.append(f"Feasibility {old_color}→{new_color} post-mig_248 rescoring.")

        merged_parts = []
        if cleaned:
            merged_parts.append(cleaned)
        merged_parts.extend(post_bits)
        if mig248_note:
            merged_parts.append(mig248_note)
        merged_gating = ". ".join(p for p in merged_parts if p).strip()

        new_rec = (old_rec or "").strip()
        if old_color == "RED" and new_color == "GREEN" and not any_missing:
            new_rec = "Re-evaluate cohort; consider draft start"
        elif new_color == "GREEN" and old_color == "GREEN" and mig248_note:
            if new_rec and "refreshed post-mig_248" not in new_rec.lower():
                new_rec = new_rec + "; refreshed post-mig_248"
            elif not new_rec:
                new_rec = "Refreshed post-mig_248"

        cov_sql = sq_dbl_arr(coverages)
        sql = (
            "UPDATE manuscript_workspace.manuscript_feasibility_v1 SET "
            + f"candidate_n = {int(cohort_n)}, "
            + f"variable_coverage_pct = {cov_sql}, "
            + f"feasibility_color = {sq_str(new_color)}, "
            + f"gating_issues = {sq_str(merged_gating)}, "
            + f"recommended_next_step = {sq_str(new_rec)}, "
            + f"canonical_version_at_scoring = {sq_str('v1_0_post_mig_248')}, "
            + "scored_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE) "
            + f"WHERE manuscript_id = {int(mid)};"
        )
        if apply_md:
            con.execute(sql)
        lines.append(f"-- M{mid:03d} {(title or '')[:70].replace(chr(10), ' ')}")
        lines.append(sql)
        lines.append("")

    lines.extend(
        [
            "-- =============================================================================",
            "-- Post-apply verification (see CURSOR_PROMPT_MIG_249 §4)",
            "-- =============================================================================",
        ]
    )

    out_sql.write_text("\n".join(lines) + "\n", encoding="utf-8")

    mem = REPO / "memory/project_mig_249_feasibility_re_refresh_20260501.md"
    mem_lines = [
        "# mig_249 — manuscript_feasibility_v1 re-refresh (post-mig_248)",
        "",
        "## Summary",
        "",
        f"- Rows refreshed: {len(rows)} (expected 83)",
        f"- Applied to MotherDuck: {apply_md}",
        f"- Broken cohort COUNT after mig_248: {len(broken_cohorts)}",
        "",
        "## Color transitions",
        "",
    ]
    for k in sorted(transitions.keys(), key=lambda x: transitions[x], reverse=True):
        mem_lines.append(f"| {k} | {transitions[k]} |")
    mem_lines.extend(
        [
            "",
            "## Gained GREEN",
            "",
            repr(gained_green),
            "",
            "## Worsened vs prior flag",
            "",
            repr(lost_feas),
            "",
            "## Cohort regressions (must be empty)",
            "",
        ]
    )
    for mid, vw, err in sorted(broken_cohorts, key=lambda t: t[0]):
        mem_lines.append(f"- m{mid:03d} `{vw}`: {err[:200]}")
    mem.write_text("\n".join(mem_lines) + "\n", encoding="utf-8")

    print("Wrote", out_sql)
    print("Wrote", mem)
    print("Transitions:", dict(transitions))
    print("Gained GREEN:", gained_green)
    if broken_cohorts:
        print("WARNING broken cohorts:", broken_cohorts)


if __name__ == "__main__":
    main()
