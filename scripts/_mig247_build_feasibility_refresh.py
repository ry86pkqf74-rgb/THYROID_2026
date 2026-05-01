#!/usr/bin/env python3
"""
One-off helper for mig_247: compute refreshed feasibility rows against live CPM
and emit qc_framework_v1/migrations/247_feasibility_refresh_20260501.sql

Not intended for CLI reuse; run from repo root:
  .venv/bin/python scripts/_mig247_build_feasibility_refresh.py
  .venv/bin/python scripts/_mig247_build_feasibility_refresh.py --apply

When --apply is set, emits the same migration file and EXECUTES updates on MotherDuck.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import PUBLICATION_DB, connect_locked  # noqa: E402


TOTAL_CPM = 10871

# Documented rename hints (do NOT mutate key_variables[]; annotate in gating_issues only).
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
    """Render DOUBLE[] literal for DuckDB UPDATE."""
    parts = []
    for v in vals:
        if v is None:
            parts.append("NULL::DOUBLE")
        else:
            parts.append(str(float(v)))
    return "[" + ", ".join(parts) + "]::DOUBLE[]"


def color_rank(c: str) -> int:
    return {"RED": 0, "YELLOW": 1, "GREEN": 2}.get(c.upper(), -1)


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
    # All known coverages ≥80%, but cohort / denominator not large enough for GREEN
    return "YELLOW"


def main() -> None:
    out_sql = REPO / "qc_framework_v1/migrations/247_feasibility_refresh_20260501.sql"
    con = connect_locked()
    db = PUBLICATION_DB

    cols = {
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

    dive = {
        mid: vn
        for mid, vn in con.execute(
            f"""
            SELECT manuscript_id, cohort_view_name
            FROM {db}.manuscript_workspace.manuscript_dive_map_v1
            """
        ).fetchall()
    }

    rows = con.execute(
        f"""
        SELECT manuscript_id, title, status, candidate_n, key_variables,
               variable_coverage_pct, feasibility_color, gating_issues,
               recommended_next_step, v1_1_upgrade_prediction
        FROM {db}.manuscript_workspace.manuscript_feasibility_v1
        ORDER BY manuscript_id
        """
    ).fetchall()

    needed_cols: set[str] = set()
    missing_by_mid: dict[int, list[str]] = defaultdict(list)
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
        if not kv:
            continue
        for v in kv:
            if v in cols:
                needed_cols.add(v)
            else:
                missing_by_mid[mid].append(v)

    counts: dict[str, int] = {}
    for col in sorted(needed_cols):
        cnt = con.execute(
            f"""
            SELECT SUM(CASE WHEN "{col}" IS NOT NULL THEN 1 ELSE 0 END)
            FROM {db}.main.canonical_patient_master
            """
        ).fetchone()[0]
        counts[col] = int(cnt)

    broken_cohorts: list[tuple[int, str, str]] = []
    lines: list[str] = []
    transitions: dict[str, int] = defaultdict(int)
    gained_green: list[int] = []
    lost_feas: list[int] = []
    rename_notes: defaultdict[str, set[str]] = defaultdict(set)

    lines.append("-- =============================================================================")
    lines.append("-- mig_247 — manuscript_workspace.manuscript_feasibility_v1 refresh")
    lines.append("-- Date:    2026-05-01")
    lines.append("-- DB:      thyroid_canonical_publication_v1_0")
    lines.append("--")
    lines.append("-- Re-scores 83 manuscripts against live canonical_patient_master + cohort views.")
    lines.append("-- Generated by scripts/_mig247_build_feasibility_refresh.py (do not hand-edit IDs).")
    lines.append("-- Apply: run in MotherDuck session with USE publication DB.")
    lines.append("-- =============================================================================")
    lines.append("")
    lines.append(f'USE "{db}";')
    lines.append("USE main;")
    lines.append("")

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
            if v in cols:
                pct = 100.0 * counts[v] / TOTAL_CPM
                coverages.append(round(pct, 2))
            else:
                coverages.append(None)
                rn = RENAME_DOC.get(v)
                if rn:
                    miss_detail.append(f"{v} MISSING (rename hint: {rn})")
                    rename_notes[v].add(rn)
                else:
                    miss_detail.append(f"{v} MISSING from canonical_patient_master")

        any_missing = any(v not in cols for v in kv_list)

        cohort_view = dive.get(mid)
        cohort_ok = False
        cohort_n = old_cn
        cohort_err = ""
        if cohort_view:
            fq = f'{db}.manuscript_workspace."{cohort_view}"'
            try:
                cohort_n = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
                cohort_ok = True
            except Exception as e:
                cohort_err = str(e).replace("\n", " ")[:320]
                broken_cohorts.append((mid, cohort_view, cohort_err))
                wheres = []
                for v in kv_list:
                    if v in cols:
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

        elif not cohort_view:
            cohort_n = old_cn

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

        post_bits: list[str] = []

        if not cohort_ok and cohort_view and cohort_err:
            post_bits.append(
                f"post-v17: cohort SELECT failed for {cohort_view} ({cohort_err}); "
                f"candidate_n={cohort_n} from CPM intersect of non-null resolvable keys (mig_248 repair)."
            )
        elif not cohort_ok and cohort_view:
            post_bits.append(
                "post-v17: cohort view error; fallback candidate_n computed on CPM (see mig_247 notes)."
            )

        if miss_detail:
            post_bits.append(
                "post-v17 schema: "
                + "; ".join(miss_detail[:6])
                + ("; …" if len(miss_detail) > 6 else "")
            )

        if new_color != old_color:
            post_bits.append(
                f"post-v17 feasibility {old_color}→{new_color} (coverage/cohort reassessed)."
            )
            if (
                old_color != "GREEN"
                and new_color == "GREEN"
                and not any_missing
                and cohort_ok
            ):
                post_bits.append(
                    "post-v17: NEW feasibility GREEN — prior gate cleared on coverage/cohort sizing."
                )

        merged_gating = (old_gating or "").strip()
        if merged_gating and post_bits:
            merged_gating += " "
        merged_gating += " ".join(post_bits)

        new_rec = old_rec or ""
        if color_rank(new_color) > color_rank(old_color):
            new_rec = "Reassess after v17 schema improvements"
        elif color_rank(new_color) < color_rank(old_color) and (
            old_rec or ""
        ).strip() == "Reassess after v17 schema improvements":
            new_rec = (
                "Revisit variable mapping + cohort DDL (see gating_issues); feasibility regressed post-v17."
            )

        cov_sql = sq_dbl_arr(coverages)
        sql = (
            "UPDATE manuscript_workspace.manuscript_feasibility_v1 SET "
            + f"candidate_n = {int(cohort_n)}, "
            + f"variable_coverage_pct = {cov_sql}, "
            + f"feasibility_color = {sq_str(new_color)}, "
            + f"gating_issues = {sq_str(merged_gating)}, "
            + f"recommended_next_step = {sq_str(new_rec)}, "
            + f"canonical_version_at_scoring = {sq_str('v1_0_post_mig_246')}, "
            + "scored_at = CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH TIME ZONE) "
            + f"WHERE manuscript_id = {int(mid)};"
        )
        if "--apply" in sys.argv:
            con.execute(sql)
        title_snip = (title or "")[:70].replace("\n", " ")
        lines.append(f"-- M{mid:03d} {title_snip}")
        lines.append(sql)
        lines.append("")

    lines.append("-- =============================================================================")
    lines.append("-- Verification (run after COMMIT)")
    lines.append("-- =============================================================================")
    lines.append(
        "-- SELECT feasibility_color, COUNT(*) FROM manuscript_workspace.manuscript_feasibility_v1 GROUP BY 1;"
    )

    out_sql.write_text("\n".join(lines) + "\n", encoding="utf-8")

    mem = REPO / "memory/project_mig_247_feasibility_refresh_20260501.md"
    mem_lines = [
        "# mig_247 — manuscript_feasibility_v1 refresh (2026-05-01)",
        "",
        "## Summary",
        "",
        f"- Rows refreshed: {len(rows)} (expected 83)",
        f"- Broken cohort COUNT(*) queries (binder on syn_isthmus_size_cm): {len(set(b for _, b, _ in broken_cohorts))} distinct view roots",
        f"- manuscript_ids affected by broken cohort: {sorted({m for m, _, _ in broken_cohorts})}",
        "",
        "## Color transitions (old→new counts)",
        "",
    ]
    for k in sorted(transitions.keys(), key=lambda x: transitions[x], reverse=True):
        mem_lines.append(f"| {k} | {transitions[k]} |")
    mem_lines.extend(
        [
            "",
            "## Manuscript IDs that transitioned to GREEN (rank improved to GREEN)",
            "",
            repr(gained_green),
            "",
            "## Manuscript IDs with worsened rank vs pre-refresh",
            "",
            repr(lost_feas),
            "",
            "## Variables MISSING from canonical_patient_master (key_variables unchanged)",
            "",
            repr(sorted({v for mids in missing_by_mid.values() for v in mids})),
            "",
            "## Rename hints documented in gating (not promoted into key_variables[])",
            "",
            repr({k: "; ".join(v) for k, v in rename_notes.items()}),
            "",
            "## Cohort views broken during refresh (mig_248 scope)",
            "",
        ]
    )
    for mid, vw, err in sorted(broken_cohorts, key=lambda t: t[0]):
        mem_lines.append(f"- m{mid:03d} `{vw}`: {err[:160]}...")
    mem_lines.append("")
    mem.write_text("\n".join(mem_lines), encoding="utf-8")

    print("Wrote", out_sql)
    print("Wrote", mem)
    print("Transitions:", dict(transitions))
    print("Broken cohort mids:", sorted({m for m, _, _ in broken_cohorts}))


if __name__ == "__main__":
    main()
