#!/usr/bin/env python3
"""Independent local DuckDB audit: completion thyroidectomy vs manuscript claims.

Findings (see completion_logic_trace.md): study logic uses ONLY
operative_episode_detail_v2 rows strictly after index; path_synoptics may
document second surgeries missing from operative_details / OED.

Does not modify frozen study CSVs. Writes under completion_audit_YYYYMMDD/.

Usage:
  .venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/run_completion_independent_audit.py
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
STUDY = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from local DuckDB_client import local DuckDBClient, local DuckDBConfig, get_token, token_mode  # noqa: E402

import cohort_logic as cl  # noqa: E402

AUDIT_TAG = datetime.now(timezone.utc).strftime("%Y%m%d")
OUT = STUDY / f"completion_audit_{AUDIT_TAG}"
PATIENT_CSV = STUDY / "patient_level_dataset.csv"
SQL_LOG = OUT / "audit_sql_executed.sql"

COMPLETION_RAW_RE = re.compile(
    r"completion|compl\.?\s*thyroid|complete\s+thyroid|contralateral\s+lobe|"
    r"remaining\s+lobe|second\s*[- ]stage|re-?operative\s+thyroid|"
    r"total\s+thyroidectomy\s+after|completion\s+of\s+thyroid",
    re.I,
)


def log_sql(fh, label: str, sql: str) -> None:
    fh.write(f"\n-- === {label} ===\n")
    fh.write(sql.rstrip() + "\n")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    if not get_token():
        raise SystemExit("No local DuckDB token.")

    pl = pd.read_csv(PATIENT_CSV, low_memory=False)
    pl["research_id"] = pd.to_numeric(pl["research_id"], errors="coerce").astype("Int64")
    n_primary = int(pl["research_id"].nunique())

    con = local DuckDBClient(local DuckDBConfig()).connect_rw()
    fh = SQL_LOG.open("w")
    fh.write(
        f"-- Independent completion audit {AUDIT_TAG}\n"
        f"-- token_mode={token_mode()}\n"
        f"-- primary_cohort_N={n_primary}\n"
    )

    sql_ops = """
    SELECT research_id, surgery_episode_id, resolved_surgery_date,
           procedure_normalized, procedure_raw, laterality
    FROM operative_episode_detail_v2
    """
    log_sql(fh, "operative_episode_detail_v2_full", sql_ops)
    fh.flush()
    ops = con.execute(sql_ops).df()
    fh.close()

    ops["research_id"] = pd.to_numeric(ops["research_id"], errors="coerce").astype("Int64")
    ops["sx_dt"] = pd.to_datetime(ops["resolved_surgery_date"], errors="coerce")

    sql_ps = "SELECT research_id, surg_date, thyroid_procedure, completion FROM path_synoptics"
    with SQL_LOG.open("a") as fh:
        log_sql(fh, "path_synoptics_full", sql_ps)
    path_all = con.execute(sql_ps).df()
    path_all["research_id"] = pd.to_numeric(path_all["research_id"], errors="coerce")
    path_all["sd"] = pd.to_datetime(path_all["surg_date"], errors="coerce")

    sql_od = "SELECT research_id, COUNT(*) AS n FROM operative_details GROUP BY 1"
    with SQL_LOG.open("a") as fh:
        log_sql(fh, "operative_details_counts", sql_od)
    od_counts = con.execute(sql_od).df()
    od_counts["research_id"] = pd.to_numeric(od_counts["research_id"], errors="coerce").astype("Int64")
    od_n_map = od_counts.set_index("research_id")["n"].to_dict()

    first_clean = cl.first_qualifying_surgeries(
        ops, frozenset({"hemithyroidectomy", "total_thyroidectomy"})
    )
    comp_study = cl.completion_after_lobectomy(ops, first_clean)
    comp_study_map = comp_study.set_index("research_id")

    lob_pl = pl[pl["initial_lobectomy"] == 1].copy()
    tot_pl = pl[pl["initial_total"] == 1].copy()
    lob_pl["idx"] = pd.to_datetime(lob_pl["index_surgery_date"], errors="coerce")
    n_lob = len(lob_pl)
    n_tot = len(tot_pl)

    def study_flag(rid: int) -> bool:
        if rid not in comp_study_map.index:
            return False
        return bool(comp_study_map.loc[rid, "completion_total_flag"])

    # --- OED later-surgery audit (strict >) ---
    inv_rows: list[dict] = []
    rollup_rows: list[dict] = []
    for _, prow in lob_pl.iterrows():
        rid = int(prow["research_id"])
        idx_dt = prow["idx"]
        study_f = bool(prow["completion_total_flag"]) if pd.notna(prow["completion_total_flag"]) else False
        study_replay = study_flag(rid)
        g = ops[ops["research_id"] == rid].sort_values("sx_dt")
        later = g[g["sx_dt"] > idx_dt]
        later_total = (later["procedure_normalized"] == "total_thyroidectomy").any()
        later_comp_norm = (later["procedure_normalized"] == "completion_thyroidectomy").any()
        later_second_hemi = (later["procedure_normalized"] == "hemithyroidectomy").any()
        raw_comp_lang = later["procedure_raw"].fillna("").astype(str).apply(lambda s: bool(COMPLETION_RAW_RE.search(s)))

        # duplicate identical OED rows
        dup_oed = len(g) != g.drop_duplicates(
            subset=["surgery_episode_id", "resolved_surgery_date", "procedure_normalized"]
        ).shape[0]

        n_od = int(od_n_map.get(rid, 0))

        # Path synoptics: any row AFTER index
        ps_sub = path_all[(path_all["research_id"] == rid) & path_all["sd"].notna()]
        ps_later = ps_sub[ps_sub["sd"] > idx_dt]
        ps_later_completion_yes = ps_later["completion"].astype(str).str.lower().isin({"yes", "y"})
        n_ps_after = len(ps_later)
        n_ps_comp_yes = int(ps_later_completion_yes.sum())

        independent_oed_completion = later_total or later_comp_norm
        independent_path_completion = n_ps_comp_yes > 0

        rollup_rows.append(
            {
                "research_id": rid,
                "index_surgery_date": idx_dt.isoformat() if pd.notna(idx_dt) else "",
                "n_operative_episode_rows": len(g),
                "n_operative_episode_strictly_after_index": len(later),
                "duplicate_operative_row_pattern": dup_oed,
                "n_operative_details_rows_care_team": n_od,
                "later_total_thyroidectomy_oed": bool(later_total),
                "later_completion_thyroidectomy_oed": bool(later_comp_norm),
                "later_second_hemithyroidectomy_oed": bool(later_second_hemi),
                "later_procedure_raw_completion_regex": bool(raw_comp_lang.any()),
                "n_path_syn_rows_after_index": n_ps_after,
                "n_path_syn_after_index_with_completion_col_yes": n_ps_comp_yes,
                "study_completion_total_flag_csv": study_f,
                "study_completion_replay_python": study_replay,
                "csv_vs_replay_match": study_f == study_replay,
                "independent_audit_oed_total_or_completion_norm": independent_oed_completion,
                "independent_audit_path_completion_yes_later": independent_path_completion,
                "missed_by_study_if_path_says_completion_yes": independent_path_completion and not study_f,
            }
        )

        for _, orow in later.iterrows():
            pn = str(orow["procedure_normalized"] or "")
            raw = str(orow["procedure_raw"] or "")
            inv_rows.append(
                {
                    "research_id": rid,
                    "index_surgery_date": idx_dt.isoformat() if pd.notna(idx_dt) else "",
                    "index_procedure": prow["procedure_normalized"],
                    "later_surgery_date": orow["sx_dt"].isoformat() if pd.notna(orow["sx_dt"]) else "",
                    "later_procedure_normalized": pn,
                    "later_procedure_raw_snippet": raw[:500],
                    "current_study_logic_counts_completion": study_f,
                    "independent_audit_should_count_completion_oed_rule": pn
                    in ("total_thyroidectomy", "completion_thyroidectomy"),
                    "reason_if_disagreement": "",
                }
            )

    rollup = pd.DataFrame(rollup_rows)
    detail_oed = pd.DataFrame(inv_rows)
    if detail_oed.empty:
        detail_oed = pd.DataFrame(
            columns=[
                "research_id",
                "index_surgery_date",
                "index_procedure",
                "later_surgery_date",
                "later_procedure_normalized",
                "later_procedure_raw_snippet",
                "current_study_logic_counts_completion",
                "independent_audit_should_count_completion_oed_rule",
                "reason_if_disagreement",
            ]
        )

    # Path-detail rows for candidates
    path_detail_rows: list[dict] = []
    for _, r in rollup.iterrows():
        if r["n_path_syn_after_index_with_completion_col_yes"] == 0:
            continue
        rid = int(r["research_id"])
        idx_dt = pd.to_datetime(r["index_surgery_date"])
        ps_later = path_all[(path_all["research_id"] == rid) & (path_all["sd"] > idx_dt)]
        for _, pr in ps_later.iterrows():
            path_detail_rows.append(
                {
                    "research_id": rid,
                    "index_surgery_date": r["index_surgery_date"],
                    "path_later_surg_date": pr["sd"].isoformat() if pd.notna(pr["sd"]) else "",
                    "path_thyroid_procedure": pr["thyroid_procedure"],
                    "path_completion_column": pr["completion"],
                    "study_oed_based_completion_flag": r["study_completion_total_flag_csv"],
                    "interpretation": "path_synoptic later row; OED lacks second episode — study logic cannot see this",
                }
            )

    path_detail = pd.DataFrame(path_detail_rows)

    # Merge for manuscript-style candidate sheet
    cand = path_detail.drop_duplicates(subset=["research_id", "path_later_surg_date"]).copy()
    idx_proc_map = lob_pl.set_index("research_id")["procedure_normalized"].to_dict()
    cand["index_procedure"] = cand["research_id"].map(idx_proc_map)
    cand["later_procedure_normalized_operative_v2"] = None
    cand["later_procedure_text_path_synoptic"] = cand["path_thyroid_procedure"]
    pc = cand["path_completion_column"].astype(str).str.lower()
    cand["independent_audit_counts_completion"] = pc.isin({"yes", "y"})
    cand["reason_for_disagreement_with_study"] = (
        "path_synoptic documents dated procedure after index with completion flag; no second operative_episode_detail_v2 row"
    )

    missed = rollup[
        (~rollup["study_completion_total_flag_csv"])
        & (rollup["n_path_syn_rows_after_index"] > 0)
    ].copy()

    # Procedure inventory (all ops for cohort)
    cids = pl["research_id"].dropna().astype(int).unique().tolist()
    con.register("_coh", pd.DataFrame({"research_id": cids}))
    inv_sql = """
    SELECT o.procedure_normalized, o.procedure_raw, COUNT(*) AS n_rows
    FROM operative_episode_detail_v2 o
    INNER JOIN _coh ON CAST(o.research_id AS BIGINT) = _coh.research_id
    GROUP BY 1, 2
    ORDER BY n_rows DESC
    """
    with SQL_LOG.open("a") as fh:
        log_sql(fh, "procedure_inventory_cohort", inv_sql)
    proc_inv = con.execute(inv_sql).df()
    con.unregister("_coh")

    # High-suspicion: aggressive pathology + path completion yes but study 0
    ap_map = lob_pl.set_index("research_id")["aggressive_pathology"].to_dict() if "aggressive_pathology" in lob_pl.columns else {}
    rollup["aggressive_pathology_initial"] = rollup["research_id"].map(lambda x: ap_map.get(int(x), None))
    review_set = rollup[
        (rollup["n_path_syn_after_index_with_completion_col_yes"] > 0) & (~rollup["study_completion_total_flag_csv"])
    ].copy()

    study_lob_completion = int(lob_pl["completion_total_flag"].fillna(0).astype(bool).sum())
    n_path_comp_yes_patients = int((rollup["n_path_syn_after_index_with_completion_col_yes"] > 0).sum())
    n_oed_any_later = int((rollup["n_operative_episode_strictly_after_index"] > 0).sum())

    summary = pd.DataFrame(
        [
            {
                "metric": "definition_study_completion",
                "value": "completion_after_lobectomy (cohort_logic.py): initial hemithyroidectomy in first_qualifying_surgeries; later row in operative_episode_detail_v2 with procedure_normalized=='total_thyroidectomy' ONLY; date strictly > index",
            },
            {"metric": "operative_v2_also_maps_completion_string", "value": "script 22 maps path_synoptics LIKE '%completion%' to procedure_normalized completion_thyroidectomy — never observed on LATER oed row in this cohort because OED has no second row"},
            {"metric": "primary_cohort_unique_patients", "value": n_primary},
            {"metric": "initial_lobectomy_n", "value": n_lob},
            {"metric": "initial_total_n", "value": n_tot},
            {"metric": "study_csv_completion_true_among_lobectomy", "value": study_lob_completion},
            {"metric": "oed_patients_with_any_row_strictly_after_index", "value": n_oed_any_later},
            {"metric": "path_syn_patients_later_row_with_completion_col_yes", "value": n_path_comp_yes_patients},
            {"metric": "path_syn_rows_after_index_total_lob_cohort", "value": int(rollup["n_path_syn_rows_after_index"].sum())},
            {"metric": "csv_vs_python_replay_mismatch_count", "value": int((~rollup["csv_vs_replay_match"]).sum())},
            {"metric": "token_mode", "value": token_mode()},
        ]
    )
    summary.to_csv(OUT / "completion_audit_summary.csv", index=False)

    cand.to_csv(OUT / "candidate_completion_cases.csv", index=False)
    missed.to_csv(OUT / "missed_or_ambiguous_completion_cases.csv", index=False)
    proc_inv.to_csv(OUT / "procedure_label_inventory.csv", index=False)
    rollup.to_csv(OUT / "patient_completion_crosswalk_rollup.csv", index=False)
    detail_oed.to_csv(OUT / "operative_later_than_index_detail.csv", index=False)
    path_detail.to_csv(OUT / "path_synoptic_later_completion_detail.csv", index=False)
    review_set.to_csv(OUT / "high_suspicion_path_completion_not_in_oed.csv", index=False)

    # Narrative
    verdict = [
        "# completion_logic_trace — independent audit",
        "",
        f"**Audit UTC tag:** {AUDIT_TAG}",
        f"**Token mode:** {token_mode()}",
        f"**Patient cohort:** `{PATIENT_CSV.relative_to(ROOT)}` (primary preop 2–4 cm, strict LN exclusion)",
        "",
        "## 1. Repository logic (verified in code)",
        "",
        "- **Initial / first surgery:** `cohort_logic.first_qualifying_surgeries()` on `operative_episode_detail_v2`, earliest row among `hemithyroidectomy` ∪ `total_thyroidectomy`.",
        "- **Completion after lobectomy:** `completion_after_lobectomy()` — ONLY flags when a **later** `operative_episode_detail_v2` row has `procedure_normalized == 'total_thyroidectomy'` (strict `>` on `resolved_surgery_date`). Does **not** include `completion_thyroidectomy` normalized label.",
        "- **Ultimate total:** `initial_total OR completion_total_flag` from above.",
        "- **Pipeline call:** `study_pipeline.py` builds `comp_df` from full OED pull + `first_clean`, merges into `patient_level_dataset.csv`.",
        "",
        "## 2. Three-way cross-check results",
        "",
        f"- **A. Study replay vs CSV:** mismatched rows = **{int((~rollup['csv_vs_replay_match']).sum())}** (expect 0).",
        f"- **B. OED timeline:** lobectomy patients with **any** OED row strictly after index date = **{n_oed_any_later}**.",
        f"- **C. Path synoptics:** lobectomy patients with a **later** `path_synoptics` row where `completion` ∈ {{yes,y}} = **{n_path_comp_yes_patients}**.",
        "",
        "## 3. Root cause when path shows completion but study shows 0",
        "",
        "`operative_episode_detail_v2` is anchored on `operative_details` (one primary row per patient in affected cases). Second surgeries appear in `path_synoptics` but often **do not** generate a second OED row, so `completion_after_lobectomy` never sees them.",
        "",
        "## 4. Manuscript claim assessment",
        "",
        '- Phrasing such as **“0 patients underwent completion thyroidectomy”** without qualification is **overstated** for **clinical** completion if path_synoptic `completion=yes` on a dated second procedure is accepted as evidence.',
        f"- **Under strict OED + study rule:** **{study_lob_completion} / {n_lob}** — supported.",
        f"- **Under path_synoptic later + completion=yes:** **≥ {n_path_comp_yes_patients} / {n_lob}** patients merit review as staged/completion procedures not visible in OED (plus possible additional ambiguous rows without `completion` filled).",
        "",
        "## 5. Safest wording pending chart review",
        "",
        '"Zero **pipeline-detected** completion thyroidectomies in `operative_episode_detail_v2` after index lobectomy (`table7`); separate **path_synoptic** rows suggest **additional** second-stage thyroid procedures in a subset — see independent audit bundle."',
        "",
        "## 6. SQL",
        "",
        f"All statements logged in `{SQL_LOG.relative_to(ROOT)}`.",
        "",
    ]
    (OUT / "completion_logic_trace.md").write_text("\n".join(verdict))

    (OUT / "audit_manifest.json").write_text(
        json.dumps(
            {
                "audit_utc": AUDIT_TAG,
                "cohort_csv": str(PATIENT_CSV),
                "output_dir": str(OUT),
                "counts": {
                    "primary_n": n_primary,
                    "lobe_n": n_lob,
                    "study_oed_completion": study_lob_completion,
                    "oed_any_later": n_oed_any_later,
                    "path_completion_yes_patients": n_path_comp_yes_patients,
                },
            },
            indent=2,
        )
    )

    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
