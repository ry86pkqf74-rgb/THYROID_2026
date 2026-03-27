#!/usr/bin/env python3
"""Full MotherDuck audit: completion thyroidectomy after index lobectomy (2–4 cm manuscript cohort).

Primary cohort = rows in patient_level_dataset.csv (preop 2–4 cm, strict nodal exclusion).
Writes stable paths under completion_audit_outputs/:
  completion_audit_summary.csv, candidate_completion_cases.csv,
  procedure_label_inventory.csv, final_verdict.md
Plus audit_sql_log.sql, later_thyroid_surgeries_long.csv, audit_manifest.json

Usage:
  .venv/bin/python studies/proposal_2to4cm_extent_molecular_20260326/run_completion_audit_motherduck.py
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

from motherduck_client import MotherDuckClient, MotherDuckConfig, get_token, token_mode  # noqa: E402

import cohort_logic as cl  # noqa: E402

STABLE_OUT = STUDY / "completion_audit_outputs"
PATIENT_CSV = STUDY / "patient_level_dataset.csv"
SQL_LOG = STABLE_OUT / "audit_sql_log.sql"

TP_COMPLETION_RE = re.compile(
    r"(completion\s+thyroidectomy|compl\.?\s*thyroidectomy|complete\s+thyroidectomy|"
    r"completion\s+of\s+(the\s+)?thyroid|second\s*[- ]stage\s+thyroidectomy|"
    r"re-?operative\s+thyroidectomy|total\s+thyroidectomy\s*\(?\s*completion|"
    r"thyroidectomy\s*,\s*completion)",
    re.I,
)
RAW_COMPLETION_RE = re.compile(
    r"completion|compl\.?\s*thyroid|complete\s+thyroid|contralateral\s+lobe|"
    r"remaining\s+lobe|second\s*[- ]stage|re-?operative\s+thyroid",
    re.I,
)

AUDIT_UTC = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_sql(fh, label: str, sql: str) -> None:
    fh.write(f"\n-- === {label} ===\n")
    fh.write(sql.rstrip() + "\n")


def classify_path_procedure(tp: str | None, completion_col: object) -> str:
    """definite_completion | ambiguous_likely_staged | not_completion_indicator | unknown"""
    tp = (tp or "").strip()
    comp = str(completion_col).strip().lower() if completion_col is not None and str(completion_col).strip() != "" else ""
    if comp in ("yes", "y"):
        return "definite_completion"
    if TP_COMPLETION_RE.search(tp):
        return "definite_completion"
    tl = tp.lower()
    if "lobectomy" in tl or "hemithyroidectomy" in tl or "thyroidectomy" in tl:
        if comp in ("no", "n", "false", "0"):
            return "not_completion_indicator"
        if comp == "" or pd.isna(completion_col):
            return "ambiguous_likely_staged"
        return "ambiguous_likely_staged"
    return "unknown"


def main() -> None:
    if not get_token():
        raise SystemExit("No MotherDuck token (set MOTHERDUCK_TOKEN or use .streamlit/secrets.toml).")

    STABLE_OUT.mkdir(parents=True, exist_ok=True)
    pl = pd.read_csv(PATIENT_CSV, low_memory=False)
    pl["research_id"] = pd.to_numeric(pl["research_id"], errors="coerce").astype("Int64")
    n_primary = int(pl["research_id"].nunique())

    lob = pl[pl["initial_lobectomy"] == 1].copy()
    lob["index_dt"] = pd.to_datetime(lob["index_surgery_date"], errors="coerce")
    n_lob = len(lob)
    n_tot = int((pl["initial_total"] == 1).sum())

    con = MotherDuckClient(MotherDuckConfig()).connect_rw()
    fh = SQL_LOG.open("w")
    fh.write(
        f"-- completion audit SQL log\n-- run_utc={AUDIT_UTC}\n-- token_mode={token_mode()}\n"
        f"-- cohort_file={PATIENT_CSV.name} primary_N={n_primary} lobectomy_N={n_lob}\n"
    )

    sql_ops = """
    SELECT research_id, surgery_episode_id, resolved_surgery_date,
           procedure_normalized, procedure_raw, laterality
    FROM operative_episode_detail_v2
    """
    log_sql(fh, "operative_episode_detail_v2", sql_ops)
    fh.flush()
    ops = con.execute(sql_ops).df()
    ops["research_id"] = pd.to_numeric(ops["research_id"], errors="coerce").astype("Int64")
    ops["sx_dt"] = pd.to_datetime(ops["resolved_surgery_date"], errors="coerce")

    sql_ps = """
    SELECT research_id, surg_date, thyroid_procedure, completion, thyroid_procedure AS procedure_text_raw
    FROM path_synoptics
    """
    log_sql(fh, "path_synoptics", sql_ps)
    path_all = con.execute(sql_ps).df()
    path_all["research_id"] = pd.to_numeric(path_all["research_id"], errors="coerce")
    path_all["sd"] = pd.to_datetime(path_all["surg_date"], errors="coerce")

    fh.close()

    first_clean = cl.first_qualifying_surgeries(
        ops, frozenset({"hemithyroidectomy", "total_thyroidectomy"})
    )
    comp_study = cl.completion_after_lobectomy(ops, first_clean)
    comp_map = comp_study.set_index("research_id")

    long_rows: list[dict] = []
    rollup_rows: list[dict] = []

    for _, row in lob.iterrows():
        rid = int(row["research_id"])
        idx = row["index_dt"]
        study_csv = bool(row["completion_total_flag"]) if pd.notna(row["completion_total_flag"]) else False
        study_py = bool(comp_map.loc[rid, "completion_total_flag"]) if rid in comp_map.index else False

        g = ops[ops["research_id"] == rid].sort_values("sx_dt")
        later_oed = g[g["sx_dt"] > idx]

        for _, o in later_oed.iterrows():
            pn = str(o["procedure_normalized"] or "")
            raw = str(o["procedure_raw"] or "")
            oed_class = "not_thyroid_related"
            if pn in ("total_thyroidectomy", "completion_thyroidectomy", "hemithyroidectomy"):
                if pn == "total_thyroidectomy":
                    oed_class = "definite_completion_or_total"
                elif pn == "completion_thyroidectomy":
                    oed_class = "definite_completion"
                else:
                    oed_class = "ambiguous_second_lobe_or_staged"
            elif pn in ("other", "unknown"):
                if RAW_COMPLETION_RE.search(raw):
                    oed_class = "ambiguous_raw_language"
                else:
                    oed_class = "ambiguous_unknown_procedure_bucket"
            long_rows.append(
                {
                    "research_id": rid,
                    "source": "operative_episode_detail_v2",
                    "event_date": o["sx_dt"].isoformat() if pd.notna(o["sx_dt"]) else "",
                    "normalized_label": pn,
                    "raw_text": (raw or "")[:2000],
                    "path_completion_column": "",
                    "later_than_index": True,
                    "row_classification": oed_class,
                }
            )

        ps_sub = path_all[(path_all["research_id"] == rid) & path_all["sd"].notna()]
        ps_later = ps_sub[ps_sub["sd"] > idx]
        for _, p in ps_later.iterrows():
            tp = p["thyroid_procedure"]
            pclass = classify_path_procedure(tp, p["completion"])
            long_rows.append(
                {
                    "research_id": rid,
                    "source": "path_synoptics",
                    "event_date": p["sd"].isoformat() if pd.notna(p["sd"]) else "",
                    "normalized_label": "",
                    "raw_text": str(tp or "")[:2000],
                    "path_completion_column": p["completion"],
                    "later_than_index": True,
                    "row_classification": pclass,
                }
            )

        # same-day index duplicates on OED
        n_oed_later = len(later_oed)
        n_ps_later = len(ps_later)
        def_any = (
            (later_oed["procedure_normalized"] == "total_thyroidectomy").any()
            or (later_oed["procedure_normalized"] == "completion_thyroidectomy").any()
            or (ps_later["completion"].astype(str).str.lower().isin({"yes", "y"})).any()
            or ps_later["thyroid_procedure"]
            .fillna("")
            .astype(str)
            .map(lambda s: bool(TP_COMPLETION_RE.search(s)))
            .any()
        )
        amb_any = False
        if n_ps_later > 0 and not def_any:
            amb_any = True
        if (later_oed["procedure_normalized"] == "hemithyroidectomy").any():
            amb_any = True
        if (
            later_oed["procedure_normalized"].isin(["other", "unknown"]).any()
            and later_oed["procedure_raw"].fillna("").astype(str).map(lambda s: bool(RAW_COMPLETION_RE.search(s))).any()
        ):
            amb_any = True

        rollup_rows.append(
            {
                "research_id": rid,
                "index_surgery_date": idx.isoformat() if pd.notna(idx) else "",
                "n_later_oed_events": n_oed_later,
                "n_later_path_syn_rows": n_ps_later,
                "has_definite_completion_audit": bool(def_any),
                "has_ambiguous_later_thyroid": bool(amb_any) or (n_ps_later > 0 and not def_any),
                "study_completion_total_flag_csv": study_csv,
                "study_completion_replay_python": study_py,
            }
        )

    long_df = pd.DataFrame(long_rows)
    rollup_df = pd.DataFrame(rollup_rows)

    any_later_mask = (rollup_df["n_later_oed_events"] > 0) | (rollup_df["n_later_path_syn_rows"] > 0)
    any_later_patients = int(any_later_mask.sum())
    definite_patients = int(rollup_df["has_definite_completion_audit"].sum())
    ambiguous_only_patients = int((any_later_mask & ~rollup_df["has_definite_completion_audit"]).sum())
    study_completion_count = int(rollup_df["study_completion_total_flag_csv"].astype(bool).sum())
    csv_py_mismatch = int(
        (rollup_df["study_completion_total_flag_csv"].astype(bool) != rollup_df["study_completion_replay_python"].astype(bool)).sum()
    )

    long_df.to_csv(STABLE_OUT / "later_thyroid_surgeries_long.csv", index=False)
    rollup_df.to_csv(STABLE_OUT / "patient_later_surgery_rollup.csv", index=False)

    def _audit_tier(row_class: str) -> str:
        if row_class in ("definite_completion", "definite_completion_or_total"):
            return "definite"
        if row_class in (
            "ambiguous_likely_staged",
            "ambiguous_second_lobe_or_staged",
            "ambiguous_raw_language",
            "ambiguous_unknown_procedure_bucket",
        ):
            return "ambiguous"
        return "other"

    cand = long_df.copy()
    cand["audit_tier"] = cand["row_classification"].map(_audit_tier)
    cand = cand[cand["audit_tier"].isin(["definite", "ambiguous"])].copy()
    cand.to_csv(STABLE_OUT / "candidate_completion_cases.csv", index=False)

    # Procedure inventory: OED + path for primary cohort (all lob + all primary for context)
    cids = pl["research_id"].dropna().astype(int).tolist()
    lob_ids = lob["research_id"].dropna().astype(int).tolist()
    con.register("_coh_primary", pd.DataFrame({"research_id": cids}))
    con.register("_coh_lob", pd.DataFrame({"research_id": lob_ids}))

    inv_oed_sql = """
    SELECT 'operative_episode_detail_v2' AS source_table,
           o.procedure_normalized AS normalized_label,
           o.procedure_raw AS raw_label,
           COUNT(*) AS n_row_instances
    FROM operative_episode_detail_v2 o
    INNER JOIN _coh_primary ON CAST(o.research_id AS BIGINT) = _coh_primary.research_id
    GROUP BY 1, 2, 3
    ORDER BY n_row_instances DESC
    """
    inv_path_sql = """
    SELECT DISTINCT 'path_synoptics' AS source_table,
      CASE
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%completion%' THEN 'TEXT_CONTAINS_completion'
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%total%thyroid%' OR LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%near-total%' THEN 'TEXT_CONTAINS_total_thyroid'
        WHEN LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%lobectomy%' OR LOWER(COALESCE(ps.thyroid_procedure,'')) LIKE '%hemithyroid%' THEN 'TEXT_CONTAINS_lobectomy'
        ELSE 'TEXT_OTHER'
      END AS normalized_label,
      ps.thyroid_procedure AS raw_label,
      NULL::BIGINT AS n_row_instances
    FROM path_synoptics ps
    INNER JOIN _coh_lob ON CAST(ps.research_id AS BIGINT) = _coh_lob.research_id
    WHERE ps.thyroid_procedure IS NOT NULL AND TRIM(CAST(ps.thyroid_procedure AS VARCHAR)) <> ''
    """
    inv_path_counts_sql = """
    SELECT 'path_synoptics' AS source_table,
           ps.thyroid_procedure AS raw_label,
           LOWER(COALESCE(CAST(ps.completion AS VARCHAR), '')) AS completion_column_value,
           COUNT(*) AS n_row_instances
    FROM path_synoptics ps
    INNER JOIN _coh_lob ON CAST(ps.research_id AS BIGINT) = _coh_lob.research_id
    WHERE ps.thyroid_procedure IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY n_row_instances DESC
    """
    with SQL_LOG.open("a") as fh:
        log_sql(fh, "procedure_inv_oed_cohort_primary", inv_oed_sql)
        log_sql(fh, "procedure_inv_path_distinct_patterns", inv_path_sql)
        log_sql(fh, "procedure_inv_path_counts_lob_cohort", inv_path_counts_sql)
    inv_oed = con.execute(inv_oed_sql).df()
    inv_path_patterns = con.execute(inv_path_sql).df()
    inv_path_ct = con.execute(inv_path_counts_sql).df()
    con.unregister("_coh_primary")
    con.unregister("_coh_lob")

    inv_oed_out = inv_oed.rename(columns={"n_row_instances": "n_instances"}).assign(inventory_kind="oed_grouped")
    inv_path_out = inv_path_ct.rename(
        columns={
            "completion_column_value": "completion_col_lower",
            "n_row_instances": "n_instances",
        }
    ).assign(
        inventory_kind="path_syn_grouped",
        normalized_label="(see raw_label)",
    )
    inv_pat_out = inv_path_patterns.rename(columns={"n_row_instances": "n_instances"}).assign(
        inventory_kind="path_pattern_distinct"
    )
    proc_inventory = pd.concat([inv_oed_out, inv_path_out, inv_pat_out], ignore_index=True, sort=False)
    proc_inventory["inventory_scope"] = proc_inventory["source_table"].map(
        lambda s: "operative_episode_all_rows_primary_558"
        if s == "operative_episode_detail_v2"
        else "path_synoptics_all_rows_lobectomy_238"
    )
    proc_inventory.to_csv(STABLE_OUT / "procedure_label_inventory.csv", index=False)

    summary = pd.DataFrame(
        [
            {"metric": "cohort_definition", "value": "patient_level_dataset.csv rows (preop imaging 2–4 cm index nodule, strict LN exclusion)"},
            {"metric": "primary_cohort_n_patients", "value": n_primary},
            {"metric": "initial_lobectomy_n", "value": n_lob},
            {"metric": "initial_total_n", "value": n_tot},
            {"metric": "definite_completion_patients_independent_audit", "value": definite_patients},
            {"metric": "patients_any_later_but_no_definite_completion_ambiguous_bucket", "value": ambiguous_only_patients},
            {"metric": "any_later_thyroid_related_row_oed_or_path_patients", "value": any_later_patients},
            {"metric": "study_pipeline_completion_total_flag_true_among_lobectomy", "value": study_completion_count},
            {"metric": "csv_vs_completion_after_lobectomy_replay_mismatch", "value": csv_py_mismatch},
            {"metric": "manuscript_zero_claim_oed_only", "value": "TRUE if phrase means operative_episode_detail_v2 + total_thyroidectomy after index only"},
            {
                "metric": "manuscript_zero_claim_unqualified_clinical",
                "value": f"FALSE if synoptic/path evidence counts — definite_completion_patients={definite_patients}",
            },
            {"metric": "token_mode", "value": token_mode()},
            {"metric": "audit_utc", "value": AUDIT_UTC},
        ]
    )
    summary.to_csv(STABLE_OUT / "completion_audit_summary.csv", index=False)

    verdict = f"""# Final verdict — completion thyroidectomy audit (2–4 cm manuscript cohort)

**Audit UTC:** {AUDIT_UTC}
**MotherDuck token mode:** {token_mode()}
**Primary cohort:** `{PATIENT_CSV.relative_to(ROOT)}` — **N = {n_primary}**
**Initial lobectomy (manuscript arm):** **N = {n_lob}**

## Question

Did any patients in this cohort have **completion thyroidectomy** (or equivalent staged totalization) **after** index lobectomy?

## Methods (independent)

1. **Index surgery** — from frozen `patient_level_dataset.csv`: `index_surgery_date` and `procedure_normalized` for each patient (study-defined preop cohort).
2. **Later operative episodes** — all `operative_episode_detail_v2` rows with event time **strictly after** index (`resolved_surgery_date` > index).
3. **Later pathology synoptics** — all `path_synoptics` rows with `surg_date` **strictly after** index.
4. **Definite completion (audit rule)** — any of:
   - Later OED `procedure_normalized` ∈ `{{total_thyroidectomy, completion_thyroidectomy}}`
   - Later path row with synoptic **`completion` ∈ {{yes, y}}**
   - Later path `thyroid_procedure` text matching completion-thyroidectomy patterns (regex; see script).
5. **Ambiguous** — later thyroid-related path/OED signal without crossing the definite rule (e.g. second lobectomy row with blank `completion`).

## Counts

| Metric | Count |
|--------|------:|
| Patients with **any** later thyroid-related row (OED or path) | {any_later_patients} |
| Patients with **definite** completion per audit rule | {definite_patients} |
| Patients with **any** later event but **no** definite completion rule | {ambiguous_only_patients} |
| Study CSV `completion_total_flag` true (OED pipeline logic) | {study_completion_count} |

## Proof / disproof of “0 completion thyroidectomy”

- **If the claim means** the frozen pipeline flag on `operative_episode_detail_v2` detecting only **later `total_thyroidectomy`**: **supported — {study_completion_count} / {n_lob}**.
- **If the claim means** no patient had a **documented completion / second-stage thyroid procedure** in **path_synoptics** after index lobectomy: **disproved — `has_definite_completion_audit` = {definite_patients} / {n_lob}** (see `candidate_completion_cases.csv` and `later_thyroid_surgeries_long.csv`).

**Root cause** of the discrepancy: second procedures often appear on **`path_synoptics`** with **`completion = yes`** but do not produce a second row in **`operative_episode_detail_v2`** (anchored on sparse `operative_details`), so `cohort_logic.completion_after_lobectomy()` never fires.

## Artifacts

- `completion_audit_summary.csv` — metrics
- `candidate_completion_cases.csv` — case-level later events (definite + ambiguous)
- `procedure_label_inventory.csv` — OED + path procedure strings (cohort-scoped)
- `later_thyroid_surgeries_long.csv` — full long event list
- `audit_sql_log.sql` — SQL pulled in MotherDuck

## Future studies — recommendations

1. Derive **surgery timeline** from a **union** of `operative_episode_detail_v2`, `path_synoptics.surg_date`, and (if available) `tumor_episode_master_v2` surgery dates; dedupe by `(research_id, date)`.
2. Define completion as: **later total OR completion-normalized label OR synoptic completion flag OR second lobectomy with completion context** — with explicit tiers in methods.
3. Keep **pipeline-only** vs **chart-integrated** metrics **separate** in tables (two columns, not one overloaded “0”).

---
*Generated by `run_completion_audit_motherduck.py`*
"""
    (STABLE_OUT / "final_verdict.md").write_text(verdict)

    (STABLE_OUT / "audit_manifest.json").write_text(
        json.dumps(
            {
                "audit_utc": AUDIT_UTC,
                "token_mode": token_mode(),
                "cohort_csv": str(PATIENT_CSV.relative_to(ROOT)),
                "counts": {
                    "primary_n": n_primary,
                    "lobectomy_n": n_lob,
                    "definite_completion_patients": definite_patients,
                    "ambiguous_only_patients": ambiguous_only_patients,
                    "any_later_patients": any_later_patients,
                    "study_pipeline_completion_true": study_completion_count,
                },
            },
            indent=2,
        )
    )

    print(f"Wrote stable audit outputs to {STABLE_OUT}")


if __name__ == "__main__":
    main()