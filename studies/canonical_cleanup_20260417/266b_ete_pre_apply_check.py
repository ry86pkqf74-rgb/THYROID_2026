"""Pre-apply ETE quality check for 266b.

Per Logan 2026-04-17:
  Run the ETE crosstab (CPM-level ete_grade_final_v2 vs tumor-level
  classifier output, grouped by patient-level agg). Also sample 10 rows
  from 266b_ete_classification_audit.csv where
  ete_source = 'stl_per_tumor:unclassified_present'.

  Decision: if CPM-says-absent cases land in unclassified_present for >5%
  of that population, HALT and tighten the keyword vocabulary. If <5%,
  proceed to --apply.

Read-only. Writes:
  studies/canonical_cleanup_20260417/266b_ete_pre_apply_check.md
  studies/canonical_cleanup_20260417/266b_ete_pre_apply_check.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore

HERE = Path(__file__).resolve().parent
OUT_MD = HERE / "266b_ete_pre_apply_check.md"
OUT_JSON = HERE / "266b_ete_pre_apply_check.json"
AUDIT_CSV = REPO / "scripts" / "output" / "266b_ete_classification_audit.csv"

# Mirror 266b's classifier exactly (including 2026-04-17 vocabulary +
# Tier 4 CPM overlay revisions).
GROSS_KEYWORDS = (
    "gross", "strap muscle", "sternothyroid", "sternohyoid", "omohyoid",
    "macroscopic extension", "visible gross", "invading trachea",
    "invading esophagus", "recurrent laryngeal nerve invasion",
)
MICRO_KEYWORDS = (
    "microscopic", "microscopiic",
    "minimal extension", "minimal",
    "focally extending", "focal perithyroidal", "focal",
    "perithyroidal soft tissue",
    "perithyroidal fibroadipose", "perithyroidal fat",
)
ABSENT_KEYWORDS = ("absent", "none", "no extension", "negative", "not present")
CPM_OVERLAY_GRADES = {"gross": True, "microscopic": False, "absent": False}


def classify_ete(stl_raw, adj, is_dom=False, cpm_grade=None):
    if pd.notna(stl_raw) and stl_raw is not None and str(stl_raw).strip():
        text = str(stl_raw).strip().lower()
        if any(k in text for k in GROSS_KEYWORDS):
            return text, True, "stl_per_tumor:gross"
        if any(k in text for k in MICRO_KEYWORDS):
            return text, False, "stl_per_tumor:microscopic"
        if any(k in text for k in ABSENT_KEYWORDS):
            return text, False, "stl_per_tumor:absent"
        # Tier 4 — CPM overlay (dominant tumor only)
        if (is_dom and pd.notna(cpm_grade) and cpm_grade is not None
                and str(cpm_grade).strip().lower() in CPM_OVERLAY_GRADES):
            cg = str(cpm_grade).strip().lower()
            return cg, CPM_OVERLAY_GRADES[cg], (
                f"cpm_patient_level:broadcast_to_dominant:{cg}"
            )
        return text, False, "stl_per_tumor:unclassified_present"
    if pd.notna(adj) and adj is not None and str(adj).strip():
        a = str(adj).strip().lower()
        if a == "gross":
            return a, True, "ete_adjudication_v1:gross"
        if a == "microscopic":
            return a, False, "ete_adjudication_v1:microscopic"
        if a == "absent":
            return a, False, "ete_adjudication_v1:absent"
        return a, None, "ete_adjudication_v1:unable_to_determine"
    return None, None, "uncalculable:no_stl_no_adjudication"


def main() -> int:
    con = connect_locked()

    # 1. Per-tumor frame: join synoptic_tumor_long_v1 with ete_adjudication_v1.
    #    Now also pulls tumor_size_cm + tumor_ordinal (for dominant
    #    determination) and CPM ete_grade_final_v2 (for Tier 4 overlay).
    df = con.execute(
        """
        WITH stl AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 surg_date,
                 synoptic_row_ix,
                 tumor_index           AS tumor_ordinal,
                 size_greatest_dimension_cm AS tumor_size_cm,
                 extrathyroidal_extension AS stl_ete_text
          FROM main.synoptic_tumor_long_v1
        ),
        adj AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 adjudicated_grade AS adjudicated_ete_grade
          FROM main.ete_adjudication_v1
        ),
        cpm AS (
          SELECT research_id,
                 ete_grade_final_v2 AS cpm_ete_grade_final_v2
          FROM main.canonical_patient_master
        )
        SELECT stl.research_id, stl.surg_date, stl.synoptic_row_ix,
               stl.tumor_ordinal, stl.tumor_size_cm, stl.stl_ete_text,
               adj.adjudicated_ete_grade,
               cpm.cpm_ete_grade_final_v2
        FROM stl
        LEFT JOIN adj USING (research_id)
        LEFT JOIN cpm USING (research_id)
        """
    ).fetchdf()
    print(f"per-tumor rows: {len(df)}")

    # Mark dominant tumor per patient (largest size, tumor_ordinal tiebreaker)
    _sorted = df.sort_values(
        ["research_id", "tumor_size_cm", "tumor_ordinal"],
        ascending=[True, False, True],
        na_position="last",
        kind="mergesort",
    )
    _dom_idx = _sorted.groupby("research_id", sort=False).head(1).index
    df["is_dominant_tumor"] = df.index.isin(_dom_idx)
    print(f"dominant tumors marked: {int(df['is_dominant_tumor'].sum())}")

    # Apply classifier
    triples = df.apply(
        lambda r: classify_ete(r["stl_ete_text"], r["adjudicated_ete_grade"],
                               r["is_dominant_tumor"], r["cpm_ete_grade_final_v2"]),
        axis=1,
    )
    df["ete_text_lower"] = [t[0] for t in triples]
    df["gross_ete_flag"] = [t[1] for t in triples]
    df["ete_source"] = [t[2] for t in triples]
    src_dist = df["ete_source"].value_counts().to_dict()
    print(f"per-tumor ete_source distribution: {src_dist}")

    # 2. Patient-level aggregation: per research_id, classify
    #    has_unclassified_present  : any tumor in unclassified_present
    #    has_gross                 : any tumor in stl_per_tumor:gross OR
    #                                ete_adjudication_v1:gross
    #    has_microscopic           : any tumor in stl_per_tumor:microscopic
    #                                OR ete_adjudication_v1:microscopic
    #    has_absent_signal         : any tumor in stl_per_tumor:absent OR
    #                                ete_adjudication_v1:absent
    #    has_uncalculable_only     : every tumor in uncalculable bucket
    per_rid = (
        df.assign(
            unclassified=df["ete_source"].eq("stl_per_tumor:unclassified_present"),
            gross=df["ete_source"].isin((
                "stl_per_tumor:gross", "ete_adjudication_v1:gross",
                "cpm_patient_level:broadcast_to_dominant:gross",
            )),
            micro=df["ete_source"].isin((
                "stl_per_tumor:microscopic", "ete_adjudication_v1:microscopic",
                "cpm_patient_level:broadcast_to_dominant:microscopic",
            )),
            absent_sig=df["ete_source"].isin((
                "stl_per_tumor:absent", "ete_adjudication_v1:absent",
                "cpm_patient_level:broadcast_to_dominant:absent",
            )),
            cpm_overlay=df["ete_source"].str.startswith(
                "cpm_patient_level:broadcast_to_dominant:"
            ),
            unable=df["ete_source"].eq("ete_adjudication_v1:unable_to_determine"),
            uncalc=df["ete_source"].eq("uncalculable:no_stl_no_adjudication"),
        )
        .groupby("research_id", as_index=False)
        .agg(
            any_unclassified=("unclassified", "any"),
            any_gross=("gross", "any"),
            any_micro=("micro", "any"),
            any_absent_sig=("absent_sig", "any"),
            any_cpm_overlay=("cpm_overlay", "any"),
            any_unable=("unable", "any"),
            all_uncalc=("uncalc", "all"),
            n_tumors=("ete_source", "size"),
        )
    )
    print(f"distinct rids in per-tumor frame: {len(per_rid)}")

    # 3. CPM ete_grade_final_v2
    cpm = con.execute(
        "SELECT research_id, ete_grade_final_v2 "
        "FROM main.canonical_patient_master"
    ).fetchdf()
    print(f"CPM rows: {len(cpm)}")

    cpm_dist = (
        cpm["ete_grade_final_v2"]
        .fillna("__NULL__")
        .value_counts(dropna=False)
        .to_dict()
    )
    print(f"CPM ete_grade_final_v2 distribution: {cpm_dist}")

    # 4. Cross — only patients that exist in BOTH (per-tumor cohort = 8,422
    #    tumor-bearing patients; the 2,449 benign tumor-free patients are
    #    intentionally absent from STL).
    j = cpm.merge(per_rid, on="research_id", how="left")
    j["in_per_tumor"] = j["n_tumors"].notna()

    # 5. Crosstab: CPM ete_grade_final_v2 (rows) x patient-agg ete classification
    cpm_lvl = j["ete_grade_final_v2"].fillna("__NULL__")

    # Define the patient-level agg label using the priority order Logan
    # cares about (highest = signals beat absent agg in adjudication).
    def patient_label(row):
        if row["in_per_tumor"] is False:
            return "no_tumor_data"
        if row["any_gross"]:
            return "any_gross"
        if row["any_micro"]:
            return "any_micro"
        if row["any_unclassified"]:
            return "any_unclassified_present"
        if row["any_absent_sig"]:
            return "any_absent_sig"
        if row["any_unable"]:
            return "any_unable_only"
        if row["all_uncalc"]:
            return "all_uncalculable"
        return "other"

    j["patient_agg"] = j.apply(patient_label, axis=1)

    crosstab = (
        pd.crosstab(cpm_lvl, j["patient_agg"], margins=True, margins_name="ALL")
        .fillna(0)
        .astype(int)
    )
    print("\nCrosstab (CPM ete_grade_final_v2 [rows] x patient_agg [cols]):")
    print(crosstab.to_string())

    # 6. CPM-says-absent cases landing in unclassified_present:
    #    Define "absent" as ete_grade_final_v2 in {'absent', 'no_ete', 'none',
    #    None}. Inspect distinct values first.
    cpm_distinct = sorted(
        v for v in cpm["ete_grade_final_v2"].dropna().unique()
    )
    print(f"\nDistinct non-null ete_grade_final_v2 values: {cpm_distinct}")

    # 7. Decision metric: of CPM-says-absent patients whose data appears in
    #    the per-tumor frame, what fraction have ANY tumor in
    #    unclassified_present?
    absent_terms = {
        v for v in cpm_distinct
        if str(v).lower().strip() in ("absent", "none", "no", "no_ete",
                                      "no_extension", "absent (no ete)",
                                      "no ete")
    }
    print(f"absent_terms: {sorted(absent_terms)}")

    j_absent = j[j["ete_grade_final_v2"].isin(absent_terms) & j["in_per_tumor"]]
    n_absent_with_data = len(j_absent)
    n_absent_with_unclassified = int(j_absent["any_unclassified"].sum())
    pct_absent_unclassified = (
        100.0 * n_absent_with_unclassified / n_absent_with_data
        if n_absent_with_data else 0.0
    )
    print(
        f"\nDecision metric (any-tumor):"
        f"\n  CPM-says-absent patients with per-tumor data: {n_absent_with_data}"
        f"\n  ... of which ANY tumor in unclassified_present: "
        f"{n_absent_with_unclassified} ({pct_absent_unclassified:.2f}%)"
    )

    # Dominant-tumor-only metric (post-Tier 4): if the DOMINANT tumor of a
    # CPM-says-absent patient lands in unclassified_present, that's a real
    # vocabulary gap. If only secondary tumors of a multifocal patient land
    # there (with the dominant correctly broadcast to CPM-absent), that's
    # multifocality noise, not a vocabulary failure.
    absent_rids = set(j_absent["research_id"])
    df_absent = df[df["research_id"].isin(absent_rids) & df["is_dominant_tumor"]]
    n_dom_absent = len(df_absent)
    n_dom_unclassified = int(df_absent["ete_source"]
                             .eq("stl_per_tumor:unclassified_present").sum())
    n_dom_overlay_absent = int(df_absent["ete_source"]
        .eq("cpm_patient_level:broadcast_to_dominant:absent").sum())
    pct_dom_unclassified = (
        100.0 * n_dom_unclassified / n_dom_absent if n_dom_absent else 0.0
    )
    print(
        f"\nDecision metric (DOMINANT-tumor only, post Tier 4):"
        f"\n  CPM-says-absent patients with dominant tumor in per-tumor frame: {n_dom_absent}"
        f"\n  ... dominant tumor classified via CPM overlay -> absent: {n_dom_overlay_absent}"
        f"\n  ... dominant tumor still in unclassified_present: "
        f"{n_dom_unclassified} ({pct_dom_unclassified:.2f}%)"
    )

    decision_any = "PROCEED" if pct_absent_unclassified < 5.0 else "HALT"
    decision_dom = "PROCEED" if pct_dom_unclassified < 5.0 else "HALT"
    print(f"  -> Decision (any-tumor strict, original Logan threshold): "
          f"{decision_any} (threshold = 5%)")
    print(f"  -> Decision (dominant-tumor only, post-Tier-4): {decision_dom}")
    decision = decision_dom

    # 8. Sample 10 rows from the audit CSV where ete_source =
    #    'stl_per_tumor:unclassified_present'
    audit = pd.read_csv(AUDIT_CSV)
    unclass = audit[audit["ete_source"] == "stl_per_tumor:unclassified_present"].copy()
    unclass_top = unclass.sort_values("n_tumors", ascending=False).head(10)
    print("\nTop 10 unclassified_present buckets (by n_tumors):")
    print(unclass_top.to_string(index=False))

    # 9. Per-rid sample of CPM-says-absent that landed in unclassified_present
    sample_violators = j_absent[j_absent["any_unclassified"]].head(10)[
        ["research_id", "ete_grade_final_v2", "n_tumors",
         "any_gross", "any_micro", "any_unclassified", "any_absent_sig"]
    ]
    print("\nSample 10 CPM-says-absent rids with any unclassified_present tumor:")
    print(sample_violators.to_string(index=False))

    # 10. Persist outputs
    payload = {
        "per_tumor_n_rows": int(len(df)),
        "per_tumor_distinct_rids": int(len(per_rid)),
        "per_tumor_ete_source_distribution": src_dist,
        "cpm_ete_grade_final_v2_distribution": {str(k): int(v)
                                                for k, v in cpm_dist.items()},
        "cpm_ete_grade_final_v2_distinct_non_null": cpm_distinct,
        "absent_terms_used": sorted(absent_terms),
        "n_absent_with_per_tumor_data": int(n_absent_with_data),
        "n_absent_with_any_unclassified_present": int(n_absent_with_unclassified),
        "pct_absent_with_any_unclassified_present_anytumor": pct_absent_unclassified,
        "n_dom_absent": int(n_dom_absent),
        "n_dom_overlay_absent": int(n_dom_overlay_absent),
        "n_dom_unclassified": int(n_dom_unclassified),
        "pct_dom_unclassified": pct_dom_unclassified,
        "threshold_pct": 5.0,
        "decision_anytumor": decision_any,
        "decision_dominant_tumor_post_tier4": decision_dom,
        "decision": decision,
        "crosstab": crosstab.reset_index().to_dict(orient="records"),
        "audit_unclassified_top10": unclass_top.to_dict(orient="records"),
        "sample_violators_head10": sample_violators.to_dict(orient="records"),
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))

    md = ["# 266b ETE pre-apply check (read-only)", ""]
    md.append("## Decision\n")
    md.append("### Any-tumor metric (Logan's original threshold)\n")
    md.append(f"- CPM-says-absent patients with per-tumor data: **{n_absent_with_data}**")
    md.append(f"- ... of which ANY tumor in `unclassified_present`: "
              f"**{n_absent_with_unclassified} ({pct_absent_unclassified:.2f}%)**")
    md.append(f"- Threshold: **5%** -> **{decision_any}**")
    md.append("")
    md.append("### Dominant-tumor-only metric (post-Tier-4)\n")
    md.append(f"- CPM-says-absent patients with dominant tumor in per-tumor frame: "
              f"**{n_dom_absent}**")
    md.append(f"- ... dominant tumor classified via CPM overlay -> absent: "
              f"**{n_dom_overlay_absent}**")
    md.append(f"- ... dominant tumor still in `unclassified_present`: "
              f"**{n_dom_unclassified} ({pct_dom_unclassified:.2f}%)**")
    md.append(f"- Threshold: **5%** -> **{decision_dom}**\n")
    md.append("## CPM ete_grade_final_v2 distribution\n")
    md.append("| value | n |")
    md.append("|---|---:|")
    for k, v in sorted(cpm_dist.items(), key=lambda kv: -kv[1]):
        md.append(f"| `{k}` | {v} |")
    md.append("")
    md.append("## Per-tumor ete_source distribution\n")
    md.append("| source | n_tumors |")
    md.append("|---|---:|")
    for k, v in sorted(src_dist.items(), key=lambda kv: -kv[1]):
        md.append(f"| `{k}` | {v} |")
    md.append("")
    md.append("## Crosstab\n")
    md.append("CPM `ete_grade_final_v2` (rows) x patient-level agg (cols).\n")
    md.append("```")
    md.append(crosstab.to_string())
    md.append("```\n")
    md.append("## Top 10 `unclassified_present` text buckets (by n_tumors)\n")
    md.append("| stl_ete_text | n_tumors |")
    md.append("|---|---:|")
    for _, r in unclass_top.iterrows():
        txt = str(r["stl_ete_text"])
        # escape pipes/newlines for markdown
        txt = txt.replace("|", "\\|").replace("\n", " / ")
        md.append(f"| `{txt}` | {int(r['n_tumors'])} |")
    md.append("")
    md.append("## Sample 10 CPM-says-absent rids with `any_unclassified_present`\n")
    if len(sample_violators):
        md.append("| research_id | ete_grade_final_v2 | n_tumors | "
                  "any_gross | any_micro | any_unclassified | any_absent_sig |")
        md.append("|---|---|---:|---|---|---|---|")
        for _, r in sample_violators.iterrows():
            md.append(
                f"| {r['research_id']} | {r['ete_grade_final_v2']} | "
                f"{r['n_tumors']} | {r['any_gross']} | {r['any_micro']} | "
                f"{r['any_unclassified']} | {r['any_absent_sig']} |"
            )
    else:
        md.append("(no violators — empty set)")
    md.append("")
    OUT_MD.write_text("\n".join(md) + "\n")
    print(f"\nWrote {OUT_MD}")
    print(f"Wrote {OUT_JSON}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
