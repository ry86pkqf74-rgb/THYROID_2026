"""Phase 1.6 Tg drift audit (read-only).

Why: live deltas are smaller than prompt expected:
  TG count : prompt ~1,444 newly populated; live +397 (2,528 -> 2,925)
  TGAB     : prompt ~1,675 newly populated; live +397 (2,641 -> 3,038)
  peak     : prompt ~503  newly populated; live +359 (2,561 -> 2,920)
  nadir    : prompt ~535  newly populated; live +359 (2,561 -> 2,920)

Per Logan: investigate before rebuilding. 5-10 minute scope:
  1) Sample 10 patients from the +397 TG-count delta cohort.
  2) Sample 10 patients from the +359 peak/nadir delta cohort.
  3) Look for structural patterns (TgAb interference? recent labs? specific
     analytes? flat cross-section?).

Output: studies/canonical_cleanup_20260417/phase1_6_tg_drift_audit.md
        studies/canonical_cleanup_20260417/phase1_6_tg_drift_audit.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

HERE = Path(__file__).resolve().parent
OUT_JSON = HERE / "phase1_6_tg_drift_audit.json"
OUT_MD = HERE / "phase1_6_tg_drift_audit.md"

CLASSIFIER = (
    "CASE WHEN LOWER(analyte) LIKE '%antibod%' OR LOWER(analyte) LIKE 'tgab%' "
    "THEN 'TGAB' WHEN LOWER(analyte) LIKE 'thyroglobulin%' OR LOWER(analyte) = 'tg' "
    "THEN 'TG' ELSE 'OTHER' END"
)


def main() -> int:
    con = connect_locked()
    out: dict = {"summary": {}, "samples": {}, "patterns": {}}

    # --- 1. Per-rid live counts vs current CPM ---
    delta_view = f"""
        WITH live AS (
          SELECT
            research_id::INTEGER AS research_id,
            COUNT(*) FILTER (WHERE {CLASSIFIER} = 'TG')   AS live_tg,
            COUNT(*) FILTER (WHERE {CLASSIFIER} = 'TGAB') AS live_tgab,
            MAX(result_numeric) FILTER (WHERE {CLASSIFIER} = 'TG'
                                         AND result_numeric IS NOT NULL) AS live_peak,
            MIN(result_numeric) FILTER (WHERE {CLASSIFIER} = 'TG'
                                         AND result_numeric IS NOT NULL) AS live_nadir,
            MIN(specimen_collect_dt) FILTER (WHERE {CLASSIFIER} = 'TG') AS first_tg_dt,
            MAX(specimen_collect_dt) FILTER (WHERE {CLASSIFIER} = 'TG') AS last_tg_dt
          FROM main.thyroglobulin_lab_canonical_v1
          GROUP BY 1
        )
        SELECT
          cpm.research_id,
          cpm.n_tg_measurements_structured AS cpm_tg,
          live.live_tg,
          live.live_tg - COALESCE(cpm.n_tg_measurements_structured, 0) AS d_tg,
          cpm.n_tgab_measurements AS cpm_tgab,
          live.live_tgab,
          live.live_tgab - COALESCE(cpm.n_tgab_measurements, 0) AS d_tgab,
          cpm.tg_peak AS cpm_peak,
          live.live_peak,
          cpm.tg_nadir AS cpm_nadir,
          live.live_nadir,
          live.first_tg_dt,
          live.last_tg_dt
        FROM main.canonical_patient_master cpm
        JOIN live USING(research_id)
    """

    # --- 2. Aggregate drift summary ---
    agg = con.execute(
        f"""
        SELECT
          COUNT(*) AS n_with_lab_data,
          COUNT(*) FILTER (WHERE COALESCE(cpm_tg,0) <> live_tg)         AS n_tg_diff,
          COUNT(*) FILTER (WHERE COALESCE(cpm_tg,0) <  live_tg)         AS n_tg_under,
          COUNT(*) FILTER (WHERE COALESCE(cpm_tg,0) >  live_tg)         AS n_tg_over,
          COUNT(*) FILTER (WHERE COALESCE(cpm_tgab,0) <> live_tgab)     AS n_tgab_diff,
          COUNT(*) FILTER (WHERE cpm_peak  IS DISTINCT FROM live_peak)  AS n_peak_diff,
          COUNT(*) FILTER (WHERE cpm_nadir IS DISTINCT FROM live_nadir) AS n_nadir_diff
        FROM ({delta_view}) t
        """
    ).fetchone()
    cols = ["n_with_lab_data", "n_tg_diff", "n_tg_under", "n_tg_over",
            "n_tgab_diff", "n_peak_diff", "n_nadir_diff"]
    out["summary"] = dict(zip(cols, agg))

    # --- 3. Distribution of d_tg (TG count delta) ---
    out["d_tg_distribution"] = con.execute(
        f"""
        SELECT
          CASE WHEN d_tg = 0 THEN '0'
               WHEN d_tg BETWEEN 1 AND 5 THEN '1-5'
               WHEN d_tg BETWEEN 6 AND 20 THEN '6-20'
               WHEN d_tg BETWEEN 21 AND 100 THEN '21-100'
               WHEN d_tg > 100 THEN '>100'
               WHEN d_tg < 0 THEN 'NEGATIVE'
          END AS bucket,
          COUNT(*) AS n_patients
        FROM ({delta_view}) t
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()

    # --- 4. Sample 10 from TG-count delta cohort (under-counts) ---
    out["samples"]["tg_count_undercounts"] = con.execute(
        f"""
        SELECT *
        FROM ({delta_view}) t
        WHERE d_tg > 0
        ORDER BY d_tg DESC
        LIMIT 10
        """
    ).fetchall()

    # --- 5. Sample 10 from peak/nadir delta cohort ---
    out["samples"]["peak_nadir_delta"] = con.execute(
        f"""
        SELECT *
        FROM ({delta_view}) t
        WHERE cpm_peak IS DISTINCT FROM live_peak
           OR cpm_nadir IS DISTINCT FROM live_nadir
        ORDER BY ABS(COALESCE(cpm_peak,0) - COALESCE(live_peak,0)) DESC NULLS LAST
        LIMIT 10
        """
    ).fetchall()

    # --- 6. Pattern probes ---
    # 6a. Are undercounts concentrated in TgAb-interference patients?
    out["patterns"]["tgab_interference_in_undercounts"] = con.execute(
        f"""
        WITH d AS ({delta_view})
        SELECT
          COUNT(*) FILTER (WHERE d.d_tg > 0 AND d.live_tgab > 0)
            AS undercount_with_tgab,
          COUNT(*) FILTER (WHERE d.d_tg > 0 AND d.live_tgab = 0)
            AS undercount_without_tgab,
          COUNT(*) FILTER (WHERE d.d_tg = 0 AND d.live_tgab > 0)
            AS aligned_with_tgab,
          COUNT(*) FILTER (WHERE d.d_tg = 0 AND d.live_tgab = 0)
            AS aligned_without_tgab
        FROM d
        """
    ).fetchone()

    # 6b. Date-window pattern: are deltas concentrated in patients with
    #     recent labs (e.g., post a CPM build cutoff)?
    out["patterns"]["delta_by_last_tg_year"] = con.execute(
        f"""
        SELECT
          EXTRACT(YEAR FROM last_tg_dt) AS yr,
          COUNT(*) FILTER (WHERE d_tg > 0) AS n_undercount,
          COUNT(*) FILTER (WHERE d_tg = 0) AS n_aligned,
          COUNT(*) FILTER (WHERE d_tg < 0) AS n_overcount
        FROM ({delta_view}) t
        GROUP BY 1 ORDER BY 1 NULLS LAST
        """
    ).fetchall()

    # 6c. Distinct analytes that fall into TG vs TGAB
    out["patterns"]["analyte_class_breakdown"] = con.execute(
        f"""
        SELECT {CLASSIFIER} AS cls, analyte, COUNT(*) AS n
        FROM main.thyroglobulin_lab_canonical_v1
        GROUP BY 1,2 ORDER BY 1,3 DESC
        """
    ).fetchall()

    # 6d. Spot-check: are CPM tg counts inflated by some that are NOT in lab table?
    out["patterns"]["cpm_has_count_no_lab_rows"] = con.execute(
        """
        WITH lab_rids AS (
          SELECT DISTINCT research_id::INTEGER AS rid
          FROM main.thyroglobulin_lab_canonical_v1
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        LEFT JOIN lab_rids ON lab_rids.rid = cpm.research_id
        WHERE cpm.n_tg_measurements_structured > 0
          AND lab_rids.rid IS NULL
        """
    ).fetchone()[0]

    # ---- write ----
    OUT_JSON.write_text(json.dumps(out, indent=2, default=str))

    md = ["# Phase 1.6 Tg drift audit (read-only)", "",
          "Database: `thyroid_canonical_publication_v1_0`", ""]
    md.append("## Summary (rids with at least one lab row)\n")
    md.append("| Metric | Value |")
    md.append("|---|---:|")
    for k, v in out["summary"].items():
        md.append(f"| `{k}` | {v} |")
    md.append("")
    md.append("## Distribution of TG count delta (live - cpm)\n")
    md.append("| Bucket | Patients |")
    md.append("|---|---:|")
    for b, n in out["d_tg_distribution"]:
        md.append(f"| `{b}` | {n:,} |")
    md.append("")
    md.append("## Pattern probes\n")
    md.append("### TgAb-interference vs undercount (rids with d_tg > 0)\n")
    p = out["patterns"]["tgab_interference_in_undercounts"]
    md.append(f"- Undercount WITH any TGAB labs: **{p[0]}**")
    md.append(f"- Undercount WITHOUT TGAB labs: **{p[1]}**")
    md.append(f"- Aligned WITH any TGAB labs: **{p[2]}**")
    md.append(f"- Aligned WITHOUT TGAB labs: **{p[3]}**\n")

    md.append("### Delta by year of last_tg_dt\n")
    md.append("| Year | Undercount | Aligned | Overcount |")
    md.append("|---:|---:|---:|---:|")
    for r in out["patterns"]["delta_by_last_tg_year"]:
        md.append(f"| {r[0]} | {r[1]:,} | {r[2]:,} | {r[3]:,} |")
    md.append("")

    md.append("### Analyte classification\n")
    md.append("| Class | Analyte | n_rows |")
    md.append("|---|---|---:|")
    for r in out["patterns"]["analyte_class_breakdown"]:
        md.append(f"| {r[0]} | `{r[1]}` | {r[2]:,} |")
    md.append("")

    md.append(
        f"### CPM patients with `n_tg_measurements_structured > 0` but NO rows "
        f"in `thyroglobulin_lab_canonical_v1`: **{out['patterns']['cpm_has_count_no_lab_rows']:,}**"
    )
    md.append("")

    md.append("## Sample: top 10 TG count undercounts (sorted by d_tg DESC)\n")
    headers = [
        "research_id", "cpm_tg", "live_tg", "d_tg",
        "cpm_tgab", "live_tgab", "d_tgab",
        "cpm_peak", "live_peak", "cpm_nadir", "live_nadir",
        "first_tg_dt", "last_tg_dt",
    ]
    md.append("| " + " | ".join(headers) + " |")
    md.append("|" + "|".join("---" for _ in headers) + "|")
    for row in out["samples"]["tg_count_undercounts"]:
        md.append("| " + " | ".join(str(c) for c in row) + " |")
    md.append("")
    md.append("## Sample: top 10 peak/nadir deltas (sorted by |peak diff| DESC)\n")
    md.append("| " + " | ".join(headers) + " |")
    md.append("|" + "|".join("---" for _ in headers) + "|")
    for row in out["samples"]["peak_nadir_delta"]:
        md.append("| " + " | ".join(str(c) for c in row) + " |")
    md.append("")
    OUT_MD.write_text("\n".join(md) + "\n")

    print(f"Wrote {OUT_JSON} and {OUT_MD}")
    print("Summary:", json.dumps(out["summary"], indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
