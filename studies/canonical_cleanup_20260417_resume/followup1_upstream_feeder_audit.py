"""Follow-up 1 — Upstream feeder audit for the surgery-1-only feeder pattern.

Hypothesis: the same feeder set that under-reports tumor_size_cm_max for
80 multi-surgery patients also drives other patient-level worst-case
columns (ete_grade_final_v2, lvi_ordinal_worst, margin_involved_any,
multifocal_flag_path, n_tumors_path, histology_final). If those CPM
columns also reflect surgery 1 only when surgery 2 had the worse value,
the correction queue is much larger than 80 patients and spans multiple
columns.

For each of the 80 F-bucket rids:
  1. Pull every TEM row (all surgeries).
  2. For each interesting CPM column, derive an ordinal worst across
     all surgeries from TEM, vs surgery-1-only.
  3. Compare to CPM value.
  4. Classify per rid x column: AGREES_WORST / UNDER_REPORTS_LIKE_TUMOR_SIZE /
     N/A / AMBIGUOUS.

Strictly READ-ONLY. Writes only studies/canonical_cleanup_20260417_resume/
followup1_*.{csv,md,json,log}.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "followup1_upstream_feeder_audit.log"
CSV_PATH = HERE / "followup1_upstream_feeder_audit.csv"
MD_PATH = HERE / "followup1_upstream_feeder_audit.md"
JSON_PATH = HERE / "followup1_upstream_feeder_audit.json"

# Ordinal maps for VARCHAR worst-case derivation
ETE_ORDINAL = {
    None: -1,
    "": -1,
    "unknown": -1,
    "no": 0, "none": 0, "absent": 0, "negative": 0,
    "no, not identified": 0,
    "no, microscopic only": 1,
    "yes": 3, "present": 3,
    "yes, focal": 1,
    "yes, microscopic": 1, "microscopic": 1,
    "minimal/microscopic": 1, "minimal": 1,
    "yes, extensive": 4, "extensive": 4, "gross": 4,
    "yes, focal, microscopic": 1,
    "minimal (microscopic)": 1,
    "moderate": 2,
}

# yes/no positivity for VI/LI/margin
POS_VALUES = {"yes", "y", "present", "positive", "true", "1"}
NEG_VALUES = {"no", "n", "absent", "negative", "false", "0",
              "not identified", "no, not identified"}


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def ete_score(v) -> int:
    if v is None:
        return -1
    s = str(v).strip().lower()
    return ETE_ORDINAL.get(s, -1)


def yn(v) -> str:
    if v is None:
        return "unknown"
    s = str(v).strip().lower()
    if s in POS_VALUES:
        return "positive"
    if s in NEG_VALUES:
        return "negative"
    if "positive" in s:
        return "positive"
    if "negative" in s:
        return "negative"
    if "yes" in s:
        return "positive"
    if "no" in s and "not" not in s:
        return "negative"
    return "unknown"


def main() -> int:
    LOG.write_text("")
    con = connect_locked()
    log("Follow-up 1 — upstream feeder audit (read-only).")

    # 80 F-bucket rids from the correction queue
    rid_rows = con.execute(
        "SELECT research_id FROM "
        "manuscript_workspace.path_tumor_size_correction_queue_v1 "
        "ORDER BY CAST(research_id AS BIGINT)"
    ).fetchall()
    rids = [r[0] for r in rid_rows]
    log(f"  loaded {len(rids)} F-bucket rids")
    if len(rids) != 80:
        raise SystemExit(f"Expected 80 rids; got {len(rids)}")

    rid_int_csv = ",".join(str(int(r)) for r in rids)
    rid_str_csv = ",".join(f"'{r}'" for r in rids)

    # CPM patient-level worst-case columns
    log("  loading CPM worst-case columns for the 80 rids...")
    cpm = fetch_dicts(
        con,
        f"""
        SELECT
          research_id,
          ete_grade_final_v2,
          lvi_ordinal_worst,
          margin_involved_any,
          multifocal_flag_path,
          n_tumors_path,
          histology_final
        FROM main.canonical_patient_master
        WHERE research_id IN ({rid_str_csv})
        """,
    )
    cpm_by_rid = {str(c["research_id"]): c for c in cpm}

    # TEM rows (all surgeries)
    log("  loading TEM rows (all surgeries)...")
    tem_rows = fetch_dicts(
        con,
        f"""
        SELECT
          research_id, surgery_episode_id, surgery_date, tumor_ordinal,
          extrathyroidal_extension, gross_ete,
          vascular_invasion, lymphatic_invasion,
          margin_status, multifocality_flag, number_of_tumors,
          primary_histology, tumor_size_cm
        FROM main.tumor_episode_master_v2
        WHERE research_id IN ({rid_int_csv})
        ORDER BY research_id, surgery_date, surgery_episode_id, tumor_ordinal
        """,
    )
    tem_by_rid: dict[str, list[dict]] = defaultdict(list)
    for r in tem_rows:
        tem_by_rid[str(r["research_id"])].append(r)
    log(f"  TEM rows total: {len(tem_rows)}")

    out_rows: list[dict] = []
    summary = {
        "ete": defaultdict(int),
        "lvi": defaultdict(int),
        "margin": defaultdict(int),
        "multifocal": defaultdict(int),
        "n_tumors": defaultdict(int),
    }
    rid_with_any_under_report = set()

    for rid in rids:
        cpm_row = cpm_by_rid.get(rid) or {}
        tem = tem_by_rid.get(rid) or []
        if not tem:
            out_rows.append(
                {
                    "research_id": rid,
                    "audit_status": "TEM_EMPTY",
                    "n_tem_rows": 0,
                    "n_distinct_surgeries": 0,
                }
            )
            continue

        # Identify earliest vs later surgeries
        # Use surgery_date as the ordering key; fall back to surgery_episode_id
        sorted_surg = sorted(
            {(r["surgery_date"], r["surgery_episode_id"]) for r in tem},
            key=lambda x: (x[0] is None, x[0]),
        )
        if not sorted_surg:
            continue
        earliest_key = sorted_surg[0]
        s1_rows = [
            r for r in tem
            if (r["surgery_date"], r["surgery_episode_id"]) == earliest_key
        ]
        all_rows = tem
        n_surg = len({r["surgery_episode_id"] for r in tem})

        # ---- ETE ----
        ete_s1_max_score = max((ete_score(r["extrathyroidal_extension"]) for r in s1_rows), default=-1)
        ete_all_max_score = max((ete_score(r["extrathyroidal_extension"]) for r in all_rows), default=-1)
        ete_s1_worst_text = next(
            (r["extrathyroidal_extension"] for r in s1_rows
             if ete_score(r["extrathyroidal_extension"]) == ete_s1_max_score
             and ete_s1_max_score >= 0), None,
        )
        ete_all_worst_text = next(
            (r["extrathyroidal_extension"] for r in all_rows
             if ete_score(r["extrathyroidal_extension"]) == ete_all_max_score
             and ete_all_max_score >= 0), None,
        )
        cpm_ete = cpm_row.get("ete_grade_final_v2")
        cpm_ete_score = ete_score(cpm_ete)

        if ete_all_max_score < 0:
            ete_status = "N/A_TEM_NO_ETE_DATA"
        elif ete_all_max_score > ete_s1_max_score and cpm_ete_score < ete_all_max_score:
            ete_status = "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        elif cpm_ete_score >= ete_all_max_score:
            ete_status = "AGREES_WORST"
        elif cpm_ete_score == ete_s1_max_score and ete_s1_max_score == ete_all_max_score:
            ete_status = "AGREES_WORST"
        else:
            ete_status = "AMBIGUOUS"
        summary["ete"][ete_status] += 1

        # ---- LVI (combine vascular + lymphatic) ----
        def lvi_present(r):
            return (
                yn(r.get("vascular_invasion")) == "positive"
                or yn(r.get("lymphatic_invasion")) == "positive"
            )

        lvi_s1 = any(lvi_present(r) for r in s1_rows)
        lvi_all = any(lvi_present(r) for r in all_rows)
        cpm_lvi = cpm_row.get("lvi_ordinal_worst")
        cpm_lvi_pos = (cpm_lvi is not None and cpm_lvi != 0)

        any_lvi_data = any(
            (r.get("vascular_invasion") is not None
             or r.get("lymphatic_invasion") is not None)
            for r in all_rows
        )
        if not any_lvi_data:
            lvi_status = "N/A_TEM_NO_LVI_DATA"
        elif lvi_all and not lvi_s1 and not cpm_lvi_pos:
            lvi_status = "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        elif lvi_all == cpm_lvi_pos:
            lvi_status = "AGREES_WORST"
        else:
            lvi_status = "AMBIGUOUS"
        summary["lvi"][lvi_status] += 1

        # ---- Margin ----
        def margin_pos(r):
            return yn(r.get("margin_status")) == "positive"
        margin_s1 = any(margin_pos(r) for r in s1_rows)
        margin_all = any(margin_pos(r) for r in all_rows)
        any_margin = any(r.get("margin_status") is not None for r in all_rows)
        cpm_margin = cpm_row.get("margin_involved_any")
        cpm_margin_pos = bool(cpm_margin) if cpm_margin is not None else None

        if not any_margin:
            margin_status_audit = "N/A_TEM_NO_MARGIN_DATA"
        elif margin_all and not margin_s1 and not cpm_margin_pos:
            margin_status_audit = "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        elif margin_all == bool(cpm_margin_pos):
            margin_status_audit = "AGREES_WORST"
        else:
            margin_status_audit = "AMBIGUOUS"
        summary["margin"][margin_status_audit] += 1

        # ---- Multifocal ----
        def mf_true(r):
            mf = r.get("multifocality_flag")
            if mf is True:
                return True
            if mf is None:
                # Use number_of_tumors > 1 as fallback
                n = r.get("number_of_tumors")
                return n is not None and n > 1
            return bool(mf)

        mf_s1 = any(mf_true(r) for r in s1_rows)
        mf_all = any(mf_true(r) for r in all_rows)
        any_mf = any(
            r.get("multifocality_flag") is not None
            or r.get("number_of_tumors") is not None
            for r in all_rows
        )
        cpm_mf = cpm_row.get("multifocal_flag_path")
        if isinstance(cpm_mf, str):
            cpm_mf_bool = cpm_mf.lower() in ("true", "yes", "1")
        else:
            cpm_mf_bool = bool(cpm_mf) if cpm_mf is not None else None

        if not any_mf:
            mf_status = "N/A_TEM_NO_MF_DATA"
        elif mf_all and not mf_s1 and not cpm_mf_bool:
            mf_status = "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        elif mf_all == bool(cpm_mf_bool):
            mf_status = "AGREES_WORST"
        else:
            mf_status = "AMBIGUOUS"
        summary["multifocal"][mf_status] += 1

        # ---- n_tumors ----
        n_s1 = sum(
            (r.get("number_of_tumors") or 0) for r in s1_rows
            if r.get("number_of_tumors") is not None
        )
        n_all = sum(
            (r.get("number_of_tumors") or 0) for r in all_rows
            if r.get("number_of_tumors") is not None
        )
        cpm_n = cpm_row.get("n_tumors_path")
        any_n = any(r.get("number_of_tumors") is not None for r in all_rows)
        if not any_n:
            n_status = "N/A_TEM_NO_N_TUMORS"
        elif n_all > n_s1 and (cpm_n is None or cpm_n < n_all):
            n_status = "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        elif cpm_n is not None and cpm_n >= n_all:
            n_status = "AGREES_WORST"
        else:
            n_status = "AMBIGUOUS"
        summary["n_tumors"][n_status] += 1

        # Track rids with any under-report flag
        if (
            ete_status == "UNDER_REPORTS_LIKE_TUMOR_SIZE"
            or lvi_status == "UNDER_REPORTS_LIKE_TUMOR_SIZE"
            or margin_status_audit == "UNDER_REPORTS_LIKE_TUMOR_SIZE"
            or mf_status == "UNDER_REPORTS_LIKE_TUMOR_SIZE"
            or n_status == "UNDER_REPORTS_LIKE_TUMOR_SIZE"
        ):
            rid_with_any_under_report.add(rid)

        out_rows.append(
            {
                "research_id": rid,
                "audit_status": "AUDITED",
                "n_tem_rows": len(all_rows),
                "n_distinct_surgeries": n_surg,
                # ETE
                "ete_s1_worst_text": ete_s1_worst_text,
                "ete_all_worst_text": ete_all_worst_text,
                "cpm_ete_grade_final_v2": cpm_ete,
                "ete_audit": ete_status,
                # LVI
                "tem_lvi_s1_present": lvi_s1,
                "tem_lvi_all_present": lvi_all,
                "cpm_lvi_ordinal_worst": cpm_lvi,
                "lvi_audit": lvi_status,
                # Margin
                "tem_margin_s1_pos": margin_s1,
                "tem_margin_all_pos": margin_all,
                "cpm_margin_involved_any": cpm_margin,
                "margin_audit": margin_status_audit,
                # Multifocal
                "tem_multifocal_s1": mf_s1,
                "tem_multifocal_all": mf_all,
                "cpm_multifocal_flag_path": cpm_mf,
                "multifocal_audit": mf_status,
                # n_tumors
                "tem_n_tumors_s1_sum": n_s1,
                "tem_n_tumors_all_sum": n_all,
                "cpm_n_tumors_path": cpm_n,
                "n_tumors_audit": n_status,
            }
        )

    # Convert defaultdicts -> dicts for JSON
    summary_clean = {k: dict(v) for k, v in summary.items()}
    log(f"  summary by column: {summary_clean}")
    log(
        f"  rids with at least one UNDER_REPORTS_LIKE_TUMOR_SIZE flag "
        f"on a non-tumor-size column: "
        f"{len(rid_with_any_under_report)} / {len(rids)}"
    )

    # CSV
    fieldnames = list(out_rows[0].keys())
    with CSV_PATH.open("w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)
    log(f"  CSV -> {CSV_PATH}")

    # MD
    md = ["# Follow-up 1 — Upstream feeder audit (80 F-bucket rids)", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_  ")
    md.append(
        "_Strictly read-only on `canonical_patient_master`. Audits whether "
        "the surgery-1-only feeder issue that breaks `tumor_size_cm_max` "
        "also affects ETE / LVI / margin / multifocal / n_tumors._  "
    )
    md.append("")
    md.append("## Summary by column")
    md.append("")
    md.append(
        "| column | AGREES_WORST | UNDER_REPORTS_LIKE_TUMOR_SIZE | AMBIGUOUS | N/A_NO_TEM_DATA |"
    )
    md.append("|:---|---:|---:|---:|---:|")
    for col_label, col_summary in [
        ("ete_grade_final_v2", summary_clean["ete"]),
        ("lvi_ordinal_worst", summary_clean["lvi"]),
        ("margin_involved_any", summary_clean["margin"]),
        ("multifocal_flag_path", summary_clean["multifocal"]),
        ("n_tumors_path", summary_clean["n_tumors"]),
    ]:
        agrees = col_summary.get("AGREES_WORST", 0)
        under = col_summary.get("UNDER_REPORTS_LIKE_TUMOR_SIZE", 0)
        amb = col_summary.get("AMBIGUOUS", 0)
        na = sum(v for k, v in col_summary.items() if k.startswith("N/A_"))
        md.append(f"| `{col_label}` | {agrees} | **{under}** | {amb} | {na} |")
    md.append("")
    md.append(
        f"**Rids with ≥1 `UNDER_REPORTS_LIKE_TUMOR_SIZE` flag** on a non-"
        f"tumor-size column: **{len(rid_with_any_under_report)} / {len(rids)}**."
    )
    md.append("")
    md.append("## Interpretation")
    md.append("")
    if any(
        summary_clean[k].get("UNDER_REPORTS_LIKE_TUMOR_SIZE", 0) > 0
        for k in summary_clean
    ):
        md.append(
            "The same surgery-1-only feeder pattern affects multiple "
            "patient-level worst-case columns. The `tumor_size_cm_max` "
            "correction queue is therefore the visible tip of a broader "
            "pattern. Recommend a multi-column upstream feeder fix before "
            "row-by-row sign-off on the existing 80-rid queue."
        )
    else:
        md.append(
            "No additional under-reporting detected on the audited columns "
            "for the 80 F-bucket rids. The `tumor_size_cm_max` issue may be "
            "isolated to that column's specific feeder logic. The 80-rid "
            "correction queue can proceed to row-by-row sign-off without "
            "expanding scope."
        )
    md.append("")
    md.append("## Per-rid sample (first 15)")
    md.append("")
    md.append(
        "| rid | n_surg | ETE | LVI | margin | multifocal | n_tumors |"
    )
    md.append("|---:|---:|:---|:---|:---|:---|:---|")
    for r in out_rows[:15]:
        if r["audit_status"] == "TEM_EMPTY":
            md.append(
                f"| {r['research_id']} | 0 | (no TEM rows) | | | | |"
            )
            continue

        def short(s):
            t = {
                "AGREES_WORST": "OK",
                "UNDER_REPORTS_LIKE_TUMOR_SIZE": "**UNDER**",
                "AMBIGUOUS": "?",
            }
            return t.get(s, s.replace("N/A_", ""))

        md.append(
            f"| {r['research_id']} | {r['n_distinct_surgeries']} | "
            f"{short(r['ete_audit'])} | {short(r['lvi_audit'])} | "
            f"{short(r['margin_audit'])} | "
            f"{short(r['multifocal_audit'])} | {short(r['n_tumors_audit'])} |"
        )
    md.append("")
    md.append("_Full per-rid table_: [`followup1_upstream_feeder_audit.csv`](./followup1_upstream_feeder_audit.csv)")
    MD_PATH.write_text("\n".join(md) + "\n")
    log(f"  MD -> {MD_PATH}")

    JSON_PATH.write_text(
        json.dumps(
            {
                "n_rids_audited": len(rids),
                "summary_by_column": summary_clean,
                "rids_with_any_under_report": sorted(
                    rid_with_any_under_report, key=lambda x: int(x)
                ),
                "n_rids_with_any_under_report": len(rid_with_any_under_report),
                "per_rid": out_rows,
            },
            indent=2,
            default=str,
        )
    )
    log(f"  JSON -> {JSON_PATH}")

    # CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(f"CPM invariant regressed: {n_rows}/{n_distinct}")
    log(f"  CPM invariant: {n_rows}/{n_distinct} OK")
    log("Follow-up 1 complete (read-only).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
