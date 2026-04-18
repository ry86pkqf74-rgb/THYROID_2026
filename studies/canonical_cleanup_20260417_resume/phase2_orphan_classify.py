"""Phase 2 — Tg-lab orphan classification (403 patients).

Read-only against canonical tables. Writes only:
  - studies/canonical_cleanup_20260417_resume/tg_orphan_decisions.md
  - studies/canonical_cleanup_20260417_resume/tg_orphan_decisions.csv
  - studies/canonical_cleanup_20260417_resume/phase2_run.log
  - studies/canonical_cleanup_20260417_resume/phase2_summary.json

Does NOT delete from main.thyroglobulin_lab_canonical_v1, NOT delete from
main.longitudinal_lab_canonical_v1, NOT insert into canonical_patient_master.

Classifier (over the 5 cancer-evidence tables Logan named):
  fna_episode_master_v2, tumor_episode_master_v2, synoptic_tumor_long_v1,
  path_synoptics, imaging_nodule_master_v1.

Rule:
  zero/5            -> likely_non_cancer       -> recommend DELETE
  any of {tumor, syn-long, path-synoptic} TRUE -> likely_dropped_from_CPM
                                                  (strong cancer evidence:
                                                   pathology / synoptic /
                                                   tumor episode)         -> recommend ADMIT
  otherwise (1-2/5 from {fna, imaging} only)   -> ambiguous              -> recommend HOLD

Per Logan's call-out: for every likely_dropped_from_CPM case the report
must include first/last Tg lab date, earliest cancer-evidence date across
the 5 tables, and a lab_first_after_cohort_freeze_flag (true if the first
Tg lab post-dates the maximum cpm_built_at value in CPM).
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "phase2_run.log"
CSV_PATH = HERE / "tg_orphan_decisions.csv"
MD_PATH = HERE / "tg_orphan_decisions.md"
JSON_PATH = HERE / "phase2_summary.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    LOG_PATH.write_text("")
    log("Opening locked MotherDuck connection...")
    con = connect_locked()

    cohort_freeze = con.execute(
        "SELECT MAX(cpm_built_at) FROM main.canonical_patient_master"
    ).fetchone()[0]
    log(f"Cohort freeze (max cpm_built_at): {cohort_freeze}")

    # Pull the 403 orphan rids and existing audit fields. lab_orphan_audit_v1
    # research_id is VARCHAR; cast to BIGINT for joins.
    log("Loading 403 orphan rids from manuscript_workspace.lab_orphan_audit_v1...")
    audit = fetch_dicts(
        con,
        """
        SELECT
          CAST(research_id AS BIGINT)            AS research_id,
          n_lab_rows,
          first_lab,
          last_lab,
          n_analytes,
          has_fna_episode,
          has_tumor_episode,
          has_synoptic_tumor,
          has_path_synoptic,
          has_imaging_nodule,
          classification AS classification_existing
        FROM manuscript_workspace.lab_orphan_audit_v1
        ORDER BY 1
        """,
    )
    log(f"  {len(audit)} rows loaded")
    if len(audit) != 403:
        raise SystemExit(f"Expected 403 rows, got {len(audit)}")

    # Cross-verify against cohort_review_v1 (has has_op + Tg dates duplicated)
    review = {
        r["research_id"]: r
        for r in fetch_dicts(
            con,
            """
            SELECT research_id, has_fna, has_tumor, has_syn, has_path,
                   has_imaging, has_op, n_tg_rows, n_long_lab_rows,
                   first_tg_dt, last_tg_dt
            FROM manuscript_workspace.lab_orphan_cohort_review_v1
            """,
        )
    }

    # Build a temp table of the 403 rids to enable per-table EXISTS lookups
    # in a single-pass query.
    rid_csv = ",".join(str(r["research_id"]) for r in audit)
    log("Computing per-rid earliest evidence date across the 5 tables...")

    # Earliest evidence date per rid per table.
    # Schemas (from probe):
    #   fna_episode_master_v2 : has fna_date (probe shows it had multiple date cols; check)
    # Let's introspect each table's date columns to choose a representative
    # 'earliest' date.
    def first_date_col(schema: str, table: str, candidates: list[str]) -> str | None:
        cols = {
            r[0]
            for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
                "AND table_schema=? AND table_name=?",
                [schema, table],
            ).fetchall()
        }
        for c in candidates:
            if c in cols:
                return c
        return None

    fna_date = first_date_col(
        "main", "fna_episode_master_v2",
        ["fna_date", "fna_date_resolved", "earliest_fna_date", "episode_date"],
    )
    tumor_date = first_date_col(
        "main", "tumor_episode_master_v2",
        ["surgery_date", "earliest_path_date", "tumor_date", "diagnosis_date",
         "first_diagnosis_date"],
    )
    syn_date = first_date_col(
        "main", "synoptic_tumor_long_v1",
        ["surgery_date", "earliest_path_date", "specimen_date",
         "tumor_date"],
    )
    path_date = first_date_col(
        "main", "path_synoptics",
        ["surgery_date", "specimen_date", "earliest_path_date",
         "synoptic_date", "report_date", "diagnosis_date", "path_date"],
    )
    img_date = first_date_col(
        "main", "imaging_nodule_master_v1",
        ["imaging_date", "study_date", "exam_date", "earliest_imaging_date"],
    )

    log(
        f"  date columns resolved: fna={fna_date} tumor={tumor_date} "
        f"syn={syn_date} path={path_date} img={img_date}"
    )

    # Build per-rid earliest dates. If a date column isn't found for a table,
    # we can still detect presence via row existence; earliest_*_dt will be NULL.
    def safe_min(table: str, col: str | None, schema: str = "main") -> str:
        if col is None:
            return "NULL::TIMESTAMP"
        # Cast column to TIMESTAMP for safe MIN comparison
        return f'MIN(CAST("{col}" AS TIMESTAMP))'

    # Per-rid earliest evidence per table
    earliest_per_rid_sql = f"""
    WITH cohort AS (SELECT UNNEST([{rid_csv}]) AS research_id),
    fna AS (
      SELECT research_id, {safe_min('fna_episode_master_v2', fna_date)} AS earliest_fna_dt
      FROM main.fna_episode_master_v2
      WHERE research_id IN ({rid_csv})
      GROUP BY research_id
    ),
    tum AS (
      SELECT research_id, {safe_min('tumor_episode_master_v2', tumor_date)} AS earliest_tumor_dt
      FROM main.tumor_episode_master_v2
      WHERE research_id IN ({rid_csv})
      GROUP BY research_id
    ),
    syn AS (
      SELECT research_id, {safe_min('synoptic_tumor_long_v1', syn_date)} AS earliest_syn_dt
      FROM main.synoptic_tumor_long_v1
      WHERE research_id IN ({rid_csv})
      GROUP BY research_id
    ),
    pth AS (
      SELECT research_id, {safe_min('path_synoptics', path_date)} AS earliest_path_dt
      FROM main.path_synoptics
      WHERE research_id IN ({rid_csv})
      GROUP BY research_id
    ),
    img AS (
      SELECT research_id, {safe_min('imaging_nodule_master_v1', img_date)} AS earliest_img_dt
      FROM main.imaging_nodule_master_v1
      WHERE research_id IN ({rid_csv})
      GROUP BY research_id
    )
    SELECT c.research_id,
           fna.earliest_fna_dt,
           tum.earliest_tumor_dt,
           syn.earliest_syn_dt,
           pth.earliest_path_dt,
           img.earliest_img_dt
    FROM cohort c
    LEFT JOIN fna USING (research_id)
    LEFT JOIN tum USING (research_id)
    LEFT JOIN syn USING (research_id)
    LEFT JOIN pth USING (research_id)
    LEFT JOIN img USING (research_id)
    """
    earliest = {r["research_id"]: r for r in fetch_dicts(con, earliest_per_rid_sql)}
    log(f"  per-rid earliest evidence dates loaded for {len(earliest)} rids")

    # Build classified rows
    classified: list[dict[str, Any]] = []
    counts = {"likely_non_cancer": 0, "likely_dropped_from_CPM": 0, "ambiguous": 0}
    audit_class_divergence = 0

    for r in audit:
        rid = r["research_id"]
        ev_flags = {
            "has_fna_episode": bool(r["has_fna_episode"]),
            "has_tumor_episode": bool(r["has_tumor_episode"]),
            "has_synoptic_tumor": bool(r["has_synoptic_tumor"]),
            "has_path_synoptic": bool(r["has_path_synoptic"]),
            "has_imaging_nodule": bool(r["has_imaging_nodule"]),
        }
        n_evidence = sum(ev_flags.values())
        strong = (
            ev_flags["has_tumor_episode"]
            or ev_flags["has_synoptic_tumor"]
            or ev_flags["has_path_synoptic"]
        )
        if n_evidence == 0:
            cls = "likely_non_cancer"
            recommendation = "DELETE_orphan_lab_rows"
        elif strong:
            cls = "likely_dropped_from_CPM"
            recommendation = "ADMIT_to_CPM_or_refresh_feed"
        else:
            cls = "ambiguous"
            recommendation = "HOLD_for_chart_review"

        # Earliest cancer-evidence date across the five
        evidence_dates = []
        e = earliest.get(rid, {})
        for key in (
            "earliest_fna_dt",
            "earliest_tumor_dt",
            "earliest_syn_dt",
            "earliest_path_dt",
            "earliest_img_dt",
        ):
            v = e.get(key)
            if v is not None:
                evidence_dates.append(v)
        earliest_evidence_dt = min(evidence_dates) if evidence_dates else None

        first_lab = r["first_lab"]
        last_lab = r["last_lab"]
        if first_lab is not None and cohort_freeze is not None:
            # Both are TIMESTAMP-ish; ensure tz-naive comparison
            try:
                fl = first_lab if not hasattr(first_lab, "tzinfo") or first_lab.tzinfo is None else first_lab.replace(tzinfo=None)
                cf = cohort_freeze if not hasattr(cohort_freeze, "tzinfo") or cohort_freeze.tzinfo is None else cohort_freeze.replace(tzinfo=None)
                lab_first_after_cohort_freeze_flag = bool(fl > cf)
            except Exception:
                lab_first_after_cohort_freeze_flag = None
        else:
            lab_first_after_cohort_freeze_flag = None

        rev = review.get(rid, {})

        if r["classification_existing"] != cls:
            audit_class_divergence += 1

        classified.append(
            {
                "research_id": rid,
                "classification": cls,
                "recommendation": recommendation,
                "n_lab_rows": r["n_lab_rows"],
                "n_analytes": r["n_analytes"],
                "first_tg_lab_dt": first_lab,
                "last_tg_lab_dt": last_lab,
                "has_fna_episode": ev_flags["has_fna_episode"],
                "has_tumor_episode": ev_flags["has_tumor_episode"],
                "has_synoptic_tumor": ev_flags["has_synoptic_tumor"],
                "has_path_synoptic": ev_flags["has_path_synoptic"],
                "has_imaging_nodule": ev_flags["has_imaging_nodule"],
                "n_evidence_tables": n_evidence,
                "has_op_episode_supplemental": rev.get("has_op"),
                "earliest_fna_dt": e.get("earliest_fna_dt"),
                "earliest_tumor_dt": e.get("earliest_tumor_dt"),
                "earliest_syn_dt": e.get("earliest_syn_dt"),
                "earliest_path_dt": e.get("earliest_path_dt"),
                "earliest_img_dt": e.get("earliest_img_dt"),
                "earliest_cancer_evidence_dt": earliest_evidence_dt,
                "cohort_freeze_dt": cohort_freeze,
                "lab_first_after_cohort_freeze_flag": (
                    lab_first_after_cohort_freeze_flag
                ),
                "classification_existing_in_audit": r["classification_existing"],
            }
        )
        counts[cls] += 1

    log(f"  3-way counts: {counts}")
    log(
        f"  classification divergence vs lab_orphan_audit_v1.classification: "
        f"{audit_class_divergence} / {len(classified)}"
    )

    # Write CSV
    fieldnames = list(classified[0].keys())
    with CSV_PATH.open("w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for row in classified:
            w.writerow({k: ("" if v is None else v) for k, v in row.items()})
    log(f"  CSV -> {CSV_PATH} ({len(classified)} rows)")

    # 10 samples per class, sorted by rid
    samples = {c: [] for c in counts}
    for row in sorted(classified, key=lambda r: r["research_id"]):
        c = row["classification"]
        if len(samples[c]) < 10:
            samples[c].append(row)

    # Build markdown
    md = ["# Phase 2 — Tg-lab orphan classification (403 patients)", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_  ")
    md.append(
        f"_Cohort freeze (max `cpm_built_at`): **{cohort_freeze}**_  "
    )
    md.append(
        "_Read-only: no rows deleted from `thyroglobulin_lab_canonical_v1` "
        "or `longitudinal_lab_canonical_v1`; no rows inserted into "
        "`canonical_patient_master`._"
    )
    md.append("")
    md.append("## 3-way counts")
    md.append("")
    md.append("| classification | recommendation | n |")
    md.append("|:---|:---|---:|")
    md.append(
        f"| `likely_non_cancer`        | DELETE orphan lab rows               | "
        f"{counts['likely_non_cancer']} |"
    )
    md.append(
        f"| `likely_dropped_from_CPM`  | ADMIT to CPM **OR** refresh lab feed  | "
        f"{counts['likely_dropped_from_CPM']} |"
    )
    md.append(
        f"| `ambiguous`                | HOLD for chart review                | "
        f"{counts['ambiguous']} |"
    )
    md.append(
        f"| **TOTAL**                  |                                       | "
        f"{sum(counts.values())} |"
    )
    md.append("")
    md.append(
        f"_Divergence vs the pre-existing "
        f"`lab_orphan_audit_v1.classification` column: "
        f"**{audit_class_divergence} / {len(classified)}**._"
    )
    md.append("")
    md.append("## Classifier rules")
    md.append("")
    md.append(
        "1. `n_evidence == 0` → `likely_non_cancer` → recommend **DELETE**.\n"
        "2. Any of `has_tumor_episode`, `has_synoptic_tumor`, "
        "`has_path_synoptic` is TRUE → `likely_dropped_from_CPM` → "
        "recommend **ADMIT** (or refresh feed if "
        "`lab_first_after_cohort_freeze_flag = TRUE`).\n"
        "3. Otherwise (only FNA and/or imaging evidence) → `ambiguous` → "
        "recommend **HOLD**."
    )
    md.append("")
    md.append("## Temporal-context call-out for `likely_dropped_from_CPM`")
    md.append("")
    md.append(
        "For each ADMIT-candidate the table below shows first/last Tg lab "
        "date, the earliest cancer-evidence date across the 5 tables, and "
        "`lab_first_after_cohort_freeze_flag`. When the flag is TRUE the "
        "remediation is **REFRESH the lab feed**, not admit a stale patient "
        "to the cohort."
    )
    md.append("")
    md.append(
        "| rid | n_lab | first_tg | last_tg | earliest_cancer_evidence | "
        "evidence_tables | lab_first_after_cohort_freeze | recommendation |"
    )
    md.append("|---:|---:|:---|:---|:---|:---|:---:|:---|")
    dropped = [
        r for r in classified if r["classification"] == "likely_dropped_from_CPM"
    ]
    for row in sorted(dropped, key=lambda r: r["research_id"]):
        evidence_tables = []
        for k, label in [
            ("has_fna_episode", "fna"),
            ("has_tumor_episode", "tumor"),
            ("has_synoptic_tumor", "syn"),
            ("has_path_synoptic", "path"),
            ("has_imaging_nodule", "img"),
        ]:
            if row[k]:
                evidence_tables.append(label)
        flag = row["lab_first_after_cohort_freeze_flag"]
        flag_disp = "TRUE" if flag else ("FALSE" if flag is False else "?")
        rec = (
            "REFRESH lab feed (post-freeze)"
            if flag
            else "ADMIT to CPM (review)"
        )
        md.append(
            f"| {row['research_id']} | {row['n_lab_rows']} | "
            f"{row['first_tg_lab_dt']} | {row['last_tg_lab_dt']} | "
            f"{row['earliest_cancer_evidence_dt']} | "
            f"{','.join(evidence_tables)} | {flag_disp} | {rec} |"
        )
    md.append("")

    md.append("## 10 sample rids per class")
    md.append("")
    for cls in ("likely_non_cancer", "likely_dropped_from_CPM", "ambiguous"):
        md.append(f"### {cls} ({counts[cls]} total — first 10 by rid)")
        md.append("")
        md.append(
            "| rid | n_lab | first_tg | last_tg | "
            "fna | tum | syn | path | img | "
            "earliest_cancer_evidence | lab_first_after_cohort_freeze |"
        )
        md.append("|---:|---:|:---|:---|:---:|:---:|:---:|:---:|:---:|:---|:---:|")
        for row in samples[cls]:
            flag = row["lab_first_after_cohort_freeze_flag"]
            flag_disp = "T" if flag else ("F" if flag is False else "?")
            md.append(
                f"| {row['research_id']} | {row['n_lab_rows']} | "
                f"{row['first_tg_lab_dt']} | {row['last_tg_lab_dt']} | "
                f"{'T' if row['has_fna_episode'] else '.'} | "
                f"{'T' if row['has_tumor_episode'] else '.'} | "
                f"{'T' if row['has_synoptic_tumor'] else '.'} | "
                f"{'T' if row['has_path_synoptic'] else '.'} | "
                f"{'T' if row['has_imaging_nodule'] else '.'} | "
                f"{row['earliest_cancer_evidence_dt']} | {flag_disp} |"
            )
        md.append("")

    md.append("## What Logan needs to decide before Phase 3")
    md.append("")
    md.append(
        f"1. Approve the **{counts['likely_non_cancer']}** `likely_non_cancer` "
        "rids for DELETE from `main.thyroglobulin_lab_canonical_v1` and "
        "`main.longitudinal_lab_canonical_v1`?"
    )
    md.append(
        f"2. Triage the **{counts['likely_dropped_from_CPM']}** "
        "`likely_dropped_from_CPM` rids: how many were post-cohort-freeze "
        "feed drift (refresh) vs true admit? See temporal-context table "
        "above."
    )
    md.append(
        f"3. The **{counts['ambiguous']}** `ambiguous` rids stay HELD pending "
        "chart review."
    )
    md.append("")
    md.append(
        "_Full per-patient table_: "
        "[`tg_orphan_decisions.csv`](./tg_orphan_decisions.csv)"
    )

    MD_PATH.write_text("\n".join(md) + "\n")
    log(f"  Markdown -> {MD_PATH}")

    summary = {
        "n_total": len(classified),
        "counts": counts,
        "audit_class_divergence_count": audit_class_divergence,
        "cohort_freeze_dt": str(cohort_freeze),
        "n_lab_first_after_cohort_freeze": sum(
            1 for r in classified if r["lab_first_after_cohort_freeze_flag"]
        ),
        "csv_path": str(CSV_PATH),
        "md_path": str(MD_PATH),
    }
    JSON_PATH.write_text(json.dumps(summary, indent=2, default=str))
    log(f"  Summary -> {JSON_PATH}: {summary}")

    log("Phase 2 complete (classification only; no DML against main.* lab tables).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
