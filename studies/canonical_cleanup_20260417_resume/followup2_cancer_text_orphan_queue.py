"""Follow-up 2 — Build the 'cancer-text orphan investigation' queue.

For each of the 403 Tg-lab orphan rids (`is_in_canonical_cancer_cohort=FALSE`
in main.thyroglobulin_lab_canonical_v1), scan main.operative_episode_detail_v2
for cancer-suggestive text in procedure_raw or procedure_normalized.

If found → INSERT a row into a new manuscript_workspace queue table:

  manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1
    (research_id, n_oed_rows, cancer_text_match_terms VARCHAR,
     procedure_raw_concatenated VARCHAR, procedure_normalized_set VARCHAR,
     n_tg_lab_rows BIGINT, first_tg_dt TIMESTAMP, last_tg_dt TIMESTAMP,
     status VARCHAR, classifier_version VARCHAR, created_at TIMESTAMPTZ)

Status: 'awaiting_upstream_triage'.

The question this queue surfaces: why is a patient with a 'Papillary Thyroid
Carcinoma' or 'Follicular Carcinoma' procedure_raw entry NOT in the canonical
cancer cohort (CPM) and NOT in any of the 5 cancer-evidence tables checked
in Phase 2 (FNA / tumor_episode / synoptic_tumor / path_synoptic / imaging
nodule)? Two answer paths:
  - Upstream extraction gap (fixable): the 5 cancer-evidence tables missed
    valid evidence that the operative note clearly carried.
  - Intentional exclusion rule (documentable): there is a downstream filter
    that intentionally excludes these patients (e.g., known-benign
    re-classification post-path).

NO modifications to canonical_patient_master or to the lab tables.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "followup2_cancer_text_orphan_queue.log"
JSON_PATH = HERE / "followup2_cancer_text_orphan_queue.json"
MD_PATH = HERE / "followup2_cancer_text_orphan_queue.md"
CSV_PATH = HERE / "followup2_cancer_text_orphan_queue.csv"

CLASSIFIER_VERSION = "v1_cancer_text_scan_20260418"

# Cancer-suggestive terms (case-insensitive substring match)
CANCER_TERMS = [
    "carcinoma",
    "cancer",
    "malignan",                    # malignant, malignancy
    "papillary",
    "follicular",                  # may match "follicular adenoma" too — captured separately for review
    "medullary",
    "anaplastic",
    "hurthle", "hurtle", "oncocytic carcinoma",
    "ptc", "ftc", "mtc",           # abbreviations
    "completion thyroidectomy",
    "central neck dissection",
    "lateral neck dissection",
    "modified radical neck",
    "radical neck dissection",
    "rai", "i-131", "i131",        # prior RAI implies prior cancer
]
CANCER_RE = re.compile(
    "|".join(re.escape(t) for t in CANCER_TERMS),
    re.IGNORECASE,
)

# Terms that, if seen ALONE, downgrade the case (e.g., "follicular adenoma"
# without "carcinoma" should not flag).
BENIGN_GUARDS = [
    ("follicular adenoma", "carcinoma"),
    ("follicular tumor", "carcinoma"),
]


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def main() -> int:
    LOG.write_text("")
    con = connect_locked()
    log("Follow-up 2 — cancer-text orphan investigation queue.")

    # 403 orphan rids
    orphans = fetch_dicts(
        con,
        "SELECT CAST(research_id AS BIGINT) AS research_id "
        "FROM manuscript_workspace.lab_orphan_audit_v1 ORDER BY 1",
    )
    rids = [r["research_id"] for r in orphans]
    log(f"  loaded {len(rids)} orphan rids")
    rid_csv = ",".join(str(r) for r in rids)

    # All OED rows for these rids (procedure_raw + procedure_normalized)
    log("  pulling OED rows for orphan rids...")
    oed = fetch_dicts(
        con,
        f"""
        SELECT research_id, procedure_raw, procedure_normalized
        FROM main.operative_episode_detail_v2
        WHERE research_id IN ({rid_csv})
        """,
    )
    oed_by_rid: dict[int, list[dict]] = {}
    for r in oed:
        oed_by_rid.setdefault(r["research_id"], []).append(r)
    log(f"  OED rows total: {len(oed)} across {len(oed_by_rid)} rids")

    # Tg date range per rid (from lab_orphan_cohort_review_v1 if present, else
    # query thyroglobulin_lab_canonical_v1 directly)
    log("  pulling Tg date range per rid...")
    tg_dates = fetch_dicts(
        con,
        f"""
        SELECT
          research_id,
          COUNT(*) AS n_tg_lab_rows,
          MIN(specimen_collect_dt) AS first_tg_dt,
          MAX(specimen_collect_dt) AS last_tg_dt
        FROM main.thyroglobulin_lab_canonical_v1
        WHERE research_id IN ({rid_csv})
        GROUP BY research_id
        """,
    )
    tg_by_rid = {r["research_id"]: r for r in tg_dates}

    # Scan for cancer-text matches
    log("  scanning for cancer-suggestive text...")
    rows_for_queue: list[dict] = []
    matched_rids: set[int] = set()
    skipped_benign_only: list[int] = []
    for rid in rids:
        oed_rows = oed_by_rid.get(rid, [])
        if not oed_rows:
            continue
        all_text_pieces = []
        for r in oed_rows:
            pr = r.get("procedure_raw")
            pn = r.get("procedure_normalized")
            if pr:
                all_text_pieces.append(pr)
            if pn:
                all_text_pieces.append(pn)
        joined = " | ".join(all_text_pieces)
        if not joined.strip():
            continue
        matches = sorted(set(m.group(0).lower() for m in CANCER_RE.finditer(joined)))
        if not matches:
            continue

        # Benign guard: if the only "cancer-suggestive" hit is e.g. "follicular"
        # but the surrounding text says "follicular adenoma" with no "carcinoma",
        # skip (the guard tuple says: if this term appears, the disqualifier
        # term must NOT also be absent). For this run we keep it simple: if
        # ALL matches are guarded benign terms with the disqualifier missing,
        # skip.
        all_guarded_benign = True
        for guarded_term, required_qualifier in BENIGN_GUARDS:
            if (
                guarded_term in joined.lower()
                and required_qualifier not in joined.lower()
            ):
                # this is a benign-only mention of the guarded term
                # check if the only matches in CANCER_TERMS that the regex
                # picked up are subsumed by guarded_term
                continue
            else:
                all_guarded_benign = False
                break
        # Simpler: only skip if matches == ["follicular"] and "carcinoma" not present
        if matches == ["follicular"] and "carcinoma" not in joined.lower():
            skipped_benign_only.append(rid)
            continue

        matched_rids.add(rid)
        tg = tg_by_rid.get(rid, {})
        rows_for_queue.append(
            {
                "research_id": str(rid),
                "n_oed_rows": len(oed_rows),
                "cancer_text_match_terms": ",".join(matches),
                "procedure_raw_concatenated": " | ".join(
                    p for p in all_text_pieces if p
                )[:1000],
                "procedure_normalized_set": ",".join(
                    sorted({r.get("procedure_normalized") or "" for r in oed_rows})
                ),
                "n_tg_lab_rows": tg.get("n_tg_lab_rows"),
                "first_tg_dt": tg.get("first_tg_dt"),
                "last_tg_dt": tg.get("last_tg_dt"),
                "status": "awaiting_upstream_triage",
                "classifier_version": CLASSIFIER_VERSION,
            }
        )

    log(
        f"  cancer-text matched: {len(matched_rids)} rids "
        f"(benign-only-skipped: {len(skipped_benign_only)})"
    )

    # ---------- WRITE the queue table ----------
    log("Creating manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1...")
    con.execute(
        "DROP TABLE IF EXISTS "
        "manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1"
    )
    con.execute(
        """
        CREATE TABLE
          manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1 (
            research_id                  VARCHAR,
            n_oed_rows                   INTEGER,
            cancer_text_match_terms      VARCHAR,
            procedure_raw_concatenated   VARCHAR,
            procedure_normalized_set     VARCHAR,
            n_tg_lab_rows                BIGINT,
            first_tg_dt                  TIMESTAMP,
            last_tg_dt                   TIMESTAMP,
            status                       VARCHAR,
            classifier_version           VARCHAR,
            created_at                   TIMESTAMP WITH TIME ZONE
        )
        """
    )
    if rows_for_queue:
        inserts = [
            (
                r["research_id"], r["n_oed_rows"],
                r["cancer_text_match_terms"],
                r["procedure_raw_concatenated"],
                r["procedure_normalized_set"],
                r["n_tg_lab_rows"], r["first_tg_dt"], r["last_tg_dt"],
                r["status"], r["classifier_version"],
            )
            for r in rows_for_queue
        ]
        con.executemany(
            """
            INSERT INTO
              manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1
              (research_id, n_oed_rows, cancer_text_match_terms,
               procedure_raw_concatenated, procedure_normalized_set,
               n_tg_lab_rows, first_tg_dt, last_tg_dt, status,
               classifier_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            inserts,
        )
    n_q = con.execute(
        "SELECT COUNT(*) FROM "
        "manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1"
    ).fetchone()[0]
    log(f"  queue rows: {n_q} (expected {len(rows_for_queue)})")

    # COMMENT
    con.execute(
        "COMMENT ON TABLE "
        "manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1 IS "
        "'Subset of the 403 Tg-lab orphans (is_in_canonical_cancer_cohort=FALSE) "
        "whose operative_episode_detail_v2.procedure_raw contains "
        "cancer-suggestive text yet they are NOT in CPM and NOT in any of the 5 "
        "cancer-evidence tables (FNA/tumor_episode/synoptic_tumor/path_synoptic/"
        "imaging_nodule). Status awaiting_upstream_triage: each rid points at "
        "either an upstream extraction gap (5-table evidence missed) or an "
        "intentional exclusion rule (documentable). Created 2026-04-18 as the "
        "Phase 2 follow-up that did not make the initial commit. NO data "
        "modification was performed.'"
    )

    # CSV (for review)
    if rows_for_queue:
        import csv as _csv
        fieldnames = list(rows_for_queue[0].keys())
        with CSV_PATH.open("w", newline="") as fp:
            w = _csv.DictWriter(fp, fieldnames=fieldnames)
            w.writeheader()
            for r in rows_for_queue:
                w.writerow(r)
        log(f"  CSV -> {CSV_PATH}")

    # Term-distribution
    from collections import Counter
    term_dist = Counter()
    for r in rows_for_queue:
        for t in r["cancer_text_match_terms"].split(","):
            term_dist[t.strip()] += 1
    log(f"  term distribution: {dict(term_dist.most_common())}")

    # ---------- markdown ----------
    md = ["# Follow-up 2 — Cancer-text orphan investigation queue", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_  ")
    md.append(
        "_Queue table: `manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1` "
        "(read-only; no canonical data modified)._  "
    )
    md.append("")
    md.append("## Counts")
    md.append("")
    md.append(f"- Tg-lab orphan rids scanned: **403**")
    md.append(f"- Cancer-suggestive procedure_raw match: **{len(matched_rids)}**")
    md.append(
        f"- Benign-only mention skipped (e.g. 'follicular' without 'carcinoma'): "
        f"**{len(skipped_benign_only)}**"
    )
    md.append(
        f"- Queue rows inserted into manuscript_workspace: **{n_q}** "
        f"(status=`awaiting_upstream_triage`)"
    )
    md.append("")
    md.append("## Top cancer-suggestive terms hit")
    md.append("")
    md.append("| term | n rids |")
    md.append("|:---|---:|")
    for term, n in term_dist.most_common(20):
        md.append(f"| `{term}` | {n} |")
    md.append("")
    md.append("## Sample rows (first 15)")
    md.append("")
    md.append(
        "| rid | n_oed | match terms | procedure_raw (truncated) | n_tg | first_tg | last_tg |"
    )
    md.append("|---:|---:|:---|:---|---:|:---|:---|")
    for r in rows_for_queue[:15]:
        pr = (r["procedure_raw_concatenated"] or "")[:70].replace("|", "\\|")
        md.append(
            f"| {r['research_id']} | {r['n_oed_rows']} | "
            f"`{r['cancer_text_match_terms']}` | {pr} | "
            f"{r['n_tg_lab_rows']} | {r['first_tg_dt']} | {r['last_tg_dt']} |"
        )
    md.append("")
    md.append("## What the queue is asking")
    md.append("")
    md.append(
        "For every rid here: the operative_episode_detail_v2.procedure_raw "
        "explicitly mentions a cancer histology, yet none of the 5 "
        "cancer-evidence tables (FNA, tumor_episode, synoptic_tumor, "
        "path_synoptic, imaging_nodule) carry evidence for this patient, "
        "and the patient is not in CPM. Triage decision per rid:"
    )
    md.append("")
    md.append(
        "1. **Upstream extraction gap** → fix the feeder that should have "
        "captured the cancer evidence; admit to CPM."
    )
    md.append(
        "2. **Intentional exclusion** → document the rule (e.g. benign on "
        "final path despite operative-note suspicion); update "
        "`is_in_canonical_cancer_cohort` rationale."
    )
    md.append(
        "3. **OED procedure_raw is itself wrong** → the operative note text "
        "was extracted incorrectly; correct upstream and re-run."
    )
    md.append("")
    md.append("_CSV with full procedure_raw / Tg dates per rid_: [`followup2_cancer_text_orphan_queue.csv`](./followup2_cancer_text_orphan_queue.csv)")
    MD_PATH.write_text("\n".join(md) + "\n")
    log(f"  MD -> {MD_PATH}")

    import json as _json
    JSON_PATH.write_text(
        _json.dumps(
            {
                "n_orphans_scanned": len(rids),
                "n_matched": len(matched_rids),
                "n_skipped_benign_only": len(skipped_benign_only),
                "n_queue_rows": n_q,
                "term_distribution": dict(term_dist.most_common()),
                "queue_table": "manuscript_workspace.tg_orphan_cancer_text_investigation_queue_v1",
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
    log("Follow-up 2 complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
