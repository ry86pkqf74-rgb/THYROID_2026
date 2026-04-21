"""Script 343 — VC tier diagnostic + completeness confirmation.

Problem (verified 2026-04-21):
  - Script 295/330 populated comp_vc_paralysis_evidence_tier (34 nonnull) and
    comp_vc_paresis_evidence_tier (22 nonnull) on canonical_patient_master.
  - Denominator is small: 19 paralysis confirmed (10 tier=1 + 9 tier=2) and
    13 paresis confirmed (11 tier=1 + 2 tier=2). The remaining tier=2/3
    rows are "neither confirmed nor suspected" status — driven by NLP
    mention/suspected logic from Script 295.

This script verifies:
  - For every RID with comp_vc_*_confirmed = TRUE, the tier IS NOT NULL.
  - For every RID with comp_vc_*_suspected = TRUE but confirmed=FALSE,
    the tier is set (typically >= 2).
  - For every RID with neither confirmed nor suspected and tier IS NOT NULL,
    we log the source so the audit can confirm intentional NLP/single-mention
    rows are appropriate.

Repairs (only if violations found):
  - confirmed = TRUE AND tier IS NULL → set tier = 1.
  - suspected = TRUE AND confirmed = FALSE AND tier IS NULL → set tier = 2.
  Repairs respect the v1-NULL-only invariant and are logged.

PHI safety: research_id only.

Usage:
    .venv/bin/python scripts/343_vc_tier_diagnostic.py            # dry-run
    .venv/bin/python scripts/343_vc_tier_diagnostic.py --commit   # apply repairs
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "343_vc_tier_diagnostic"

ENTITIES = [
    ("paralysis", "comp_vc_paralysis_evidence_tier",
     "comp_vc_paralysis_confirmed", "comp_vc_paralysis_suspected"),
    ("paresis", "comp_vc_paresis_evidence_tier",
     "comp_vc_paresis_confirmed", "comp_vc_paresis_suspected"),
]


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def ensure_log_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.prompt5_remediation_log_v1 (
            ts TIMESTAMP, script_n VARCHAR, phase VARCHAR,
            target_table VARCHAR, target_column VARCHAR,
            metric_name VARCHAR, metric_value DOUBLE,
            metric_text VARCHAR, notes VARCHAR
        )
    """)


def log_metric(con, phase, target_table, target_column, metric_name,
               metric_value=None, metric_text=None, notes=None):
    con.execute("""
        INSERT INTO manuscript_workspace.prompt5_remediation_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), SCRIPT, phase, target_table, target_column,
          metric_name,
          float(metric_value) if metric_value is not None else None,
          metric_text, notes])


def cross_tab(con, tier_col, conf_col, susp_col, label):
    rows = con.execute(f"""
        SELECT
          CASE WHEN "{conf_col}" = TRUE THEN 'confirmed'
               WHEN "{susp_col}" = TRUE THEN 'suspected'
               ELSE 'neither' END AS status,
          "{tier_col}" AS tier,
          COUNT(*) AS n
        FROM main.canonical_patient_master
       GROUP BY 1, 2
       ORDER BY 1, 2
    """).fetchall()
    log(f"    [{label}] {tier_col}:")
    for r in rows:
        log(f"      status={r[0]:10s} tier={str(r[1]):>5s} n={r[2]}")
    return rows


def violations(con, tier_col, conf_col, susp_col):
    conf_no_tier = con.execute(f"""
        SELECT COUNT(*) FROM main.canonical_patient_master
         WHERE "{conf_col}" = TRUE AND "{tier_col}" IS NULL
    """).fetchone()[0]
    susp_no_tier = con.execute(f"""
        SELECT COUNT(*) FROM main.canonical_patient_master
         WHERE "{susp_col}" = TRUE AND COALESCE("{conf_col}", FALSE) = FALSE
           AND "{tier_col}" IS NULL
    """).fetchone()[0]
    susp_low_tier = con.execute(f"""
        SELECT COUNT(*) FROM main.canonical_patient_master
         WHERE "{susp_col}" = TRUE AND COALESCE("{conf_col}", FALSE) = FALSE
           AND "{tier_col}" IS NOT NULL AND "{tier_col}" < 2
    """).fetchone()[0]
    return conf_no_tier, susp_no_tier, susp_low_tier


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_tables(con)
    log("=" * 72)
    log(f"Script 343 — VC tier diagnostic "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")

    total_violations = 0
    for entity, tier_col, conf_col, susp_col in ENTITIES:
        log(f"  Entity: {entity}")
        # Pre-state log
        cross_tab(con, tier_col, conf_col, susp_col, "pre")
        nn_pre = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
             WHERE "{tier_col}" IS NOT NULL
        """).fetchone()[0]
        log_metric(con, "pre", "canonical_patient_master", tier_col, "nonnull", nn_pre)

        conf_no_tier, susp_no_tier, susp_low_tier = violations(
            con, tier_col, conf_col, susp_col)
        log(f"    violations: confirmed_without_tier={conf_no_tier} "
            f"suspected_without_tier={susp_no_tier} suspected_with_tier_lt_2={susp_low_tier}")
        log_metric(con, "pre", "canonical_patient_master", tier_col,
                   "confirmed_without_tier", conf_no_tier)
        log_metric(con, "pre", "canonical_patient_master", tier_col,
                   "suspected_without_tier", susp_no_tier)
        log_metric(con, "pre", "canonical_patient_master", tier_col,
                   "suspected_with_tier_lt_2", susp_low_tier)
        total_violations += conf_no_tier + susp_no_tier

    if total_violations == 0:
        log("  No violations found. VC tiering already accurate against the "
            "small VC cohort. No repairs needed.")
        log_metric(con, "diagnostic", "canonical_patient_master", None,
                   "total_violations", 0,
                   notes="Script 330 VC tiering confirmed accurate; sparse "
                         "counts reflect small VC cohort, not a defect.")
    else:
        log(f"  Found {total_violations} violations across both entities.")

    if not args.commit:
        log("  (dry-run) — diagnostic only, no repairs")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run) re-run with --commit to apply any needed repairs.")
        return

    if total_violations == 0:
        cpm_invariants(con, "post")
        log("=" * 72)
        log("Script 343 complete. No repairs needed.")
        return

    log("  Applying repairs...")
    for entity, tier_col, conf_col, susp_col in ENTITIES:
        # Repair 1: confirmed=TRUE AND tier IS NULL → tier = 1
        n = con.execute(f"""
            UPDATE main.canonical_patient_master
               SET "{tier_col}" = 1
             WHERE "{conf_col}" = TRUE AND "{tier_col}" IS NULL
            RETURNING 1
        """).fetchall()
        n_repaired_conf = len(n)
        log(f"    {entity}: confirmed → tier=1: {n_repaired_conf} rows")
        log_metric(con, "repair", "canonical_patient_master", tier_col,
                   "set_tier1_for_confirmed", n_repaired_conf)

        # Repair 2: suspected=TRUE AND confirmed=FALSE AND tier IS NULL → tier=2
        n = con.execute(f"""
            UPDATE main.canonical_patient_master
               SET "{tier_col}" = 2
             WHERE "{susp_col}" = TRUE
               AND COALESCE("{conf_col}", FALSE) = FALSE
               AND "{tier_col}" IS NULL
            RETURNING 1
        """).fetchall()
        n_repaired_susp = len(n)
        log(f"    {entity}: suspected → tier=2: {n_repaired_susp} rows")
        log_metric(con, "repair", "canonical_patient_master", tier_col,
                   "set_tier2_for_suspected", n_repaired_susp)

    log("  Post-repair verification:")
    final_violations = 0
    for entity, tier_col, conf_col, susp_col in ENTITIES:
        cross_tab(con, tier_col, conf_col, susp_col, "post")
        c, s, _ = violations(con, tier_col, conf_col, susp_col)
        log(f"    {entity}: confirmed_without_tier={c} suspected_without_tier={s}")
        log_metric(con, "post", "canonical_patient_master", tier_col,
                   "confirmed_without_tier", c)
        log_metric(con, "post", "canonical_patient_master", tier_col,
                   "suspected_without_tier", s)
        final_violations += c + s
    if final_violations > 0:
        raise SystemExit(
            f"FAIL: {final_violations} violations remain after repair"
        )
    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 343 complete. All VC tier violations resolved.")


if __name__ == "__main__":
    main()
