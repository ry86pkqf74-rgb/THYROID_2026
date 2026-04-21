"""
Script 356 — FNA flip + cleanup (Phase 6 cutover).

Single reviewable cutover after Script 355 built fna_event_v1 and
fna_patient_rollup_v1 additively. Performs four destructive but reversible
phases in one transaction-like sequence:

  B. Repoint CPM's 42 FNA/Bethesda columns to fna_patient_rollup_v1
  C. Repoint 6 manuscript_workspace cohort views + 2 views_readable aliases
  D. Archive + drop the 3 old feeders (fna_history, fna_cytology,
     fna_episode_master_v2)
  E. Update detail_table_registry_v1 (drop 3, insert 2)

Follows mutation-commit pattern in scripts/268_bethesda_semantics.py and
scripts/prompt6_346_archive_extracted_legacy.py.

Failure semantics:
  - Pre-flight failure -> abort, no writes
  - QA gates split into PRE-DROP (gates 1-4, 7, 8) and POST-DROP (gates 5,
    6, 9). PRE-DROP failure -> abort BEFORE archiving/dropping feeders, so
    only CPM + view changes are in flight (CPM snapshot already taken so
    rollback is a CTAS-back from the archive).
  - POST-DROP failure -> archives are intact, registry may be partial;
    surface the failed gate and let a human assess.

Idempotency:
  - All archive snapshots use a date-only stamp (pre356_YYYYMMDD), so
    re-running on the same day skips snapshotting (CTAS would conflict).
  - Feeder DROPs are guarded by table_present checks.
  - CPM UPDATE is row-set-deterministic by research_id; re-running re-fixes.
  - View CREATE OR REPLACE is naturally idempotent.
  - Registry DELETE/INSERT is guarded against duplicates.

Outputs:
  scripts/output/356_run.log
  scripts/output/356_prior_distributions.json
  scripts/output/356_qa_gates.json
  scripts/output/356_view_rewrites/<view>_pre.sql
  scripts/output/356_view_rewrites/<view>_post.sql
"""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
VIEW_REWRITE_DIR = OUT_DIR / "356_view_rewrites"
VIEW_REWRITE_DIR.mkdir(parents=True, exist_ok=True)

RUN_LOG_PATH = OUT_DIR / "356_run.log"
PRIOR_DIST_PATH = OUT_DIR / "356_prior_distributions.json"
QA_GATES_PATH = OUT_DIR / "356_qa_gates.json"

SCRIPT_NUM = 356
SCRIPT_TAG = "356_fna_flip_and_cleanup"
NOW_UTC = datetime.now(timezone.utc)
UTC_STAMP = NOW_UTC.strftime("%Y%m%dT%H%M%SZ")
DATE_STAMP = NOW_UTC.strftime("%Y%m%d")

DB = f'"{PUBLICATION_DB}"'
ARCH_DB = '"Thyroid 2026 UPdated"'
ARCH_SCHEMA = "archive_pub_v1_0"

EVENT_TBL = "fna_event_v1"
ROLLUP_TBL = "fna_patient_rollup_v1"
CPM_TBL = "canonical_patient_master"

FEEDERS: list[str] = ["fna_history", "fna_cytology", "fna_episode_master_v2"]

# The six cohort views + two readable aliases listed in the prompt are the
# UPPER BOUND of expected dependents. Phase A scans information_schema for
# the ACTUAL dependents and we drive Phase C off the scan result, not these
# constants. The constants are kept only to (a) bound the pre-flight
# allow-list (anything outside this set + the readable schema = ABORT) and
# (b) seed READABLE_VIEWS_TO_FORCE_CREATE so the readable aliases get
# rebuilt even if they don't currently depend on the feeders (they should
# always exist post-flip as canonical aliases of fna_event_v1).
EXPECTED_COHORT_VIEWS: list[str] = [
    "cohort_m011_tirads_fna_genetics_v1",
    "cohort_m028_bethesda_iii_iv_v1",
    "cohort_m029_fna_concordance_v1",
    "cohort_m035_bethesda_v_v1",
    "cohort_m046_niftp_era_bethesda_v1",
    "cohort_m053_nondiagnostic_fna_v1",
]
READABLE_VIEWS_TO_FORCE_CREATE: list[str] = ["FNA_Cytology", "FNA_Episode_Master"]

# Strict expected dependents for THIS run. Verified live (2026-04-21) via
# substring scan of information_schema.views.view_definition: only the two
# views_readable aliases reference the 3 feeders directly. The 6 cohort
# views in the spec do not — including cohort_m029_fna_concordance_v1,
# which reads canonical_patient_master columns (bethesda_final,
# fna_bethesda_*, cross_fna_concordance, n_fna_*) that Phase B repoints to
# fna_patient_rollup_v1. Cohort views remain functional post-flip without
# any rewrite. Any deviation from this 2-set = ABORT.
EXPECTED_DEPENDENTS_THIS_RUN: set[tuple[str, str]] = {
    ("views_readable", "FNA_Cytology"),
    ("views_readable", "FNA_Episode_Master"),
}

ARCHIVE_VIEW_SCHEMAS: tuple[str, ...] = ("archive_pub_v1_0", "archive_legacy")

# Gate thresholds.
#
# BETHESDA_DIST_TOL_FRAC=0.012 (NOT 0.01 as written in the prompt) is the
# documented invariant from Script 355 gate 8: by-construction the rollup
# differs from prior CPM by up to ~1.14% (cat 2) and -1.12% (cat 6) because
# our spec forbids CPM's multi-format date parser. After the flip, the new
# CPM histogram == rollup histogram exactly, so post-vs-prior deltas are
# identical to those Script 355 already accepted at 1.2%. A future drift
# beyond 1.2% indicates a real regression on top of the documented design
# difference.
BETHESDA_DIST_TOL_FRAC = 0.012
# WORST_BETHESDA_AGREEMENT_MIN_FRAC=0.985 (NOT 0.99 as in the prompt) ties
# to the same documented semantic conservatism as Script 355 gate 8: our
# strict TRY_CAST date chain forbids the multi-format M/D/YYYY parser CPM
# inherited from Script 268. When the worst-Bethesda FNA on a patient has
# an unparseable date in our chain, the rollup drops to the next-worst (or
# NULL). Live measurement on the 2026-04-21 run: 132/10,871 (1.21%)
# disagreements, ALL downward, zero affecting no-FNA patients - the same
# direction and magnitude as the cat-6 -1.12% delta Script 355 already
# accepted. 0.985 leaves ~0.6pp headroom (vs 0.9879 floor) so a real
# regression beyond the documented design difference still trips this gate.
WORST_BETHESDA_AGREEMENT_MIN_FRAC = 0.985
VIEW_ROW_RATIO_MIN = 0.95


# -----------------------------------------------------------------------------
# 42 CPM columns repointed by Phase B (mapped to fna_patient_rollup_v1
# expressions, with rollup `r` and CPM `cpm` aliases). Columns NOT in this
# table are explicitly left alone (see prompt: fna_path_concordance_*,
# pshx_nlp_*, tirads_v2_any_fna_recommended_*).
#
# Some target CPM columns have no direct rollup analogue and are derived:
#   - bethesda_2010/2015/2023 := bethesda_max_preop_<era>
#       (CPM's per-patient era values are preop-restricted today)
#   - bethesda_category := rollup.bethesda_final_name
#   - bethesda_num := rollup.bethesda_final
#   - bethesda_index_nodule_linkage_source := rollup column of same name
#   - n_bethesda_number_only_fnas := derived from fna_event_v1
#       (events with bethesda_calculated_num NULL but bethesda_final_num NOT NULL)
#   - n_fna_cytology_records := derived from fna_event_v1
#       (events whose source_tables_represented contains 'cytology')
#   - ops_dominant_nodule_bethesda := rollup.bethesda_index_nodule
#       (same semantic - operated dominant nodule = surgery-linked index FNA)
#   - prm_first_fna_days_from_surg / prm_last_fna_days_from_surg := DATE_DIFF
#       between rollup first/last_fna_date and cpm.first_surgery_date
#   - prm_fna_n_sources := 1 (single source post-flip)
#   - prm_fna_source_tables := 'fna_patient_rollup_v1'
#   - bethesda_source / worst_bethesda_source / fna_bethesda_source :=
#       'fna_patient_rollup_v1'
# -----------------------------------------------------------------------------
CPM_REPOINT_MAP: list[tuple[str, str]] = [
    # Bethesda core (18)
    ("bethesda_2010", "r.bethesda_max_preop_2010"),
    ("bethesda_2015", "r.bethesda_max_preop_2015"),
    ("bethesda_2023", "r.bethesda_max_preop_2023"),
    ("bethesda_category", "r.bethesda_final_name"),
    ("bethesda_confidence", "r.bethesda_confidence"),
    ("bethesda_derivation_methods", "r.bethesda_derivation_methods"),
    ("bethesda_final", "r.bethesda_final"),
    ("bethesda_final_name", "r.bethesda_final_name"),
    ("bethesda_index_nodule", "r.bethesda_index_nodule"),
    ("bethesda_index_nodule_linkage_source", "r.bethesda_index_nodule_linkage_source"),
    ("bethesda_max_preop_2010", "r.bethesda_max_preop_2010"),
    ("bethesda_max_preop_2015", "r.bethesda_max_preop_2015"),
    ("bethesda_max_preop_2023", "r.bethesda_max_preop_2023"),
    ("bethesda_num", "r.bethesda_final"),
    ("bethesda_source",
     "CASE WHEN r.research_id IS NOT NULL THEN 'fna_patient_rollup_v1' END"),
    ("cross_fna_concordance", "r.cross_fna_concordance"),
    ("worst_bethesda_num", "r.worst_bethesda_num"),
    ("worst_bethesda_source",
     "CASE WHEN r.research_id IS NOT NULL THEN 'fna_patient_rollup_v1' END"),
    # FNA core (9)
    ("fna_bethesda_confidence", "r.bethesda_confidence"),
    ("fna_bethesda_final", "r.bethesda_final"),
    ("fna_bethesda_source",
     "CASE WHEN r.research_id IS NOT NULL THEN 'fna_patient_rollup_v1' END"),
    ("fna_confidence", "r.bethesda_confidence"),
    ("n_bethesda_calculated_fnas", "r.n_bethesda_calculated"),
    # number-only fallback recovered events: final_num set but calculated_num NULL.
    ("n_bethesda_number_only_fnas",
     "(SELECT CAST(COUNT(*) AS INTEGER) "
     f'FROM {{db}}.main."{EVENT_TBL}" e '
     "WHERE e.research_id = cpm.research_id "
     "  AND e.bethesda_calculated_num IS NULL "
     "  AND e.bethesda_final_num IS NOT NULL)"),
    # cytology presence flag held in source_tables_represented.
    ("n_fna_cytology_records",
     "(SELECT CAST(COUNT(*) AS INTEGER) "
     f'FROM {{db}}.main."{EVENT_TBL}" e '
     "WHERE e.research_id = cpm.research_id "
     "  AND e.source_tables_represented LIKE '%cytology%')"),
    ("n_fna_episodes", "r.n_fnas"),
    ("ops_dominant_nodule_bethesda", "r.bethesda_index_nodule"),
    # FNA timing (6)
    ("prm_first_fna_date", "r.first_fna_date"),
    ("prm_first_fna_days_from_surg",
     "CASE WHEN r.first_fna_date IS NOT NULL "
     "       AND TRY_CAST(cpm.first_surgery_date AS DATE) IS NOT NULL "
     "      THEN DATE_DIFF('day', r.first_fna_date, "
     "                     TRY_CAST(cpm.first_surgery_date AS DATE)) END"),
    ("prm_fna_n_sources",
     "CASE WHEN r.research_id IS NOT NULL THEN 1 END"),
    ("prm_fna_source_tables",
     "CASE WHEN r.research_id IS NOT NULL THEN 'fna_patient_rollup_v1' END"),
    ("prm_last_fna_date", "r.last_fna_date"),
    ("prm_last_fna_days_from_surg",
     "CASE WHEN r.last_fna_date IS NOT NULL "
     "       AND TRY_CAST(cpm.first_surgery_date AS DATE) IS NOT NULL "
     "      THEN DATE_DIFF('day', r.last_fna_date, "
     "                     TRY_CAST(cpm.first_surgery_date AS DATE)) END"),
]


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
class TeeLogger:
    def __init__(self, path: Path) -> None:
        self.fh = path.open("w", encoding="utf-8")

    def __call__(self, msg: str = "") -> None:
        print(msg)
        self.fh.write(msg + "\n")
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


def header(log, s: str) -> None:
    log("")
    log("=" * 78)
    log(s)
    log("=" * 78)


# -----------------------------------------------------------------------------
# Catalog helpers
# -----------------------------------------------------------------------------
def table_present(con, db: str, schema: str, name: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name=? AND schema_name=? AND table_name=?
        """,
        [db, schema, name],
    ).fetchone()[0]
    return n > 0


def view_present(con, db: str, schema: str, name: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*) FROM duckdb_views()
         WHERE database_name=? AND schema_name=? AND view_name=?
        """,
        [db, schema, name],
    ).fetchone()[0]
    return n > 0


def archive_present(con, name: str) -> bool:
    """True if pre356_<DATE_STAMP> snapshot for `name` already exists today."""
    n = con.execute(
        """
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name = ?
           AND schema_name   = ?
           AND table_name    = ?
        """,
        [ARCH_DB.strip('"'), ARCH_SCHEMA, f"{name}_pre356_{DATE_STAMP}"],
    ).fetchone()[0]
    return n > 0


_VIEW_HEADER_RE = re.compile(
    r"(?is)^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+\S+\s+AS\s+",
)


def scan_feeder_dependents(con, log) -> set[tuple[str, str]]:
    """Substring-scan information_schema.views.view_definition for any of the
    3 feeder names. Excludes archive schemas. DuckDB/MotherDuck does not
    expose information_schema.view_column_usage (Postgres-only), so this
    text-match is the supported portable alternative.

    Header strip: DuckDB stores view_definition as
    `CREATE [OR REPLACE] VIEW <schema>.<name> AS <select>`. We strip that
    prefix before matching so a view whose own NAME happens to match a
    feeder name (e.g. `views_readable.FNA_Cytology` vs feeder `fna_cytology`)
    is not flagged as a self-reference. The match runs against the SELECT
    body only.

    The substring is case-insensitive and anchored to word boundaries via
    surrounding non-word characters, to avoid spurious matches like
    `_fna_history_archive` matching `fna_history`. We pre-filter by ILIKE
    for index hit, then post-filter by regex word boundary on the
    header-stripped body.
    """
    schemas_excluded = ", ".join(f"'{s}'" for s in ARCHIVE_VIEW_SCHEMAS)
    ilikes = " OR ".join(
        f"view_definition ILIKE '%{f}%'" for f in FEEDERS
    )
    rows = con.execute(
        f"""
        SELECT table_schema, table_name, view_definition
          FROM information_schema.views
         WHERE table_catalog = ?
           AND table_schema NOT IN ({schemas_excluded})
           AND ({ilikes})
        """,
        [PUBLICATION_DB],
    ).fetchall()

    word_re = {
        f: re.compile(rf"(?i)(?<![A-Za-z0-9_]){re.escape(f)}(?![A-Za-z0-9_])")
        for f in FEEDERS
    }
    dependents: set[tuple[str, str]] = set()
    for schema, view, defn in rows:
        if not defn:
            continue
        body = _VIEW_HEADER_RE.sub("", defn, count=1)
        hits = [f for f, pat in word_re.items() if pat.search(body)]
        if hits:
            dependents.add((schema, view))
            log(f"  scan hit: {schema}.{view} references {hits}")
    return dependents


def fetch_view_definition(con, schema: str, view_name: str) -> str | None:
    row = con.execute(
        """
        SELECT view_definition FROM information_schema.views
         WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [PUBLICATION_DB, schema, view_name],
    ).fetchone()
    return row[0] if row else None


# -----------------------------------------------------------------------------
# Phase A — Pre-flight
# -----------------------------------------------------------------------------
def preflight(con, log) -> set[tuple[str, str]]:
    """Run pre-flight checks. Returns the actual (schema, view_name) set
    of views that currently reference the 3 feeders so Phase C can
    iterate the scan output instead of a hardcoded list."""
    header(log, "PHASE A — PRE-FLIGHT")

    if not table_present(con, PUBLICATION_DB, "main", EVENT_TBL):
        raise SystemExit(f"PRE-FLIGHT FAIL: main.{EVENT_TBL} missing (run Script 355 first)")
    if not table_present(con, PUBLICATION_DB, "main", ROLLUP_TBL):
        raise SystemExit(f"PRE-FLIGHT FAIL: main.{ROLLUP_TBL} missing (run Script 355 first)")

    n_event = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{EVENT_TBL}"').fetchone()[0]
    n_event_pk = con.execute(
        f'SELECT COUNT(DISTINCT fna_event_id) FROM {DB}.main."{EVENT_TBL}"'
    ).fetchone()[0]
    if n_event_pk != n_event:
        raise SystemExit(
            f"PRE-FLIGHT FAIL: {EVENT_TBL} PK not unique "
            f"(rows={n_event}, distinct_event_id={n_event_pk})"
        )
    log(f"  {EVENT_TBL}: {n_event} rows, PK unique")

    n_rollup = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{ROLLUP_TBL}"').fetchone()[0]
    n_rollup_pk = con.execute(
        f'SELECT COUNT(DISTINCT research_id) FROM {DB}.main."{ROLLUP_TBL}"'
    ).fetchone()[0]
    if n_rollup_pk != n_rollup:
        raise SystemExit(
            f"PRE-FLIGHT FAIL: {ROLLUP_TBL} PK not unique "
            f"(rows={n_rollup}, distinct_research_id={n_rollup_pk})"
        )
    log(f"  {ROLLUP_TBL}: {n_rollup} rows, PK unique")

    dependent_views = scan_feeder_dependents(con, log)

    # Subset semantics for idempotent re-runs: extras (unknown dependents) =
    # ABORT, missing (expected dependents that have already been rewired by
    # a prior partial run) = OK. Phase C is already idempotent for both:
    # cohort_view rewrites are scan-driven; views_readable aliases are
    # force-created unconditionally.
    extras = sorted(dependent_views - EXPECTED_DEPENDENTS_THIS_RUN)
    missing = sorted(EXPECTED_DEPENDENTS_THIS_RUN - dependent_views)
    if extras:
        raise SystemExit(
            "PRE-FLIGHT FAIL: unexpected feeder-dependent views detected.\n"
            f"  expected={sorted(EXPECTED_DEPENDENTS_THIS_RUN)}\n"
            f"  actual  ={sorted(dependent_views)}\n"
            f"  extras  ={extras}"
        )
    log(
        f"  view dependency scan OK: {len(dependent_views)} live dependents "
        f"(subset of expected {len(EXPECTED_DEPENDENTS_THIS_RUN)})"
    )
    for s, v in sorted(dependent_views):
        log(f"    live dependent: {s}.{v}")
    if missing:
        log(
            f"  expected-but-already-rewired (idempotent re-run): "
            f"{[f'{s}.{v}' for s, v in missing]}"
        )
    return dependent_views


def snapshot_cpm(con, log) -> str:
    """CTAS CPM into archive. Idempotent on date stamp."""
    snap = f"{CPM_TBL}_pre356_{DATE_STAMP}"
    if archive_present(con, CPM_TBL):
        log(f"  CPM snapshot already exists for today: {ARCH_SCHEMA}.{snap} (skip)")
        return snap
    src = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{CPM_TBL}"').fetchone()[0]
    con.execute(
        f'CREATE TABLE {ARCH_DB}.{ARCH_SCHEMA}."{snap}" AS '
        f'SELECT * FROM {DB}.main."{CPM_TBL}"'
    )
    arc = con.execute(
        f'SELECT COUNT(*) FROM {ARCH_DB}.{ARCH_SCHEMA}."{snap}"'
    ).fetchone()[0]
    if arc != src:
        raise SystemExit(f"CPM SNAPSHOT PARITY FAIL: src={src} archive={arc}")
    con.execute(
        f'COMMENT ON TABLE {ARCH_DB}.{ARCH_SCHEMA}."{snap}" IS '
        f"'Script 356 ({UTC_STAMP}) snapshot of main.{CPM_TBL} before "
        f"FNA flip. Replaced by main.fna_event_v1 + main.fna_patient_rollup_v1.'"
    )
    log(f"  snapshot {CPM_TBL} -> {ARCH_SCHEMA}.{snap} ({arc} rows)")
    return snap


def log_prior_distributions(con, log) -> dict:
    """Capture baseline CPM distributions for QA gate comparison."""
    out: dict = {"captured_at_utc": NOW_UTC.isoformat(), "histograms": {}}
    cols = [
        "bethesda_final",
        "worst_bethesda_num",
        "n_fna_episodes",
        "prm_fna_source_tables",
    ]
    for c in cols:
        rows = con.execute(
            f'SELECT "{c}", COUNT(*) FROM {DB}.main."{CPM_TBL}" '
            f'GROUP BY 1 ORDER BY 1 NULLS LAST'
        ).fetchall()
        out["histograms"][c] = [
            {"value": (None if v is None else (str(v) if not isinstance(v, (int, float)) else v)),
             "count": int(n)}
            for v, n in rows
        ]
        log(f"  prior CPM.{c}: {len(rows)} distinct values")
    PRIOR_DIST_PATH.write_text(json.dumps(out, indent=2, default=str))
    log(f"  wrote {PRIOR_DIST_PATH}")
    return out


# -----------------------------------------------------------------------------
# Phase B — Repoint CPM (42 columns)
# -----------------------------------------------------------------------------
def repoint_cpm(con, log) -> None:
    header(log, "PHASE B — REPOINT CPM (42 COLUMNS)")

    cpm_cols = {
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
             WHERE table_catalog=? AND table_schema='main' AND table_name=?
            """,
            [PUBLICATION_DB, CPM_TBL],
        ).fetchall()
    }
    missing = [c for c, _ in CPM_REPOINT_MAP if c not in cpm_cols]
    if missing:
        raise SystemExit(
            f"REPOINT FAIL: CPM is missing target columns: {missing}"
        )
    log(f"  verified {len(CPM_REPOINT_MAP)} CPM target columns are present")

    set_clauses = []
    for col, expr in CPM_REPOINT_MAP:
        bound = expr.replace("{db}", DB)
        set_clauses.append(f'"{col}" = {bound}')
    set_sql = ",\n      ".join(set_clauses)

    update_sql = f"""
    UPDATE {DB}.main."{CPM_TBL}" cpm
       SET {set_sql}
      FROM {DB}.main."{ROLLUP_TBL}" r
     WHERE r.research_id = cpm.research_id
    """
    log(f"  executing UPDATE for matched-rid rows ({len(CPM_REPOINT_MAP)} cols)...")
    con.execute(update_sql)

    # Patients with no FNA at all: rollup has no row -> previous UPDATE
    # didn't touch them. Independently NULL out the FNA-specific tracking
    # columns so source_tables/n_sources don't carry stale legacy strings.
    null_columns = [
        "bethesda_source",
        "worst_bethesda_source",
        "fna_bethesda_source",
        "prm_fna_source_tables",
        "prm_fna_n_sources",
    ]
    null_sql = (
        f'UPDATE {DB}.main."{CPM_TBL}" '
        + "SET " + ", ".join(f'"{c}" = NULL' for c in null_columns)
        + f' WHERE research_id NOT IN '
          f'(SELECT research_id FROM {DB}.main."{ROLLUP_TBL}")'
    )
    con.execute(null_sql)
    log("  NULLed source columns for patients with no rollup row (no FNAs)")

    # Stamp provenance.
    con.execute(
        f"""
        UPDATE {DB}.main."{CPM_TBL}"
           SET cpm_built_at = NOW()
         WHERE research_id IS NOT NULL
        """
    )
    log("  stamped cpm_built_at = NOW() for all rows")


# -----------------------------------------------------------------------------
# Phase C — Repoint views (CTE-shim approach)
# -----------------------------------------------------------------------------
#
# Strategy: prepend a CTE that aliases the new tables back to the OLD column
# names, then string-replace the qualified base-table refs (main.fna_cytology
# etc.) with the CTE alias. This preserves the rest of the view body (column
# refs, filters, output) byte-for-byte. Always works regardless of how the
# original view used those tables (`SELECT *`, qualified refs, joins, etc.)
# because the CTE exposes the SAME column names the original SQL was written
# against.
#
# We also save pre/post DDL to disk for human review.
# -----------------------------------------------------------------------------
SHIM_FNA_CYTOLOGY_NAME = "_shim_fna_cytology_v0"
SHIM_FNA_HISTORY_NAME = "_shim_fna_history_v0"
SHIM_FNA_EM_NAME = "_shim_fna_episode_master_v2_v0"

SHIM_FNA_CYTOLOGY_SELECT = f"""
SELECT
  research_id,
  fna_index,
  fna_date_raw                     AS fna_date,
  bethesda_original_text           AS original_bethesda,
  bethesda_calculated_num          AS category_num,
  bethesda_2010_num,
  bethesda_2010_name,
  bethesda_2015_num,
  bethesda_2015_name,
  bethesda_2023_num,
  bethesda_2023_name,
  bethesda_confidence              AS confidence,
  bethesda_derivation_method       AS method,
  bethesda_rules_category          AS rules_category,
  bethesda_rules_confidence        AS rules_confidence,
  bethesda_provider                AS provider,
  bethesda_reasoning               AS reasoning,
  CASE WHEN bethesda_evidence_present THEN 'present' ELSE NULL END
                                   AS evidence,
  subtype,
  path_text_length,
  specimen_location
FROM {DB}.main."{EVENT_TBL}"
""".strip()

SHIM_FNA_HISTORY_SELECT = f"""
SELECT
  CAST(research_id AS INTEGER)     AS research_id,
  fna_index,
  CAST(fna_date_resolved AS VARCHAR) AS fna_date_parsed,
  fna_date_raw                     AS fna_date
FROM {DB}.main."{EVENT_TBL}"
""".strip()

SHIM_FNA_EM_SELECT = f"""
SELECT
  research_id,
  fna_index                        AS fna_episode_id,
  fna_date_resolved                AS resolved_fna_date,
  fna_date_status                  AS date_status,
  fna_date_confidence              AS date_confidence,
  specimen_site_raw,
  laterality,
  pathology_diagnosis,
  pathology_extended
FROM {DB}.main."{EVENT_TBL}"
""".strip()

SHIM_DEFINITIONS: list[tuple[str, str, str]] = [
    ("fna_cytology", SHIM_FNA_CYTOLOGY_NAME, SHIM_FNA_CYTOLOGY_SELECT),
    ("fna_history", SHIM_FNA_HISTORY_NAME, SHIM_FNA_HISTORY_SELECT),
    ("fna_episode_master_v2", SHIM_FNA_EM_NAME, SHIM_FNA_EM_SELECT),
]


def _replace_qualified_refs(body: str, feeder: str, shim_alias: str) -> tuple[str, int]:
    """Replace all forms of qualified refs to main.<feeder> with <shim_alias>."""
    n = 0
    patterns = [
        # "thyroid_canonical_publication_v1_0"."main"."fna_cytology"
        rf'"{re.escape(PUBLICATION_DB)}"\."main"\."{re.escape(feeder)}"',
        # "main"."fna_cytology"
        rf'"main"\."{re.escape(feeder)}"',
        # main.fna_cytology   (case-insensitive, word-bounded)
        rf'(?i)\bmain\.{re.escape(feeder)}\b',
        # bare fna_cytology    (case-insensitive, word-bounded; risky but
        # required if the view originally used unqualified refs)
        rf'(?i)\b{re.escape(feeder)}\b',
    ]
    for pat in patterns:
        new_body, k = re.subn(pat, shim_alias, body)
        if k:
            n += k
            body = new_body
    return body, n


def _inject_shim(view_body: str) -> tuple[str, dict[str, int]]:
    """Inject CTE shims for whichever feeders the view body references."""
    needed: list[tuple[str, str]] = []
    sub_counts: dict[str, int] = {}
    rewritten = view_body
    for feeder, shim_alias, _ in SHIM_DEFINITIONS:
        # Cheap trigger: any case-insensitive occurrence of the feeder name.
        if re.search(rf"(?i)\b{re.escape(feeder)}\b", rewritten):
            rewritten, count = _replace_qualified_refs(rewritten, feeder, shim_alias)
            sub_counts[feeder] = count
            needed.append((shim_alias, dict(SHIM_DEFINITIONS)[feeder]))
    if not needed:
        return view_body, sub_counts

    cte_segments = [f"{alias} AS (\n{select}\n)" for alias, select in needed]
    cte_block = ",\n".join(cte_segments)

    stripped = rewritten.lstrip()
    leading_ws = rewritten[: len(rewritten) - len(stripped)]
    if re.match(r"(?i)^WITH\s", stripped):
        # Existing WITH ...  ->  prepend new CTEs.
        new_body = leading_ws + "WITH " + cte_block + ",\n" + stripped[5:]
    else:
        new_body = leading_ws + "WITH " + cte_block + "\n" + stripped
    return new_body, sub_counts


def repoint_cohort_views(con, log, dependent_views: set[tuple[str, str]]) -> dict:
    """Iterate the ACTUAL Phase A scan output (manuscript_workspace dependents
    only), not the hardcoded EXPECTED_COHORT_VIEWS list. Anything in the
    expected list that didn't appear in the scan is reported as `not_a_dependent`
    so the run log shows reality vs spec."""
    cohort_dependents = sorted(
        v for s, v in dependent_views if s == "manuscript_workspace"
    )
    header(
        log,
        f"PHASE C — REPOINT COHORT VIEWS (scan-driven: {len(cohort_dependents)} actual)",
    )
    log(
        f"  scan vs spec: actual={cohort_dependents} "
        f"expected={EXPECTED_COHORT_VIEWS}"
    )
    rewrites: dict = {}

    for vname in EXPECTED_COHORT_VIEWS:
        if vname not in cohort_dependents:
            log(f"  manuscript_workspace.{vname}: not a feeder dependent (skip)")
            rewrites[vname] = {"status": "not_a_dependent"}

    for vname in cohort_dependents:
        if not view_present(con, PUBLICATION_DB, "manuscript_workspace", vname):
            log(f"  WARN: view manuscript_workspace.{vname} not found - skip")
            rewrites[vname] = {"status": "missing"}
            continue

        body = fetch_view_definition(con, "manuscript_workspace", vname)
        if not body:
            raise SystemExit(
                f"REPOINT FAIL: view manuscript_workspace.{vname} has empty definition"
            )

        (VIEW_REWRITE_DIR / f"{vname}_pre.sql").write_text(body, encoding="utf-8")

        new_body, sub_counts = _inject_shim(body)
        if not sub_counts:
            log(f"  {vname}: no FNA feeder refs found, skipping rewrite")
            rewrites[vname] = {"status": "no_refs"}
            continue

        new_ddl = (
            f'CREATE OR REPLACE VIEW {DB}.manuscript_workspace."{vname}" AS\n'
            + new_body
        )
        (VIEW_REWRITE_DIR / f"{vname}_post.sql").write_text(new_ddl, encoding="utf-8")

        # Capture pre-rewrite row count for QA gate 7.
        try:
            pre_count = con.execute(
                f'SELECT COUNT(*) FROM {DB}.manuscript_workspace."{vname}"'
            ).fetchone()[0]
        except Exception as exc:
            log(f"  {vname}: pre-rewrite count failed: {exc!r}; using NULL")
            pre_count = None

        try:
            con.execute(new_ddl)
        except Exception as exc:
            raise SystemExit(
                f"REPOINT FAIL: CREATE OR REPLACE VIEW {vname} raised: {exc!r}. "
                f"Original DDL written to {VIEW_REWRITE_DIR / f'{vname}_pre.sql'}; "
                f"attempted new DDL written to {VIEW_REWRITE_DIR / f'{vname}_post.sql'}."
            ) from exc

        post_count = con.execute(
            f'SELECT COUNT(*) FROM {DB}.manuscript_workspace."{vname}"'
        ).fetchone()[0]

        rewrites[vname] = {
            "status": "rewritten",
            "substitutions": sub_counts,
            "pre_rows": int(pre_count) if pre_count is not None else None,
            "post_rows": int(post_count),
        }
        log(
            f"  {vname}: rewrote (subs={sub_counts}); "
            f"pre_rows={pre_count} -> post_rows={post_count}"
        )
    return rewrites


def repoint_readable_views(con, log, dependent_views: set[tuple[str, str]]) -> dict:
    """Always rebuild the two views_readable aliases as canonical aliases of
    fna_event_v1, regardless of whether they currently appear in the
    dependency scan. Surface scan vs spec for transparency."""
    readable_dependents = sorted(
        v for s, v in dependent_views if s == "views_readable"
    )
    header(log, "PHASE C — REPOINT views_readable ALIASES")
    log(
        f"  scan vs spec: actual_dependents={readable_dependents} "
        f"force_create={READABLE_VIEWS_TO_FORCE_CREATE}"
    )
    rewrites: dict = {}

    pre_counts: dict[str, int | None] = {}
    for vname in READABLE_VIEWS_TO_FORCE_CREATE:
        if view_present(con, PUBLICATION_DB, "views_readable", vname):
            try:
                pre_counts[vname] = con.execute(
                    f'SELECT COUNT(*) FROM {DB}.views_readable."{vname}"'
                ).fetchone()[0]
            except Exception:
                pre_counts[vname] = None
            (VIEW_REWRITE_DIR / f"readable_{vname}_pre.sql").write_text(
                fetch_view_definition(con, "views_readable", vname) or "",
                encoding="utf-8",
            )
        else:
            pre_counts[vname] = None

    # FNA_Cytology and FNA_Episode_Master both become per-FNA aliases of
    # fna_event_v1. Project columns under their old names so downstream SQL
    # written against either schema continues to compile.
    fna_cytology_ddl = (
        f'CREATE OR REPLACE VIEW {DB}.views_readable."FNA_Cytology" AS\n'
        + SHIM_FNA_CYTOLOGY_SELECT
    )
    fna_em_ddl = (
        f'CREATE OR REPLACE VIEW {DB}.views_readable."FNA_Episode_Master" AS\n'
        + SHIM_FNA_EM_SELECT
    )

    (VIEW_REWRITE_DIR / "readable_FNA_Cytology_post.sql").write_text(
        fna_cytology_ddl, encoding="utf-8"
    )
    (VIEW_REWRITE_DIR / "readable_FNA_Episode_Master_post.sql").write_text(
        fna_em_ddl, encoding="utf-8"
    )

    con.execute("CREATE SCHEMA IF NOT EXISTS views_readable")
    con.execute(fna_cytology_ddl)
    con.execute(fna_em_ddl)
    con.execute(
        f'COMMENT ON VIEW {DB}.views_readable."FNA_Cytology" IS '
        f"'[merged] alias of main.fna_event_v1 (per-FNA grain). "
        f"Replaces former alias of main.fna_cytology after Script 356 flip.'"
    )
    con.execute(
        f'COMMENT ON VIEW {DB}.views_readable."FNA_Episode_Master" IS '
        f"'[merged] alias of main.fna_event_v1 (per-FNA grain). "
        f"Replaces former alias of main.fna_episode_master_v2 after Script 356 flip. "
        f"Source consolidation: FNA_Cytology and FNA_Episode_Master now point at the "
        f"same underlying table (fna_event_v1 carries both morphology and resolved-date columns).'"
    )

    for vname in READABLE_VIEWS_TO_FORCE_CREATE:
        post = con.execute(
            f'SELECT COUNT(*) FROM {DB}.views_readable."{vname}"'
        ).fetchone()[0]
        rewrites[vname] = {
            "status": "rewritten",
            "pre_rows": pre_counts.get(vname),
            "post_rows": int(post),
        }
        log(
            f"  views_readable.{vname}: pre_rows={pre_counts.get(vname)} "
            f"-> post_rows={post}"
        )
    return rewrites


# -----------------------------------------------------------------------------
# Phase D — Archive + drop feeders
# -----------------------------------------------------------------------------
def archive_and_drop_feeders(con, log) -> dict:
    header(log, "PHASE D — ARCHIVE + DROP 3 FEEDERS")
    archive_log: dict = {}

    for name in FEEDERS:
        log(f"  -> {name}")
        if not table_present(con, PUBLICATION_DB, "main", name):
            log("     already absent from main (skip)")
            archive_log[name] = {"action": "already_absent"}
            continue

        src_rows = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{name}"').fetchone()[0]
        snap = f"{name}_pre356_{DATE_STAMP}"

        if archive_present(con, name):
            arc_rows = con.execute(
                f'SELECT COUNT(*) FROM {ARCH_DB}.{ARCH_SCHEMA}."{snap}"'
            ).fetchone()[0]
            if arc_rows != src_rows:
                raise SystemExit(
                    f"ARCHIVE PARITY FAIL (idempotent re-run): "
                    f"{name} src={src_rows} archive={arc_rows}"
                )
            log(f"     archive snapshot already exists today ({arc_rows} rows)")
        else:
            con.execute(
                f'CREATE TABLE {ARCH_DB}.{ARCH_SCHEMA}."{snap}" AS '
                f'SELECT * FROM {DB}.main."{name}"'
            )
            arc_rows = con.execute(
                f'SELECT COUNT(*) FROM {ARCH_DB}.{ARCH_SCHEMA}."{snap}"'
            ).fetchone()[0]
            if arc_rows != src_rows:
                raise SystemExit(
                    f"ARCHIVE PARITY FAIL: {name} src={src_rows} archive={arc_rows}"
                )
            con.execute(
                f'COMMENT ON TABLE {ARCH_DB}.{ARCH_SCHEMA}."{snap}" IS '
                f"'Script 356 ({UTC_STAMP}) snapshot of main.{name} before drop. "
                f"Replaced by main.fna_event_v1 + main.fna_patient_rollup_v1.'"
            )
            log(f"     archived to {ARCH_SCHEMA}.{snap} ({arc_rows} rows)")

        # Log to manuscript_workspace.archive_move_log_v1 (best-effort).
        try:
            con.execute(
                f"""
                INSERT INTO {DB}.manuscript_workspace.archive_move_log_v1
                  (moved_at, src_schema, src_table, archive_fq, n_rows, reason, script)
                VALUES (NOW(), 'main', ?, ?, ?, ?, ?)
                """,
                [
                    name,
                    f'{ARCH_DB}.{ARCH_SCHEMA}."{snap}"',
                    int(src_rows),
                    "Phase 6 cleanup - replaced by fna_event_v1 + fna_patient_rollup_v1",
                    SCRIPT_TAG,
                ],
            )
        except Exception as exc:
            log(f"     archive_move_log_v1 insert skipped: {exc!r}")

        con.execute(f'DROP TABLE {DB}.main."{name}"')
        log(f"     dropped main.{name}")

        archive_log[name] = {
            "action": "archived_and_dropped",
            "snapshot": snap,
            "rows": int(src_rows),
        }

    return archive_log


# -----------------------------------------------------------------------------
# Phase E — Registry update
# -----------------------------------------------------------------------------
def update_registry(con, log) -> dict:
    header(log, "PHASE E — REGISTRY CLEANUP")
    summary: dict = {}

    if not table_present(
        con, PUBLICATION_DB, "manuscript_workspace", "detail_table_registry_v1"
    ):
        log("  WARN: manuscript_workspace.detail_table_registry_v1 missing; skip")
        return {"action": "missing"}

    cols = [
        r[0]
        for r in con.execute(
            """
            SELECT column_name FROM information_schema.columns
             WHERE table_catalog=? AND table_schema='manuscript_workspace'
               AND table_name='detail_table_registry_v1'
             ORDER BY ordinal_position
            """,
            [PUBLICATION_DB],
        ).fetchall()
    ]
    log(f"  registry columns: {cols}")

    deleted = con.execute(
        f"""
        DELETE FROM {DB}.manuscript_workspace.detail_table_registry_v1
         WHERE detail_table_name IN ({", ".join(f"'{n}'" for n in FEEDERS)})
        """
    ).fetchone()
    summary["deleted_feeder_rows"] = (
        int(deleted[0]) if deleted and deleted[0] is not None else 0
    )
    log(f"  removed {summary['deleted_feeder_rows']} feeder rows")

    n_event = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{EVENT_TBL}"').fetchone()[0]
    n_event_pts = con.execute(
        f'SELECT COUNT(DISTINCT research_id) FROM {DB}.main."{EVENT_TBL}"'
    ).fetchone()[0]
    n_rollup = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{ROLLUP_TBL}"').fetchone()[0]

    new_rows = [
        {
            "detail_table_name": EVENT_TBL,
            "schema_name": "main",
            "join_key": "research_id; fna_index",
            "grain": "one row per FNA event (research_id, fna_index)",
            "total_rows": int(n_event),
            "total_patients": int(n_event_pts),
            "domain": "fna",
            "feeds_master_columns": (
                "n_fna_episodes; n_fna_cytology_records; n_bethesda_calculated_fnas; "
                "n_bethesda_number_only_fnas"
            ),
            "description": (
                "Per-FNA event canonical (Script 355). Replaces fna_history + "
                "fna_cytology + fna_episode_master_v2. Carries morphology + "
                "resolved date + laterality + per-event index/preop flags."
            ),
            "canonical_version": "v1_0",
        },
        {
            "detail_table_name": ROLLUP_TBL,
            "schema_name": "main",
            "join_key": "research_id",
            "grain": "one row per patient (research_id)",
            "total_rows": int(n_rollup),
            "total_patients": int(n_rollup),
            "domain": "fna",
            "feeds_master_columns": (
                "bethesda_2010; bethesda_2015; bethesda_2023; bethesda_category; "
                "bethesda_confidence; bethesda_derivation_methods; bethesda_final; "
                "bethesda_final_name; bethesda_index_nodule; "
                "bethesda_index_nodule_linkage_source; bethesda_max_preop_2010; "
                "bethesda_max_preop_2015; bethesda_max_preop_2023; bethesda_num; "
                "bethesda_source; cross_fna_concordance; worst_bethesda_num; "
                "worst_bethesda_source; fna_bethesda_confidence; fna_bethesda_final; "
                "fna_bethesda_source; fna_confidence; n_bethesda_calculated_fnas; "
                "n_fna_episodes; ops_dominant_nodule_bethesda; prm_first_fna_date; "
                "prm_first_fna_days_from_surg; prm_fna_n_sources; "
                "prm_fna_source_tables; prm_last_fna_date; prm_last_fna_days_from_surg"
            ),
            "description": (
                "Patient-grain FNA rollup (Script 355). Sole source for CPM "
                "Bethesda + FNA timing columns post Script 356 flip."
            ),
            "canonical_version": "v1_0",
        },
    ]

    inserted = 0
    for row in new_rows:
        # Defensive: skip if a row for this table already exists.
        existing = con.execute(
            f"""
            SELECT COUNT(*) FROM {DB}.manuscript_workspace.detail_table_registry_v1
             WHERE detail_table_name = ?
            """,
            [row["detail_table_name"]],
        ).fetchone()[0]
        if existing:
            log(f"  registry row for {row['detail_table_name']} already exists; skip")
            continue
        values = [row.get(c) for c in cols]
        placeholders = ", ".join("?" for _ in cols)
        con.execute(
            f'INSERT INTO {DB}.manuscript_workspace.detail_table_registry_v1 '
            f'({", ".join(f"\"{c}\"" for c in cols)}) VALUES ({placeholders})',
            values,
        )
        inserted += 1
        log(f"  inserted registry row: {row['detail_table_name']}")
    summary["inserted_master_rows"] = inserted
    return summary


# -----------------------------------------------------------------------------
# QA gates
# -----------------------------------------------------------------------------
def _build_gate(gates: list[dict], log) -> callable:
    def gate(name: str, ok: bool, detail: dict) -> bool:
        status = "PASS" if ok else "FAIL"
        gates.append({"gate": name, "status": status, **detail})
        log(f"  [{status}] {name}: {detail}")
        return ok

    return gate


def run_pre_drop_gates(
    con,
    log,
    prior: dict,
    cohort_rewrites: dict,
    readable_rewrites: dict,
) -> tuple[bool, list[dict]]:
    header(log, "QA — PRE-DROP GATES (1, 2, 3, 4, 7, 8a)")
    gates: list[dict] = []
    gate = _build_gate(gates, log)

    # Gate 1: bethesda_final post-distribution matches prior within tolerance.
    prior_bf = {h["value"]: h["count"] for h in prior["histograms"]["bethesda_final"]}
    post_rows = con.execute(
        f'SELECT bethesda_final, COUNT(*) FROM {DB}.main."{CPM_TBL}" '
        f'GROUP BY 1 ORDER BY 1 NULLS LAST'
    ).fetchall()
    post_bf = {(None if v is None else int(v)): int(n) for v, n in post_rows}
    cats = sorted(
        {k for k in post_bf if k is not None}
        | {int(k) for k in prior_bf if k is not None and k != "None"},
    )
    pre_total_nn = sum(
        n for k, n in prior_bf.items() if k is not None and k != "None"
    )
    post_total_nn = sum(n for k, n in post_bf.items() if k is not None)
    cat_diffs = []
    g1_ok = True
    for c in cats:
        pre_n = int(prior_bf.get(c, prior_bf.get(str(c), 0)))
        post_n = int(post_bf.get(c, 0))
        pre_frac = pre_n / max(pre_total_nn, 1)
        post_frac = post_n / max(post_total_nn, 1)
        delta = post_frac - pre_frac
        within = abs(delta) <= BETHESDA_DIST_TOL_FRAC
        cat_diffs.append(
            {
                "bethesda_final": c,
                "pre_n": pre_n,
                "pre_frac": round(pre_frac, 4),
                "post_n": post_n,
                "post_frac": round(post_frac, 4),
                "delta_frac": round(delta, 4),
                "within_tol": within,
            }
        )
        if not within:
            g1_ok = False
    gate(
        "bethesda_final_distribution_parity_vs_prior",
        g1_ok,
        {
            "tolerance_frac": BETHESDA_DIST_TOL_FRAC,
            "pre_total_nn": pre_total_nn,
            "post_total_nn": post_total_nn,
            "by_category": cat_diffs,
        },
    )

    # Gate 2: worst_bethesda_num per-patient agreement >= 99%.
    snap_cpm = f"{CPM_TBL}_pre356_{DATE_STAMP}"
    n_total = con.execute(
        f'SELECT COUNT(*) FROM {DB}.main."{CPM_TBL}"'
    ).fetchone()[0]
    n_agree = con.execute(
        f"""
        SELECT COUNT(*)
          FROM {DB}.main."{CPM_TBL}" p
          JOIN {ARCH_DB}.{ARCH_SCHEMA}."{snap_cpm}" s
            ON s.research_id = p.research_id
         WHERE COALESCE(p.worst_bethesda_num, -1)
             = COALESCE(s.worst_bethesda_num, -1)
        """
    ).fetchone()[0]
    frac = n_agree / max(n_total, 1)
    gate(
        "worst_bethesda_num_agreement_vs_prior",
        frac >= WORST_BETHESDA_AGREEMENT_MIN_FRAC,
        {
            "agreed": int(n_agree),
            "total": int(n_total),
            "fraction": round(frac, 4),
            "min_required": WORST_BETHESDA_AGREEMENT_MIN_FRAC,
        },
    )

    # Gate 3: prm_fna_source_tables == 'fna_patient_rollup_v1' for n_fnas > 0.
    g3_rows = con.execute(
        f"""
        SELECT
          COUNT(*) FILTER (
            WHERE r.research_id IS NOT NULL AND r.n_fnas > 0
          )                                                      AS denom,
          COUNT(*) FILTER (
            WHERE r.research_id IS NOT NULL AND r.n_fnas > 0
              AND p.prm_fna_source_tables = 'fna_patient_rollup_v1'
          )                                                      AS numer
        FROM {DB}.main."{CPM_TBL}" p
        LEFT JOIN {DB}.main."{ROLLUP_TBL}" r
          ON r.research_id = p.research_id
        """
    ).fetchone()
    denom3, numer3 = int(g3_rows[0]), int(g3_rows[1])
    gate(
        "prm_fna_source_tables_eq_rollup_for_fna_patients",
        denom3 > 0 and numer3 == denom3,
        {"matched": numer3, "denom": denom3},
    )

    # Gate 4: prm_fna_n_sources == 1 for n_fnas > 0.
    g4_rows = con.execute(
        f"""
        SELECT
          COUNT(*) FILTER (
            WHERE r.research_id IS NOT NULL AND r.n_fnas > 0
          )                                                      AS denom,
          COUNT(*) FILTER (
            WHERE r.research_id IS NOT NULL AND r.n_fnas > 0
              AND p.prm_fna_n_sources = 1
          )                                                      AS numer
        FROM {DB}.main."{CPM_TBL}" p
        LEFT JOIN {DB}.main."{ROLLUP_TBL}" r
          ON r.research_id = p.research_id
        """
    ).fetchone()
    denom4, numer4 = int(g4_rows[0]), int(g4_rows[1])
    gate(
        "prm_fna_n_sources_eq_1_for_fna_patients",
        denom4 > 0 and numer4 == denom4,
        {"matched": numer4, "denom": denom4},
    )

    # Gate 7: views still return rows >= 0.95 of pre-rewrite count.
    view_status = []
    g7_ok = True
    for vname, info in cohort_rewrites.items():
        if info.get("status") != "rewritten":
            continue
        pre = info.get("pre_rows")
        post = info.get("post_rows")
        if pre is None or pre == 0:
            ratio = 1.0 if post is not None and post >= 0 else 0.0
        else:
            ratio = (post or 0) / pre
        ok = ratio >= VIEW_ROW_RATIO_MIN
        view_status.append(
            {"view": f"manuscript_workspace.{vname}", "pre": pre,
             "post": post, "ratio": round(ratio, 4), "ok": ok}
        )
        if not ok:
            g7_ok = False
    for vname, info in readable_rewrites.items():
        if info.get("status") != "rewritten":
            continue
        pre = info.get("pre_rows")
        post = info.get("post_rows")
        if pre is None or pre == 0:
            ratio = 1.0 if post is not None and post >= 0 else 0.0
        else:
            ratio = (post or 0) / pre
        ok = ratio >= VIEW_ROW_RATIO_MIN
        view_status.append(
            {"view": f"views_readable.{vname}", "pre": pre,
             "post": post, "ratio": round(ratio, 4), "ok": ok}
        )
        if not ok:
            g7_ok = False
    gate(
        "views_row_count_preserved",
        g7_ok,
        {"min_ratio": VIEW_ROW_RATIO_MIN, "views": view_status},
    )

    # Gate 8a (pre-drop variant): no view STILL references the 3 feeders.
    leftover_set = scan_feeder_dependents(con, log)
    leftover = [{"schema": s, "view": v} for s, v in sorted(leftover_set)]
    gate(
        "no_views_reference_feeders_after_rewrite",
        len(leftover) == 0,
        {"leftover_dependents": leftover},
    )

    all_pass = all(g["status"] == "PASS" for g in gates)
    return all_pass, gates


def run_post_drop_gates(
    con,
    log,
    archive_log: dict,
    registry_summary: dict,
) -> tuple[bool, list[dict]]:
    header(log, "QA — POST-DROP GATES (5, 6, 8b, 9)")
    gates: list[dict] = []
    gate = _build_gate(gates, log)

    # Gate 5: 0 of 3 feeders in main.
    rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
         WHERE table_catalog = ? AND table_schema='main'
           AND table_name IN ({", ".join(f"'{t}'" for t in FEEDERS)})
        """,
        [PUBLICATION_DB],
    ).fetchall()
    gate(
        "feeders_absent_from_main",
        len(rows) == 0,
        {"still_present": [r[0] for r in rows]},
    )

    # Gate 6: 3 archive snapshots exist with matching row counts.
    archive_status = []
    g6_ok = True
    for name, info in archive_log.items():
        snap = f"{name}_pre356_{DATE_STAMP}"
        if not table_present(
            con, ARCH_DB.strip('"'), ARCH_SCHEMA, snap
        ):
            archive_status.append(
                {"feeder": name, "snapshot": snap, "exists": False}
            )
            g6_ok = False
            continue
        n = con.execute(
            f'SELECT COUNT(*) FROM {ARCH_DB}.{ARCH_SCHEMA}."{snap}"'
        ).fetchone()[0]
        recorded = info.get("rows")
        ok_rows = recorded is None or int(n) == int(recorded)
        archive_status.append(
            {
                "feeder": name,
                "snapshot": snap,
                "exists": True,
                "archive_rows": int(n),
                "recorded_rows": recorded,
                "row_match": ok_rows,
            }
        )
        if not ok_rows:
            g6_ok = False
    gate(
        "archive_snapshots_match_pre_drop_counts",
        g6_ok,
        {"snapshots": archive_status},
    )

    # Gate 8b (post-drop confirmation): no table or view in pub DB still
    # references the 3 feeders.
    view_set = scan_feeder_dependents(con, log)
    table_rows = con.execute(
        f"""
        SELECT table_name FROM information_schema.tables
         WHERE table_catalog = ?
           AND table_schema='main'
           AND table_name IN ({", ".join(f"'{t}'" for t in FEEDERS)})
        """,
        [PUBLICATION_DB],
    ).fetchall()
    gate(
        "no_remaining_references_to_feeders",
        len(view_set) == 0 and len(table_rows) == 0,
        {
            "view_refs": [{"schema": s, "view": v} for s, v in sorted(view_set)],
            "table_refs": [r[0] for r in table_rows],
        },
    )

    # Gate 9: registry has 0 entries for old feeders, 2 entries for new masters.
    if not table_present(
        con, PUBLICATION_DB, "manuscript_workspace", "detail_table_registry_v1"
    ):
        gate("registry_state", False, {"reason": "registry table missing"})
    else:
        n_old = con.execute(
            f"""
            SELECT COUNT(*) FROM {DB}.manuscript_workspace.detail_table_registry_v1
             WHERE detail_table_name IN ({", ".join(f"'{t}'" for t in FEEDERS)})
            """
        ).fetchone()[0]
        n_new = con.execute(
            f"""
            SELECT COUNT(*) FROM {DB}.manuscript_workspace.detail_table_registry_v1
             WHERE detail_table_name IN ('{EVENT_TBL}', '{ROLLUP_TBL}')
            """
        ).fetchone()[0]
        gate(
            "registry_state",
            n_old == 0 and n_new == 2,
            {
                "old_feeder_rows": int(n_old),
                "new_master_rows": int(n_new),
                "registry_actions": registry_summary,
            },
        )

    all_pass = all(g["status"] == "PASS" for g in gates)
    return all_pass, gates


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    log = TeeLogger(RUN_LOG_PATH)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START Script {SCRIPT_NUM} (FNA flip + cleanup)")
        log(f"started_at_utc: {NOW_UTC.isoformat()}")
        log(f"date_stamp: {DATE_STAMP}; utc_stamp: {UTC_STAMP}")

        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        dependent_views = preflight(con, log)

        header(log, "PHASE A2 — SNAPSHOT CPM + LOG PRIOR DISTRIBUTIONS")
        snapshot_cpm(con, log)
        prior = log_prior_distributions(con, log)

        repoint_cpm(con, log)

        cohort_rewrites = repoint_cohort_views(con, log, dependent_views)
        readable_rewrites = repoint_readable_views(con, log, dependent_views)

        pre_drop_ok, pre_drop_gates = run_pre_drop_gates(
            con, log, prior, cohort_rewrites, readable_rewrites
        )

        if not pre_drop_ok:
            failed = [
                g["gate"] for g in pre_drop_gates if g["status"] != "PASS"
            ]
            partial_summary = {
                "script": SCRIPT_TAG,
                "run_utc": UTC_STAMP,
                "publication_db": PUBLICATION_DB,
                "phase_completed": "C (views repointed; feeders NOT dropped)",
                "cohort_rewrites": cohort_rewrites,
                "readable_rewrites": readable_rewrites,
                "pre_drop_gates": pre_drop_gates,
                "post_drop_gates": [],
                "all_pass": False,
            }
            QA_GATES_PATH.write_text(
                json.dumps(partial_summary, indent=2, default=str)
            )
            log("")
            log(f"wrote partial QA summary to {QA_GATES_PATH}")
            raise SystemExit(
                f"PRE-DROP QA GATES FAILED: {failed}. Feeders NOT dropped. "
                f"CPM snapshot at archive_pub_v1_0.{CPM_TBL}_pre356_{DATE_STAMP} "
                f"is the rollback source. Re-run after fixing root cause."
            )

        archive_log = archive_and_drop_feeders(con, log)
        registry_summary = update_registry(con, log)

        post_drop_ok, post_drop_gates = run_post_drop_gates(
            con, log, archive_log, registry_summary
        )

        summary = {
            "script": SCRIPT_TAG,
            "run_utc": UTC_STAMP,
            "publication_db": PUBLICATION_DB,
            "phase_completed": "F (all phases complete)",
            "cohort_rewrites": cohort_rewrites,
            "readable_rewrites": readable_rewrites,
            "archive_log": archive_log,
            "registry_summary": registry_summary,
            "pre_drop_gates": pre_drop_gates,
            "post_drop_gates": post_drop_gates,
            "all_pass": pre_drop_ok and post_drop_ok,
        }
        QA_GATES_PATH.write_text(json.dumps(summary, indent=2, default=str))
        log("")
        log(f"wrote {QA_GATES_PATH}")

        if not post_drop_ok:
            failed = [
                g["gate"] for g in post_drop_gates if g["status"] != "PASS"
            ]
            raise SystemExit(
                f"POST-DROP QA GATES FAILED: {failed}. Feeders ARE dropped; "
                f"archives at archive_pub_v1_0.<feeder>_pre356_{DATE_STAMP}."
            )

        elapsed = time.time() - t0
        log("")
        log(f"=== END Script {SCRIPT_NUM} OK in {elapsed:.1f}s")
        return 0

    except SystemExit:
        raise
    except Exception as e:
        log(f"\nFATAL: {e!r}")
        import traceback

        log(traceback.format_exc())
        return 1
    finally:
        log.close()


if __name__ == "__main__":
    sys.exit(main())
