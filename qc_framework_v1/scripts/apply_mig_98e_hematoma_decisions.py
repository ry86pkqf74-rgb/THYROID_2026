#!/usr/bin/env python3
"""mig_98e — Apply Logan-ratified bulk dispositions for hematoma.

Logan's directive (paraphrased): same protocol as mig_98c — trust date-based
attribution + structured/REAL signals; bulk-apply with no individual review.

Per-pt disposition rule (priority order):
  1. timing_days_post_surgery < 0  →  PMH (PREEXISTING)
  2. timing_days > 5y              →  PMH (NOT_OPERATIVE)
  3. timing_days 1-5y              →  PMH (POSSIBLY_PRIOR_OP)
  4. STRUCTURED_SIGNAL (phen_confirmed OR refined_v5) + timing 0-365d
                                    →  KEEP (OPERATIVE / POSTOP_LATE)
  5. REAL note-text mention + timing 0-365d
                                    →  KEEP (OPERATIVE / POSTOP_LATE)
  6. timing 31-365d (no structured/REAL, defensive)
                                    →  KEEP (POSTOP_LATE)
  7. else (0-30d, no structured, no REAL — template noise)
                                    →  DELETE

evidence_strength standardization on KEEP rows:
  - phen_confirmed AND phen_treat_req       →  definitive
  - phen_confirmed (no treat)               →  probable
  - REAL note-text + 0-30d (no phen)        →  probable
  - REAL note-text + 31-365d                →  probable
  - 31-365d only (no structured / no REAL)  →  possible

Phases:
  1. Snapshot canonical_complications_events_v1 hematoma rows
  2. Compute disposition map (in-memory)
  3. PMH INSERT (before delete) for PMH-attributed pts
  4. DELETE hematoma present + suspected rows for non-keep pts
  5. Standardize evidence_strength on KEEP rows
  6. Rebuild canonical_complications_patient_rollup_v1
  7. Provenance row

Author: Logan Glosser <logan.glosser@gmail.com>
"""
from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PUB_DB = "thyroid_canonical_publication_v1_0"
EVENTS = "main.canonical_complications_events_v1"
PMH_EVENTS = "main.canonical_pmh_events_v1"
ARCHIVE_DB = '"Thyroid 2026 UPdated".archive_pub_v1_0'
PHEN_PRE = f"{ARCHIVE_DB}.complication_phenotype_v1_pre364_20260422_050902"

# REAL/TEMPLATE patterns (same as builder; kept here for self-contained apply)
HEMATOMA_RE = re.compile(r"\bhematoma", re.IGNORECASE)

TEMPLATE_PATTERNS = [
    # Consent risk lists — common pairings: hematoma, seroma, infection, hoarseness, hypocalcemia
    r"\brisks?\s+(?:include|of|involved)[^.]{0,300}hematoma",
    r"\bconsent[^.]{0,200}hematoma",
    r"\bcomplications?\s+(?:include|may include|of\s+(?:thyroid|surgery|the\s+procedure))[^.]{0,250}hematoma",
    r"\bcounseled\s+(?:about|on|regarding)[^.]{0,80}hematoma",
    r"\bdiscussed[^.]{0,150}\b(risks?|complications?|possibilit|sequelae)\b[^.]{0,150}hematoma",
    # Negation
    r"\bno\s+(?:evidence\s+of\s+)?hematoma",
    r"\bwithout\s+(?:a\s+)?hematoma",
    r"\bnegative\s+for\s+hematoma",
    r"\bhematoma\s+(?:was\s+)?not\s+(?:noted|seen|present|identified)",
    r"\bmonitor(?:ed|ing)?\s+for[^.]{0,40}hematoma",
    # Boilerplate co-occurrence with seroma / infection
    r"\bhematoma[, /][^.]{0,80}\bseroma",
    r"\bseroma[, /][^.]{0,80}\bhematoma",
    r"\binfection[, /][^.]{0,40}hematoma",
    r"\bhematoma[, /][^.]{0,80}\binfection",
    # "hemostasis was achieved" — close to template since it's standard op-note language
    r"\bhemostasis\s+(?:was\s+)?(?:achieved|confirmed|secured)",
    # Prophylactic drain placement
    r"\bdrain\s+(?:was\s+)?placed[^.]{0,80}(?:to\s+)?prevent\s+hematoma",
    r"\bto\s+prevent\s+(?:a\s+)?hematoma",
]
REAL_PATTERNS = [
    # Drainage / evacuation / aspiration
    r"\b(?:drained|drainage|aspirat(?:ed|ion)|evacuat(?:ed|ion)|incision\s+and\s+drainage|i\s*&\s*d)\s+(?:of\s+)?(?:the\s+)?hematoma",
    r"\bhematoma[^.]{0,120}\b(?:drained|drainage|aspirat(?:ed|ion)|evacuat(?:ed|ion))",
    r"\bneedle\s+aspirat(?:ed|ion)[^.]{0,80}hematoma",
    # Reoperation
    r"\b(?:return(?:ed)?\s+to|back\s+to|brought\s+back\s+to)\s+(?:the\s+)?(?:OR|operating\s+room)[^.]{0,80}hematoma",
    r"\breoperat(?:ion|ed)\s+for[^.]{0,80}hematoma",
    r"\b(?:wound|neck)\s+exploration\s+for\s+hematoma",
    # Active progression / clinical
    r"\bexpanding\s+hematoma",
    r"\b(?:large|massive|significant)\s+hematoma",
    r"\b(?:airway\s+compromise|stridor|respiratory\s+distress)[^.]{0,80}hematoma",
    # Documented complication language
    r"\b(?:complicated\s+by|notable for|c\/b)\s+(?:a\s+|the\s+)?hematoma",
    r"\bs/p[^.]{0,60}c\/b[^.]{0,60}hematoma",
    r"\b(?:diagnosis|admit(?:ted|ting)?(?:\s+with)?)[^.]{0,80}hematoma",
    # Active management / persistence
    r"\bhematoma[^.]{0,120}\b(?:persistent|persisted|prolonged|increasing|recurrent|expanding)",
    r"\bhematoma\s+(?:was\s+)?(?:treated|managed|addressed|noted|developed|present|formed)",
    r"\bhematoma[^.]{0,120}(?:incision|wound|neck)\s+(?:exploration|reopened|opened)",
    # Postop day mention
    r"\b(?:on |at |started\s+on\s+)POD\s*\d+[^.]{0,80}hematoma",
    r"hematoma[^.]{0,80}\bPOD\s*\d+",
    # Pressure dressing for hematoma
    r"\bpressure\s+dressing[^.]{0,40}hematoma",
    r"hematoma[^.]{0,40}\bpressure\s+dressing",
]

TEMPLATE_RE = [re.compile(p, re.IGNORECASE) for p in TEMPLATE_PATTERNS]
REAL_RE = [re.compile(p, re.IGNORECASE) for p in REAL_PATTERNS]


def _classify_ctx(ctx):
    is_t = any(r.search(ctx) for r in TEMPLATE_RE)
    is_r = any(r.search(ctx) for r in REAL_RE)
    return is_t, is_r


def _connect_md():
    from motherduck_client import MotherDuckClient, MotherDuckConfig

    cfg = MotherDuckConfig(database=PUB_DB)
    con = MotherDuckClient(cfg).connect_rw()
    con.execute(f"USE {PUB_DB}")
    return con


def _load_364():
    path = REPO_ROOT / "scripts" / "364_complications_consolidation.py"
    spec = importlib.util.spec_from_file_location("complications_364", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def compute_dispositions(con):
    """Return dict[rid] = {decision, evidence_strength, basis}."""
    rids = [r[0] for r in con.execute(
        f"SELECT DISTINCT research_id FROM {EVENTS} "
        f"WHERE complication_type='hematoma' AND finding_status IN ('present','suspected')"
    ).fetchall()]
    rid_list = "','".join(rids)

    # First-surgery + earliest finding date (cast both to DATE for diff)
    timing_q = (
        f"SELECT c.research_id, "
        f"DATE_DIFF('day', "
        f"  CAST(MIN(op.surgery_date_native) AS DATE), "
        f"  CAST(MIN(c.finding_date) AS DATE)) AS days "
        f"FROM {EVENTS} c "
        f"JOIN main.canonical_operative_events_v1 op USING (research_id) "
        f"WHERE c.complication_type='hematoma' AND c.finding_status IN ('present','suspected') "
        f"AND c.research_id IN ('{rid_list}') "
        f"GROUP BY c.research_id"
    )
    timing = {r[0]: r[1] for r in con.execute(timing_q).fetchall()}

    # Structured signal (phen_confirmed OR refined_v5)
    struct_q = (
        f"WITH phen_conf AS ( "
        f"  SELECT DISTINCT c.research_id "
        f"  FROM {EVENTS} c "
        f"  LEFT JOIN {PHEN_PRE} phen "
        f"    ON c.source_table='complication_phenotype_v1' "
        f"   AND c.source_row_id=CAST(hash(phen.research_id, phen.complication_entity, phen.detection_date) AS VARCHAR) "
        f"  WHERE c.complication_type='hematoma' AND phen.confirmed_flag=TRUE "
        f"), refined AS ( "
        f"  SELECT DISTINCT research_id FROM {EVENTS} "
        f"  WHERE complication_type='hematoma' AND source_table='extracted_complications_refined_v5' "
        f")"
        f"SELECT research_id FROM phen_conf UNION SELECT research_id FROM refined"
    )
    struct_set = {r[0] for r in con.execute(struct_q).fetchall()}

    # Phenotype flags for evidence_strength
    phen_q = (
        f"SELECT phen.research_id::VARCHAR, "
        f"BOOL_OR(phen.confirmed_flag) AS confirmed, "
        f"BOOL_OR(phen.treatment_requiring_flag) AS treat_req "
        f"FROM {PHEN_PRE} phen "
        f"WHERE phen.research_id::VARCHAR IN ('{rid_list}') "
        f"AND phen.complication_entity ILIKE '%hematoma%' "
        f"GROUP BY phen.research_id"
    )
    phen_flags = {r[0]: (r[1], r[2]) for r in con.execute(phen_q).fetchall()}

    # Real-text classification
    con.execute("CREATE OR REPLACE TEMP TABLE tmp_pts (rid VARCHAR)")
    con.executemany("INSERT INTO tmp_pts VALUES (?)", [(r,) for r in rids])
    notes = con.execute(
        "SELECT cnl.research_id, cnl.note_text "
        "FROM main.clinical_notes_long cnl JOIN tmp_pts t ON cnl.research_id = t.rid "
        "WHERE POSITION('hematoma' IN LOWER(cnl.note_text)) > 0"
    ).fetchall()

    has_real = defaultdict(bool)
    for rid, txt in notes:
        if not txt:
            continue
        for m in HEMATOMA_RE.finditer(txt):
            pos = m.start()
            ctx = txt[max(0, pos - 200): pos + 300]
            is_t, is_r = _classify_ctx(ctx)
            if is_r:
                has_real[rid] = True
                break  # one is enough

    # Compute disposition per rid
    dispositions = {}
    for rid in rids:
        td = timing.get(rid)
        is_struct = rid in struct_set
        is_real = has_real.get(rid, False)
        ph = phen_flags.get(rid, (None, None))

        # priority order
        if td is not None and td < 0:
            d = "PMH_PREEXISTING"
            es = None
            basis = f"timing_days={td} (pre-surgery)"
        elif td is not None and td > 365 * 5:
            d = "PMH_NOT_OPERATIVE"
            es = None
            basis = f"timing_days={td} (>5y)"
        elif td is not None and td > 365:
            d = "PMH_POSSIBLY_PRIOR_OP"
            es = None
            basis = f"timing_days={td} (1-5y)"
        elif is_struct and td is not None and 0 <= td <= 365:
            d = "KEEP_OPERATIVE" if td <= 30 else "KEEP_POSTOP_LATE"
            if ph[0] and ph[1]:
                es = "definitive"
            elif ph[0]:
                es = "probable"
            else:
                es = "probable"
            basis = f"structured_signal=TRUE; timing_days={td}"
        elif is_real and td is not None and 0 <= td <= 365:
            d = "KEEP_OPERATIVE" if td <= 30 else "KEEP_POSTOP_LATE"
            es = "probable"
            basis = f"REAL note-text; timing_days={td}"
        elif td is not None and 31 <= td <= 365:
            d = "KEEP_POSTOP_LATE"
            es = "possible"
            basis = f"timing_days={td} (defensive keep, no structured/REAL)"
        elif td is not None and 0 <= td <= 30:
            d = "DELETE"
            es = None
            basis = f"timing_days={td}; no structured/REAL — template FP"
        else:
            d = "DELETE"
            es = None
            basis = f"timing_days={td}; no structured/REAL — default delete"

        dispositions[rid] = {
            "decision": d,
            "evidence_strength": es,
            "basis": basis,
            "is_structured": is_struct,
            "is_real": is_real,
            "timing_days": td,
        }

    return dispositions


def _counts(con):
    def one(sql):
        r = con.execute(sql).fetchone()
        return int(r[0]) if r and r[0] is not None else 0
    return {
        "present_rows": one(f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='hematoma' AND finding_status='present'"),
        "present_pts": one(f"SELECT COUNT(DISTINCT research_id) FROM {EVENTS} WHERE complication_type='hematoma' AND finding_status='present'"),
        "suspected_rows": one(f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='hematoma' AND finding_status='suspected'"),
        "absent_rows": one(f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='hematoma' AND finding_status='absent'"),
        "pmh_hematoma": one(f"SELECT COUNT(*) FROM {PMH_EVENTS} WHERE finding_value_norm='hematoma'"),
    }


def phase_snapshot(con, do_writes, ts_tag):
    snap = f'{ARCHIVE_DB}.canonical_complications_events_v1_pre98e_hematoma_{ts_tag}'
    if do_writes:
        con.execute(f"CREATE TABLE {snap} AS SELECT * FROM {EVENTS} WHERE complication_type='hematoma'")
        n = con.execute(f"SELECT COUNT(*) FROM {snap}").fetchone()[0]
        print(f"  snapshot {snap}: {n} rows")
    else:
        n = con.execute(f"SELECT COUNT(*) FROM {EVENTS} WHERE complication_type='hematoma'").fetchone()[0]
        print(f"  [dry-run] would snapshot ({n} rows)")


def phase_pmh_insert(con, do_writes, dispositions):
    pmh_rids = [(rid, info) for rid, info in dispositions.items()
                if info["decision"].startswith("PMH_")]
    print(f"  PMH INSERT — {len(pmh_rids)} rids")

    inserted = 0
    for rid, info in pmh_rids:
        existing = con.execute(
            f"SELECT COUNT(*) FROM {PMH_EVENTS} "
            f"WHERE research_id='{rid}' AND finding_value_norm='hematoma'"
        ).fetchone()[0]
        if int(existing) > 0:
            continue

        # Earliest hematoma finding_date for this pt
        edt_row = con.execute(
            f"SELECT MIN(finding_date)::VARCHAR FROM {EVENTS} "
            f"WHERE research_id='{rid}' AND complication_type='hematoma' "
            f"AND finding_status IN ('present','suspected')"
        ).fetchone()
        edt = edt_row[0] if edt_row else None
        if not edt:
            continue

        decision_label = info["decision"].replace("PMH_", "")
        finding_text = (
            f"hematoma attributed to {decision_label} ({info['basis']}); "
            f"mig_98e_classifier_logan_curated"
        ).replace("'", "''")

        if not do_writes:
            inserted += 1
            continue

        sha_input = finding_text
        src_row_sql = (
            f"CAST(hash('{rid}', 'hematoma', CAST('{edt}' AS DATE), 'mig98e_pmh_synthetic') AS VARCHAR)"
        )
        sql = f"""
        INSERT INTO {PMH_EVENTS} (
          research_id, source_table, source_row_id, source_note_type,
          llm_confidence, extractor_name, finding_text, finding_value, finding_value_norm,
          finding_date, mention_note_date, finding_status, evidence_strength,
          days_from_first_thyroidectomy, is_preexisting, anchor_source, med_status,
          evidence_span_hash, build_ts
        )
        SELECT
          '{rid}', 'mig_98e_pmh_synthetic', {src_row_sql}, 'other_history',
          NULL, 'mig_98e_hematoma_classifier_logan_curated',
          '{finding_text}', 'hematoma', 'hematoma',
          CAST('{edt}' AS DATE), CAST('{edt}' AS DATE),
          'present', 'definitive', NULL, TRUE,
          'mig_98e_classifier_logan_curated', NULL,
          sha256('{sha_input}'),
          CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        """
        con.execute(sql)
        inserted += 1

    if do_writes:
        print(f"  PMH INSERT — inserted {inserted} rows")
    else:
        print(f"  [dry-run] would INSERT {inserted} PMH rows")


def phase_delete_non_keep(con, do_writes, dispositions):
    keep_rids = [rid for rid, info in dispositions.items()
                 if info["decision"].startswith("KEEP_")]
    keep_sql = "(" + ",".join(f"'{r}'" for r in keep_rids) + ")"
    pre_present = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='hematoma' AND finding_status='present' "
        f"AND research_id NOT IN {keep_sql}"
    ).fetchone()[0]
    pre_susp = con.execute(
        f"SELECT COUNT(*) FROM {EVENTS} "
        f"WHERE complication_type='hematoma' AND finding_status='suspected' "
        f"AND research_id NOT IN {keep_sql}"
    ).fetchone()[0]
    if do_writes:
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='hematoma' AND finding_status='present' "
            f"AND research_id NOT IN {keep_sql}"
        )
        con.execute(
            f"DELETE FROM {EVENTS} "
            f"WHERE complication_type='hematoma' AND finding_status='suspected' "
            f"AND research_id NOT IN {keep_sql}"
        )
        print(f"  DELETE non-keep present: {pre_present}; suspected: {pre_susp}")
    else:
        print(f"  [dry-run] would DELETE {pre_present} present + {pre_susp} suspected")


def phase_standardize_evidence(con, do_writes, dispositions):
    by_es = defaultdict(list)
    for rid, info in dispositions.items():
        if info["decision"].startswith("KEEP_") and info.get("evidence_strength"):
            by_es[info["evidence_strength"]].append(rid)
    for es, rids in by_es.items():
        rid_sql = "(" + ",".join(f"'{r}'" for r in rids) + ")"
        pre = con.execute(
            f"SELECT COUNT(*) FROM {EVENTS} "
            f"WHERE complication_type='hematoma' AND finding_status='present' "
            f"AND research_id IN {rid_sql} AND evidence_strength != '{es}'"
        ).fetchone()[0]
        if do_writes:
            con.execute(
                f"UPDATE {EVENTS} "
                f"SET evidence_strength='{es}', build_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP) "
                f"WHERE complication_type='hematoma' AND finding_status='present' "
                f"AND research_id IN {rid_sql}"
            )
            print(f"  evidence_strength → {es}: {pre} rows ({len(rids)} pts)")
        else:
            print(f"  [dry-run] would set evidence_strength → {es} for {pre} rows ({len(rids)} pts)")


def phase_provenance(con, do_writes):
    run_id = f"mig98e_hematoma_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    if not do_writes:
        print(f"  [dry-run] would INSERT provenance run_id={run_id}")
        return
    con.execute(
        "DELETE FROM manuscript_workspace.cpm_reconciliation_provenance_v1 WHERE run_id = ?",
        [run_id],
    )
    con.execute(
        """
        INSERT INTO manuscript_workspace.cpm_reconciliation_provenance_v1
          (run_id, started_at, ended_at, phases_applied,
           critical_findings_cleared, high_findings_cleared,
           med_findings_cleared, held_for_adjudication)
        VALUES (
            ?,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            'mig98e_hematoma;snapshot;pmh_insert;delete_non_keep;evidence_strength;rollup_step5',
            '0', '0', '0',
            'bulk-applied: 873 pts → KEEP/PMH/DELETE per priority rule'
        )
        """,
        [run_id],
    )
    print(f"  provenance — inserted {run_id}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-rollup", action="store_true")
    parser.add_argument("--skip-snapshot", action="store_true")
    args = parser.parse_args()

    do_writes = bool(args.apply)
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"mig_98e hematoma apply — do_writes={do_writes}  ts={ts_tag}")
    con = _connect_md()
    try:
        before = _counts(con)
        print("  pre-counts:", before)

        # 1. snapshot
        if not args.skip_snapshot:
            phase_snapshot(con, do_writes, ts_tag)

        # 2. compute dispositions
        dispositions = compute_dispositions(con)
        from collections import Counter
        c = Counter(info["decision"] for info in dispositions.values())
        print("  Disposition distribution:")
        for k, v in c.most_common():
            print(f"    {v:4}  {k}")

        # 3. PMH insert (before delete)
        phase_pmh_insert(con, do_writes, dispositions)

        # 4. DELETE non-keep
        phase_delete_non_keep(con, do_writes, dispositions)

        # 5. Standardize evidence_strength
        phase_standardize_evidence(con, do_writes, dispositions)

        # 6. Rebuild rollup
        if not args.skip_rollup and do_writes:
            mod = _load_364()
            print("  Script 364 step_5_build_rollup running...")
            mod.step_5_build_rollup(con, True)
            print("  Script 364 step_5_build_rollup OK")
        elif args.skip_rollup:
            print("  SKIP rollup")
        else:
            print("  [dry-run] would run Script 364 step_5_build_rollup")

        # 7. provenance
        phase_provenance(con, do_writes)

        after = _counts(con)
        print("  post-counts:", after)
        print("mig_98e — complete")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
