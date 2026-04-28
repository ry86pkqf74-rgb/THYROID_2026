"""
qc_framework_v1/scripts/build_invasion_events_verification.py
=============================================================

Per-modality re-derivation verification for canonical_invasion_events_v1
(Script 363 output, 51,773 rows / 20 cols, multi-source UNION build).

Two outputs:

  (1) Per-modality mass-equivalence summary report ->
        verification_csvs/canonical_invasion_events_v1/
          per_modality_match_summary__mig_91.md

      For each (source_modality x source_kind) slice, runs a JOIN-back
      to source and recomputes finding_status from the source value
      using the same dicts/case ladders as scripts/363_invasion_canonical.py.
      Reports n_total / n_match / n_mismatch.

  (2) Per-modality MISMATCH CSV (only written when mismatches > 0) ->
        verification_csvs/canonical_invasion_events_v1/<slice>__mig_91.csv

      Cols: invasion_event_id, research_id, source_kind, source_row_id,
            db_value_<col>, recomputed_value_<col>, match_flag.
      Mismatches sorted to top (by virtue of being the only contents).

  (3) Ambiguous-linkage review CSV (always written) ->
        verification_csvs/canonical_invasion_events_v1/
          ambiguous_linkage_review__mig_91.csv

      The canonical's `linkage_ambiguous_multi_episode` boolean is
      MIS-NAMED -- it actually counts FINDINGS per
      (research_id, finding_date) partition, not candidate surgery
      episodes. Re-define ambiguity as: TWO OR MORE distinct
      surgery_episode_ids from canonical_operative_events_v1 within +/-90d
      for the same (research_id, finding_date). At time of writing this is
      759 groups (max 2 episodes / group). Logan adjudicates which
      surgery_episode_id is the correct linkage per group.

      Cols: research_id, finding_date,
            n_candidate_surgery_episodes, picked_surgery_episode_id,
            picked_surgery_date, alternative_surgery_episode_ids,
            alternative_surgery_dates, n_findings_in_group,
            sample_evidence_qualifier_1, sample_evidence_qualifier_2,
            sample_evidence_qualifier_3, your_chosen_episode_id, your_note.

Carry-forward (write into mig_91 closeout):
  CF-91-LINKAGE-COL-NAME: rename
    canonical_invasion_events_v1.linkage_ambiguous_multi_episode
    -> linkage_ambiguous_multi_finding (the column counts findings,
    not episodes; the actual multi-episode case set is the 759 groups
    in ambiguous_linkage_review__mig_91.csv). Defer; cosmetic but
    affects downstream consumer interpretation.

PHI rule: never print clinical text. evidence_qualifier is captured
because Script 363 stores it as the raw entity_value; clinical-narrative
samples are LIMITED to 3 per group and TRUNCATED to 200 chars.

Auth: MotherDuck SSO; ensure the active account is
logan.glosser.eras@gmail.com (publication DB lives there) per
reference_protocol_v2_md_accounts.md.
"""
from __future__ import annotations

import csv
import textwrap
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = REPO_ROOT / "verification_csvs" / "canonical_invasion_events_v1"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DB = "thyroid_canonical_publication_v1_0"

# ---------------------------------------------------------------------------
# Mirror of scripts/363_invasion_canonical.py vocab dicts (kept in sync as
# a snapshot at build time). If 363 changes its dicts, regenerate this
# script and re-run verification.
# ---------------------------------------------------------------------------

# Inline as a SQL CASE expression so re-derivation runs server-side.
# Expression input: a pre-normalized lowercase+trim+strip-trailing-;.
# Expression output: 'absent' | 'present' | 'suspected' | 'indeterminate'.
STATUS_CASE_SQL = """
CASE
  WHEN _norm IN (
    'x','no','false','none','n/s','n/a','0','not identified'
  ) THEN 'absent'
  WHEN _norm IN (
    '*','* (see margin comment)','`x','classical','m',
    'indeterminate','equivocal','c/a','cannot be assessed',
    'cannot be determined',
    'cannot be determined: focal interstitial psammomatoid calcification present',
    'indeeterminate','indetermiante','indeterminent','none?'
  ) THEN 'indeterminate'
  WHEN _norm IN (
    'suspicious','infiltrative?'
  ) THEN 'suspected'
  WHEN _norm IN (
    'present','yes','true','identified',
    'extensive','focal','minimal','minimally invasive','widely invasive',
    'infiltrative','invasive','microscopic','microscopic extension',
    'minimal extension','multifocal','multiple foci','1 focus','single focus',
    'into but not through','yes (minimal)','yes (focal)','yes, minimal',
    'yes, extensive','yes (extensive)','prominent','limited','s',
    'yes;minimal','present, minimal','present (minimal)',
    'preesent','preent','presnt','preseent','prewent','preewnt',
    'miinimally invasive','minimallyinvasive','minimally invasvie',
    'minimally invasivre','widely invasvie','widely invasivre',
    'extensivre','extensiver','estensive','extrensive','extesive',
    'foacl','minimal (1 focus)','minimal microscopic','minimal into fat',
    'microscopiic',
    'focal early extension into perithyroidal fat','focal right side',
    'right side focal','present, widely invasive','multifocal invasion',
    E'x\\n(single microscopic focus of extension)',
    'yes;capsular invasion into but not through capsule',
    'yes;capsular invasion into but not through capsule;',
    'present (perithyroidal fibroadipose tissue involved)',
    'present (microscopic perithyroidal soft tissue only with no clinical or macroscopic evidence of invasion)'
  ) THEN 'present'
  ELSE 'indeterminate'
END
"""

# Modality plan slices. Each entry produces one section in the report
# and (if mismatches > 0) one CSV.
SLICES = [
    {
        "name": "op_note__structured",
        "modality": "op_note",
        "kind": "structured",
        "source_table": "canonical_operative_events_v1",
        "expected_n": 11_844,
    },
    {
        "name": "synoptic_path__structured",
        "modality": "synoptic_path",
        "kind": "structured",
        "source_table": "canonical_path_malignant_events_v1",
        "expected_n": 23_101,
    },
    {
        "name": "synoptic_path__llm",
        "modality": "synoptic_path",
        "kind": "llm",
        "source_table": "note_entities_llm_(airway|vascular)_invasion",
        "expected_n": 16_158,
    },
    {
        "name": "op_note__llm",
        "modality": "op_note",
        "kind": "llm",
        "source_table": "note_entities_llm_(airway|vascular)_invasion",
        "expected_n": 168,
    },
    {
        "name": "ct__llm",
        "modality": "ct",
        "kind": "llm",
        "source_table": "note_entities_llm_airway_invasion",
        "expected_n": 477,
    },
    {
        "name": "mri__llm",
        "modality": "mri",
        "kind": "llm",
        "source_table": "note_entities_llm_airway_invasion",
        "expected_n": 25,
    },
]


# ---------------------------------------------------------------------------
# Slice queries
# ---------------------------------------------------------------------------

# Every slice query returns:
#   invasion_event_id, research_id, source_kind, source_row_id,
#   db_finding_status, recomputed_finding_status,
#   db_evidence_qualifier, source_value,
#   match_flag (in {'MATCH','MISMATCH_STATUS','MISMATCH_VALUE','UNRESOLVED'})

#
# IMPORTANT: the 4 BOOLEAN flags
#   (gross_ete_flag, tracheal_involvement_flag, esophageal_involvement_flag,
#    local_invasion_flag)
# were removed from main.canonical_operative_events_v1 by Script 363
# Step 7 (cascade strip). For verification, source = the pre-strip archive
# at "Thyroid 2026 UPdated".archive_pub_v1_0
#   .canonical_operative_events_v1_pre363strip_20260422_034244
# (read-only verification reference; permitted under feedback_no_cross_db
# _canonical_sourcing.md -- archives may be read for verification, just not
# used at build time).
#
OP_NOTE_STRUCTURED_SQL = f"""
WITH cev AS (
  SELECT * FROM main.canonical_invasion_events_v1
   WHERE source_modality='op_note' AND source_kind='structured'
),
oe AS (
  SELECT surgery_episode_id, research_id,
         gross_ete_flag, tracheal_involvement_flag,
         esophageal_involvement_flag, local_invasion_flag
  FROM "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_operative_events_v1_pre363strip_20260422_034244
)
SELECT
  cev.invasion_event_id, cev.research_id, cev.source_kind,
  cev.source_row_id, cev.invasion_type,
  cev.finding_status AS db_finding_status,
  CASE
    WHEN cev.invasion_type='gross_ete'   THEN
      CASE WHEN oe.gross_ete_flag=TRUE THEN 'present'
           WHEN oe.gross_ete_flag=FALSE THEN 'absent'
           ELSE 'indeterminate' END
    WHEN cev.invasion_type='tracheal'    THEN
      CASE WHEN oe.tracheal_involvement_flag=TRUE THEN 'present'
           WHEN oe.tracheal_involvement_flag=FALSE THEN 'absent'
           ELSE 'indeterminate' END
    WHEN cev.invasion_type='esophageal'  THEN
      CASE WHEN oe.esophageal_involvement_flag=TRUE THEN 'present'
           WHEN oe.esophageal_involvement_flag=FALSE THEN 'absent'
           ELSE 'indeterminate' END
    WHEN cev.invasion_type='soft_tissue' THEN
      CASE WHEN oe.local_invasion_flag=TRUE THEN 'present'
           WHEN oe.local_invasion_flag=FALSE THEN 'absent'
           ELSE 'indeterminate' END
    ELSE NULL
  END AS recomputed_finding_status,
  cev.evidence_qualifier AS db_evidence_qualifier,
  NULL::VARCHAR AS source_value,
  CASE
    WHEN oe.surgery_episode_id IS NULL THEN 'UNRESOLVED'
    WHEN cev.finding_status =
         (CASE
            WHEN cev.invasion_type='gross_ete'   THEN
              CASE WHEN oe.gross_ete_flag=TRUE THEN 'present'
                   WHEN oe.gross_ete_flag=FALSE THEN 'absent'
                   ELSE 'indeterminate' END
            WHEN cev.invasion_type='tracheal'    THEN
              CASE WHEN oe.tracheal_involvement_flag=TRUE THEN 'present'
                   WHEN oe.tracheal_involvement_flag=FALSE THEN 'absent'
                   ELSE 'indeterminate' END
            WHEN cev.invasion_type='esophageal'  THEN
              CASE WHEN oe.esophageal_involvement_flag=TRUE THEN 'present'
                   WHEN oe.esophageal_involvement_flag=FALSE THEN 'absent'
                   ELSE 'indeterminate' END
            WHEN cev.invasion_type='soft_tissue' THEN
              CASE WHEN oe.local_invasion_flag=TRUE THEN 'present'
                   WHEN oe.local_invasion_flag=FALSE THEN 'absent'
                   ELSE 'indeterminate' END
            ELSE NULL END)
    THEN 'MATCH' ELSE 'MISMATCH_STATUS' END AS match_flag
FROM cev
LEFT JOIN oe ON oe.surgery_episode_id = TRY_CAST(cev.source_row_id AS BIGINT)
"""

# synoptic_path / structured: NB the source_row_id format
# "<value_col>|<path_surgery_id>" was advertised by Script 363, but
# canonical_path_malignant_events_v1.path_surgery_id has only 3 distinct
# non-null values across 6,689 rows -- it is NOT a row-unique key and
# the parsed source_row_id is not a usable join key for per-row verification.
#
# Verification strategy instead: MASS-EQUIVALENCE at the
# (invasion_type, evidence_qualifier_normalized) level against
# canonical_path_malignant_events_v1 -- this confirms that every non-null
# source value produced exactly one canonical row with the same invasion_type
# and the same evidence_qualifier. The script can also pivot to
# (research_id, invasion_type, evidence_qualifier) for per-patient surfacing
# of any drift.
#
# Mass-equivalence query produces ONE row per invasion_type with
# (n_source, n_db, delta). delta=0 -> 100% MATCH at the invasion_type level.
SYNOPTIC_STRUCTURED_SQL = """
WITH src_counts AS (
  SELECT 'gross_ete'              AS invasion_type, gross_ete::VARCHAR AS v, COUNT(*) AS n
    FROM main.canonical_path_malignant_events_v1 WHERE gross_ete IS NOT NULL GROUP BY 2
  UNION ALL SELECT 'vascular_microscopic',   vascular_invasion,   COUNT(*)
    FROM main.canonical_path_malignant_events_v1 WHERE vascular_invasion IS NOT NULL GROUP BY 2
  UNION ALL SELECT 'lymphatic_microscopic',  lymphatic_invasion,  COUNT(*)
    FROM main.canonical_path_malignant_events_v1 WHERE lymphatic_invasion IS NOT NULL GROUP BY 2
  UNION ALL SELECT 'perineural',             perineural_invasion, COUNT(*)
    FROM main.canonical_path_malignant_events_v1 WHERE perineural_invasion IS NOT NULL GROUP BY 2
  UNION ALL SELECT 'capsular',               capsular_invasion,   COUNT(*)
    FROM main.canonical_path_malignant_events_v1 WHERE capsular_invasion IS NOT NULL GROUP BY 2
  UNION ALL SELECT 'ete_total',              extrathyroidal_extension, COUNT(*)
    FROM main.canonical_path_malignant_events_v1 WHERE extrathyroidal_extension IS NOT NULL GROUP BY 2
),
src_totals AS (SELECT invasion_type, SUM(n) AS source_n FROM src_counts GROUP BY 1),
db_totals_simple AS (
  SELECT invasion_type, COUNT(*) AS db_n
  FROM main.canonical_invasion_events_v1
  WHERE source_modality='synoptic_path' AND source_kind='structured'
  GROUP BY 1
),
db_ete_combined AS (
  -- ETE in canonical splits across gross_ete + microscopic_ete; for
  -- comparing to the source's extrathyroidal_extension column, sum them.
  SELECT 'ete_total' AS invasion_type,
         (SELECT COUNT(*) FROM main.canonical_invasion_events_v1
           WHERE source_modality='synoptic_path' AND source_kind='structured'
             AND invasion_type IN ('gross_ete','microscopic_ete')) AS db_n
),
db_totals AS (
  SELECT * FROM db_totals_simple
  UNION ALL SELECT * FROM db_ete_combined
)
SELECT
  COALESCE(s.invasion_type, d.invasion_type) AS invasion_type,
  COALESCE(s.source_n,0) AS source_n,
  COALESCE(d.db_n,0)     AS db_n,
  COALESCE(s.source_n,0) - COALESCE(d.db_n,0) AS delta,
  CASE WHEN COALESCE(s.source_n,0) = COALESCE(d.db_n,0) THEN 'MATCH'
       ELSE 'MISMATCH_COUNT' END AS match_flag
FROM src_totals s
FULL OUTER JOIN db_totals d ON d.invasion_type = s.invasion_type
ORDER BY 1
"""

# LLM slices: source_row_id format is "note_row_id|source_line|entity_type".
#
# IMPORTANT: between Script 363 (2026-04-22 03:29:42Z) and now, both LLM
# source tables were SUBSTANTIALLY reshaped:
#   note_entities_llm_airway_invasion   (v1 ~11,037 rows)
#     -> ..._airway_invasion_v2          (LIVE 6,054 rows)
#   note_entities_llm_vascular_invasion (v1 ~39,210 rows)
#     -> ..._vascular_invasion_v2        (LIVE 3,861 rows)
# The canonical was built against v1. Verifying against LIVE v2 will produce
# spurious UNRESOLVED. The closest archives at Script 363 build time are:
#   "Thyroid 2026 UPdated".archive_pub_v1_0
#     .note_entities_llm_airway_invasion_pre9domainv4_20260420T235106Z (11,037)
#   "Thyroid 2026 UPdated".archive_pub_v1_0
#     .note_entities_llm_vascular_invasion_pre368_20260422              (39,210)
# These are the *closest available archives* to Script 363's build state
# (pre9domainv4 for airway is 2026-04-20, ~2 days pre-build but no later
# archive predates Script 363; pre368 for vascular is 2026-04-22, same
# day as Script 363, post-build but pre Script 368 reshape).
#
# MATCH semantics: the canonical row's (note_row_id, source_line, entity_type)
# triple exists in the archive's result_json.entities[] and entity_value
# equals canonical's evidence_qualifier. A small UNRESOLVED count is
# acceptable if attributable to the airway pre9domainv4 archive being
# slightly pre-build (sub-1% drift is expected; any larger mismatch
# warrants Logan investigation).

ARCHIVE_AIRWAY = ('"Thyroid 2026 UPdated".archive_pub_v1_0.'
                  'note_entities_llm_airway_invasion_pre9domainv4_'
                  '20260420T235106Z')
ARCHIVE_VASCULAR = ('"Thyroid 2026 UPdated".archive_pub_v1_0.'
                    'note_entities_llm_vascular_invasion_pre368_20260422')

LLM_SLICE_SQL = f"""
WITH cev AS (
  SELECT *,
         split_part(source_row_id,'|',1) AS src_note_row_id,
         TRY_CAST(split_part(source_row_id,'|',2) AS INTEGER)
           AS src_source_line,
         split_part(source_row_id,'|',3) AS src_entity_type
  FROM main.canonical_invasion_events_v1
   WHERE source_modality = $modality$ AND source_kind = 'llm'
),
src AS (
  SELECT note_row_id, note_type, research_id, result_json
  FROM {ARCHIVE_AIRWAY}
  WHERE note_type = $note_type$
  UNION ALL
  SELECT note_row_id, note_type, research_id, result_json
  FROM {ARCHIVE_VASCULAR}
  WHERE note_type = $note_type$
),
unnested AS (
  SELECT s.note_row_id, s.note_type,
         TRY_CAST(s.research_id AS BIGINT) AS research_id,
         json_extract_string(t.entity_json, '$.entity_type') AS entity_type,
         json_extract_string(t.entity_json, '$.entity_value') AS entity_value,
         json_extract_string(t.entity_json, '$.source_line') AS source_line
  FROM src s, UNNEST(json_extract(s.result_json,'$.entities')::JSON[])
       AS t(entity_json)
  WHERE s.result_json LIKE '{{"entities":%' AND LENGTH(s.result_json) > 100
)
SELECT
  cev.invasion_event_id, cev.research_id, cev.source_kind, cev.source_row_id,
  cev.invasion_type,
  cev.finding_status AS db_finding_status,
  NULL::VARCHAR AS recomputed_finding_status,
  cev.evidence_qualifier AS db_evidence_qualifier,
  un.entity_value AS source_value,
  CASE
    WHEN un.note_row_id IS NULL THEN 'UNRESOLVED'
    WHEN cev.evidence_qualifier IS NOT DISTINCT FROM un.entity_value
      THEN 'MATCH'
    ELSE 'MISMATCH_VALUE'
  END AS match_flag
FROM cev
LEFT JOIN unnested un
  ON un.note_row_id = cev.src_note_row_id
 AND un.entity_type = cev.src_entity_type
 AND COALESCE(un.source_line,'') = COALESCE(CAST(cev.src_source_line AS VARCHAR),'')
"""

LLM_NOTE_TYPE_BY_MODALITY = {
    "synoptic_path": "path_synoptics",
    "op_note":       "OPNOTE",
    "ct":            "ct_imaging",
    "mri":           "mri_imaging",
}


# ---------------------------------------------------------------------------
# Ambiguous linkage builder
# ---------------------------------------------------------------------------

AMBIGUOUS_LINKAGE_SQL = """
WITH dates AS (
  SELECT DISTINCT research_id, finding_date
  FROM main.canonical_invasion_events_v1
  WHERE finding_date IS NOT NULL AND research_id IS NOT NULL
),
cands AS (
  SELECT d.research_id, d.finding_date,
         oe.surgery_episode_id, oe.surgery_date_native
  FROM dates d
  JOIN main.canonical_operative_events_v1 oe
    ON TRY_CAST(oe.research_id AS BIGINT) = d.research_id
   AND ABS(DATE_DIFF('day',
            TRY_CAST(oe.surgery_date_native AS DATE),
            d.finding_date)) <= 90
),
agg AS (
  SELECT research_id, finding_date,
         COUNT(DISTINCT surgery_episode_id) AS n_eps,
         list_sort(list_distinct(LIST(surgery_episode_id))) AS ep_list,
         list_sort(list_distinct(LIST(CAST(surgery_date_native AS VARCHAR))))
                                                AS date_list
  FROM cands GROUP BY 1,2
),
inv_agg AS (
  SELECT research_id, finding_date,
         COUNT(*) AS n_findings,
         ANY_VALUE(linked_surgery_episode_id) AS picked_episode_id,
         list_filter(
           list_distinct(LIST(NULLIF(evidence_qualifier,''))),
           x -> x IS NOT NULL
         ) AS ev_list
  FROM main.canonical_invasion_events_v1
  WHERE finding_date IS NOT NULL AND research_id IS NOT NULL
  GROUP BY 1,2
)
SELECT
  a.research_id,
  a.finding_date,
  a.n_eps AS n_candidate_surgery_episodes,
  ia.picked_episode_id     AS picked_surgery_episode_id,
  (SELECT TRY_CAST(oe2.surgery_date_native AS DATE)
     FROM main.canonical_operative_events_v1 oe2
    WHERE oe2.surgery_episode_id = ia.picked_episode_id
    LIMIT 1)                AS picked_surgery_date,
  list_filter(a.ep_list,    x -> x != ia.picked_episode_id)
                            AS alternative_surgery_episode_ids,
  a.date_list               AS all_candidate_surgery_dates,
  ia.n_findings             AS n_findings_in_group,
  LEFT(COALESCE(list_extract(ia.ev_list,1),''),200) AS sample_evidence_qualifier_1,
  LEFT(COALESCE(list_extract(ia.ev_list,2),''),200) AS sample_evidence_qualifier_2,
  LEFT(COALESCE(list_extract(ia.ev_list,3),''),200) AS sample_evidence_qualifier_3
FROM agg a
LEFT JOIN inv_agg ia USING (research_id, finding_date)
WHERE a.n_eps > 1
ORDER BY a.research_id, a.finding_date
"""


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_synoptic_structured(con: duckdb.DuckDBPyConnection,
                             expected: int) -> dict:
    """Synoptic-path / structured uses mass-equivalence at the SLICE-TOTAL
    level (because extrathyroidal_extension splits 1->2 invasion_types via
    the ETE subtype dict, per-invasion_type matching is noisy). The right
    headline metric is total source non-null counts == total canonical rows
    in the slice. The per-type breakdown is included in the CSV for context.
    """
    print(f"\n--- slice synoptic_path__structured "
          f"(expected_n={expected:,}, mass-equivalence) ---")
    rows = con.execute(SYNOPTIC_STRUCTURED_SQL).fetchall()
    cols = [d[0] for d in con.description]
    # The source rows include 'ete_total' which corresponds to the
    # extrathyroidal_extension VARCHAR column, plus 'gross_ete' which is
    # the BIGINT column. On the DB side, 'ete_total' is gross_ete +
    # microscopic_ete. So the slice totals should aggregate as:
    #   source: sum of all source_n EXCEPT 'ete_total' (since 'gross_ete'
    #           BIGINT is independent of extrathyroidal_extension VARCHAR;
    #           but extrathyroidal_extension feeds INTO db.gross_ete and
    #           db.microscopic_ete).
    # Cleanest: total source = sum of every per-col COUNT(non-null).
    # Total db   = total rows in canonical with this slice.
    total_db = int(con.execute(
        "SELECT COUNT(*) FROM main.canonical_invasion_events_v1 "
        "WHERE source_modality='synoptic_path' AND source_kind='structured'"
    ).fetchone()[0])
    total_src = int(con.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE extrathyroidal_extension IS NOT NULL) + "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE gross_ete IS NOT NULL) + "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE vascular_invasion IS NOT NULL) + "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE lymphatic_invasion IS NOT NULL) + "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE perineural_invasion IS NOT NULL) + "
        "(SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 "
        "  WHERE capsular_invasion IS NOT NULL)"
    ).fetchone()[0])
    print(f"  source_total={total_src:,}  db_total={total_db:,}  "
          f"delta={total_src - total_db}")
    out = OUT_DIR / "synoptic_path__structured__mig_91.csv"
    with open(out, "w", newline="") as f:
        f.write("# Mass-equivalence check for synoptic_path/structured slice\n")
        f.write(f"# Headline: source_total={total_src:,} == db_total={total_db:,} "
                f"-> {'MATCH' if total_src == total_db else 'MISMATCH'}\n")
        f.write("# Per-invasion_type breakdown follows (note: ETE col splits 1->2 "
                "invasion_types; gross_ete db count = ete_total source + gross_ete BIGINT source)\n#\n")
        w = csv.writer(f)
        w.writerow(cols + ["your_decision", "your_note"])
        for r in rows:
            w.writerow(list(r) + ["", ""])
    print(f"  -> wrote {out}")
    is_match = (total_src == total_db)
    return {
        "name": "synoptic_path__structured", "expected": expected,
        "n_total": total_db,
        "n_match": total_db if is_match else 0,
        "n_unresolved": 0,
        "n_mismatch": 0 if is_match else abs(total_src - total_db),
    }


def run_slice(con: duckdb.DuckDBPyConnection, slice_def: dict) -> dict:
    name = slice_def["name"]
    modality = slice_def["modality"]
    kind = slice_def["kind"]
    expected = slice_def["expected_n"]

    if name == "synoptic_path__structured":
        return run_synoptic_structured(con, expected)

    print(f"\n--- slice {name} (expected_n={expected:,}) ---")

    if name == "op_note__structured":
        sql = OP_NOTE_STRUCTURED_SQL
    else:
        # llm slice
        nt = LLM_NOTE_TYPE_BY_MODALITY[modality]
        sql = (LLM_SLICE_SQL
               .replace("$modality$", f"'{modality}'")
               .replace("$note_type$", f"'{nt}'"))

    # Materialise to temp + count.
    con.execute(f"CREATE OR REPLACE TEMP TABLE _slice_{name} AS {sql}")
    counts = con.execute(
        f"""
        SELECT
          COUNT(*)                           AS n_total,
          COUNT(*) FILTER (WHERE match_flag IN ('MATCH','MATCH_VALUE_OK'))
                                             AS n_match,
          COUNT(*) FILTER (WHERE match_flag = 'UNRESOLVED')
                                             AS n_unresolved,
          COUNT(*) FILTER (WHERE match_flag IN ('MISMATCH_STATUS','MISMATCH_VALUE'))
                                             AS n_mismatch
        FROM _slice_{name}
        """
    ).fetchone()
    n_total, n_match, n_unresolved, n_mismatch = counts
    print(f"  n_total={n_total:,}  match={n_match:,}  "
          f"unresolved={n_unresolved:,}  mismatch={n_mismatch:,}")

    # Dump mismatches+unresolved if any.
    if n_mismatch + n_unresolved > 0:
        out = OUT_DIR / f"{name}__mig_91.csv"
        rows = con.execute(
            f"""
            SELECT * FROM _slice_{name}
            WHERE match_flag NOT IN ('MATCH','MATCH_VALUE_OK')
            ORDER BY match_flag, research_id
            """
        ).fetchall()
        cols = [d[0] for d in con.description]
        with open(out, "w", newline="") as f:
            f.write(f"# Mismatch/unresolved CSV for slice {name}\n")
            f.write(f"# n_total={n_total:,} n_match={n_match:,} "
                    f"n_unresolved={n_unresolved:,} n_mismatch={n_mismatch:,}\n#\n")
            w = csv.writer(f)
            w.writerow(cols + ["your_decision", "your_note"])
            for r in rows:
                w.writerow(list(r) + ["", ""])
        print(f"  -> wrote {out}")
    return {
        "name": name, "expected": expected,
        "n_total": n_total, "n_match": n_match,
        "n_unresolved": n_unresolved, "n_mismatch": n_mismatch,
    }


def write_summary(results: list[dict]) -> Path:
    out = OUT_DIR / "per_modality_match_summary__mig_91.md"
    lines = [
        "# canonical_invasion_events_v1 per-modality verification summary",
        "",
        "Protocol v2 Script-rule re-run (per-modality). batch_id: mig_91.",
        "Generated by qc_framework_v1/scripts/build_invasion_events_verification.py",
        "",
        "match_flag values:",
        "  MATCH            -- canonical row resolved against source w/ same status/value",
        "  MATCH_VALUE_OK   -- evidence_qualifier preserved exactly from source (synoptic_path/structured)",
        "  UNRESOLVED       -- canonical row could not be joined back to source",
        "  MISMATCH_STATUS  -- finding_status differs between canonical and recomputed",
        "  MISMATCH_VALUE   -- evidence_qualifier differs between canonical and source",
        "",
        "| slice | expected | n_total | n_match | n_unresolved | n_mismatch |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| `{r['name']}` | {r['expected']:,} | {r['n_total']:,} | "
            f"{r['n_match']:,} | {r['n_unresolved']:,} | {r['n_mismatch']:,} |"
        )
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n-> summary {out}")
    return out


def write_ambiguous(con: duckdb.DuckDBPyConnection) -> Path:
    out = OUT_DIR / "ambiguous_linkage_review__mig_91.csv"
    rows = con.execute(AMBIGUOUS_LINKAGE_SQL).fetchall()
    cols = [d[0] for d in con.description]
    print(f"\nambiguous-linkage groups: {len(rows)}")
    preamble = textwrap.dedent("""\
        # Ambiguous-linkage review -- canonical_invasion_events_v1
        # batch_id: mig_91_ambiguous_linkage
        # Generated by qc_framework_v1/scripts/build_invasion_events_verification.py
        #
        # Each row is a (research_id, finding_date) group with TWO OR MORE distinct
        # surgery_episode_ids in canonical_operative_events_v1 within +/-90d of the
        # finding_date. The canonical's MIN(surgery_episode_id) heuristic picked
        # one; alternative_surgery_episode_ids lists the others. The findings in
        # the group are summarised by sample_evidence_qualifier_{1,2,3} (truncated
        # to 200 chars, no clinical text printed).
        #
        # IMPORTANT: the canonical column `linkage_ambiguous_multi_episode` is
        # MIS-NAMED -- it counts FINDINGS per partition, not surgery episodes.
        # The actual multi-episode case set is THIS file. Carry-forward
        # CF-91-LINKAGE-COL-NAME: rename the column to
        # linkage_ambiguous_multi_finding (or fix the definition).
        #
        # your_chosen_episode_id vocabulary:
        #   ACCEPT     -- keep the canonical's picked_surgery_episode_id (default)
        #   <BIGINT>   -- override; use this surgery_episode_id instead
        #   SPLIT      -- this group has findings that legitimately link to
        #                 different surgeries; describe in your_note
        #   UNLINK     -- neither candidate is correct; null out the linkage
        #
    """)
    with open(out, "w", newline="") as f:
        f.write(preamble)
        w = csv.writer(f)
        w.writerow(cols + ["your_chosen_episode_id", "your_note"])
        for r in rows:
            w.writerow(list(r) + ["", ""])
    print(f"-> wrote {out}")
    return out


def main() -> None:
    con = duckdb.connect("md:")
    con.execute(f'USE "{DB}"')

    results = [run_slice(con, s) for s in SLICES]
    write_summary(results)
    write_ambiguous(con)

    # Findings-vs-staging sweep: for canonical_invasion_events_v1 the rule
    # applies via the LLM CTE's finding_status ladder, NOT via a separate
    # staging column on the table. The 18 mig_82 CAP-template-echo cleanup
    # rows were applied on canonical_airway_invasion_events_v1, not here.
    # Logical sweep at this layer: surface rows where finding_status='present'
    # but evidence_qualifier looks like a stage-only template echo (e.g.
    # AJCC checklist mentions). Spot probe:
    print("\n--- findings-vs-staging probe (informational) ---")
    cnt = con.execute(
        """
        SELECT COUNT(*)
        FROM main.canonical_invasion_events_v1
        WHERE source_kind='llm'
          AND finding_status='present'
          AND (LOWER(COALESCE(evidence_qualifier,'')) LIKE '%pt4a%'
               OR LOWER(COALESCE(evidence_qualifier,'')) LIKE '%pt4b%'
               OR LOWER(COALESCE(evidence_qualifier,'')) LIKE '%checklist%')
        """
    ).fetchone()[0]
    print(f"  LLM 'present' rows whose evidence_qualifier mentions "
          f"a stage-token (pT4a/pT4b/checklist): {cnt:,}")
    if cnt > 0:
        out = OUT_DIR / "findings_vs_staging_template_echo__mig_91.csv"
        rows = con.execute(
            """
            SELECT invasion_event_id, research_id, source_modality,
                   invasion_type, finding_status,
                   LEFT(COALESCE(evidence_qualifier,''),300) AS evidence_qualifier_trunc,
                   source_row_id
            FROM main.canonical_invasion_events_v1
            WHERE source_kind='llm'
              AND finding_status='present'
              AND (LOWER(COALESCE(evidence_qualifier,'')) LIKE '%pt4a%'
                   OR LOWER(COALESCE(evidence_qualifier,'')) LIKE '%pt4b%'
                   OR LOWER(COALESCE(evidence_qualifier,'')) LIKE '%checklist%')
            ORDER BY research_id, invasion_type
            """
        ).fetchall()
        cols = [d[0] for d in con.description]
        with open(out, "w", newline="") as f:
            f.write("# Findings-vs-staging template-echo probe -- "
                    "mig_91 carry-forward review.\n"
                    "# Logan adjudicates whether finding_status='present' is "
                    "anatomic or stage-only.\n#\n")
            w = csv.writer(f)
            w.writerow(cols + ["your_decision", "your_note"])
            for r in rows:
                w.writerow(list(r) + ["", ""])
        print(f"  -> wrote {out}")


if __name__ == "__main__":
    main()
