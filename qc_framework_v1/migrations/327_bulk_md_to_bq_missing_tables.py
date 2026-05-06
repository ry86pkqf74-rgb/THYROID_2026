#!/usr/bin/env python3
"""mig_327_bulk_md_to_bq_missing_tables — bulk export tables missing from BQ pub_canonical.

Exports every table that is in MotherDuck main (or readonly_share) but absent from
pub_canonical, then BQ-loads them. Includes inline rebuild of
canonical_tumor_characteristics_v1 from synoptic/specimen/TEM sources in readonly_share.

Run:
  cd <repo>
  .venv/bin/python qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py \\
      [--only TABLE1,TABLE2] [--skip-load] [--dry-run]

Prereqs:
  * motherduck.local.toml with MD_SA_TOKEN / MOTHERDUCK_TOKEN
  * gcloud auth / bq CLI authenticated to thyroid-canonical-pub-2026
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts._md_connect import connect_locked  # noqa: E402

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_canonical"
LOCATION = "us-central1"
OUT_DIR = _REPO / "exports" / "bq_bulk_mig327"

# PHI column name substrings — drop from every parquet
_PHI_SUBS = ("mrn", "medical_record", "patient_name", "first_name", "last_name",
              "full_name", "ssn", "social_security", "phone", "address",
              "street", "email", "zip_code", "postal")
_PHI_EXACT = frozenset({"dob", "date_of_birth", "birth_date", "name",
                         "patient_first_name", "patient_last_name",
                         "dob_timestamp", "patient_dob"})


def _phi(name: str) -> bool:
    n = name.strip().lower()
    return n in _PHI_EXACT or any(s in n for s in _PHI_SUBS)


# ── Table catalogue ──────────────────────────────────────────────────────────
# (table_name, source_schema, tier)
# source_schema = 'main' | 'readonly_share' | '__ctc_rebuild__'
TABLES: list[tuple[str, str, str]] = [
    # canonical_tumor_characteristics_v1 — rebuilt inline from readonly_share sources
    ("canonical_tumor_characteristics_v1", "__ctc_rebuild__", "canonical_base"),
    # canonical base tables
    ("manuscript_cohort_v1",             "main",            "canonical_base"),
    ("path_synoptics",                   "main",            "canonical_base"),
    ("tumor_stage_heterogeneity_v1",     "main",            "canonical_base"),
    ("rai_treatment_episode_v2",         "main",            "canonical_base"),
    ("specimen_tumor_focus_v1",          "readonly_share",  "canonical_base"),
    ("specimen_master_v1",               "main",            "canonical_base"),
    ("patient_cross_domain_timeline_v2", "main",            "canonical_base"),
    # derived tables
    ("patient_completion_oed_path_linkage_v1", "main",      "derived"),
    ("specimen_genomic_assay_v1",        "main",            "derived"),
    ("specimen_source_xref_v1",          "main",            "derived"),
    ("tg_postop_surveillance_windows_v1","main",            "derived"),
    ("tg_timeline_patient_summary_v1",   "main",            "derived"),
    # source tables
    ("mri_imaging",                      "main",            "source"),
    ("nuclear_med",                      "main",            "source"),
    ("nsqip_enrichment",                 "main",            "source"),
    ("nsqip_patient_summary",            "main",            "source"),
    ("thyroid_sizes",                    "main",            "source"),
    ("thyroid_weights",                  "main",            "source"),
    # NLP sources
    ("note_entities_llm_airway_invasion_v2",    "main",     "nlp_source"),
    ("note_entities_llm_cervical_ln_detail",    "main",     "nlp_source"),
    ("note_entities_llm_dynamic_risk_response", "main",     "nlp_source"),
    ("note_entities_llm_esophageal_invasion",   "main",     "nlp_source"),
    ("note_entities_llm_ete_subgrade_v1",       "main",     "nlp_source"),
    ("note_entities_llm_frozen_section_detail", "main",     "nlp_source"),
    ("note_entities_llm_parathyroid_detail_v1", "main",     "nlp_source"),
    ("note_entities_llm_past_medical_hx",       "main",     "nlp_source"),
    ("note_entities_llm_past_surgical_hx",      "main",     "nlp_source"),
    ("note_entities_llm_pathology",             "main",     "nlp_source"),
    ("note_entities_llm_presenting_symptoms",   "main",     "nlp_source"),
    ("note_entities_llm_rai_detailed",          "main",     "nlp_source"),
    ("note_entities_llm_recurrence",            "main",     "nlp_source"),
    ("note_entities_llm_t4b_invasion_v1",       "main",     "nlp_source"),
    ("note_entities_llm_tirads_granular",       "main",     "nlp_source"),
    ("note_entities_llm_vascular_invasion_v2",  "main",     "nlp_source"),
    ("note_entities_operative_detail",          "main",     "nlp_source"),
    ("note_entities_procedures",                "main",     "nlp_source"),
    # governance
    ("signoff_migration",                "main",            "governance"),
    ("pub_release_manifest_v1_1",        "main",            "governance"),
]

# ── CTC inline rebuild (from script 245 SQL, sourced from readonly_share) ────
_CTC_SQL = """
WITH brk_dedup AS (
  SELECT
    research_id, synoptic_row_ix, surgery_episode_id,
    path_surgery_id, specimen_id,
    tumor_ordinal AS broker_tumor_ordinal
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id, synoptic_row_ix
        ORDER BY tumor_ordinal NULLS LAST, specimen_id
      ) AS rn
    FROM readonly_share.specimen_tumor_focus_v1
  )
  WHERE rn = 1
),
tem_dedup AS (
  SELECT
    research_id, surgery_episode_id,
    surgery_date AS tem_surgery_date,
    primary_histology,
    histology_variant     AS tem_histology_variant,
    histology_source,
    t_stage    AS t_stage_ajcc8,
    n_stage    AS n_stage_ajcc8,
    m_stage    AS m_stage_ajcc8,
    CAST(overall_stage AS VARCHAR) AS overall_stage_ajcc8,
    NULL       AS stage_group_ajcc8,
    tumor_size_cm         AS tem_tumor_size_cm,
    gross_ete,
    vascular_invasion     AS tem_vascular_invasion,
    extrathyroidal_extension AS tem_ete,
    lymphatic_invasion    AS tem_lvi,
    perineural_invasion   AS tem_pni,
    capsular_invasion     AS tem_capsular,
    margin_status         AS tem_margin_status,
    nodal_disease_positive_count,
    nodal_disease_total_count,
    extranodal_extension,
    laterality            AS tem_laterality,
    number_of_tumors,
    multifocality_flag,
    histology_discordance_flag,
    t_stage_discordance_flag
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY research_id, surgery_episode_id
                         ORDER BY surgery_episode_id) AS rn2
    FROM readonly_share.tumor_episode_master_v2
  ) WHERE rn2 = 1
)
SELECT
  CAST(s.research_id AS INTEGER)                   AS research_id,
  b.surgery_episode_id,
  s.tumor_index                                    AS tumor_ordinal,
  COALESCE(t.tem_surgery_date, s.surg_date)        AS surgery_date,
  b.path_surgery_id,
  b.specimen_id,
  s.synoptic_row_ix,
  COALESCE(t.tem_laterality, s.site)               AS laterality,
  s.site,
  s.size_greatest_dimension_cm,
  t.tem_tumor_size_cm                              AS tumor_size_cm_per_surgery,
  COALESCE(s.histologic_type, t.primary_histology) AS primary_histology,
  COALESCE(s.histologic_variant, t.tem_histology_variant) AS histology_variant,
  t.histology_source,
  t.t_stage_ajcc8,
  t.n_stage_ajcc8,
  t.m_stage_ajcc8,
  t.overall_stage_ajcc8,
  t.stage_group_ajcc8,
  COALESCE(s.extrathyroidal_extension, t.tem_ete) AS extrathyroidal_extension,
  t.gross_ete,
  COALESCE(s.lymphatic_invasion, t.tem_lvi)       AS lymphatic_invasion,
  COALESCE(s.angioinvasion, t.tem_vascular_invasion) AS vascular_invasion,
  s.angioinvasion_quantify,
  COALESCE(s.perineural_invasion, t.tem_pni)      AS perineural_invasion,
  COALESCE(s.capsular_invasion, t.tem_capsular)   AS capsular_invasion,
  COALESCE(s.margin_status, t.tem_margin_status)  AS margin_status,
  s.ln_examined,
  s.ln_involved,
  t.nodal_disease_positive_count,
  t.nodal_disease_total_count,
  t.extranodal_extension,
  t.number_of_tumors,
  t.multifocality_flag,
  CASE
    WHEN t.surgery_episode_id IS NOT NULL
      THEN 'synoptic_tumor_long_v1|tumor_episode_master_v2'
    ELSE 'synoptic_tumor_long_v1'
  END                                              AS source_tables,
  CASE
    WHEN t.surgery_episode_id IS NOT NULL THEN 'STL+TEM' ELSE 'STL_only'
  END                                              AS resolution_rule
FROM readonly_share.synoptic_tumor_long_v1 s
LEFT JOIN brk_dedup b
  ON b.research_id = s.research_id AND b.synoptic_row_ix = s.synoptic_row_ix
LEFT JOIN tem_dedup t
  ON t.research_id = s.research_id AND t.surgery_episode_id = b.surgery_episode_id
"""


def _cols(con, tbl: str, schema: str) -> tuple[list[str], list[str]]:
    """Return (kept_cols, dropped_phi_cols) for a MD table."""
    if schema == "__ctc_rebuild__":
        return [], []  # CTC columns are fixed by the rebuild SQL above
    # information_schema.columns covers both 'main' and 'readonly_share'
    # under the publication catalog (both are accessible from this session)
    rows = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{tbl}'
        ORDER BY ordinal_position
        """
    ).fetchall()
    # Deduplicate while preserving order (readonly_share returns duplicates)
    seen: set[str] = set()
    cols: list[str] = []
    for r in rows:
        c = r[0]
        if c not in seen:
            seen.add(c)
            cols.append(c)
    if not cols:
        raise ValueError(f"No columns found for {schema}.{tbl}")
    kept = [c for c in cols if not _phi(c)]
    dropped = [c for c in cols if _phi(c)]
    return kept, dropped


def _export_table(con, tbl: str, schema: str, out_dir: Path, dry_run: bool) -> int:
    pq = out_dir / f"{tbl}.parquet"

    if schema == "__ctc_rebuild__":
        n = con.execute(f"SELECT COUNT(*) FROM ({_CTC_SQL}) _").fetchone()[0]
        if not dry_run:
            con.execute(
                f"COPY ({_CTC_SQL}) TO '{pq.as_posix()}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        dropped: list[str] = []
    else:
        kept, dropped = _cols(con, tbl, schema)
        if dropped:
            print(f"  [PHI-drop] {tbl}: {dropped}")
        ref = f'readonly_share."{tbl}"' if schema == "readonly_share" else f'"{tbl}"'
        sel = ", ".join(f'"{c}"' for c in kept)
        n = con.execute(f"SELECT COUNT(*) FROM {ref}").fetchone()[0]
        if not dry_run:
            con.execute(
                f"COPY (SELECT {sel} FROM {ref}) "
                f"TO '{pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    return n


def _bq_load(tbl: str, out_dir: Path, project: str, dataset: str, location: str) -> int:
    pq = out_dir / f"{tbl}.parquet"
    dest = f"{project}:{dataset}.{tbl}"
    cmd = ["bq", f"--location={location}", "load",
           "--replace", "--source_format=PARQUET", "--clustering_fields=research_id",
           dest, str(pq.resolve())]
    return subprocess.call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="Comma-separated table names to process")
    ap.add_argument("--skip-load", action="store_true", help="Export only, no bq load")
    ap.add_argument("--dry-run", action="store_true", help="Count rows, no file write")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    args = ap.parse_args()

    out_dir: Path = args.out_dir
    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    only_set: set[str] | None = (
        {t.strip() for t in args.only.split(",")} if args.only else None
    )

    con = connect_locked()
    manifest: list[dict] = []
    errors: list[str] = []

    for tbl, schema, tier in TABLES:
        if only_set and tbl not in only_set:
            continue
        print(f"\n[mig_327] {tbl} (schema={schema}, tier={tier})")
        try:
            n = _export_table(con, tbl, schema, out_dir, args.dry_run)
        except Exception as exc:
            print(f"  EXPORT ERROR: {exc}", file=sys.stderr)
            errors.append(f"export:{tbl}:{exc}")
            continue
        print(f"  rows: {n}")
        entry = {"table": tbl, "schema": schema, "tier": tier, "rows": n}
        if not args.skip_load and not args.dry_run:
            rc = _bq_load(tbl, out_dir, PROJECT, DATASET, LOCATION)
            entry["bq_load_rc"] = rc
            if rc != 0:
                errors.append(f"load:{tbl}:rc={rc}")
            else:
                print("  BQ load: OK")
        manifest.append(entry)

    manifest_data = {
        "migration_id": "mig_327_bulk_md_to_bq_missing_tables",
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "tables": manifest,
        "errors": errors,
    }
    if not args.dry_run:
        mf = out_dir / "manifest.json"
        mf.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
        print(f"\n[mig_327] Manifest: {mf}")
    else:
        print("\n[mig_327] DRY RUN summary:")
        for e in manifest:
            print(f"  {e['table']}: {e['rows']} rows")
    if errors:
        print(f"\n[mig_327] ERRORS ({len(errors)}):", errors)
        sys.exit(1)
    else:
        print("\n[mig_327] All done. Run 328_signoff_registry_sync.sql next.")


if __name__ == "__main__":
    main()
