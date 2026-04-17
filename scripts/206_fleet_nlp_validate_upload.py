#!/usr/bin/env python3
"""
Script 206: Validate fleet NLP extraction results and upload to MotherDuck.

Fleet: 15-pod RunPod (qwen3:32b), 26,643 clinical notes, 3 domains.
Source: processed/remaining/partials/consolidated/*.ckpt*.jsonl

Key design decisions:
- Existing MD tables store NOTE-LEVEL rows (one per extracted note, result_json
  contains entities as JSON blob). We upload at this grain, not entity-level.
- Dedup key: note_row_id (existing rows in MD are skipped)
- Extra columns added by prior ingestion pipeline (linkage_date, source_workbook,
  etc.) are set to NULL for fleet rows — they were not recorded during fleet extraction.
- Flat entity analysis is produced separately for validation reporting and saved
  as parquet (useful for cross-validation, confidence filtering, etc.)
- Only entities with confidence >= MIN_ENTITY_CONFIDENCE are counted in flat stats;
  the full result_json is uploaded as-is (filtering happens downstream).

Usage:
    python scripts/206_fleet_nlp_validate_upload.py [--dry-run] [--min-conf 0.3]
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
CONSOLIDATED = REPO / "processed" / "remaining" / "partials" / "consolidated"
OUT_DIR = REPO / "scripts" / "output"

# Minimum confidence for inclusion in flat entity parquets (stats + cross-validation).
# Full result_json is always uploaded regardless.
DEFAULT_MIN_CONF = 0.3
DEFAULT_HIGH_CONF = 0.5  # threshold flagged as "high quality" in summary

# Fleet domains → MD table mapping and known-good entity types
DOMAINS: dict[str, dict[str, Any]] = {
    "tirads_granular": {
        "glob_patterns": ["note_entities_llm_tirads_granular.ckpt*.jsonl"],
        "md_table": "note_entities_llm_tirads_granular",
        "known_entity_types": {
            # Core TIRADS classification
            "tirads_category", "tirads_score", "tirads_total_points",
            "tirads_composition", "tirads_echogenicity", "tirads_shape",
            "tirads_margin", "tirads_echogenic_foci", "tirads_size_cm",
            "tirads_recommendation", "tirads_vascularity",
            "tirads_subcategory", "tirads_feature",
            # Nodule descriptors produced by qwen3:32b
            "nodule_location", "nodule_dimensions", "nodule_identifier",
            "nodule_stability", "nodule_growth_rate", "nodule_volume",
            "us_visit_number",
        },
    },
    "cervical_ln_detail": {
        "glob_patterns": ["note_entities_llm_cervical_ln_detail.ckpt*.jsonl"],
        "md_table": "note_entities_llm_cervical_ln_detail",
        "known_entity_types": {
            "cervical_ln_level", "cervical_ln_size", "cervical_ln_assessment",
            "cervical_ln_suspicious", "cervical_ln_biopsy_result",
            "cervical_ln_dissection", "cervical_ln_count",
            "cervical_ln_location", "lymph_node_level",
            # Produced by qwen3:32b
            "ln_level", "ln_number_per_level", "ln_size", "ln_laterality",
            "ln_morphology", "fna_of_ln", "suspicious_features_count",
            "microcalcifications_ln", "cystic_change",
        },
    },
    "pathology": {
        "glob_patterns": ["note_entities_llm_pathology.ckpt*.jsonl"],
        "md_table": "note_entities_llm_pathology",
        "known_entity_types": {
            "histology_type", "histology_variant", "tumor_size", "tumor_variant",
            "margin_status", "vascular_invasion", "lymphatic_invasion",
            "lymphovascular_invasion", "perineural_invasion", "capsular_invasion",
            "ete_status", "extrathyroidal_extension",
            "ln_examined", "ln_positive", "ln_level", "bethesda_category",
            "bethesda_class", "stage_t", "stage_n", "stage_m", "stage_group",
            # Produced by qwen3:32b — all clinically meaningful
            "surgical_pathology", "fna_cytology", "benign_pathology",
            "lymph_node_pathology", "molecular_testing", "multifocality",
            "frozen_section", "cancer_type", "invasion_type",
            "resection_margin", "lymph_node_status", "lymph_node_involvement",
            "lymph_node_metastasis", "extranodal_extension",
            "procedure_performed", "procedures_performed",
            "tumor_stage", "pathologic_stage", "tumor_margin",
        },
    },
}

# Columns present on existing MD tables (the "canonical" schema).
# Fleet JSONL provides the first ~9; the rest are set to NULL.
MD_TABLE_COLUMNS = [
    "note_row_id", "research_id", "note_type", "note_date",
    "domain", "llm_model", "llm_base_url", "extracted_at", "result_json",
    # enrichment columns from prior ingestion pipeline (NULL for fleet)
    "linkage_date", "source_workbook", "source_sheet", "source_column",
    "note_index", "preprocess_batch_id", "preprocessed_at_utc",
    "preprocess_script_version", "entity_domain", "llm_provider",
    "llm_sdk", "llm_sdk_version", "provider_returned_model",
    "provider_system_fingerprint",
]

def _safe_float(val) -> float | None:
    """Coerce a value to float; return None if conversion fails."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


ENTITY_FLAT_COLUMNS = [
    "note_row_id", "research_id", "note_type", "note_date", "domain", "llm_model",
    "entity_type", "entity_value", "entity_date", "date_confidence",
    "date_source_keyword", "present_or_negated", "confidence",
    "evidence_text", "source_line",
]


def _parse_jsonl_files(domain_key: str, glob_patterns: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse all JSONL files for a domain.

    Returns:
        note_df  — note-level DataFrame (one row per note, result_json kept intact)
        entity_df — flat entity-level DataFrame (one row per entity)
    """
    note_rows: list[dict] = []
    entity_rows: list[dict] = []
    file_count = 0

    for pattern in glob_patterns:
        files = sorted(CONSOLIDATED.glob(pattern))
        for fpath in files:
            file_count += 1
            with open(fpath) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)

                    # Note-level row (for MD upload)
                    note_row: dict[str, Any] = {col: None for col in MD_TABLE_COLUMNS}
                    for col in ["note_row_id", "research_id", "note_type", "note_date",
                                "domain", "llm_model", "llm_base_url", "extracted_at", "result_json"]:
                        note_row[col] = d.get(col)

                    # Ensure result_json is a string (some files store it already serialised)
                    if isinstance(note_row["result_json"], (dict, list)):
                        note_row["result_json"] = json.dumps(note_row["result_json"])

                    note_rows.append(note_row)

                    # Flat entity-level rows (for analysis / validation)
                    try:
                        rj = d.get("result_json", "{}")
                        if isinstance(rj, str):
                            rj = json.loads(rj)
                        entities = rj.get("entities", []) if isinstance(rj, dict) else []
                    except Exception:
                        entities = []

                    for ent in entities:
                        entity_rows.append({
                            "note_row_id": d.get("note_row_id"),
                            "research_id": str(d.get("research_id", "")),
                            "note_type": d.get("note_type"),
                            "note_date": d.get("note_date"),
                            "domain": domain_key,
                            "llm_model": d.get("llm_model", "qwen3:32b"),
                            "entity_type": ent.get("entity_type"),
                            "entity_value": ent.get("entity_value"),
                            "entity_date": ent.get("entity_date"),
                            # Coerce to float — qwen3 sometimes emits as string
                            "date_confidence": _safe_float(ent.get("date_confidence")),
                            "date_source_keyword": ent.get("date_source_keyword"),
                            "present_or_negated": ent.get("present_or_negated"),
                            "confidence": _safe_float(ent.get("confidence")),
                            "evidence_text": ent.get("evidence_text"),
                            "source_line": ent.get("source_line"),
                        })

    note_df = pd.DataFrame(note_rows, columns=MD_TABLE_COLUMNS) if note_rows else pd.DataFrame(columns=MD_TABLE_COLUMNS)
    entity_df = pd.DataFrame(entity_rows, columns=ENTITY_FLAT_COLUMNS) if entity_rows else pd.DataFrame(columns=ENTITY_FLAT_COLUMNS)
    print(f"  Files parsed: {file_count}")
    return note_df, entity_df


def _validate_and_report(domain_key: str, note_df: pd.DataFrame, entity_df: pd.DataFrame,
                         known_types: set[str], min_conf: float) -> None:
    """Print validation summary for a domain."""
    print(f"\n{'─'*60}")
    print(f"  VALIDATION — {domain_key}")
    print(f"{'─'*60}")
    print(f"  Note rows      : {len(note_df):>8,}")
    print(f"  Unique RIDs    : {note_df['research_id'].nunique():>8,}")
    print(f"  Unique note_ids: {note_df['note_row_id'].nunique():>8,}")

    if entity_df.empty:
        print("  ⚠ WARNING: No entities found — check JSONL result_json content")
        return

    # Notes with no entities
    notes_with_entities = entity_df["note_row_id"].nunique()
    notes_empty = note_df["note_row_id"].nunique() - notes_with_entities
    print("\n  Entity totals:")
    print(f"    All entities       : {len(entity_df):>8,}")
    print(f"    Notes with entities: {notes_with_entities:>8,}")
    print(f"    Notes empty (no entities): {notes_empty:>6,}")
    print(f"    Unique entity RIDs : {entity_df['research_id'].nunique():>8,}")

    # Confidence distribution
    conf = entity_df["confidence"].dropna()
    if len(conf):
        above_high = (conf >= DEFAULT_HIGH_CONF).sum()
        above_min = (conf >= min_conf).sum()
        below_min = (conf < min_conf).sum()
        print(f"\n  Confidence distribution (n={len(conf):,}):")
        print(f"    mean={conf.mean():.3f}  median={conf.median():.3f}  "
              f"p10={conf.quantile(0.1):.3f}  p90={conf.quantile(0.9):.3f}")
        print(f"    >= {DEFAULT_HIGH_CONF} (high):  {above_high:>7,}  ({100*above_high/len(conf):.1f}%)")
        print(f"    >= {min_conf} (min):    {above_min:>7,}  ({100*above_min/len(conf):.1f}%)")
        print(f"    <  {min_conf} (filtered): {below_min:>5,}  ({100*below_min/len(conf):.1f}%)")

    # Entity type distribution
    print("\n  Entity type distribution:")
    for et, cnt in entity_df["entity_type"].value_counts().items():
        flag = "✓" if et in known_types else "⚠ UNEXPECTED"
        print(f"    {et:<40s}: {cnt:>6,}  {flag}")

    # Polarity
    if "present_or_negated" in entity_df.columns:
        print("\n  Polarity (present_or_negated):")
        for val, cnt in entity_df["present_or_negated"].value_counts(dropna=False).items():
            print(f"    {str(val):<20s}: {cnt:>6,}")


def _dedup_against_md(domain_key: str, note_df: pd.DataFrame, con, md_table: str) -> pd.DataFrame:
    """Return only the note rows whose note_row_id is not already in the MD table."""
    try:
        existing = con.execute(
            f"SELECT COUNT(*) AS n, COUNT(DISTINCT note_row_id) AS uid FROM {md_table}"
        ).fetchone()
        print(f"\n  Existing on MD ({md_table}): {existing[0]:,} rows, {existing[1]:,} unique note_row_ids")

        existing_ids = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT note_row_id FROM {md_table}"
            ).fetchall()
        }
    except Exception as e:
        print(f"\n  Table {md_table} not found or error: {e}")
        print(f"  Will create new table with all {len(note_df):,} rows")
        return note_df

    fleet_ids = set(note_df["note_row_id"].dropna())
    overlap = len(fleet_ids & existing_ids)
    new_ids = fleet_ids - existing_ids
    print(f"  Fleet note_row_ids : {len(fleet_ids):>8,}")
    print(f"  Already in MD      : {overlap:>8,}")
    print(f"  Truly new          : {len(new_ids):>8,}")

    new_rows = note_df[note_df["note_row_id"].isin(new_ids)].copy()
    return new_rows


def _upload_to_md(domain_key: str, new_rows: pd.DataFrame, con, md_table: str,
                  dry_run: bool) -> None:
    """Upload new note-level rows to MD table, creating it if needed."""
    if new_rows.empty:
        print(f"  Nothing new to upload for {domain_key} — skipping")
        return

    # Save to local parquet so DuckDB can read_parquet()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = OUT_DIR / f"fleet_{domain_key}_new_rows.parquet"
    new_rows.to_parquet(parquet_path, index=False)
    print(f"  Saved note-level parquet: {parquet_path} ({len(new_rows):,} rows)")

    if dry_run:
        print(f"  [DRY RUN] Would INSERT {len(new_rows):,} rows into {md_table}")
        return

    # Check if table exists; if not, create it from the parquet
    table_exists = False
    try:
        con.execute(f"SELECT 1 FROM {md_table} LIMIT 1")
        table_exists = True
    except Exception:
        pass

    if not table_exists:
        print(f"  Creating new table {md_table} from parquet …")
        con.execute(f"""
            CREATE TABLE {md_table} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
    else:
        # Column-aligned insert: only insert columns that exist in the target table
        target_cols = [r[0] for r in con.execute(f"DESCRIBE {md_table}").fetchall()]
        src_cols = list(new_rows.columns)
        shared_cols = [c for c in target_cols if c in src_cols]
        col_list = ", ".join(shared_cols)
        print(f"  Inserting {len(new_rows):,} rows ({len(shared_cols)}/{len(target_cols)} columns) …")
        con.execute(f"""
            INSERT INTO {md_table} ({col_list})
            SELECT {col_list} FROM read_parquet('{parquet_path}')
        """)

    final_count = con.execute(f"SELECT COUNT(*) FROM {md_table}").fetchone()[0]
    print(f"  ✓ {md_table} now has {final_count:,} total rows")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and upload fleet NLP results to MotherDuck")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and validate only; do not write to MotherDuck")
    parser.add_argument("--min-conf", type=float, default=DEFAULT_MIN_CONF,
                        help=f"Minimum confidence for entity flat parquet (default {DEFAULT_MIN_CONF})")
    args = parser.parse_args()

    print("=" * 65)
    print("Script 206 — Fleet NLP Validation & MotherDuck Upload")
    print(f"  Source  : {CONSOLIDATED}")
    print(f"  Dry run : {args.dry_run}")
    print(f"  Min conf: {args.min_conf}")
    print("=" * 65)

    # Connect to MotherDuck
    import duckdb
    sys.path.insert(0, str(REPO))
    from motherduck_client import get_token  # type: ignore

    token = get_token()
    if not token:
        print("ERROR: MotherDuck token not found — check motherduck.local.toml")
        sys.exit(1)
    print(f"\nMotherDuck token: SET (length={len(token)})")

    if not args.dry_run:
        con = duckdb.connect(f"md:thyroid_ete_fix_20260413?motherduck_token={token}")
        print("Connected to md:thyroid_ete_fix_20260413")
    else:
        con = None
        print("[DRY RUN] Skipping MotherDuck connection")

    # Summary tracking
    upload_summary: dict[str, dict] = {}

    for domain_key, cfg in DOMAINS.items():
        print(f"\n{'═'*65}")
        print(f"  DOMAIN: {domain_key}")
        print(f"{'═'*65}")

        # 1. Parse JSONL
        note_df, entity_df = _parse_jsonl_files(domain_key, cfg["glob_patterns"])

        if note_df.empty:
            print("  ⚠ No rows found — check CONSOLIDATED path and glob patterns")
            upload_summary[domain_key] = {"status": "empty", "new_rows": 0}
            continue

        # 2. Validate
        _validate_and_report(domain_key, note_df, entity_df,
                              cfg["known_entity_types"], args.min_conf)

        # 3. Save flat entity parquet (for analysis, filtered by min confidence)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        if not entity_df.empty:
            entity_filtered = entity_df[
                entity_df["confidence"].isna() | (entity_df["confidence"] >= args.min_conf)
            ].copy()
            entity_path = OUT_DIR / f"fleet_{domain_key}_entities_validated.parquet"
            entity_filtered.to_parquet(entity_path, index=False)
            print(f"\n  Flat entity parquet: {entity_path}")
            print(f"    {len(entity_filtered):,} entities (conf >= {args.min_conf})")
            print(f"    {entity_df['research_id'].nunique():,} unique RIDs")

        # 4. Dedup against MD
        if args.dry_run:
            new_rows = note_df  # skip actual MD query in dry run
            print(f"\n  [DRY RUN] Would dedup {len(note_df):,} fleet rows against MD")
        else:
            new_rows = _dedup_against_md(domain_key, note_df, con, cfg["md_table"])

        # 5. Upload
        if not args.dry_run and con is not None:
            _upload_to_md(domain_key, new_rows, con, cfg["md_table"], dry_run=False)
        elif args.dry_run and not new_rows.empty:
            parquet_path = OUT_DIR / f"fleet_{domain_key}_new_rows.parquet"
            new_rows.to_parquet(parquet_path, index=False)
            print(f"  [DRY RUN] Saved new rows parquet: {parquet_path}")

        upload_summary[domain_key] = {
            "status": "uploaded" if not args.dry_run else "dry_run",
            "fleet_note_rows": len(note_df),
            "fleet_rids": note_df["research_id"].nunique(),
            "new_rows": len(new_rows),
            "entity_count": len(entity_df),
            "entity_rids": entity_df["research_id"].nunique() if not entity_df.empty else 0,
        }

    if con is not None:
        con.close()

    # Final summary
    print(f"\n{'═'*65}")
    print("  FINAL UPLOAD SUMMARY")
    print(f"{'═'*65}")
    for domain, stats in upload_summary.items():
        print(f"\n  {domain}:")
        for k, v in stats.items():
            print(f"    {k:<25s}: {v:,}" if isinstance(v, int) else f"    {k:<25s}: {v}")

    print(f"\n{'═'*65}")
    print("  ✓ Script 206 complete")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    main()
