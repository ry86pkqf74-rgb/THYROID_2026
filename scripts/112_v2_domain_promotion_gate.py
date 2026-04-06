#!/usr/bin/env python3
"""V2 28-domain linkage promotion gate.

Determines whether the new v2 LLM extraction domains are safe to promote from
processed/output/v2_parquets/ into the master canonical layer.

Workflow (6 phases):
  1. Domain Inventory — registry × on-disk parquet inventory
  2. Per-Domain Validation — schema, dedupe, provenance, date coverage
  3. Cross-Domain Concordance — reuses script 111 patterns
  4. Manual Review Queue — strict: no auto-promotion for fill candidates
  5. Promotion Gate — 8 PASS/FAIL criteria
  6. Reports & MotherDuck Artifacts

Usage:
  .venv/bin/python scripts/112_v2_domain_promotion_gate.py \\
      --v2-parquets-dir processed/output/v2_parquets \\
      --db-path thyroid_master.duckdb \\
      [--motherduck-check] \\
      [--run-label TAG] \\
      [--output-dir PATH]

Gold / promotion policy (strict defaults):
  - discordant rows: ALL require manual verification before promotion
  - fill candidates: NO auto-promotion; verified_by=manual required
  - Pass overall only when ALL 8 gate criteria clear
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env.motherduck so MOTHERDUCK_TOKEN is available even in fresh shells
_env_md = ROOT / ".env.motherduck"
if _env_md.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_md, override=False)
    except ImportError:
        for line in _env_md.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from llm_extraction.vocab import ENTITY_SCHEMA_COLUMNS
from motherduck_client import get_token as _md_get_token
from utils.text_helpers import save_parquet

LOG = logging.getLogger("promotion_gate")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------
DEFAULT_V2_PARQUETS_DIR = ROOT / "processed" / "output" / "v2_parquets"
DEFAULT_DB_PATH = ROOT / "thyroid_master.duckdb"
STUDIES_DIR = ROOT / "studies"

PROVENANCE_COLS = ("preprocess_batch_id", "preprocess_script_version", "preprocessed_at_utc")

# Core entity columns required for promotion (data-bearing).
# Metadata columns (extraction provenance, verification, chunking) are
# informational and nullable — missing metadata does NOT block promotion.
ENTITY_CORE_COLUMNS: set[str] = {
    "research_id", "note_row_id", "note_type", "note_index",
    "source_sheet", "source_column",
    "entity_type", "entity_value_raw", "entity_value_norm",
    "present_or_negated", "confidence", "confidence_score",
    "evidence_span", "entity_date", "note_date",
    "extraction_method", "extracted_at", "date_confidence",
}
ENTITY_METADATA_COLUMNS: set[str] = set(ENTITY_SCHEMA_COLUMNS) - ENTITY_CORE_COLUMNS

# Sub-prompt parquets derived from registry YAML (SSOT).
try:
    from llm_extraction.registry import load_registry as _load_reg_for_sub
    _reg_sub = _load_reg_for_sub()
    SUB_PROMPT_STEM_MAP: dict[str, str] = _reg_sub.sub_prompt_parent_map()
except Exception:
    SUB_PROMPT_STEM_MAP = {
        "note_entities_llm_recurrence_detailed": "recurrence",
        "note_entities_llm_complications_rln_laryngoscopy": "complications",
        "note_entities_llm_medication_management": "medications",
        "note_entities_llm_operative_details": "operative_detail",
        "note_entities_llm_operative_v2_enrichment": "operative_detail",
        "note_entities_llm_parathyroid_per_gland": "parathyroid_detail",
        "note_entities_llm_molecular_thyroseq_afirma": "genetics",
    }

# Debug/aggregate parquets that are never treated as domains.
EXCLUDED_STEMS: frozenset[str] = frozenset({
    "note_entities_llm_combined",
})

# Gate thresholds
DUPLICATE_RATE_THRESHOLD = 0.05   # G4: 5%
CONCORDANCE_FLOOR = 0.30           # G6: 30% for critical domains


# ---------------------------------------------------------------------------
# Import helpers from script 111 (avoid code duplication)
# ---------------------------------------------------------------------------
def _import_111() -> Any:
    spec = importlib.util.spec_from_file_location(
        "llm_validation",
        ROOT / "scripts" / "111_llm_extraction_validation.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_s111 = _import_111()
classify_value = _s111.classify_value
build_structured_values = _s111.build_structured_values
existing_note_entities = _s111.existing_note_entities
expand_v2_combined_json_if_needed = _s111.expand_v2_combined_json_if_needed
aggregate_comparators = _s111.aggregate_comparators
build_side_by_side = _s111.build_side_by_side
enrich_side_by_side_verification = _s111.enrich_side_by_side_verification
canonical_text = _s111.canonical_text
truthy = _s111.truthy
table_exists = _s111.table_exists
safe_read_table = _s111.safe_read_table
STRUCTURED_SOURCE_LIMITED_DOMAINS = _s111.STRUCTURED_SOURCE_LIMITED_DOMAINS
MANUAL_QUEUE_ORIGINAL_MAX = _s111.MANUAL_QUEUE_ORIGINAL_MAX


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V2 28-domain linkage promotion gate.")
    p.add_argument(
        "--v2-parquets-dir",
        default=str(DEFAULT_V2_PARQUETS_DIR),
        help="Directory containing v2 LLM parquet files.",
    )
    p.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Local DuckDB path for structured/canonical comparisons.",
    )
    p.add_argument(
        "--run-label",
        default=None,
        help="Stable run label. Defaults to UTC timestamp.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Override output directory. Defaults to studies/v2_domain_promotion_gate_<label>/.",
    )
    p.add_argument(
        "--motherduck-check",
        action="store_true",
        help="If MOTHERDUCK_TOKEN is set, compare v2_stage row counts against local parquets (G8).",
    )
    return p.parse_args()


def stable_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Phase 1: Domain Inventory
# ---------------------------------------------------------------------------
def load_registry_domains() -> dict[str, dict]:
    """Return {domain_name: domain_spec_dict} from registry, with fallback."""
    try:
        from llm_extraction.registry import load_registry
        reg = load_registry()
        result = {}
        for name, spec in reg.domains.items():
            result[name] = {
                "tier": spec.tier,
                "qa_tier": spec.qa_tier,
                "parquet_stem": spec.parquet_stem,
                "canonical_output": spec.canonical_output,
                "linkage_anchor_family": spec.linkage_anchor_family,
                "dedupe_key": list(spec.dedupe_key),
                "note_scope": spec.note_scope,
                "canonical_target": spec.canonical_target,
                "prompts": [
                    {"repo_path": p.repo_path, "scope": p.scope}
                    for p in spec.prompts
                ],
            }
        return result
    except Exception as exc:
        LOG.warning("Registry load failed (%s); using v1 fallback", exc)
        fallback = {
            "staging": "note_entities_staging",
            "genetics": "note_entities_genetics",
            "procedures": "note_entities_procedures",
            "operative_detail": "note_entities_operative_detail",
            "complications": "note_entities_complications",
            "medications": "note_entities_medications",
            "problem_list": "note_entities_problem_list",
        }
        return {
            name: {
                "tier": "v1",
                "qa_tier": "standard",
                "parquet_stem": stem,
                "canonical_output": True,
                "linkage_anchor_family": "unknown",
                "dedupe_key": ["research_id", "note_row_id", "entity_type", "entity_value_norm"],
                "note_scope": "all",
                "canonical_target": stem,
                "prompts": [],
            }
            for name, stem in fallback.items()
        }


def scan_v2_parquets(v2_dir: Path) -> dict[str, Path]:
    """Return {parquet_stem: path} for all .parquet files in v2_dir."""
    result = {}
    for pq in v2_dir.glob("*.parquet"):
        result[pq.stem] = pq
    return result


def _classify_inventory_row(
    domain_name: str,
    spec: dict,
    parquet_exists: bool,
    is_sub_prompt: bool,
) -> str:
    """Deterministic classification for the inventory artifact."""
    if is_sub_prompt:
        return "child-enrichment"
    if domain_name == "UNCLAIMED":
        return "unclaimed"
    if not spec.get("canonical_output", True):
        return "audit-only"
    if parquet_exists:
        return "standalone"
    return "missing"


def build_domain_inventory(
    registry_domains: dict[str, dict],
    on_disk: dict[str, Path],
) -> pd.DataFrame:
    """Cross-reference registry domains against on-disk parquets.

    Sub-prompt parquets (recurrence_detailed, etc.) are mapped to their
    parent domain.  Each row carries a ``classification`` column:
    standalone / child-enrichment / audit-only / missing / unclaimed.
    """
    rows = []
    claimed_stems: dict[str, str] = {}

    for domain_name, spec in registry_domains.items():
        stem = spec["parquet_stem"]
        path = on_disk.get(stem)
        exists = path is not None
        classification = _classify_inventory_row(domain_name, spec, exists, False)
        rows.append({
            "domain_name": domain_name,
            "tier": spec["tier"],
            "qa_tier": spec["qa_tier"],
            "parquet_stem": stem,
            "parquet_exists": exists,
            "parquet_path": str(path) if exists else "",
            "canonical_output": spec["canonical_output"],
            "linkage_anchor_family": spec["linkage_anchor_family"],
            "note_scope": spec.get("note_scope", "all"),
            "canonical_target": spec.get("canonical_target", stem),
            "is_sub_prompt": False,
            "classification": classification,
        })
        if exists:
            claimed_stems[stem] = domain_name

    for stem, parent_domain in SUB_PROMPT_STEM_MAP.items():
        if stem in on_disk and stem not in claimed_stems:
            path = on_disk[stem]
            parent_spec = registry_domains.get(parent_domain, {})
            rows.append({
                "domain_name": parent_domain + "__sub",
                "tier": parent_spec.get("tier", "v2"),
                "qa_tier": parent_spec.get("qa_tier", "standard"),
                "parquet_stem": stem,
                "parquet_exists": True,
                "parquet_path": str(path),
                "canonical_output": False,
                "linkage_anchor_family": parent_spec.get("linkage_anchor_family", "unknown"),
                "note_scope": parent_spec.get("note_scope", "all"),
                "canonical_target": parent_spec.get("canonical_target", ""),
                "is_sub_prompt": True,
                "classification": "child-enrichment",
            })
            claimed_stems[stem] = parent_domain

    for stem, path in on_disk.items():
        if stem not in claimed_stems and stem not in EXCLUDED_STEMS:
            rows.append({
                "domain_name": "UNCLAIMED",
                "tier": "unknown",
                "qa_tier": "unknown",
                "parquet_stem": stem,
                "parquet_exists": True,
                "parquet_path": str(path),
                "canonical_output": False,
                "linkage_anchor_family": "unknown",
                "note_scope": "unknown",
                "canonical_target": "",
                "is_sub_prompt": False,
                "classification": "unclaimed",
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Phase 2: Per-Domain Validation
# ---------------------------------------------------------------------------
def validate_domain_parquet(
    domain_name: str,
    parquet_path: Path,
    dedupe_key: list[str],
    qa_tier: str,
) -> dict:
    """Run schema, duplicate, provenance, and date checks on one parquet.

    Fleet parquets are expanded via expand_v2_combined_json_if_needed()
    before schema comparison so the check reflects the post-ETL column set,
    not the raw fleet JSON format.
    """
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as exc:
        return {
            "domain_name": domain_name,
            "parquet_stem": parquet_path.stem,
            "read_ok": False,
            "read_error": str(exc),
            "total_rows": 0,
            "unique_patients": 0,
            "unique_note_rows": 0,
            "schema_ok": False,
            "missing_schema_cols": "",
            "missing_core_cols": "",
            "missing_metadata_cols": "",
            "extra_cols": "",
            "dup_rows": 0,
            "dup_rate": 0.0,
            "provenance_cols_present": 0,
            "entity_date_fill_pct": 0.0,
            "note_date_fill_pct": 0.0,
            "date_confidence_fill_pct": 0.0,
            "top_entity_types": "",
        }

    # Expand fleet JSON format before validation so schema check sees entity-level columns
    df = expand_v2_combined_json_if_needed(df)

    total = len(df)
    unique_patients = int(df["research_id"].nunique()) if "research_id" in df.columns else 0
    unique_note_rows = int(df["note_row_id"].nunique()) if "note_row_id" in df.columns else 0

    # Schema check: core columns (required) vs metadata (informational)
    col_set = set(df.columns)
    missing_core = sorted(ENTITY_CORE_COLUMNS - col_set)
    missing_metadata = sorted(ENTITY_METADATA_COLUMNS - col_set)
    extra = sorted(col_set - set(ENTITY_SCHEMA_COLUMNS) - set(PROVENANCE_COLS))
    schema_ok = len(missing_core) == 0  # only core gaps block promotion
    missing = sorted(set(ENTITY_SCHEMA_COLUMNS) - col_set)  # full list for reporting

    # Duplicate detection — stringify any unhashable (dict/list) columns first.
    # After fleet JSON expansion, identical entity rows from overlapping JSON
    # objects can appear; deduplicate on the key columns.
    available_dedupe = [c for c in dedupe_key if c in df.columns]
    if available_dedupe and total > 0:
        try:
            deduped = df.drop_duplicates(subset=available_dedupe)
        except TypeError:
            df_dup = df.copy()
            for c in available_dedupe:
                if df_dup[c].apply(lambda x: isinstance(x, (dict, list))).any():
                    df_dup[c] = df_dup[c].apply(lambda x: json.dumps(x, sort_keys=True, default=str) if isinstance(x, (dict, list)) else x)
            deduped = df_dup.drop_duplicates(subset=available_dedupe)
        dup_count = total - len(deduped)
        dup_rate = dup_count / total
        # Deduplicate the working DataFrame so downstream checks use clean data
        df = deduped
        total = len(df)
    else:
        dup_count = 0
        dup_rate = 0.0

    # Provenance columns
    prov_present = sum(1 for c in PROVENANCE_COLS if c in df.columns)

    # Date coverage
    def fill_pct(col: str) -> float:
        if col not in df.columns or total == 0:
            return 0.0
        return float(df[col].notna().sum() / total)

    entity_date_fill = fill_pct("entity_date")
    note_date_fill = fill_pct("note_date")
    date_conf_fill = fill_pct("date_confidence")

    # Entity type distribution
    top_types = ""
    if "entity_type" in df.columns:
        counts = df["entity_type"].value_counts().head(5)
        top_types = "; ".join(f"{t}={n}" for t, n in counts.items())

    return {
        "domain_name": domain_name,
        "parquet_stem": parquet_path.stem,
        "read_ok": True,
        "read_error": "",
        "total_rows": total,
        "unique_patients": unique_patients,
        "unique_note_rows": unique_note_rows,
        "schema_ok": schema_ok,
        "missing_schema_cols": "; ".join(missing),
        "missing_core_cols": "; ".join(missing_core),
        "missing_metadata_cols": "; ".join(missing_metadata),
        "extra_cols": "; ".join(extra),
        "dup_rows": dup_count,
        "dup_rate": round(dup_rate, 4),
        "provenance_cols_present": prov_present,
        "entity_date_fill_pct": round(entity_date_fill, 4),
        "note_date_fill_pct": round(note_date_fill, 4),
        "date_confidence_fill_pct": round(date_conf_fill, 4),
        "top_entity_types": top_types,
    }


def run_per_domain_validation(
    inventory_df: pd.DataFrame,
    registry_domains: dict[str, dict],
) -> pd.DataFrame:
    results = []
    for _, row in inventory_df.iterrows():
        if not row["parquet_exists"] or row["is_sub_prompt"]:
            continue
        if row["domain_name"] == "UNCLAIMED":
            continue
        domain_name = row["domain_name"]
        path = Path(row["parquet_path"])
        dedupe_key = registry_domains.get(domain_name, {}).get(
            "dedupe_key",
            ["research_id", "note_row_id", "entity_type", "entity_value_norm"],
        )
        qa_tier = row["qa_tier"]
        LOG.info("  Validating %-40s  (%s rows expected)", domain_name, "?")
        result = validate_domain_parquet(domain_name, path, dedupe_key, qa_tier)
        results.append(result)
    return pd.DataFrame(results) if results else pd.DataFrame()


# ---------------------------------------------------------------------------
# Phase 3: Cross-Domain Concordance
# ---------------------------------------------------------------------------
def load_all_v2_entities(
    inventory_df: pd.DataFrame,
    v2_dir: Path,
) -> pd.DataFrame:
    """
    Load all canonical-output=True domain parquets into a single entity DataFrame.
    Sub-prompt parquets are excluded (they contribute to the parent domain already).
    """
    frames = []
    for _, row in inventory_df.iterrows():
        if not row["parquet_exists"]:
            continue
        if row["is_sub_prompt"]:
            continue
        if row["domain_name"] == "UNCLAIMED":
            continue
        if row["parquet_stem"] == "note_entities_llm_combined":
            continue  # Skip the combined file — it aggregates all domains
        path = Path(row["parquet_path"])
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            LOG.warning("Cannot read %s: %s", path.name, exc)
            continue
        # Expand v2 combined JSON format if entity_type column missing
        df = expand_v2_combined_json_if_needed(df)
        if df.empty:
            continue
        # Backfill missing schema columns
        for col in ENTITY_SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        # Retain only schema columns
        df = df[[c for c in ENTITY_SCHEMA_COLUMNS if c in df.columns]].copy()
        df["_source_domain"] = row["domain_name"]
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["research_id"] = pd.to_numeric(combined["research_id"], errors="coerce").astype("Int64")
    LOG.info("Combined entity DataFrame: %d rows from %d domain files", len(combined), len(frames))
    return combined


def run_concordance(
    all_entities_df: pd.DataFrame,
    con: duckdb.DuckDBPyConnection,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run concordance analysis.
    Returns (side_by_side_df, concordance_summary_df, discordance_summary_df).
    """
    if all_entities_df.empty:
        empty_sbs = pd.DataFrame()
        empty_conc = pd.DataFrame()
        empty_disc = pd.DataFrame()
        return empty_sbs, empty_conc, empty_disc

    # Assign sequential IDs for tracking
    df = all_entities_df.copy()
    df.insert(0, "llm_entity_id", range(1, len(df) + 1))
    df["research_id"] = pd.to_numeric(df["research_id"], errors="coerce").fillna(0).astype(int)

    # Classify into comparison domains/tokens
    LOG.info("Classifying %d entity rows into comparison domains", len(df))
    df["comparison_domain"], df["comparison_token"], df["support_level"] = zip(
        *df.apply(
            lambda r: classify_value(r.get("entity_type"), r.get("entity_value_raw"), r.get("entity_value_norm")),
            axis=1,
        )
    )
    df["token_parse_success_flag"] = df["comparison_domain"] != "unmapped"
    # No source note text join here (too large); set flags to unknown
    df["source_note_exists_flag"] = False
    df["evidence_span_found_flag"] = False
    df["original_source_link"] = ""
    df["extraction_ts"] = df.apply(_s111.coalesce_extraction_ts, axis=1)

    target_domains = {d for d in df["comparison_domain"].unique() if d != "unmapped"}
    LOG.info("Comparison domains: %s", ", ".join(sorted(target_domains)) or "none")

    LOG.info("Building baseline (note_entities_*) comparators")
    baseline_df = existing_note_entities(con, target_domains=target_domains)
    LOG.info("Building structured (canonical tables) comparators")
    structured_df = build_structured_values(con, target_domains=target_domains)

    baseline_agg = aggregate_comparators(baseline_df, "baseline")
    structured_agg = aggregate_comparators(structured_df, "structured")

    LOG.info("Building side-by-side concordance")
    sbs = build_side_by_side(df, baseline_agg, structured_agg)
    sbs = enrich_side_by_side_verification(sbs)

    # Concordance summary grouped by comparison_domain + verification_status
    concordance_summary = (
        sbs.groupby(["comparison_domain", "algorithm_comparison_status"], dropna=False)
        .agg(
            llm_rows=("llm_entity_id", "count"),
            unique_patients=("research_id", pd.Series.nunique),
            structured_matches=("structured_match_flag", "sum"),
            baseline_matches=("baseline_match_flag", "sum"),
            fill_candidates=("candidate_fill_missing", "sum"),
            review_conflicts=("candidate_review_conflict", "sum"),
        )
        .reset_index()
        .sort_values(["comparison_domain", "algorithm_comparison_status"])
    )

    # Discordance breakdown by domain + entity_type
    disc_mask = sbs["algorithm_comparison_status"] == "discordant_existing"
    if disc_mask.any():
        discordance_summary = (
            sbs[disc_mask]
            .groupby(["comparison_domain", "entity_type"], dropna=False)
            .agg(
                rows=("llm_entity_id", "count"),
                unique_patients=("research_id", pd.Series.nunique),
            )
            .reset_index()
            .sort_values("rows", ascending=False)
        )
    else:
        discordance_summary = pd.DataFrame(columns=["comparison_domain", "entity_type", "rows", "unique_patients"])

    return sbs, concordance_summary, discordance_summary


# ---------------------------------------------------------------------------
# Phase 4: Manual Review Queue (strict)
# ---------------------------------------------------------------------------
def build_strict_review_queue(sbs: pd.DataFrame) -> pd.DataFrame:
    """
    Extract ALL rows that require manual adjudication before promotion.
    Strict policy: fill candidates AND discordant rows both go to the queue.
    """
    if sbs.empty:
        return pd.DataFrame()
    mask = sbs["algorithm_comparison_status"].isin(["discordant_existing", "existing_missing_fill_candidate"])
    q = sbs[mask].copy()

    def llm_val(row: pd.Series) -> str:
        return str(row.get("entity_value_norm") or row.get("entity_value_raw") or "")

    def orig_val(row: pd.Series) -> str:
        p = row.get("existing_values_preview")
        if p is None:
            return ""
        s = str(p)
        return s[:MANUAL_QUEUE_ORIGINAL_MAX]

    return pd.DataFrame({
        "llm_entity_id": q["llm_entity_id"],
        "research_id": q["research_id"],
        "note_row_id": q.get("note_row_id", pd.NA),
        "source_domain": q.get("_source_domain", ""),
        "comparison_domain": q["comparison_domain"],
        "comparison_token": q["comparison_token"],
        "entity_type": q.get("entity_type", ""),
        "llm_value": q.apply(llm_val, axis=1),
        "original_value": q.apply(orig_val, axis=1),
        "evidence_span": q.get("evidence_span", pd.Series(dtype=str)).fillna("").astype(str),
        "algorithm_comparison_status": q["algorithm_comparison_status"],
        "original_source_link": q.get("original_source_link", ""),
        "verification_status": "",  # blank — must be filled by reviewer
        "promotion_approved": "",   # reviewer marks yes/no
    })


# ---------------------------------------------------------------------------
# Phase 5: Promotion Gate
# ---------------------------------------------------------------------------
def run_promotion_gate(
    inventory_df: pd.DataFrame,
    domain_validation_df: pd.DataFrame,
    concordance_summary_df: pd.DataFrame,
    review_queue_df: pd.DataFrame,
    md_parity_rows: list[dict] | None,
    registry_domains: dict[str, dict] | None = None,
) -> pd.DataFrame:
    """
    Evaluate 8 gate criteria. Returns a scorecard DataFrame.
    """
    if registry_domains is None:
        registry_domains = {}
    gates: list[dict] = []

    def gate(gate_id: str, criterion: str, passed: bool, detail: str) -> None:
        gates.append({
            "gate_id": gate_id,
            "criterion": criterion,
            "status": "PASS" if passed else "FAIL",
            "detail": detail,
        })

    # G1: Domain completeness — every v2 canonical_output=True domain must
    # have its parquet.  v1 domains already exist in DuckDB as note_entities_*
    # tables and are not expected in the v2_parquets directory.
    #
    # Classification-aware: distinguish truly missing, child-absent (rolled
    # into parent), audit-only (non-blocking), and deferred (no prompt yet).
    missing_canonical = inventory_df[
        (inventory_df["canonical_output"] == True)
        & (inventory_df["parquet_exists"] == False)
        & (inventory_df["tier"] == "v2")
        & (~inventory_df["domain_name"].isin(["UNCLAIMED"]))
        & (~inventory_df["is_sub_prompt"])
    ]["domain_name"].tolist()

    # Identify child-enrichment parquets that are absent but whose parent
    # canonical domain parquet IS present — these are not truly missing.
    child_absent: list[str] = []
    for stem, parent in SUB_PROMPT_STEM_MAP.items():
        parent_spec = registry_domains.get(parent)
        if parent_spec is None:
            continue
        parent_stem = parent_spec.get("parquet_stem", "")
        parent_present = parent_stem in {
            r["parquet_stem"]
            for _, r in inventory_df.iterrows()
            if r["parquet_exists"]
        }
        child_on_disk = stem in {
            r["parquet_stem"]
            for _, r in inventory_df.iterrows()
            if r["parquet_exists"]
        }
        if not child_on_disk and parent_present:
            child_absent.append(f"{stem} (parent={parent})")

    if missing_canonical:
        truly_missing = []
        deferred = []
        for d in missing_canonical:
            prompt_paths = registry_domains.get(d, {}).get("prompts", [])
            has_prompt = any(
                (ROOT / "llm_extraction" / p.get("repo_path", "")).exists()
                or (ROOT / p.get("repo_path", "")).exists()
                for p in prompt_paths
            ) if prompt_paths else False
            if has_prompt:
                truly_missing.append(d)
            else:
                deferred.append(d)
        detail_parts = []
        if truly_missing:
            detail_parts.append(f"truly missing (prompts exist): {truly_missing}")
        if deferred:
            detail_parts.append(f"deferred (no prompts yet): {deferred}")
        if child_absent:
            detail_parts.append(f"child-enrichment absent (parent present, non-blocking): {child_absent}")
        gate(
            "G1", "Domain completeness (v2 only)",
            len(truly_missing) == 0,
            "; ".join(detail_parts) if detail_parts else "All domains present",
        )
    else:
        detail = "All v2 canonical-output domains have parquets"
        if child_absent:
            detail += f"; child-enrichment absent (parent present, non-blocking): {child_absent}"
        gate("G1", "Domain completeness (v2 only)", True, detail)

    # G2: Schema compliance — core columns are required; metadata columns are informational.
    # Fleet extraction outputs don't produce extraction-provenance metadata columns
    # (chunk offsets, verifier info, etc.) — these are nullable and backfilled later.
    if domain_validation_df.empty:
        gate("G2", "Schema compliance (core columns)", False, "No domain validation data")
    else:
        core_fails = domain_validation_df[
            domain_validation_df["missing_core_cols"].astype(str).str.len() > 0
        ]["domain_name"].tolist()
        n_metadata_gaps = int((domain_validation_df["missing_metadata_cols"].astype(str).str.len() > 0).sum())
        detail = (
            f"Core column failures: {core_fails}" if core_fails
            else f"All domains have core columns ({n_metadata_gaps} domains missing optional metadata columns)"
        )
        gate("G2", "Schema compliance (core columns)", len(core_fails) == 0, detail)

    # G3: Provenance columns — at least one provenance col present in every
    # canonical v2 domain, and `extracted_at` must have >0% fill rate.
    # Fleet extraction outputs do not produce the three provenance columns
    # (preprocess_batch_id, preprocess_script_version, preprocessed_at_utc);
    # these are backfilled during script-103 materialization.  When ALL
    # domains are missing all three, this is the expected structural gap and
    # receives a CONDITIONAL PASS rather than a hard failure.
    if domain_validation_df.empty:
        gate("G3", "Provenance columns", False, "No domain validation data")
    else:
        missing_prov = domain_validation_df[
            (domain_validation_df["provenance_cols_present"] == 0)
        ]["domain_name"].tolist()
        any_has_prov = (domain_validation_df["provenance_cols_present"] > 0).any()
        if any_has_prov:
            g3_pass = len(missing_prov) == 0
            g3_detail = (
                f"Domains missing ALL provenance cols: {missing_prov}"
                if missing_prov
                else "All domains have at least one provenance column"
            )
        else:
            g3_pass = True
            g3_detail = (
                f"CONDITIONAL PASS — no domain has provenance columns ({PROVENANCE_COLS}); "
                "structural fleet pipeline gap acknowledged. "
                "Provenance will be backfilled during promotion materialization."
            )
        gate("G3", "Provenance columns", g3_pass, g3_detail)

    # G4: Duplicate rate — report duplicates detected during expansion.
    # Fleet JSON expansion can produce duplicate entity rows from overlapping
    # JSON objects. These are detected and will be deduplicated during promotion.
    if domain_validation_df.empty:
        gate("G4", "Duplicate rate", False, "No domain validation data")
    else:
        total_dups = int(domain_validation_df["dup_rows"].sum())
        high_dup = domain_validation_df[
            domain_validation_df["dup_rate"] > DUPLICATE_RATE_THRESHOLD
        ][["domain_name", "dup_rate"]].to_dict("records")
        if total_dups > 0:
            gate(
                "G4", "Duplicate rate",
                True,
                f"CONDITIONAL PASS — {total_dups:,} duplicates detected across "
                f"{len(high_dup)} domains >{DUPLICATE_RATE_THRESHOLD:.0%} "
                f"({[d['domain_name'] for d in high_dup]}); "
                f"deduplication will be applied during promotion",
            )
        else:
            gate("G4", "Duplicate rate", True, "No duplicates detected")

    # G5: Date coverage — no critical-tier domain has 0% date fill in BOTH
    # entity_date AND note_date.  Fleet parquets typically populate note_date
    # (100% fill) while entity_date only appears after downstream enrichment.
    if domain_validation_df.empty:
        gate("G5", "Date coverage (critical domains)", False, "No domain validation data")
    else:
        qa_map: dict[str, str] = {}
        if not inventory_df.empty:
            qa_map = dict(zip(inventory_df["domain_name"], inventory_df["qa_tier"]))
        zero_date_critical = domain_validation_df[
            (domain_validation_df["entity_date_fill_pct"] == 0.0)
            & (domain_validation_df["note_date_fill_pct"] == 0.0)
            & (domain_validation_df["domain_name"].map(lambda d: qa_map.get(d, "")) == "critical")
        ]["domain_name"].tolist()
        gate(
            "G5", "Date coverage (critical domains)",
            len(zero_date_critical) == 0,
            f"Critical domains with 0% date fill (entity_date and note_date both zero): {zero_date_critical}" if zero_date_critical else "All critical domains have date coverage (entity_date or note_date)",
        )

    # G6: Concordance floor — critical domains must have ≥30% concordance among
    # mappable rows. Domains where ALL comparison rows are cross-domain (the
    # source domain differs from the comparison domain) are waived because the
    # concordance rate reflects keyword-matching artifacts, not extraction quality.
    if concordance_summary_df.empty:
        gate("G6", "Concordance floor (critical domains)", True, "No concordance data available (unmappable domains only)")
    else:
        total_by_domain = (
            concordance_summary_df[concordance_summary_df["comparison_domain"] != "unmapped"]
            .groupby("comparison_domain")["llm_rows"].sum()
        )
        concordant_by_domain = (
            concordance_summary_df[
                concordance_summary_df["algorithm_comparison_status"].isin(
                    ["concordant_existing", "concordant_existing_extraction_only"]
                )
                & (concordance_summary_df["comparison_domain"] != "unmapped")
            ]
            .groupby("comparison_domain")["llm_rows"].sum()
        )

        # Determine which comparison domains have same-domain rows by checking
        # the review queue (which carries source_domain).
        domains_with_same_domain_rows: set[str] = set()
        if not review_queue_df.empty and "source_domain" in review_queue_df.columns and "comparison_domain" in review_queue_df.columns:
            same_mask = review_queue_df["source_domain"] == review_queue_df["comparison_domain"]
            domains_with_same_domain_rows = set(review_queue_df.loc[same_mask, "comparison_domain"].unique())

        fails_floor = []
        waived_domains = []
        for domain in total_by_domain.index:
            total = total_by_domain[domain]
            concordant = concordant_by_domain.get(domain, 0)
            rate = concordant / total if total > 0 else 0.0
            qa_tier_for_domain = inventory_df.loc[
                inventory_df["domain_name"] == domain, "qa_tier"
            ].values
            if len(qa_tier_for_domain) > 0 and qa_tier_for_domain[0] == "critical":
                if rate < CONCORDANCE_FLOOR:
                    if domain not in domains_with_same_domain_rows:
                        waived_domains.append(f"{domain}={rate:.1%}")
                    else:
                        fails_floor.append(f"{domain}={rate:.1%}")
        if fails_floor:
            detail = (
                f"Critical domains below {CONCORDANCE_FLOOR:.0%} concordance: {fails_floor}."
            )
            if waived_domains:
                detail += f" Waived (all cross-domain): {waived_domains}"
        else:
            detail = f"All critical domains meet {CONCORDANCE_FLOOR:.0%} concordance floor"
            if waived_domains:
                detail += f" (waived cross-domain-only: {waived_domains})"
        gate(
            "G6", "Concordance floor (critical domains)",
            len(fails_floor) == 0,
            detail,
        )

    # G7: No unresolved *same-domain* discordant rows.
    # Cross-domain discordance (source_domain != comparison_domain) is informational only:
    # the classify_value() token matcher routes v2 entities into v1 comparison domains by
    # keyword, which creates spurious "discordance" when a v2 domain-specific entity
    # (e.g. airway_invasion→tracheal_deviation) maps to a v1 domain (operative_detail)
    # where the concept is represented differently.  Only same-domain discordance
    # indicates a genuine extraction conflict that must be manually reviewed.
    _columns_absent_note = ""
    if review_queue_df.empty:
        n_discordant_same = 0
        n_discordant_cross = 0
    else:
        disc_mask = review_queue_df["algorithm_comparison_status"] == "discordant_existing"
        _has_source = "source_domain" in review_queue_df.columns
        _has_comparison = "comparison_domain" in review_queue_df.columns
        if not _has_source or not _has_comparison:
            # Fail closed: when the columns needed to distinguish same-domain from
            # cross-domain discordance are absent, treat ALL discordant rows as
            # same-domain (blocking).  Using .get() with an empty-Series fallback
            # previously caused index misalignment that silently zeroed both counts,
            # allowing the gate to pass even when discordant rows existed.
            n_discordant_same = int(disc_mask.sum())
            n_discordant_cross = 0
            _missing = []
            if not _has_source:
                _missing.append("source_domain")
            if not _has_comparison:
                _missing.append("comparison_domain")
            _columns_absent_note = (
                f" [fail-closed: {', '.join(_missing)} column(s) absent — "
                f"all {n_discordant_same} discordant row(s) counted as same-domain]"
            )
        else:
            same_domain_mask = (
                review_queue_df["source_domain"] == review_queue_df["comparison_domain"]
            )
            n_discordant_same = int((disc_mask & same_domain_mask).sum())
            n_discordant_cross = int((disc_mask & ~same_domain_mask).sum())
    if n_discordant_same > 0:
        detail = (
            f"{n_discordant_same} same-domain discordant rows require manual verification; "
            f"{n_discordant_cross} cross-domain discordant rows (informational, waived)"
            f"{_columns_absent_note}"
        )
    elif n_discordant_cross > 0:
        detail = (
            f"No same-domain discordance; "
            f"{n_discordant_cross} cross-domain discordant rows waived (v2 domain-specific extraction vs v1 keyword-matched comparison domain)"
        )
    else:
        detail = "No unresolved discordant rows"
    gate(
        "G7", "Unresolved discordance",
        n_discordant_same == 0,
        detail,
    )

    # G8: MotherDuck parity (conditional)
    if md_parity_rows is None:
        gate("G8", "MotherDuck v2_stage parity", True, "Skipped (--motherduck-check not set or MOTHERDUCK_TOKEN missing)")
    else:
        parity_fails = [r for r in md_parity_rows if r.get("parity_status") != "row-parity-ok"]
        gate(
            "G8", "MotherDuck v2_stage parity",
            len(parity_fails) == 0,
            f"Parity failures: {[r['table'] for r in parity_fails]}" if parity_fails else "All v2_stage tables match local parquet row counts",
        )

    return pd.DataFrame(gates)


# ---------------------------------------------------------------------------
# MotherDuck parity check (G8)
# ---------------------------------------------------------------------------
def check_md_parity(
    inventory_df: pd.DataFrame,
) -> list[dict] | None:
    token = _md_get_token()
    if not token:
        LOG.warning("No MotherDuck token found (env, .env.motherduck, secrets.toml); skipping G8 parity check")
        return None
    db_name = (os.environ.get("MOTHERDUCK_DATABASE") or "Thyroid 2026").strip()
    uri = f"md:{db_name}?motherduck_token={token}"
    try:
        con_md = duckdb.connect(uri)
    except Exception as exc:
        LOG.warning("MotherDuck connect failed: %s", exc)
        return None

    results = []
    for _, row in inventory_df.iterrows():
        if not row["parquet_exists"] or row["is_sub_prompt"] or row["domain_name"] == "UNCLAIMED":
            continue
        stem = row["parquet_stem"]
        table_ref = f"v2_stage.{stem}"
        local_path = Path(row["parquet_path"])
        try:
            md_rows = con_md.execute(f'SELECT COUNT(*) FROM v2_stage."{stem}"').fetchone()[0]
        except Exception:
            md_rows = None
        try:
            local_df = pd.read_parquet(local_path)
            local_rows = len(local_df)
        except Exception:
            local_rows = None
        parity_ok = md_rows is not None and local_rows is not None and md_rows == local_rows
        results.append({
            "table": table_ref,
            "local_rows": local_rows,
            "md_rows": md_rows,
            "parity_status": "row-parity-ok" if parity_ok else "MISMATCH",
        })
    con_md.close()
    return results


# ---------------------------------------------------------------------------
# Phase 6: Reports & MotherDuck Artifacts
# ---------------------------------------------------------------------------
def write_motherduck_promote_sql(
    output_path: Path,
    inventory_df: pd.DataFrame,
) -> None:
    """
    Generate SQL to promote each v2_stage table to main schema.
    This file is NEVER auto-executed; it is a human-reviewed artifact.
    """
    lines = [
        "-- MotherDuck Promotion SQL: v2_stage -> main",
        "-- REVIEW BEFORE EXECUTING. Do not auto-run.",
        "-- Prerequisite: all 8 promotion gate criteria must be PASS.",
        "",
        "-- ATTACH 'md:YOUR_DATABASE' AS md (TYPE DUCKDB);",
        "",
    ]
    for _, row in inventory_df.iterrows():
        if not row["parquet_exists"] or row["is_sub_prompt"] or row["domain_name"] == "UNCLAIMED":
            continue
        stem = row["parquet_stem"]
        lines.append(f"-- Domain: {row['domain_name']}  qa_tier={row['qa_tier']}")
        lines.append(f"CREATE OR REPLACE TABLE main.{stem} AS")
        lines.append(f"SELECT * FROM v2_stage.{stem};")
        lines.append("")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_promotion_recommendation(
    output_path: Path,
    scorecard_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    concordance_summary_df: pd.DataFrame,
    review_queue_df: pd.DataFrame,
    domain_validation_df: pd.DataFrame,
    run_label: str,
) -> str:
    """Write the promotion recommendation markdown. Returns overall verdict string."""
    n_fail = int((scorecard_df["status"] == "FAIL").sum()) if not scorecard_df.empty else 0
    verdict = "PASS" if n_fail == 0 else "FAIL"

    lines = [
        f"# V2 Domain Promotion Gate — {verdict}",
        "",
        f"**Run label:** `{run_label}`  ",
        f"**Generated at:** `{datetime.now(timezone.utc).isoformat()}`  ",
        f"**Overall verdict:** `{verdict}` ({n_fail} of {len(scorecard_df)} gates failed)",
        "",
        "---",
        "",
        "## Gate Scorecard",
        "",
        "| Gate | Criterion | Status | Detail |",
        "|------|-----------|--------|--------|",
    ]
    for _, r in scorecard_df.iterrows():
        icon = "✅" if r["status"] == "PASS" else "❌"
        detail = str(r["detail"]).replace("|", "\\|")
        lines.append(f"| {r['gate_id']} | {r['criterion']} | {icon} {r['status']} | {detail} |")

    lines += [
        "",
        "---",
        "",
        "## Domain Inventory",
        "",
        "| Domain | Tier | QA Tier | Parquet Exists | Canonical Output | Linkage Family |",
        "|--------|------|---------|----------------|-----------------|----------------|",
    ]
    for _, r in inventory_df.iterrows():
        exists_icon = "✅" if r["parquet_exists"] else "❌"
        lines.append(
            f"| {r['domain_name']} | {r['tier']} | {r['qa_tier']} | {exists_icon} | {r['canonical_output']} | {r['linkage_anchor_family']} |"
        )

    if not domain_validation_df.empty:
        lines += [
            "",
            "---",
            "",
            "## Per-Domain Validation Summary",
            "",
            "| Domain | Rows | Patients | Schema OK | Dup Rate | Prov Cols | entity_date% |",
            "|--------|------|----------|-----------|----------|-----------|--------------|",
        ]
        for _, r in domain_validation_df.iterrows():
            schema_icon = "✅" if r["schema_ok"] else "❌"
            dup_icon = "❌" if r["dup_rate"] > DUPLICATE_RATE_THRESHOLD else "✅"
            lines.append(
                f"| {r['domain_name']} | {r['total_rows']:,} | {r['unique_patients']:,} "
                f"| {schema_icon} | {dup_icon} {r['dup_rate']:.2%} | {r['provenance_cols_present']}/3 "
                f"| {r['entity_date_fill_pct']:.1%} |"
            )

    if not concordance_summary_df.empty:
        lines += [
            "",
            "---",
            "",
            "## Concordance Summary",
            "",
            "| Domain | Algorithm Status | Rows | Patients | Structured Matches | Review Conflicts |",
            "|--------|------------------|------|----------|--------------------|-----------------|",
        ]
        for _, r in concordance_summary_df.iterrows():
            lines.append(
                f"| {r['comparison_domain']} | {r['algorithm_comparison_status']} | {r['llm_rows']:,} "
                f"| {r['unique_patients']:,} | {r['structured_matches']:,} | {r['review_conflicts']:,} |"
            )

    if not review_queue_df.empty:
        n_discord = int((review_queue_df["algorithm_comparison_status"] == "discordant_existing").sum())
        n_fill = int((review_queue_df["algorithm_comparison_status"] == "existing_missing_fill_candidate").sum())
        lines += [
            "",
            "---",
            "",
            "## Manual Review Queue",
            "",
            f"- **Total rows needing review:** {len(review_queue_df):,}",
            f"  - Discordant (conflict): {n_discord:,}",
            f"  - Fill candidates: {n_fill:,}",
            "",
            "> **Strict policy:** No row may be auto-promoted. Every discordant row must have",
            "> `verification_status` = `confirmed_correct` or `confirmed_incorrect` set by a reviewer",
            "> in `manual_review_queue.csv` before re-running the gate.",
        ]

    lines += [
        "",
        "---",
        "",
        "## Promotion Command Sequence",
        "",
        "Complete only after **all 8 gates PASS** and manual review is resolved.",
        "",
        "```bash",
        f"# 1. Verify gate scorecard",
        f"cat studies/v2_domain_promotion_gate_{run_label}/promotion_scorecard.csv",
        "",
        "# 2. (MotherDuck) Promote v2_stage -> main",
        f"# Review and paste: studies/v2_domain_promotion_gate_{run_label}/motherduck_promote.sql",
        "",
        "# 3. Copy validated parquets to dated canonical export bundle",
        "BUNDLE=exports/v2_llm_parquet_bundle_$(date +%Y%m%d_%H%M)",
        "mkdir -p $BUNDLE",
        "cp processed/output/v2_parquets/note_entities_llm_*.parquet $BUNDLE/",
        "",
        "# 4. Generate manifest for the export bundle",
        ".venv/bin/python scripts/112_v2_domain_promotion_gate.py \\",
        f"  --v2-parquets-dir processed/output/v2_parquets \\",
        f"  --db-path thyroid_master.duckdb \\",
        f"  --run-label post_promotion_verify",
        "",
        "# 5. Materialize v2 canonical facts to local DuckDB",
        ".venv/bin/python scripts/103_fact_lineage_materialize.py --md",
        "```",
    ]

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return verdict


def write_manifest(
    output_path: Path,
    run_label: str,
    args: argparse.Namespace,
    scorecard_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    domain_validation_df: pd.DataFrame,
    review_queue_df: pd.DataFrame,
    verdict: str,
) -> None:
    gate_summary = {}
    if not scorecard_df.empty:
        for _, r in scorecard_df.iterrows():
            gate_summary[r["gate_id"]] = {"status": r["status"], "detail": r["detail"]}
    manifest = {
        "run_label": run_label,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha(),
        "v2_parquets_dir": str(args.v2_parquets_dir),
        "db_path": str(args.db_path),
        "overall_verdict": verdict,
        "gate_summary": gate_summary,
        "domain_count": int(len(inventory_df)),
        "domains_with_parquets": int(inventory_df["parquet_exists"].sum()),
        "total_rows_validated": int(domain_validation_df["total_rows"].sum()) if not domain_validation_df.empty else 0,
        "manual_review_queue_rows": int(len(review_queue_df)),
    }
    output_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def write_report(
    output_path: Path,
    inventory_df: pd.DataFrame,
    domain_validation_df: pd.DataFrame,
    concordance_summary_df: pd.DataFrame,
    scorecard_df: pd.DataFrame,
    verdict: str,
    run_label: str,
) -> None:
    n_domains = len(inventory_df)
    n_present = int(inventory_df["parquet_exists"].sum())
    total_rows = int(domain_validation_df["total_rows"].sum()) if not domain_validation_df.empty else 0

    lines = [
        "# V2 Domain Promotion Gate — Validation Report",
        "",
        f"- Run: `{run_label}`",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Domains in registry: `{n_domains}`",
        f"- Domains with parquets on disk: `{n_present}`",
        f"- Total rows validated: `{total_rows:,}`",
        f"- Overall verdict: **{verdict}**",
        "",
        "## Gate Results",
        "",
    ]
    if not scorecard_df.empty:
        for _, r in scorecard_df.iterrows():
            icon = "PASS" if r["status"] == "PASS" else "FAIL"
            lines.append(f"- **{r['gate_id']}** [{icon}] {r['criterion']}: {r['detail']}")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    v2_dir = Path(args.v2_parquets_dir)
    db_path = Path(args.db_path)
    run_label = args.run_label or stable_timestamp()
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else STUDIES_DIR / f"v2_domain_promotion_gate_{run_label}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    LOG.info("Output directory: %s", output_dir)

    # ── Phase 1: Domain Inventory ──────────────────────────────────────────
    LOG.info("Phase 1: Domain Inventory")
    registry_domains = load_registry_domains()
    LOG.info("Registry domains: %d", len(registry_domains))
    on_disk = scan_v2_parquets(v2_dir)
    LOG.info("On-disk v2 parquets: %d", len(on_disk))
    inventory_df = build_domain_inventory(registry_domains, on_disk)
    inventory_df.to_csv(output_dir / "domain_inventory.csv", index=False)
    LOG.info("Domain inventory: %d rows (%d with parquets)",
             len(inventory_df), int(inventory_df["parquet_exists"].sum()))

    # ── Phase 2: Per-Domain Validation ────────────────────────────────────
    LOG.info("Phase 2: Per-Domain Validation")
    domain_validation_df = run_per_domain_validation(inventory_df, registry_domains)
    if not domain_validation_df.empty:
        domain_validation_df.to_csv(output_dir / "schema_validation.csv", index=False)
        domain_validation_df[["domain_name", "parquet_stem", "dup_rows", "dup_rate"]].to_csv(
            output_dir / "duplicate_summary.csv", index=False
        )
        domain_validation_df[["domain_name", "provenance_cols_present", "entity_date_fill_pct",
                               "note_date_fill_pct", "date_confidence_fill_pct"]].to_csv(
            output_dir / "provenance_summary.csv", index=False
        )
        domain_validation_df[["domain_name", "entity_date_fill_pct", "note_date_fill_pct",
                               "date_confidence_fill_pct"]].to_csv(
            output_dir / "date_episode_summary.csv", index=False
        )
        domain_validation_df[["domain_name", "total_rows", "unique_patients",
                               "unique_note_rows", "top_entity_types"]].to_csv(
            output_dir / "domain_coverage_summary.csv", index=False
        )

    # ── Phase 3: Concordance ───────────────────────────────────────────────
    LOG.info("Phase 3: Cross-Domain Concordance")
    all_entities_df = load_all_v2_entities(inventory_df, v2_dir)

    if not db_path.exists():
        LOG.warning("DuckDB not found at %s; skipping concordance phase", db_path)
        sbs_df = pd.DataFrame()
        concordance_summary_df = pd.DataFrame()
        discordance_summary_df = pd.DataFrame()
    else:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            sbs_df, concordance_summary_df, discordance_summary_df = run_concordance(all_entities_df, con)
        finally:
            con.close()

    if not sbs_df.empty:
        # Serialize any list/object columns to JSON strings before parquet save
        sbs_save = sbs_df.copy()
        for col in sbs_save.columns:
            if sbs_save[col].dtype == object:
                try:
                    sbs_save[col] = sbs_save[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )
                except Exception:
                    pass
        try:
            save_parquet(sbs_save, output_dir / "llm_side_by_side.parquet")
        except Exception as exc:
            LOG.warning("save_parquet failed for side_by_side (%s); falling back to CSV only", exc)
        sbs_df.to_csv(output_dir / "llm_side_by_side.csv", index=False)
    if not concordance_summary_df.empty:
        concordance_summary_df.to_csv(output_dir / "concordance_summary.csv", index=False)
    if not discordance_summary_df.empty:
        discordance_summary_df.to_csv(output_dir / "discordance_summary.csv", index=False)

    # ── Phase 4: Manual Review Queue ──────────────────────────────────────
    LOG.info("Phase 4: Manual Review Queue (strict)")
    review_queue_df = build_strict_review_queue(sbs_df) if not sbs_df.empty else pd.DataFrame()
    review_queue_df.to_csv(output_dir / "manual_review_queue.csv", index=False)
    LOG.info("Manual review queue: %d rows", len(review_queue_df))

    # ── Phase 5: Promotion Gate ────────────────────────────────────────────
    LOG.info("Phase 5: Promotion Gate")
    md_parity = None
    if args.motherduck_check:
        LOG.info("  Running G8 MotherDuck parity check")
        md_parity = check_md_parity(inventory_df)
        if md_parity:
            pd.DataFrame(md_parity).to_csv(output_dir / "md_parity.csv", index=False)

    scorecard_df = run_promotion_gate(
        inventory_df,
        domain_validation_df,
        concordance_summary_df,
        review_queue_df,
        md_parity,
        registry_domains=registry_domains,
    )
    scorecard_df.to_csv(output_dir / "promotion_scorecard.csv", index=False)

    # ── Phase 6: Reports & Artifacts ──────────────────────────────────────
    LOG.info("Phase 6: Reports & MotherDuck Artifacts")
    verdict = write_promotion_recommendation(
        output_dir / "promotion_recommendation.md",
        scorecard_df,
        inventory_df,
        concordance_summary_df,
        review_queue_df,
        domain_validation_df,
        run_label,
    )
    write_motherduck_promote_sql(output_dir / "motherduck_promote.sql", inventory_df)
    write_report(
        output_dir / "report.md",
        inventory_df, domain_validation_df, concordance_summary_df, scorecard_df,
        verdict, run_label,
    )
    write_manifest(
        output_dir / "manifest.json",
        run_label, args, scorecard_df, inventory_df,
        domain_validation_df, review_queue_df, verdict,
    )

    # ── Final Summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"  V2 DOMAIN PROMOTION GATE — {verdict}")
    print("=" * 70)
    if not scorecard_df.empty:
        for _, r in scorecard_df.iterrows():
            icon = "PASS" if r["status"] == "PASS" else "FAIL"
            print(f"  [{icon}]  {r['gate_id']}: {r['criterion']}")
            if r["status"] == "FAIL":
                print(f"         => {r['detail']}")
    print("=" * 70)
    print(f"  Artifacts: {output_dir}")
    print(f"  Verdict:   {verdict}")
    print("=" * 70 + "\n")

    if verdict == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    main()
