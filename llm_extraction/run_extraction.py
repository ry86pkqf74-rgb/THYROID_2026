#!/usr/bin/env python3
"""
run_extraction.py — Run all entity extractors on clinical_notes_long

MotherDuck does not run extraction: it stores tables uploaded from this
repo (e.g. scripts/09b_motherduck_upload_notes_entities.py).

Loads processed/clinical_notes_long.parquet, applies regex (and optionally
LLM) extractors, and writes one parquet per entity domain.

Registry-driven: domain→parquet mapping is read from
config/extraction_domain_registry.yaml via llm_extraction.registry.

Usage:
  python llm_extraction/run_extraction.py
      --target medications        # re-run only the medications domain
      --research-ids ids.txt      # re-run only notes for listed research_ids
      --target genetics --research-ids ids.txt  # combined filter
      --merge-audit               # also write merged note_entities_llm audit artifact
      --validate-only             # validate registry and exit (no extraction)

LLM v2 domain fan-out (registry tier == "v2"):
  When the target domain is a v2 LLM-only domain (e.g. --target imaging),
  the runner calls LLMExtractor.extract(..., domain="imaging") on each note,
  filters notes by the domain's note_scope, and writes to the per-domain
  parquet stem (note_entities_llm_imaging.parquet).

  A full run (no --target) processes all v1 regex domains AND iterates
  every v2 registry domain with the LLM extractor.

  note_entities_llm.parquet is written ONLY when --merge-audit is passed.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.base import BaseExtractor
from llm_extraction.extract_llm import LLMExtractor
from llm_extraction.extract_operative_v2 import OperativeDetailExtractor
from llm_extraction.extract_regex import ALL_REGEX_EXTRACTORS
from llm_extraction.registry import DomainSpec, load_registry, validate_registry
from llm_extraction.run_telemetry import (
    RunTelemetryContext,
    append_note_extraction_run,
    hash_file_sha256,
    new_extraction_run_id,
)
from llm_extraction.vocab import (
    ENTITY_SCHEMA_COLUMNS,
    EXTRACTOR_BUILD_VERSION,
    sort_entities_deterministic,
)
from utils.text_helpers import save_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("extraction")

PROCESSED = ROOT / "processed"
np.random.seed(42)

PHI_SNIPPET_LEN = 80

# ---------------------------------------------------------------------------
# Registry-driven domain→parquet mapping
# ---------------------------------------------------------------------------
_registry = load_registry()
DOMAIN_TO_FILE: dict[str, str] = _registry.domain_to_parquet_stem()

# Legacy fallback kept for import compatibility — identical to registry output.
_LEGACY_V1_FALLBACK = {
    "staging": "note_entities_staging",
    "genetics": "note_entities_genetics",
    "procedures": "note_entities_procedures",
    "operative_detail": "note_entities_operative_detail",
    "complications": "note_entities_complications",
    "medications": "note_entities_medications",
    "problem_list": "note_entities_problem_list",
    "llm": "note_entities_llm",
}

# ---------------------------------------------------------------------------
# Note-scope filtering for v2 LLM domains
# ---------------------------------------------------------------------------

# Registry note_scope → frozenset of note_type values that qualify.
# None means "no filter — accept all note_type values".
_NOTE_SCOPE_TYPES: dict[str, frozenset[str] | None] = {
    "all": None,
    "op_note": frozenset({"op_note", "OPNOTE"}),
    "path_report": frozenset({"path_report"}),
}


def _filter_notes_by_scope(notes_df: pd.DataFrame, note_scope: str) -> pd.DataFrame:
    """Return the subset of notes whose note_type satisfies *note_scope*.

    Raises ValueError for unrecognised scopes so new values added to the
    registry YAML are caught immediately rather than silently processing
    all notes.
    """
    if note_scope not in _NOTE_SCOPE_TYPES:
        raise ValueError(
            f"Unknown note_scope '{note_scope}' — add it to _NOTE_SCOPE_TYPES "
            f"in run_extraction.py or fix the registry YAML. "
            f"Valid scopes: {sorted(_NOTE_SCOPE_TYPES)}"
        )
    allowed = _NOTE_SCOPE_TYPES.get(note_scope)
    if allowed is None:
        return notes_df
    mask = notes_df["note_type"].isin(allowed)
    filtered = notes_df[mask].reset_index(drop=True)
    if len(filtered) < len(notes_df):
        log.info(
            "  note_scope='%s' filter: %s/%s notes qualify",
            note_scope,
            f"{len(filtered):,}",
            f"{len(notes_df):,}",
        )
    return filtered


# ---------------------------------------------------------------------------
# Provenance helpers
# ---------------------------------------------------------------------------


def _value_or_none(value):
    if pd.isna(value):
        return None
    return value


def _is_llm_method(extraction_method: str | None) -> bool:
    """True for LLM-sourced rows, which use extraction_method='llm_*'."""
    return bool(extraction_method and str(extraction_method).startswith("llm_"))


def _stamp_row(
    ext: BaseExtractor,
    rec: dict,
    extraction_run_id: str,
    source_row: dict,
    *,
    is_llm: bool | None = None,
) -> dict:
    if rec.get("entity_domain") is None:
        dom = getattr(ext, "entity_domain", None)
        rec["entity_domain"] = dom if dom else None
    rec["extraction_run_id"] = extraction_run_id
    rec["extractor_name"] = ext.__class__.__name__
    rec["extractor_version"] = EXTRACTOR_BUILD_VERSION
    rec["episode_id"] = _value_or_none(source_row.get("episode_id"))
    rec["note_index"] = _value_or_none(source_row.get("note_index"))
    rec["source_sheet"] = _value_or_none(source_row.get("source_sheet"))
    rec["source_column"] = _value_or_none(source_row.get("source_column"))
    extracted_at = rec.get("extracted_at") or datetime.now(timezone.utc).isoformat()
    rec["extracted_at"] = extracted_at
    rec["extraction_timestamp_utc"] = extracted_at
    rec["confidence_score"] = rec.get("confidence")

    # Detect LLM vs regex by extraction_method prefix or explicit flag.
    # Checking entity_domain == "llm" is intentionally NOT used here so that
    # v2 domain rows (which have their own domain name, not "llm") are still
    # treated as LLM rows for provenance purposes.
    llm_row = is_llm if is_llm is not None else _is_llm_method(rec.get("extraction_method"))
    if not llm_row:
        rec["model_name"] = None
        rec["model_version"] = None
        rec["prompt_version"] = "regex_only"
        rec["llm_model"] = None
        rec["llm_prompt_version"] = "regex_only"
        rec["verifier_name"] = None
        rec["verifier_version"] = None
        rec["llm_provider"] = None
        rec["llm_base_url"] = None
        rec["llm_sdk"] = None
        rec["llm_sdk_version"] = None
        rec["provider_returned_model"] = None
        rec["provider_system_fingerprint"] = None
    else:
        rec["llm_model"] = rec.get("model_name")
        rec["llm_prompt_version"] = rec.get("prompt_version")
        if getattr(ext, "force_pending_verification", False):
            rec["verification_status"] = "pending"
            rec["verification_step"] = "awaiting_review"
    return rec


# ---------------------------------------------------------------------------
# V1 regex extraction (unchanged behaviour)
# ---------------------------------------------------------------------------


def run_extractors(
    notes_df: pd.DataFrame,
    extractors: list[BaseExtractor],
    max_workers: int = 1,
    *,
    extraction_run_id: str,
) -> dict[str, list[dict]]:
    """Run all extractors across every note row, grouping results by domain."""
    domain_results: dict[str, list[dict]] = {}

    has_note_date = "note_date" in notes_df.columns

    def extract_one(row: dict) -> dict[str, list[dict]]:
        note_row_id = row["note_row_id"]
        research_id = int(row["research_id"])
        note_type = row["note_type"]
        note_text = str(row["note_text"])
        note_date = row.get("note_date") if has_note_date else None
        if pd.isna(note_date):
            note_date = None

        row_results: dict[str, list[dict]] = {}
        for ext in extractors:
            matches = ext.extract(note_row_id, research_id, note_type, note_text, note_date=note_date)
            if matches:
                domain = ext.entity_domain
                row_results.setdefault(domain, [])
                for m in matches:
                    row_results[domain].append(
                        _stamp_row(ext, m.to_dict(), extraction_run_id, row)
                    )
        return row_results

    total = len(notes_df)
    rows = notes_df.to_dict("records")

    if max_workers <= 1:
        for i, row in enumerate(rows):
            row_results = extract_one(row)
            for domain, records in row_results.items():
                domain_results.setdefault(domain, []).extend(records)

            if (i + 1) % 2000 == 0:
                log.info(f"  Processed {i+1:,}/{total:,} notes ...")
        return domain_results

    log.info(f"  Parallel extraction enabled with {max_workers} worker threads")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extract_one, row) for row in rows]
        for i, future in enumerate(as_completed(futures), start=1):
            row_results = future.result()
            for domain, records in row_results.items():
                domain_results.setdefault(domain, []).extend(records)

            if i % 500 == 0 or i == total:
                log.info(f"  Processed {i:,}/{total:,} notes ...")

    return domain_results


# ---------------------------------------------------------------------------
# V2 LLM domain fan-out
# ---------------------------------------------------------------------------


def run_llm_for_domain(
    notes_df: pd.DataFrame,
    llm: LLMExtractor,
    domain_name: str,
    spec: DomainSpec,
    extraction_run_id: str,
    max_workers: int = 1,
) -> list[dict]:
    """Extract entities for one v2 registry domain using the LLM extractor.

    Applies note_scope filtering, calls llm.extract(..., domain=domain_name)
    per note, and returns a flat list of stamped row dicts.  Results are
    keyed to *domain_name* — not "llm" — so the runner can write them to the
    correct per-domain parquet.
    """
    if not llm.available:
        log.debug("  LLM unavailable; skipping domain '%s'", domain_name)
        return []

    scoped_df = _filter_notes_by_scope(notes_df, spec.note_scope)
    if scoped_df.empty:
        log.info("  Domain '%s': 0 notes after scope filter — skipping", domain_name)
        return []

    has_note_date = "note_date" in scoped_df.columns
    records: list[dict] = []
    rows = scoped_df.to_dict("records")
    total = len(rows)

    def extract_one(row: dict) -> list[dict]:
        note_row_id = row["note_row_id"]
        research_id = int(row["research_id"])
        note_type = row["note_type"]
        note_text = str(row["note_text"])
        note_date = row.get("note_date") if has_note_date else None
        if isinstance(note_date, float) and pd.isna(note_date):
            note_date = None

        matches = llm.extract(
            note_row_id,
            research_id,
            note_type,
            note_text,
            note_date=note_date,
            domain=domain_name,
        )
        row_records: list[dict] = []
        for m in matches:
            rec = m.to_dict()
            rec["entity_domain"] = domain_name
            rec = _stamp_row(llm, rec, extraction_run_id, row, is_llm=True)
            row_records.append(rec)
        return row_records

    if max_workers <= 1:
        for i, row in enumerate(rows):
            records.extend(extract_one(row))
            if (i + 1) % 1000 == 0:
                log.info(
                    "  [%s] Processed %s/%s notes ...",
                    domain_name,
                    f"{i+1:,}",
                    f"{total:,}",
                )
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(extract_one, row) for row in rows]
            for i, future in enumerate(as_completed(futures), start=1):
                records.extend(future.result())
                if i % 500 == 0 or i == total:
                    log.info(
                        "  [%s] Processed %s/%s notes ...",
                        domain_name,
                        f"{i:,}",
                        f"{total:,}",
                    )

    log.info(
        "  Domain '%s': %s entities extracted from %s notes",
        domain_name,
        f"{len(records):,}",
        f"{total:,}",
    )
    return records


# ---------------------------------------------------------------------------
# DataFrame construction & validation
# ---------------------------------------------------------------------------


def results_to_dataframes(domain_results: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    """Convert domain results to validated DataFrames."""
    dfs: dict[str, pd.DataFrame] = {}
    for domain, records in domain_results.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        for col in ENTITY_SCHEMA_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[ENTITY_SCHEMA_COLUMNS]
        BaseExtractor.validate_output(df)
        dfs[domain] = df
    return dfs


def _merge_into_existing(
    domain: str,
    new_df: pd.DataFrame,
    replace_research_ids: set[int] | None,
) -> pd.DataFrame:
    """Merge new extraction results into an existing parquet (for targeted runs).

    Replaces rows for the given research_ids (or all rows if replace_research_ids
    is None, i.e., full run) and appends any new rows not previously in the file.
    """
    file_stem = DOMAIN_TO_FILE.get(domain, f"note_entities_{domain}")
    out_path = PROCESSED / f"{file_stem}.parquet"
    if not out_path.exists() or replace_research_ids is None:
        return new_df

    existing_df = pd.read_parquet(out_path)
    if "research_id" not in existing_df.columns:
        return new_df

    mask = existing_df["research_id"].astype(int).isin(replace_research_ids)
    kept_existing = existing_df[~mask].copy()

    for col in new_df.columns:
        if col not in kept_existing.columns:
            kept_existing[col] = None
    for col in kept_existing.columns:
        if col not in new_df.columns:
            new_df[col] = None
    merged = pd.concat([kept_existing, new_df], ignore_index=True)
    return merged


def _validate_and_report(reg=None) -> bool:
    """Run registry validation and print results. Returns True if clean."""
    if reg is None:
        reg = load_registry()
    issues = validate_registry(reg)
    if not issues:
        log.info("  Registry validation: PASS (%d domains, 0 issues)", len(reg.domains))
        return True
    for issue in issues:
        log.warning("  Registry: %s", issue)
    fatal = [i for i in issues if "Duplicate" in i or "not found" in i]
    if fatal:
        log.error("  Registry validation: FAIL (%d fatal issues)", len(fatal))
        return False
    log.info(
        "  Registry validation: PASS with %d warnings (%d domains)",
        len(issues), len(reg.domains),
    )
    return True


# ---------------------------------------------------------------------------
# Per-domain write helpers
# ---------------------------------------------------------------------------


def _log_domain_summary(domain: str, df: pd.DataFrame) -> None:
    n_present = (df["present_or_negated"] == "present").sum()
    n_negated = (df["present_or_negated"] == "negated").sum()
    n_patients = df["research_id"].nunique()
    n_entity_dated = df["entity_date"].notna().sum()
    n_note_dated = df["note_date"].notna().sum()

    log.info(
        "    %-24s  %6s entities  (%s present, %s negated)  %s patients",
        domain,
        f"{len(df):,}",
        f"{n_present:,}",
        f"{n_negated:,}",
        f"{n_patients:,}",
    )
    log.info(
        "      dates: %s entity_date, %s note_date",
        f"{n_entity_dated:,}",
        f"{n_note_dated:,}",
    )
    top = df["entity_value_norm"].value_counts().head(5)
    for val, cnt in top.items():
        if cnt < 5:
            log.info("      [<5 occurrences suppressed for PHI safety]")
            break
        log.info("      %s: %s", val, f"{cnt:,}")


def _write_domain_parquet(
    domain: str,
    df: pd.DataFrame,
    research_id_filter: set[int] | None,
) -> Path:
    """Merge-into-existing and write; returns the output path."""
    file_stem = DOMAIN_TO_FILE.get(domain, f"note_entities_{domain}")
    out_path = PROCESSED / f"{file_stem}.parquet"
    final_df = sort_entities_deterministic(
        _merge_into_existing(domain, df, research_id_filter)
    )
    save_parquet(final_df, out_path)
    return out_path


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------


def main() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    extraction_run_id = new_extraction_run_id()
    telemetry = RunTelemetryContext()

    parser = argparse.ArgumentParser(
        description="Run entity extractors on clinical_notes_long"
    )
    parser.add_argument(
        "--target",
        metavar="DOMAIN",
        help=(
            "Re-extract only a specific entity domain "
            f"(choices: {', '.join(sorted(DOMAIN_TO_FILE))}). "
            "Merges results with existing parquet for unaffected research_ids."
        ),
    )
    parser.add_argument(
        "--research-ids",
        metavar="FILE",
        help=(
            "Path to a text file with one research_id per line. "
            "Only notes for those patients will be (re-)extracted. "
            "Combines with --target if both are given."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help=(
            "Note-level worker threads. Default: auto (3 when LLM is enabled, 1 otherwise). "
            "Use 1 for fully sequential extraction."
        ),
    )
    parser.add_argument(
        "--merge-audit",
        action="store_true",
        default=False,
        help=(
            "Also write a merged note_entities_llm parquet combining all LLM domain outputs. "
            "This is an audit/debug artifact only — per-domain parquets are the canonical outputs."
        ),
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        default=False,
        help="Validate the registry and exit without running extraction.",
    )
    args = parser.parse_args()

    # Always validate registry first
    reg = load_registry()
    clean = _validate_and_report(reg)

    if args.validate_only:
        sys.exit(0 if clean else 1)

    notes_path = PROCESSED / "clinical_notes_long.parquet"
    if not notes_path.exists():
        log.error(f"Input not found: {notes_path}")
        log.error("Run scripts/build_clinical_notes_long.py first.")
        sys.exit(1)

    reg_yaml = ROOT / "config" / "extraction_domain_registry.yaml"
    registry_digest: str | None = None
    if reg_yaml.is_file():
        registry_digest = hash_file_sha256(reg_yaml)
    st = notes_path.stat()
    input_file_size_bytes = int(st.st_size)
    input_mtime_utc = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
    input_sha256 = hash_file_sha256(notes_path)
    input_path_str = str(notes_path.resolve())

    # Validate --target against registry (fail loudly for unknown domains)
    target_domain: str | None = args.target
    target_spec: DomainSpec | None = None
    if target_domain:
        try:
            target_spec = reg.resolve_domain(target_domain)
        except ValueError as exc:
            parser.error(str(exc))

    # Determine whether the target is a v2 LLM-only domain
    target_is_v2_llm = (
        target_spec is not None
        and target_spec.is_v2
        and "llm" in target_spec.extractors
        and "regex" not in target_spec.extractors
    )

    # Load research_id filter
    research_id_filter: set[int] | None = None
    if args.research_ids:
        ids_path = Path(args.research_ids)
        if not ids_path.exists():
            log.error(f"research-ids file not found: {ids_path}")
            sys.exit(1)
        research_id_filter = {
            int(line.strip())
            for line in ids_path.read_text().splitlines()
            if line.strip().isdigit()
        }
        log.info(f"  Research-id filter: {len(research_id_filter):,} patients")

    log.info("=" * 70)
    log.info("  ENTITY EXTRACTION PIPELINE  (registry v3 — %d domains)", len(reg.domains))
    log.info("=" * 70)

    notes_df = pd.read_parquet(notes_path)
    log.info(f"  Loaded {len(notes_df):,} notes from {notes_path.name}")

    # Apply research_id filter
    if research_id_filter:
        before = len(notes_df)
        notes_df = notes_df[
            notes_df["research_id"].astype(int).isin(research_id_filter)
        ].reset_index(drop=True)
        log.info(
            f"  Filtered to {len(notes_df):,} notes "
            f"({before - len(notes_df):,} excluded by research-id filter)"
        )

    # Build LLM extractor (shared across all domains)
    llm = LLMExtractor(telemetry=telemetry)
    if llm.available:
        log.info(
            "  LLM extractor enabled (provider=%s; operative notes use extended chunk)",
            llm._provider,
        )
    else:
        telemetry.llm_disabled = True
        log.warning(
            "  LLM extractor disabled (set GITHUB_TOKEN or OPENAI_API_KEY) — "
            "v2 domains will be empty; this is not the same as 'no clinical findings'"
        )

    worker_count = args.workers
    if worker_count is None:
        worker_count = 3 if llm.available else 1
    worker_count = max(1, worker_count)

    log.info("  extraction_run_id=%s", extraction_run_id)
    log.info("\n  Results summary:")

    # Accumulate LLM domain DataFrames for optional --merge-audit
    llm_domain_dfs: list[pd.DataFrame] = []
    out_count = 0

    # ── Path A: targeted v2 LLM domain ──────────────────────────────────────
    if target_is_v2_llm:
        assert target_domain is not None and target_spec is not None
        log.info("  Target: v2 LLM domain '%s' (note_scope=%s)", target_domain, target_spec.note_scope)

        records = run_llm_for_domain(
            notes_df, llm, target_domain, target_spec, extraction_run_id, max_workers=worker_count
        )
        if records:
            df = pd.DataFrame(records)
            for col in ENTITY_SCHEMA_COLUMNS:
                if col not in df.columns:
                    df[col] = None
            df = df[ENTITY_SCHEMA_COLUMNS]
            BaseExtractor.validate_output(df)
            _log_domain_summary(target_domain, df)
            out_path = _write_domain_parquet(target_domain, df, research_id_filter)
            log.info("  Wrote %s → %s", f"{len(df):,}", out_path.name)
            llm_domain_dfs.append(df)
            out_count = len(df)
        else:
            log.warning(
                "  Domain '%s': no entities extracted — check API keys, input, and scope filter",
                target_domain,
            )

    # ── Path B: targeted v1 domain (regex + optional LLM audit bucket) ──────
    elif target_domain is not None:
        assert target_spec is not None
        log.info("  Target: v1 domain '%s'", target_domain)

        # Select regex extractors matching this domain
        all_extractors: list[BaseExtractor] = [
            *(cls() for cls in ALL_REGEX_EXTRACTORS),
            OperativeDetailExtractor(),
        ]
        if llm.available:
            all_extractors.append(llm)

        extractors = [e for e in all_extractors if e.entity_domain == target_domain]
        if target_domain != "operative_detail":
            extractors.extend(e for e in all_extractors if e.entity_domain == "llm")

        log.info(
            "  Target domain filter: '%s' (%d extractor(s) active)",
            target_domain,
            len(extractors),
        )

        domain_results = run_extractors(
            notes_df, extractors, max_workers=worker_count,
            extraction_run_id=extraction_run_id,
        )
        dfs = results_to_dataframes(domain_results)
        domains_to_write = {target_domain} | ({"llm"} if "llm" in dfs else set())

        for domain, df in dfs.items():
            if domain not in domains_to_write:
                continue
            _log_domain_summary(domain, df)
            out_path = _write_domain_parquet(domain, df, research_id_filter)
            log.info("  Wrote %s → %s", f"{len(df):,}", out_path.name)
            if domain == "llm" or _is_llm_method(df["extraction_method"].iloc[0] if len(df) else None):
                llm_domain_dfs.append(df)
            out_count += len(df)

    # ── Path C: full run (no --target) ──────────────────────────────────────
    else:
        log.info("  Full run: v1 regex domains + all v2 LLM domains")

        # --- v1 regex pass ---
        all_extractors: list[BaseExtractor] = [
            *(cls() for cls in ALL_REGEX_EXTRACTORS),
            OperativeDetailExtractor(),
        ]
        if llm.available:
            all_extractors.append(llm)

        domain_results = run_extractors(
            notes_df, all_extractors, max_workers=worker_count,
            extraction_run_id=extraction_run_id,
        )
        dfs = results_to_dataframes(domain_results)
        for domain, df in dfs.items():
            if domain not in DOMAIN_TO_FILE and domain != "llm":
                continue
            _log_domain_summary(domain, df)
            out_path = _write_domain_parquet(domain, df, research_id_filter)
            log.info("  Wrote %s → %s", f"{len(df):,}", out_path.name)
            if domain == "llm":
                llm_domain_dfs.append(df)
            out_count += len(df)

        # --- v2 LLM domain pass ---
        if llm.available:
            v2_llm_domains = {
                name: spec
                for name, spec in reg.v2_domains.items()
                if "llm" in spec.extractors
            }
            log.info(
                "\n  LLM v2 domain fan-out: %d domains to process ...", len(v2_llm_domains)
            )
            for domain_name, spec in v2_llm_domains.items():
                records = run_llm_for_domain(
                    notes_df, llm, domain_name, spec, extraction_run_id,
                    max_workers=worker_count,
                )
                if not records:
                    continue
                df = pd.DataFrame(records)
                for col in ENTITY_SCHEMA_COLUMNS:
                    if col not in df.columns:
                        df[col] = None
                df = df[ENTITY_SCHEMA_COLUMNS]
                BaseExtractor.validate_output(df)
                _log_domain_summary(domain_name, df)
                out_path = _write_domain_parquet(domain_name, df, research_id_filter)
                log.info("  Wrote %s → %s", f"{len(df):,}", out_path.name)
                llm_domain_dfs.append(df)
                out_count += len(df)
        else:
            log.warning(
                "  Skipping v2 LLM domain fan-out (LLM extractor disabled)"
            )

    # ── Merged audit artifact (optional) ────────────────────────────────────
    if args.merge_audit:
        if llm_domain_dfs:
            merged = sort_entities_deterministic(
                pd.concat(llm_domain_dfs, ignore_index=True)
            )
            audit_path = PROCESSED / "note_entities_llm.parquet"
            save_parquet(merged, audit_path)
            log.info(
                "\n  [--merge-audit] Merged LLM audit artifact: %s rows → %s",
                f"{len(merged):,}",
                audit_path.name,
            )
        else:
            log.info(
                "\n  [--merge-audit] No LLM domain output to merge — "
                "note_entities_llm.parquet not written"
            )
    elif not target_domain and not target_is_v2_llm:
        log.info(
            "\n  note_entities_llm.parquet NOT written (use --merge-audit for merged audit artifact)"
        )

    if out_count == 0:
        log.warning(
            "  No entities extracted for selected domains — check API keys, input paths, and filters"
        )

    # ── Telemetry ────────────────────────────────────────────────────────────
    completed_at = datetime.now(timezone.utc).isoformat()
    fstage = telemetry.failure_stage()
    pipeline_success = fstage in ("none", "llm_disabled")
    domains_written = (
        {target_domain} if target_domain else set(DOMAIN_TO_FILE.keys())
    )
    warn_obj = {
        "llm_telemetry": {
            "llm_disabled": telemetry.llm_disabled,
            "api_failures": telemetry.api_failures,
            "parse_failures": telemetry.parse_failures,
            "retry_attempts": telemetry.retry_attempts,
        },
        "target_is_v2_llm": target_is_v2_llm,
        "partial_run_target": target_domain,
        "registry_version": reg.schema_version,
    }

    append_note_extraction_run(
        PROCESSED,
        run_id=extraction_run_id,
        started_at=started_at,
        completed_at=completed_at,
        success=pipeline_success,
        failure_stage=fstage,
        retry_count=telemetry.retry_attempts,
        output_record_count=int(out_count),
        warnings=warn_obj,
        domains_requested=",".join(sorted(domains_written)),
        research_id_filter_note=args.research_ids,
        target_domain=target_domain,
        input_path=input_path_str,
        input_file_size_bytes=input_file_size_bytes,
        input_mtime_utc=input_mtime_utc,
        input_sha256=input_sha256,
        registry_schema_version=reg.schema_version,
        registry_digest=registry_digest,
    )
    log.info(
        "  note_extraction_runs updated (success=%s, failure_stage=%s, entities=%s)",
        pipeline_success,
        fstage,
        out_count,
    )

    log.info("\n" + "=" * 70)
    log.info("  EXTRACTION COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
