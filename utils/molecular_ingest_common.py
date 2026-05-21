"""
Shared utilities for ThyroSeq / Afirma governed ingest (scripts 41 & 42).

Mapping-driven column aliases, row fingerprints, payload checksums, and governed
layer column lists. Deterministic behavior only — no fuzzy matching.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from utils.molecular_report_derivation import parse_native_report_date

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

_CONFIG_CACHE: dict[str, Any] | None = None

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_ALIAS_PATH = _REPO_ROOT / "config" / "molecular_ingest_aliases.yaml"

# Governed DuckDB / export schema (must match scripts/131 and ingest scripts)
GOVERNED_MOLECULAR_RESULT_COLUMNS: tuple[str, ...] = (
    "molecular_result_id",
    "research_id",
    "source_patient_id",
    "source_specimen_id",
    "source_accession",
    "assay_name",
    "panel_version",
    "platform",
    "vendor",
    "loinc_code",
    "test_date_native",
    "test_date_parsed",
    "interpretation_summary",
    "risk_call",
    "canonical_hgvs",
    "raw_payload_json",
    "payload_checksum",
    "parse_status",
    "normalization_status",
    "qc_flags",
    "lineage_id",
    "ingestion_ts",
    "ingestion_run_id",
    "source_table",
    "source_row_fingerprint",
    "molecular_episode_id",
    "superseded_by_molecular_result_id",
)

GOVERNED_MOLECULAR_VARIANT_LONG_COLUMNS: tuple[str, ...] = (
    "molecular_variant_id",
    "molecular_result_id",
    "research_id",
    "gene_symbol",
    "transcript_id",
    "genomic_hgvs",
    "cdna_hgvs",
    "protein_hgvs",
    "canonical_hgvs",
    "variant_class",
    "allele_fraction",
    "zygosity",
    "interpretation_text",
    "risk_call",
    "parse_status",
    "normalization_status",
    "qc_flags",
    "lineage_id",
    "ingestion_ts",
    "partner_gene_symbol",
    "fusion_partner",
    "raw_variant_token",
)

_EMBEDDED_DEFAULTS: dict[str, Any] = {
    "version": 1,
    "afirma_column_aliases": {
        "research_id": ["research_id", "rid", "study_id"],
        "mrn": ["mrn", "patient_mrn", "euh_mrn", "pt_mrn"],
        "dob": ["dob", "date_of_birth", "birth_date"],
        "patient_name": ["patient_name", "patient_full_name", "full_name"],
        "last_name": ["last_name", "patient_last_name", "last_nm"],
        "first_name": ["first_name", "patient_first_name", "first_nm"],
        "specimen_id": ["specimen_id", "sample_id", "specimen_key"],
        "accession": ["accession", "accession_number", "accession_id", "case_accession"],
        "test_date": ["test_date", "result_date", "collection_date", "specimen_date"],
        "bethesda": ["bethesda", "bethesda_category", "bethesda_class", "fna_bethesda"],
        "fna_cytology": ["fna_cytology", "cytology", "cytology_summary"],
        "gec_call": ["gec_call", "gec_result", "afirma_gec", "gene_expression_call"],
        "gsc_call": ["gsc_call", "gsc_result", "afirma_gsc", "genomic_sequencing_call"],
        "panel_type": ["panel_type", "assay_panel", "afirma_panel", "assay_version"],
        "xpression_variants": [
            "xpression_variants",
            "xa_variants",
            "xpression_atlas_json",
            "variant_findings_json",
        ],
    },
    "afirma_row_hash_keys": [
        "research_id",
        "mrn",
        "dob",
        "specimen_id",
        "accession",
        "test_date",
        "gec_call",
        "gsc_call",
        "panel_type",
        "bethesda",
        "xpression_variants",
    ],
    "thyroseq_row_hash_fields": [
        "Req Patient/Source Name",
        "Pt. MRN",
        "Date of Birth",
        "Pathology",
        "Thyroseq Mutation",
        "Gene Fusions",
    ],
}


def load_molecular_ingest_config(
    path: Path | None = None,
    *,
    force_reload: bool = False,
) -> dict[str, Any]:
    """Load YAML config; fall back to embedded defaults if file or PyYAML missing."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None and not force_reload:
        return _CONFIG_CACHE

    p = path or _DEFAULT_ALIAS_PATH
    cfg: dict[str, Any] = dict(_EMBEDDED_DEFAULTS)
    if p.is_file() and yaml is not None:
        with p.open(encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        if isinstance(loaded, dict):
            cfg.update({k: v for k, v in loaded.items() if k != "version"})
    _CONFIG_CACHE = cfg
    return cfg


def clear_molecular_ingest_config_cache() -> None:
    """Test hook."""
    global _CONFIG_CACHE
    _CONFIG_CACHE = None


def normalize_header_snake(h: str) -> str:
    """Lowercase snake_case header key (matches legacy Afirma ingest)."""
    s = str(h).strip().replace("\xa0", " ")
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_]", "", s)
    return s.lower().strip("_")


def canonicalize_columns_from_map(
    df: pd.DataFrame,
    alias_map: Mapping[str, Any],
) -> pd.DataFrame:
    """Rename recognized header aliases to canonical names; unknown columns preserved."""
    inv: dict[str, str] = {}
    for canon, aliases in alias_map.items():
        if canon == "version":
            continue
        seq: Sequence[str]
        if isinstance(aliases, (list, tuple)):
            seq = aliases
        else:
            continue
        for a in seq:
            inv[normalize_header_snake(str(a))] = str(canon)
    rename: dict[str, str] = {}
    for c in df.columns:
        sk = normalize_header_snake(c)
        if sk in inv:
            rename[c] = inv[sk]
    return df.rename(columns=rename)


def get_afirma_column_alias_map(cfg: dict[str, Any] | None = None) -> dict[str, list[str]]:
    raw = (cfg or load_molecular_ingest_config()).get("afirma_column_aliases") or {}
    out: dict[str, list[str]] = {}
    for k, v in raw.items():
        if isinstance(v, (list, tuple)):
            out[str(k)] = [str(x) for x in v]
    return out


def get_afirma_row_hash_keys(cfg: dict[str, Any] | None = None) -> tuple[str, ...]:
    raw = (cfg or load_molecular_ingest_config()).get("afirma_row_hash_keys") or []
    if isinstance(raw, (list, tuple)):
        return tuple(str(x) for x in raw)
    return tuple()


def get_thyroseq_row_hash_fields(cfg: dict[str, Any] | None = None) -> tuple[str, ...]:
    raw = (cfg or load_molecular_ingest_config()).get("thyroseq_row_hash_fields") or []
    if isinstance(raw, (list, tuple)):
        return tuple(str(x) for x in raw)
    return tuple()


def compute_keyed_row_hash(
    rec: Mapping[str, Any],
    keys: Iterable[str],
    *,
    digest_chars: int = 24,
) -> str:
    """Deterministic SHA-256 fingerprint: join str(rec[k]||'') for keys in order."""
    key_list = list(keys)
    payload = "|".join(str(rec.get(k) or "") for k in key_list)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:digest_chars]


def json_friendly_scalar(x: Any) -> Any:
    """JSON-serializable scalar for raw_payload structures (matches ingest scripts)."""
    if pd.isna(x) or x is None:
        return None
    if hasattr(x, "item"):
        try:
            return x.item()
        except Exception:
            pass
    return x


def checksum_sorted_json_payload(obj: dict[str, Any]) -> str:
    """SHA-256 hex digest of sorted JSON (stable checksum for raw_payload_json)."""
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8"),
    ).hexdigest()


def molecular_variant_id_thyroseq(
    molecular_result_id: str,
    idx: int,
    spec: Mapping[str, Any],
    *,
    digest_chars: int = 32,
) -> str:
    """Variant id for script 41 (ThyroSeq) — key excludes cdna_hgvs."""
    key = "|".join([
        molecular_result_id,
        str(idx),
        str(spec.get("variant_class") or ""),
        str(spec.get("raw_variant_token") or ""),
        str(spec.get("gene_symbol") or ""),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:digest_chars]


def molecular_variant_id_afirma(
    molecular_result_id: str,
    idx: int,
    spec: Mapping[str, Any],
    *,
    digest_chars: int = 32,
) -> str:
    """Variant id for script 42 (Afirma) — key includes cdna_hgvs."""
    key = "|".join([
        molecular_result_id,
        str(idx),
        str(spec.get("variant_class") or ""),
        str(spec.get("raw_variant_token") or ""),
        str(spec.get("gene_symbol") or ""),
        str(spec.get("cdna_hgvs") or ""),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:digest_chars]


def molecular_result_id_from_parts(prefix: str, row_fingerprint: str, assay_token: str) -> str:
    """32-char molecular_result_id digest (thyroseq_excel|hash|assay or afirma|hash|key)."""
    return hashlib.sha256(
        f"{prefix}|{row_fingerprint}|{assay_token}".encode(),
    ).hexdigest()[:32]


def parse_test_date_iso_and_native(val: Any) -> tuple[str | None, Any]:
    """Afirma test_date parsing: (ISO date string or None, native_scalar_for_test_date_native)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    if hasattr(val, "strftime"):
        try:
            d = val.date() if hasattr(val, "date") else val
            return d.isoformat() if hasattr(d, "isoformat") else str(val), str(val)
        except Exception:
            pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return None, s or None
    d = parse_native_report_date(s)
    if d is not None:
        return d.isoformat(), s
    return None, s


def parse_thyroseq_workbook_test_date_cell(
    td_cell: Any,
) -> tuple[str | None, date | None]:
    """ThyroSeq ``ThyroSeq Test Date`` cell → (test_date_native, test_date_parsed)."""
    test_date_native: str | None = None
    test_date_parsed: date | None = None
    if td_cell is None or (isinstance(td_cell, float) and pd.isna(td_cell)):
        return None, None
    if isinstance(td_cell, date) and not isinstance(td_cell, datetime):
        test_date_parsed = td_cell
        test_date_native = str(td_cell)
    elif isinstance(td_cell, (datetime, pd.Timestamp)):
        test_date_parsed = td_cell.date()
        test_date_native = str(td_cell)[:120]
    else:
        parsed_date = parse_native_report_date(td_cell)
        if parsed_date is not None:
            test_date_parsed = parsed_date
            test_date_native = str(td_cell).strip()[:120]
    return test_date_native, test_date_parsed


def stamp_thyroseq_ingestion_metadata(
    df: pd.DataFrame,
    *,
    source_file: str,
    batch_id: str,
    source_sheet: str = "Sheet1",
    row_number_start: int = 2,
    imported_at_iso: str | None = None,
) -> None:
    """In-place provenance columns for ThyroSeq workbook ingest (mirrors ingest_raw)."""
    if imported_at_iso is None:
        imported_at_iso = datetime.now().isoformat()
    n = len(df)
    df["source_file"] = source_file
    df["source_sheet"] = source_sheet
    df["source_row_number"] = range(row_number_start, n + row_number_start)
    df["ingestion_batch_id"] = batch_id
    df["imported_at"] = imported_at_iso


def stamp_afirma_ingestion_metadata(
    df: pd.DataFrame,
    *,
    source_file: str,
    batch_id: str,
    row_number_start: int = 2,
    imported_at_iso: str | None = None,
) -> None:
    """In-place provenance for Afirma structured ingest."""
    if imported_at_iso is None:
        imported_at_iso = datetime.now().isoformat()
    n = len(df)
    df["source_file"] = source_file
    df["source_row_number"] = range(row_number_start, n + row_number_start)
    df["ingestion_batch_id"] = batch_id
    df["imported_at"] = imported_at_iso
