"""
Afirma structured ingest helpers — column mapping, crosswalk resolution (exact keys),
Xpression Atlas variant expansion.

Used by scripts/42_ingest_afirma.py.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import duckdb
import pandas as pd


# --- Column normalization (headers after lower + snake_case) ----------------------------

HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "research_id": ("research_id", "rid", "study_id"),
    "mrn": ("mrn", "patient_mrn", "euh_mrn", "pt_mrn", "mrn_norm"),
    "dob": ("dob", "date_of_birth", "birth_date"),
    "patient_name": ("patient_name", "patient_full_name", "full_name"),
    "last_name": ("last_name", "patient_last_name", "last_nm"),
    "first_name": ("first_name", "patient_first_name", "first_nm"),
    "specimen_id": ("specimen_id", "sample_id", "specimen_key"),
    "accession": ("accession", "accession_number", "accession_id", "case_accession"),
    "test_date": ("test_date", "result_date", "collection_date", "specimen_date"),
    "bethesda": ("bethesda", "bethesda_category", "bethesda_class", "fna_bethesda"),
    "fna_cytology": ("fna_cytology", "cytology", "cytology_summary"),
    "gec_call": ("gec_call", "gec_result", "afirma_gec", "gene_expression_call"),
    "gsc_call": ("gsc_call", "gsc_result", "afirma_gsc", "genomic_sequencing_call"),
    "panel_type": ("panel_type", "assay_panel", "afirma_panel", "assay_version"),
    "xpression_variants": (
        "xpression_variants",
        "xa_variants",
        "xpression_atlas_json",
        "variant_findings_json",
    ),
}


def _snake_header(h: str) -> str:
    s = str(h).strip().replace("\xa0", " ")
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_]", "", s)
    return s.lower().strip("_")


def canonicalize_afirma_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename recognized aliases to canonical internal names. Unknown columns preserved."""
    inv: dict[str, str] = {}
    for canon, aliases in HEADER_ALIASES.items():
        for a in aliases:
            inv[_snake_header(a)] = canon
    rename: dict[str, str] = {}
    for c in df.columns:
        sk = _snake_header(c)
        if sk in inv:
            rename[c] = inv[sk]
    out = df.rename(columns=rename)
    return out


def compute_afirma_row_hash(rec: dict[str, Any]) -> str:
    """Deterministic fingerprint for idempotency (no PHI beyond what source already encodes)."""
    keys = [
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
    ]
    payload = "|".join(str(rec.get(k) or "") for k in keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def parse_test_date(val: Any) -> tuple[str | None, Any]:
    """Return (iso_date_string_or_None, native_scalar_for_test_date_native)."""
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
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.notna(dt):
        return dt.strftime("%Y-%m-%d"), s
    return None, s


def fetch_code_crosswalk_maps(con: duckdb.DuckDBPyConnection | None) -> dict[str, dict[str, str]]:
    """Load exact source_code -> target_code maps by domain. Empty if DB unavailable."""
    if con is None:
        return {}
    try:
        df = con.execute(
            """
            SELECT domain, source_code, target_code
            FROM main.molecular_code_crosswalk
            WHERE domain IN (
                'afirma_call',
                'afirma_risk_call',
                'afirma_assay_key',
                'variant_class'
            )
            """,
        ).fetchdf()
    except Exception:
        return {}
    out: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        dom = str(r["domain"])
        out.setdefault(dom, {})[str(r["source_code"])] = str(r["target_code"])
    return out


def fetch_assay_dictionary_by_key(con: duckdb.DuckDBPyConnection | None) -> dict[str, dict[str, Any]]:
    if con is None:
        return {}
    try:
        df = con.execute(
            """
            SELECT assay_key, assay_name, panel_version, platform, vendor, loinc_code
            FROM main.molecular_assay_dictionary
            """,
        ).fetchdf()
    except Exception:
        return {}
    return {str(r["assay_key"]): r.to_dict() for _, r in df.iterrows()}


def exact_crosswalk_lookup(maps: dict[str, dict[str, str]], domain: str, key: str | None) -> str | None:
    """Exact key match only (after strip). No fuzzy / edit distance."""
    if key is None:
        return None
    s = str(key).strip()
    if not s:
        return None
    m = maps.get(domain) or {}
    if s in m:
        return m[s]
    return None


def resolve_afirma_assay_key(panel_type_raw: str | None, maps: dict[str, dict[str, str]]) -> str | None:
    """Map vendor panel hint to molecular_assay_dictionary.assay_key via crosswalk."""
    if panel_type_raw is None:
        return None
    s = str(panel_type_raw).strip()
    if not s:
        return None
    hit = exact_crosswalk_lookup(maps, "afirma_assay_key", s)
    if hit:
        return hit
    return exact_crosswalk_lookup(maps, "afirma_assay_key", s.upper())


def harmonize_calls(
    raw: str | None,
    maps: dict[str, dict[str, str]],
) -> tuple[str | None, bool]:
    """Returns (canonical afirma_call bucket or None, mapped_ok)."""
    if raw is None:
        return None, True
    if isinstance(raw, float) and pd.isna(raw):
        return None, True
    if pd.isna(raw):
        return None, True
    s = str(raw).strip()
    if not s:
        return None, True
    t = exact_crosswalk_lookup(maps, "afirma_call", s)
    if t:
        return t, True
    return None, False


def risk_call_from_gec_gsc(
    gec_h: str | None,
    gsc_h: str | None,
    maps: dict[str, dict[str, str]],
) -> str | None:
    """Prefer GSC harmonized bucket, then GEC, for molecular_results.risk_call."""
    for c in (gsc_h, gec_h):
        if c:
            rc = exact_crosswalk_lookup(maps, "afirma_risk_call", c)
            if rc:
                return rc
            return c
    return None


def _normalize_variant_class(vc: str | None, maps: dict[str, dict[str, str]]) -> tuple[str, str]:
    """Return (canonical variant_class, normalization_status)."""
    if not vc or not str(vc).strip():
        return "OTHER", "pending_review"
    s = str(vc).strip()
    t = exact_crosswalk_lookup(maps, "variant_class", s)
    if t:
        return t, "normalized"
    t2 = exact_crosswalk_lookup(maps, "variant_class", s.upper())
    if t2:
        return t2, "normalized"
    return "OTHER", "pending_review"


def parse_xpression_payload(raw: Any) -> list[dict[str, Any]]:
    """Parse JSON string or list into dict rows for variant expansion."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    s = str(raw).strip()
    if not s:
        return []
    try:
        obj = json.loads(s)
    except json.JSONDecodeError:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


def expand_xpression_variants(
    raw: Any,
    maps: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    """Build molecular_variant_long–shaped specs from structured XA payloads."""
    rows: list[dict[str, Any]] = []
    for item in parse_xpression_payload(raw):
        gene = item.get("gene_symbol") or item.get("gene")
        partner = item.get("partner_gene_symbol")
        fusion_partner = item.get("fusion_partner")
        vc_in = item.get("variant_class")
        vc, norm_st = _normalize_variant_class(vc_in, maps)

        spec: dict[str, Any] = {
            "gene_symbol": str(gene).upper() if gene else None,
            "partner_gene_symbol": str(partner).upper() if partner else None,
            "fusion_partner": str(fusion_partner)[:500] if fusion_partner else None,
            "variant_class": vc,
            "transcript_id": item.get("transcript_id"),
            "genomic_hgvs": item.get("genomic_hgvs"),
            "cdna_hgvs": item.get("cdna_hgvs"),
            "protein_hgvs": item.get("protein_hgvs"),
            "canonical_hgvs": item.get("canonical_hgvs"),
            "allele_fraction": item.get("allele_fraction"),
            "interpretation_text": item.get("interpretation_text"),
            "raw_variant_token": (item.get("raw_variant_token") or item.get("token") or "")[:500]
            or None,
            "parse_status": "ok",
            "normalization_status": norm_st,
            "af_qc_flags": [] if norm_st == "normalized" else ["xa_variant_class_unmapped"],
        }
        if not spec["raw_variant_token"]:
            parts = [spec[k] for k in ("gene_symbol", "cdna_hgvs", "protein_hgvs") if spec.get(k)]
            spec["raw_variant_token"] = "|".join(str(p) for p in parts)[:500] if parts else None

        rows.append(spec)
    return rows


EMBEDDED_AFIRMA_CROSSWALK: dict[str, dict[str, str]] = {
    "afirma_assay_key": {
        "GEC": "afirma_gec",
        "gec": "afirma_gec",
        "GSC": "afirma_gsc",
        "gsc": "afirma_gsc",
        "GEC+GSC": "afirma_combined",
        "BOTH": "afirma_combined",
        "Xpression Atlas": "afirma_xpression_atlas",
        "XA": "afirma_xpression_atlas",
        "XPRESSION_ATLAS": "afirma_xpression_atlas",
    },
    "afirma_call": {
        "Benign": "benign",
        "benign": "benign",
        "BENIGN": "benign",
        "Suspicious": "suspicious",
        "suspicious": "suspicious",
        "SUSPICIOUS": "suspicious",
        "Suspicious for malignancy": "suspicious",
        "Indeterminate": "indeterminate",
        "indeterminate": "indeterminate",
        "No result": "no_result",
        "QNS": "no_result",
        "Failed": "failed",
        "Invalid": "failed",
    },
    "afirma_risk_call": {
        "benign": "benign",
        "suspicious": "suspicious",
        "indeterminate": "indeterminate",
        "no_result": "no_result",
        "failed": "failed",
    },
    "variant_class": {
        "SNV": "SNV",
        "FUSION": "FUSION",
        "CNV": "CNV",
        "INDEL": "INDEL",
        "OTHER": "OTHER",
    },
}


EMBEDDED_ASSAY_BY_KEY: dict[str, dict[str, Any]] = {
    "afirma_gec": {
        "assay_key": "afirma_gec",
        "assay_name": "Afirma Gene Expression Classifier",
        "panel_version": "GEC",
        "platform": "Afirma",
        "vendor": "Veracyte",
        "loinc_code": None,
    },
    "afirma_gsc": {
        "assay_key": "afirma_gsc",
        "assay_name": "Afirma Genomic Sequencing Classifier",
        "panel_version": "GSC",
        "platform": "Afirma",
        "vendor": "Veracyte",
        "loinc_code": None,
    },
    "afirma_combined": {
        "assay_key": "afirma_combined",
        "assay_name": "Afirma GEC+GSC",
        "panel_version": "GEC+GSC",
        "platform": "Afirma",
        "vendor": "Veracyte",
        "loinc_code": None,
    },
    "afirma_xpression_atlas": {
        "assay_key": "afirma_xpression_atlas",
        "assay_name": "Afirma Xpression Atlas",
        "panel_version": "Xpression Atlas",
        "platform": "Afirma",
        "vendor": "Veracyte",
        "loinc_code": None,
    },
}


def default_crosswalk_for_tests() -> dict[str, dict[str, str]]:
    """Copy of embedded maps for pytest (isolates from DB)."""
    return {d: dict(m) for d, m in EMBEDDED_AFIRMA_CROSSWALK.items()}


def effective_crosswalk_maps(con: duckdb.DuckDBPyConnection | None) -> dict[str, dict[str, str]]:
    """Embedded seeds overlaid by DB (DB ``target_code`` wins on duplicate ``source_code``)."""
    merged: dict[str, dict[str, str]] = {d: dict(m) for d, m in EMBEDDED_AFIRMA_CROSSWALK.items()}
    db = fetch_code_crosswalk_maps(con)
    for dom, mp in db.items():
        merged.setdefault(dom, {})
        merged[dom].update(mp)
    return merged


def effective_assay_dictionary(con: duckdb.DuckDBPyConnection | None) -> dict[str, dict[str, Any]]:
    merged = {k: dict(v) for k, v in EMBEDDED_ASSAY_BY_KEY.items()}
    db = fetch_assay_dictionary_by_key(con)
    for k, row in db.items():
        merged[k] = row
    return merged
