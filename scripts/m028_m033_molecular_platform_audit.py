#!/usr/bin/env python3
"""M028/M033: Molecular platform audit, version resolution, and BRAF reconciliation.

Resolves the 316 ``mol_platform = 'unknown'`` patients in
``canonical_patient_master`` to specific test categories (ThyroSeq v2/v3,
Afirma GEC/GSC, single-gene BRAF, IHC, etc.) using the strongest available
evidence (raw report text in ``canonical_molecular_genetics_v2``, gene panel
size, fusion presence, BRAF detection method, and test dates), then runs a
BRAF audit with concordance tiers for the M033 manuscript.

Outputs (under ``studies/m028_m033_molecular_audit/``):
  - ``molecular_platform_resolved.csv``
  - ``afirma_subtypes_resolved.csv`` (slice)
  - ``thyroseq_versions_resolved.csv`` (slice)
  - ``dual_platform_analysis.csv``
  - ``braf_audit.csv``
  - ``platform_resolution_summary.csv``
  - ``molecular_data_quality_report.md``

MotherDuck tables created in ``manuscript_workspace`` (skipped under ``--dry-run``):
  - ``molecular_platform_resolved_v1``
  - ``braf_audit_v1``
  - ``cohort_m028_molecular_utilization_v1``
  - ``cohort_m033_braf_outcomes_v1``

Usage::

  .venv/bin/python scripts/m028_m033_molecular_platform_audit.py --dry-run
  .venv/bin/python scripts/m028_m033_molecular_platform_audit.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motherduck_client import get_token  # noqa: E402

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
OUT_DIR = ROOT / "studies" / "m028_m033_molecular_audit"

# Institutional approximations (see prompt). Both products had rolling
# adoption; we widen by ±6 months and flag ambiguous edge cases.
AFIRMA_GSC_LAUNCH = dt.date(2017, 6, 1)
AFIRMA_GSC_AMBIG_LO = dt.date(2017, 1, 1)
AFIRMA_GSC_AMBIG_HI = dt.date(2017, 12, 31)
THYROSEQ_V3_LAUNCH = dt.date(2018, 1, 1)
THYROSEQ_V3_AMBIG_LO = dt.date(2017, 6, 1)
THYROSEQ_V3_AMBIG_HI = dt.date(2018, 12, 31)
THYROSEQ_V3_GENE_THRESHOLD = 80  # v3 = 112 genes, v2 = 60 genes
PANEL_GENE_THRESHOLD = 5         # >5 distinct genes implies multi-gene panel

LOGGER = logging.getLogger("m028_m033_molecular_audit")


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

def is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def as_lower(value: Any) -> str:
    if is_missing(value):
        return ""
    return str(value).strip().lower()


def as_int(value: Any) -> int | None:
    if is_missing(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def is_true(value: Any) -> bool:
    if value is True:
        return True
    if is_missing(value):
        return False
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y"}


def coerce_date(value: Any) -> dt.date | None:
    if is_missing(value):
        return None
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    return ts.date()


# ---------------------------------------------------------------------------
# Episode-level (canonical_molecular_genetics_v2) classifier
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EpisodeClassification:
    platform_family: str          # ThyroSeq | Afirma | NGS_unspecified | Other
    platform_resolved: str        # e.g. ThyroSeq_v3, Afirma_GSC, ...
    version_evidence: str         # text/date/gene_count/etc.
    version_confidence: str       # high/medium/low/ambiguous
    is_inadequate: bool


_THYROSEQ_V3_PAT = re.compile(r"thyroseq[\s,_-]*v\.?\s*3", re.IGNORECASE)
_THYROSEQ_V2_PAT = re.compile(r"thyroseq[\s,_-]*v\.?\s*2", re.IGNORECASE)
_AFIRMA_GSC_PAT = re.compile(
    r"afirma[\s,_-]*gsc|genomic[\s_-]*sequencing[\s_-]*classifier",
    re.IGNORECASE,
)
_AFIRMA_GEC_PAT = re.compile(
    r"afirma[\s,_-]*gec|gene[\s_-]*expression[\s_-]*classifier",
    re.IGNORECASE,
)
_AFIRMA_XA_PAT = re.compile(
    r"xpression[\s_-]*atlas|afirma[\s,_-]*xa\b",
    re.IGNORECASE,
)


def classify_thyroseq_episode(
    raw_text: str,
    test_date: dt.date | None,
    n_distinct_genes: int | None,
    has_fusion: bool,
) -> tuple[str, str, str]:
    """Return (resolved, evidence, confidence)."""
    if _THYROSEQ_V3_PAT.search(raw_text):
        return "ThyroSeq_v3", "text:'ThyroSeq v3'", "high"
    if _THYROSEQ_V2_PAT.search(raw_text):
        return "ThyroSeq_v2", "text:'ThyroSeq v2'", "high"
    # Date-based heuristic
    if test_date is not None:
        if test_date < THYROSEQ_V3_AMBIG_LO:
            return "ThyroSeq_v2", f"date:{test_date.isoformat()}<2017-06", "medium"
        if test_date > THYROSEQ_V3_AMBIG_HI:
            return "ThyroSeq_v3", f"date:{test_date.isoformat()}>2018-12", "medium"
        # Ambiguity window — fall back to gene/fusion signal if any
        if n_distinct_genes is not None and n_distinct_genes >= THYROSEQ_V3_GENE_THRESHOLD:
            return "ThyroSeq_v3", f"date_ambig+gene_count={n_distinct_genes}", "medium"
        if has_fusion:
            return "ThyroSeq_v3", "date_ambig+fusion_present", "medium"
        return "ThyroSeq_version_unknown", f"date_ambig:{test_date.isoformat()}", "ambiguous"
    # No date: use structural cues
    if has_fusion:
        return "ThyroSeq_v3", "no_date+fusion_present", "low"
    if n_distinct_genes is not None and n_distinct_genes >= THYROSEQ_V3_GENE_THRESHOLD:
        return "ThyroSeq_v3", f"no_date+gene_count={n_distinct_genes}", "low"
    return "ThyroSeq_version_unknown", "no_signal", "ambiguous"


def classify_afirma_episode(
    raw_text: str,
    test_date: dt.date | None,
    n_distinct_genes: int | None,
    has_fusion: bool,
) -> tuple[str, str, str]:
    if _AFIRMA_XA_PAT.search(raw_text):
        # Xpression Atlas is an add-on to GSC — treat as GSC-class
        return "Afirma_GSC", "text:'Xpression Atlas' (GSC add-on)", "high"
    if _AFIRMA_GSC_PAT.search(raw_text):
        return "Afirma_GSC", "text:'Afirma GSC'", "high"
    if _AFIRMA_GEC_PAT.search(raw_text):
        return "Afirma_GEC", "text:'Afirma GEC'", "high"
    if test_date is not None:
        if test_date < AFIRMA_GSC_AMBIG_LO:
            return "Afirma_GEC", f"date:{test_date.isoformat()}<2017-01", "medium"
        if test_date > AFIRMA_GSC_AMBIG_HI:
            return "Afirma_GSC", f"date:{test_date.isoformat()}>2017-12", "medium"
        # Ambiguous transition window
        if has_fusion:
            return "Afirma_GSC", "date_ambig+fusion (GSC reports fusions)", "medium"
        return "Afirma_version_unknown", f"date_ambig:{test_date.isoformat()}", "ambiguous"
    if has_fusion:
        return "Afirma_GSC", "no_date+fusion (GSC reports fusions)", "low"
    return "Afirma_version_unknown", "no_signal", "ambiguous"


def classify_episode(row: pd.Series) -> EpisodeClassification:
    raw_text = str(row.get("platform_raw") or "")
    family = str(row.get("platform") or "").strip()
    test_date = coerce_date(
        row.get("resolved_test_date") or row.get("test_date_native")
    )
    n_distinct_genes = as_int(row.get("n_distinct_genes_episode"))
    has_fusion = is_true(row.get("episode_has_fusion"))
    is_inadequate = is_true(row.get("inadequate_flag")) or is_true(row.get("cancelled_flag"))

    if family == "ThyroSeq":
        resolved, evidence, conf = classify_thyroseq_episode(
            raw_text, test_date, n_distinct_genes, has_fusion
        )
        return EpisodeClassification("ThyroSeq", resolved, evidence, conf, is_inadequate)

    if family == "Afirma":
        resolved, evidence, conf = classify_afirma_episode(
            raw_text, test_date, n_distinct_genes, has_fusion
        )
        return EpisodeClassification("Afirma", resolved, evidence, conf, is_inadequate)

    if family == "NGS_unspecified":
        return EpisodeClassification(
            "NGS_unspecified",
            "multi_panel_unknown",
            "platform=NGS_unspecified",
            "low",
            is_inadequate,
        )

    return EpisodeClassification(
        "Other", "truly_unknown", "platform_family_missing", "ambiguous", is_inadequate
    )


# ---------------------------------------------------------------------------
# Patient-level classifier (CPM-only, when no CMG_v2 evidence)
# ---------------------------------------------------------------------------

def classify_patient_no_episode(row: pd.Series) -> tuple[str, str, str]:
    """Resolve a patient with mol_platform != ThyroSeq/Afirma using CPM only.

    Returns (resolved, evidence, confidence).
    """
    original = as_lower(row.get("mol_platform"))
    detection = (
        as_lower(row.get("braf_detection_method_v11"))
        or as_lower(row.get("braf_detection_method"))
    )
    ihc_present = not is_missing(row.get("ihc_braf_result_v13"))
    n_genes = as_int(row.get("mol_n_distinct_genes"))
    has_fusion = is_true(row.get("mol_has_fusion"))
    has_thyroseq = is_true(row.get("mol_has_thyroseq"))
    has_afirma = is_true(row.get("mol_has_afirma"))
    test_date = coerce_date(row.get("mol_first_test_date") or row.get("mol_test_date"))

    if original == "quest":
        return "Quest_unspecified", "mol_platform=Quest", "high"

    if has_thyroseq and not has_afirma:
        if test_date is not None and test_date < THYROSEQ_V3_AMBIG_LO:
            return "ThyroSeq_v2", f"flag+date:{test_date.isoformat()}<2017-06", "medium"
        if test_date is not None and test_date > THYROSEQ_V3_AMBIG_HI:
            return "ThyroSeq_v3", f"flag+date:{test_date.isoformat()}>2018-12", "medium"
        return "ThyroSeq_version_unknown", "mol_has_thyroseq+no_date_signal", "low"

    if has_afirma and not has_thyroseq:
        if test_date is not None and test_date < AFIRMA_GSC_AMBIG_LO:
            return "Afirma_GEC", f"flag+date:{test_date.isoformat()}<2017-01", "medium"
        if test_date is not None and test_date > AFIRMA_GSC_AMBIG_HI:
            return "Afirma_GSC", f"flag+date:{test_date.isoformat()}>2017-12", "medium"
        return "Afirma_version_unknown", "mol_has_afirma+no_date_signal", "low"

    if ihc_present:
        return "IHC_BRAF_only", "ihc_braf_result_v13_present", "high"
    if "ihc" in detection:
        return "IHC_BRAF_only", f"detection={detection}", "high"
    if "pcr" in detection:
        return "PCR_BRAF_only", f"detection={detection}", "high"

    if has_fusion or (n_genes is not None and n_genes > PANEL_GENE_THRESHOLD):
        return (
            "multi_panel_unknown",
            f"panel_signal:fusion={has_fusion},n_genes={n_genes}",
            "low",
        )

    if "ngs" in detection:
        return (
            "multi_panel_unknown",
            f"detection={detection} (NGS suggests panel)",
            "low",
        )

    if "nlp_entity" in detection:
        return (
            "single_gene_BRAF",
            f"detection={detection} (BRAF-only NLP mention)",
            "low",
        )

    if n_genes is not None and n_genes <= 1:
        return "single_gene_other", f"n_distinct_genes={n_genes}", "low"

    return "truly_unknown", "no_signal", "ambiguous"


# ---------------------------------------------------------------------------
# Patient roll-up
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PatientResolution:
    research_id: str
    mol_platform_original: str
    mol_platform_family: str
    mol_platform_resolved: str
    mol_platform_version: str
    mol_platform_evidence: str
    mol_platform_confidence: str
    n_episodes_used: int
    n_episodes_distinct_resolved: int
    has_inadequate_episode: bool


def _platform_family_from_resolved(resolved: str) -> str:
    if resolved.startswith("ThyroSeq") and "Afirma" in resolved:
        return "ThyroSeq+Afirma"
    if resolved.startswith("ThyroSeq"):
        return "ThyroSeq"
    if resolved.startswith("Afirma"):
        return "Afirma"
    if resolved == "PCR_BRAF_only":
        return "PCR_BRAF"
    if resolved == "IHC_BRAF_only":
        return "IHC_BRAF"
    if resolved.startswith("single_gene"):
        return "single_gene"
    if resolved == "multi_panel_unknown":
        return "multi_panel_unknown"
    if resolved.startswith("Quest"):
        return "Quest"
    return "unknown"


def _version_from_resolved(resolved: str) -> str:
    """Extract a short version label (v2, v3, GEC, GSC, ...)."""
    table = {
        "ThyroSeq_v2": "v2",
        "ThyroSeq_v3": "v3",
        "ThyroSeq_version_unknown": "unknown",
        "Afirma_GEC": "GEC",
        "Afirma_GSC": "GSC",
        "Afirma_version_unknown": "unknown",
    }
    return table.get(resolved, "n/a")


def _confidence_rank(conf: str) -> int:
    return {"high": 3, "medium": 2, "low": 1, "ambiguous": 0}.get(conf, 0)


def resolve_patient(
    research_id: str,
    cpm_row: pd.Series | None,
    episodes: pd.DataFrame,
) -> PatientResolution:
    original = str(cpm_row.get("mol_platform")) if cpm_row is not None else "no_cpm"
    cpm_has_thyroseq = is_true(cpm_row.get("mol_has_thyroseq")) if cpm_row is not None else False
    cpm_has_afirma = is_true(cpm_row.get("mol_has_afirma")) if cpm_row is not None else False
    cpm_dual_declared = original == "ThyroSeq+Afirma" or (cpm_has_thyroseq and cpm_has_afirma)

    if not episodes.empty:
        eps_classified: list[EpisodeClassification] = []
        for _, ep in episodes.iterrows():
            eps_classified.append(classify_episode(ep))

        # Pick best episode per family by confidence rank
        best_by_family: dict[str, EpisodeClassification] = {}
        for ec in eps_classified:
            cur = best_by_family.get(ec.platform_family)
            if cur is None or _confidence_rank(ec.version_confidence) > _confidence_rank(cur.version_confidence):
                best_by_family[ec.platform_family] = ec

        # If CPM declares dual-platform, synthesise a placeholder for the missing family
        if cpm_dual_declared:
            if "ThyroSeq" not in best_by_family:
                best_by_family["ThyroSeq"] = EpisodeClassification(
                    "ThyroSeq", "ThyroSeq_version_unknown",
                    "cpm_flag_only_no_episode", "low", False,
                )
            if "Afirma" not in best_by_family:
                best_by_family["Afirma"] = EpisodeClassification(
                    "Afirma", "Afirma_version_unknown",
                    "cpm_flag_only_no_episode", "low", False,
                )

        families = sorted(
            f for f in best_by_family
            if f in {"ThyroSeq", "Afirma", "NGS_unspecified"}
        )
        has_inadequate = any(ec.is_inadequate for ec in eps_classified)

        if "ThyroSeq" in families and "Afirma" in families:
            ts = best_by_family["ThyroSeq"]
            af = best_by_family["Afirma"]
            resolved = f"{ts.platform_resolved}+{af.platform_resolved}"
            family = "ThyroSeq+Afirma"
            evidence = f"thyroseq:[{ts.version_evidence}] | afirma:[{af.version_evidence}]"
            confidence = min(ts.version_confidence, af.version_confidence,
                             key=_confidence_rank)
            version = f"{_version_from_resolved(ts.platform_resolved)}+{_version_from_resolved(af.platform_resolved)}"
        elif families == ["ThyroSeq"]:
            ts = best_by_family["ThyroSeq"]
            resolved = ts.platform_resolved
            family = "ThyroSeq"
            evidence = ts.version_evidence
            confidence = ts.version_confidence
            version = _version_from_resolved(ts.platform_resolved)
        elif families == ["Afirma"]:
            af = best_by_family["Afirma"]
            resolved = af.platform_resolved
            family = "Afirma"
            evidence = af.version_evidence
            confidence = af.version_confidence
            version = _version_from_resolved(af.platform_resolved)
        elif families == ["NGS_unspecified"]:
            resolved = "multi_panel_unknown"
            family = "multi_panel_unknown"
            evidence = "platform=NGS_unspecified"
            confidence = "low"
            version = "n/a"
        else:
            # Empty → fall through to CPM logic
            cpm = cpm_row if cpm_row is not None else pd.Series(dtype=object)
            resolved, evidence, confidence = classify_patient_no_episode(cpm)
            family = _platform_family_from_resolved(resolved)
            version = _version_from_resolved(resolved)

        return PatientResolution(
            research_id=research_id,
            mol_platform_original=original,
            mol_platform_family=family,
            mol_platform_resolved=resolved,
            mol_platform_version=version,
            mol_platform_evidence=evidence,
            mol_platform_confidence=confidence,
            n_episodes_used=len(eps_classified),
            n_episodes_distinct_resolved=len({ec.platform_resolved for ec in eps_classified}),
            has_inadequate_episode=has_inadequate,
        )

    cpm = cpm_row if cpm_row is not None else pd.Series(dtype=object)
    resolved, evidence, confidence = classify_patient_no_episode(cpm)
    return PatientResolution(
        research_id=research_id,
        mol_platform_original=original,
        mol_platform_family=_platform_family_from_resolved(resolved),
        mol_platform_resolved=resolved,
        mol_platform_version=_version_from_resolved(resolved),
        mol_platform_evidence=evidence,
        mol_platform_confidence=confidence,
        n_episodes_used=0,
        n_episodes_distinct_resolved=0,
        has_inadequate_episode=False,
    )


# ---------------------------------------------------------------------------
# Dual-platform analysis
# ---------------------------------------------------------------------------

def build_dual_platform_table(
    cmg_episodes: pd.DataFrame,
    resolved: pd.DataFrame,
) -> pd.DataFrame:
    """For ThyroSeq+Afirma patients, identify primary vs confirmatory."""
    dual_ids = set(
        resolved.loc[resolved["mol_platform_family"] == "ThyroSeq+Afirma", "research_id"]
    )
    if not dual_ids:
        return pd.DataFrame(columns=[
            "research_id",
            "primary_platform_family",
            "primary_test_date",
            "secondary_platform_family",
            "secondary_test_date",
            "days_between_tests",
            "concordance_status",
            "primary_braf_flag",
            "secondary_braf_flag",
            "primary_ras_flag",
            "secondary_ras_flag",
        ])

    eps = cmg_episodes[cmg_episodes["research_id"].isin(dual_ids)].copy()
    eps = eps[eps["platform"].isin(["ThyroSeq", "Afirma"])]
    eps["resolved_test_date"] = pd.to_datetime(
        eps["resolved_test_date"], errors="coerce"
    )

    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for rid, grp in eps.groupby("research_id", sort=False):
        ts = grp[grp["platform"] == "ThyroSeq"].sort_values("resolved_test_date")
        af = grp[grp["platform"] == "Afirma"].sort_values("resolved_test_date")
        if ts.empty or af.empty:
            # Record CPM-only dual-platform with the available episode side
            primary_family = "ThyroSeq" if not ts.empty else "Afirma"
            secondary_family = "Afirma" if not ts.empty else "ThyroSeq"
            primary_ep = ts.iloc[0] if not ts.empty else af.iloc[0]
            primary_dt = primary_ep["resolved_test_date"]
            rows.append({
                "research_id": rid,
                "primary_platform_family": primary_family,
                "primary_test_date": primary_dt.date() if not pd.isna(primary_dt) else None,
                "secondary_platform_family": secondary_family,
                "secondary_test_date": None,
                "days_between_tests": np.nan,
                "concordance_status": "secondary_episode_missing_in_cmg_v2",
                "primary_braf_flag": is_true(primary_ep.get("braf_flag")),
                "secondary_braf_flag": None,
                "primary_ras_flag": is_true(primary_ep.get("ras_flag")),
                "secondary_ras_flag": None,
                "evidence_source": "cpm_flag_only_one_episode",
            })
            seen_ids.add(rid)
            continue
        seen_ids.add(rid)
        ts_first = ts.iloc[0]
        af_first = af.iloc[0]
        ts_dt = ts_first["resolved_test_date"]
        af_dt = af_first["resolved_test_date"]

        if pd.isna(ts_dt) and pd.isna(af_dt):
            primary_family, secondary_family = "ThyroSeq", "Afirma"
            primary_dt = secondary_dt = pd.NaT
            days_between = np.nan
        elif pd.isna(ts_dt):
            primary_family, secondary_family = "Afirma", "ThyroSeq"
            primary_dt, secondary_dt = af_dt, pd.NaT
            days_between = np.nan
        elif pd.isna(af_dt):
            primary_family, secondary_family = "ThyroSeq", "Afirma"
            primary_dt, secondary_dt = ts_dt, pd.NaT
            days_between = np.nan
        elif ts_dt <= af_dt:
            primary_family, secondary_family = "ThyroSeq", "Afirma"
            primary_dt, secondary_dt = ts_dt, af_dt
            days_between = (secondary_dt - primary_dt).days
        else:
            primary_family, secondary_family = "Afirma", "ThyroSeq"
            primary_dt, secondary_dt = af_dt, ts_dt
            days_between = (secondary_dt - primary_dt).days

        ts_braf = is_true(ts_first.get("braf_flag"))
        af_braf = is_true(af_first.get("braf_flag"))
        ts_ras = is_true(ts_first.get("ras_flag"))
        af_ras = is_true(af_first.get("ras_flag"))
        if primary_family == "ThyroSeq":
            primary_braf, secondary_braf = ts_braf, af_braf
            primary_ras, secondary_ras = ts_ras, af_ras
        else:
            primary_braf, secondary_braf = af_braf, ts_braf
            primary_ras, secondary_ras = af_ras, ts_ras

        if (primary_braf == secondary_braf) and (primary_ras == secondary_ras):
            concordance = "concordant"
        elif primary_braf != secondary_braf and primary_ras != secondary_ras:
            concordance = "discordant_braf_and_ras"
        elif primary_braf != secondary_braf:
            concordance = "discordant_braf"
        else:
            concordance = "discordant_ras"

        rows.append({
            "research_id": rid,
            "primary_platform_family": primary_family,
            "primary_test_date": primary_dt.date() if not pd.isna(primary_dt) else None,
            "secondary_platform_family": secondary_family,
            "secondary_test_date": secondary_dt.date() if not pd.isna(secondary_dt) else None,
            "days_between_tests": days_between,
            "concordance_status": concordance,
            "primary_braf_flag": primary_braf,
            "secondary_braf_flag": secondary_braf,
            "primary_ras_flag": primary_ras,
            "secondary_ras_flag": secondary_ras,
            "evidence_source": "cmg_v2_both_episodes",
        })

    # Append dual-platform patients with no CMG_v2 episodes at all
    missing = sorted(dual_ids - seen_ids)
    for rid in missing:
        rows.append({
            "research_id": rid,
            "primary_platform_family": "unknown",
            "primary_test_date": None,
            "secondary_platform_family": "unknown",
            "secondary_test_date": None,
            "days_between_tests": np.nan,
            "concordance_status": "no_cmg_v2_episodes",
            "primary_braf_flag": None,
            "secondary_braf_flag": None,
            "primary_ras_flag": None,
            "secondary_ras_flag": None,
            "evidence_source": "cpm_flag_only_no_episode",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# BRAF audit
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BrafAudit:
    research_id: str
    braf_positive_audit: bool          # final reconciled positive
    braf_variant_audit: str            # V600E / V600K / other / NA
    braf_audit_tier: str               # Tier 1..4
    braf_evidence_sources: str         # comma-sep list
    braf_method_audit: str             # NGS / IHC / PCR / NLP / unknown
    braf_discordance_flag: bool        # IHC vs molecular conflict
    braf_audit_evidence: str           # human-readable evidence trail
    notes: str


_V600E_PAT = re.compile(r"v\.?\s*600\s*e", re.IGNORECASE)
_V600K_PAT = re.compile(r"v\.?\s*600\s*k", re.IGNORECASE)


def _normalize_braf_variant(text: str) -> str:
    if not text:
        return ""
    if _V600E_PAT.search(text):
        return "V600E"
    if _V600K_PAT.search(text):
        return "V600K"
    if "v600" in text.lower():
        return "V600_other"
    return text.strip()


def audit_braf(cpm_row: pd.Series, episodes: pd.DataFrame) -> BrafAudit | None:
    rid = str(cpm_row["research_id"])
    final = is_true(cpm_row.get("braf_positive_final"))
    legacy = is_true(cpm_row.get("braf_positive"))
    legacy_v7 = is_true(cpm_row.get("braf_positive_v7"))
    ihc_value_raw = cpm_row.get("ihc_braf_result_v13")
    ihc_present = not is_missing(ihc_value_raw)
    ihc_positive = ihc_present and as_lower(ihc_value_raw) in {"positive", "pos", "true", "1"}
    ihc_negative = ihc_present and as_lower(ihc_value_raw) in {"negative", "neg", "false", "0"}

    method = (
        as_lower(cpm_row.get("braf_detection_method_v11"))
        or as_lower(cpm_row.get("braf_detection_method"))
    )

    variant_raw = " ".join(
        str(x or "") for x in (
            cpm_row.get("braf_variant"),
            cpm_row.get("braf_variant_raw"),
            cpm_row.get("braf_recovered_variant_v11"),
        )
    )
    variant_norm = _normalize_braf_variant(variant_raw)

    episode_positive = False
    episode_variants: list[str] = []
    if not episodes.empty:
        for _, ep in episodes.iterrows():
            if is_true(ep.get("braf_flag")):
                episode_positive = True
            ev = ep.get("braf_variant")
            if not is_missing(ev):
                episode_variants.append(str(ev))
    if episode_variants and not variant_norm:
        variant_norm = _normalize_braf_variant(" ".join(episode_variants))

    sources: list[str] = []
    if final:
        sources.append("braf_positive_final")
    if legacy:
        sources.append("braf_positive")
    if legacy_v7:
        sources.append("braf_positive_v7")
    if episode_positive:
        sources.append("cmg_v2_braf_flag")
    if ihc_positive:
        sources.append("ihc_braf_v13_positive")
    if ihc_negative:
        sources.append("ihc_braf_v13_negative")

    any_positive = final or legacy or legacy_v7 or episode_positive or ihc_positive
    if not any_positive and not ihc_negative:
        return None  # No BRAF evidence at all

    # Audit positivity prioritises molecular truth, with IHC fallback.
    molecular_positive = legacy or legacy_v7 or episode_positive or final
    audit_positive = bool(molecular_positive or ihc_positive)

    discordance = bool(molecular_positive and ihc_negative) or bool(
        ihc_positive and (legacy_v7 is False) and (legacy is False) and (episode_positive is False) and final is False
    )
    # Refine method
    if episode_positive:
        method_audit = "NGS"
    elif "ihc" in method or ihc_positive or ihc_negative:
        method_audit = "IHC"
    elif "pcr" in method:
        method_audit = "PCR"
    elif "ngs" in method:
        method_audit = "NGS"
    elif "nlp" in method:
        method_audit = "NLP"
    else:
        method_audit = "unknown"

    # Tier assignment
    if molecular_positive and ihc_positive:
        tier = "tier_1_molecular_ihc_concordant"
    elif (legacy or legacy_v7 or episode_positive or final) and not ihc_present:
        tier = "tier_2_single_source_confirmed"
    elif ihc_positive and not molecular_positive:
        tier = "tier_3_ihc_only"
    elif ihc_negative and molecular_positive:
        tier = "tier_4_discordant"
    elif ihc_negative and not molecular_positive:
        tier = "tier_5_negative_only"
    else:
        tier = "tier_4_inferred_or_uncertain"

    return BrafAudit(
        research_id=rid,
        braf_positive_audit=audit_positive,
        braf_variant_audit=variant_norm or ("V600E" if audit_positive else ""),
        braf_audit_tier=tier,
        braf_evidence_sources=",".join(sources) or "none",
        braf_method_audit=method_audit,
        braf_discordance_flag=discordance,
        braf_audit_evidence=(
            f"final={final};legacy={legacy};legacy_v7={legacy_v7};"
            f"episode={episode_positive};ihc={'pos' if ihc_positive else 'neg' if ihc_negative else 'none'};"
            f"method={method or 'na'}"
        ),
        notes="",
    )


# ---------------------------------------------------------------------------
# IO + orchestration
# ---------------------------------------------------------------------------

def load_data(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    LOGGER.info("Loading canonical_patient_master molecular columns ...")
    cpm_sql = """
        SELECT research_id,
               molecular_tested_confirmed,
               molecular_eligible_flag,
               mol_platform,
               mol_n_tests,
               mol_test_date,
               mol_first_test_date,
               mol_first_test_days_from_surg,
               mol_test_days_from_surg,
               mol_test_date_source,
               mol_genes_list,
               mol_variant_classes,
               mol_n_distinct_genes,
               mol_n_variants_total,
               mol_n_snvs,
               mol_n_fusions,
               mol_has_afirma,
               mol_has_thyroseq,
               mol_has_fusion,
               mol_has_snv,
               mol_has_dicer1,
               mol_has_pik3ca,
               mol_has_tshr,
               molecular_data_confidence,
               molecular_risk_tier,
               molecular_risk_calculable_flag,
               braf_positive_final,
               braf_positive,
               braf_positive_v7,
               braf_variant,
               braf_variant_raw,
               braf_source,
               braf_status_v7,
               braf_detection_method,
               braf_detection_method_v11,
               braf_recovered_status_v11,
               braf_recovered_variant_v11,
               ihc_braf_result_v13,
               ihc_braf_confidence_v13,
               ihc_braf_note_type_v13,
               ras_positive_final,
               ras_positive,
               ras_subtype,
               ras_primary_subtype_v11,
               ras_protein_change_v11,
               ras_allele_freq_v11,
               nras_positive_v11,
               hras_positive_v11,
               kras_positive_v11,
               ras_resolution_confidence_v13,
               ras_resolution_source_v13,
               nsqip_molecular_testing,
               nsqip_molecular_result,
               age_at_surgery,
               sex,
               surg_first_date,
               is_malignant,
               histology_final,
               ajcc8_t_stage,
               ajcc8_n_stage,
               ajcc8_m_stage,
               ajcc8_stage_group,
               any_recurrence_flag,
               structural_recurrence_flag,
               biochemical_recurrence_flag,
               time_to_recurrence_days,
               first_recurrence_days_from_surg
          FROM canonical_patient_master
    """
    cpm = con.execute(cpm_sql).df()
    cpm["research_id"] = cpm["research_id"].astype(str)
    LOGGER.info("CPM rows: %d (mol_tested=%d)", len(cpm),
                int(cpm["molecular_tested_confirmed"].fillna(False).sum()))

    LOGGER.info("Loading canonical_molecular_genetics_v2 episodes ...")
    cmg_sql = """
        SELECT cmg.research_id,
               cmg.molecular_episode_id,
               cmg.platform,
               cmg.platform_raw,
               cmg.platform_version,
               cmg.test_date_native,
               cmg.resolved_test_date,
               cmg.parser,
               cmg.parse_status,
               cmg.gep_status,
               cmg.gep_detail,
               cmg.afirma_braf_result,
               cmg.afirma_tert_c228t_result,
               cmg.afirma_tert_c250t_result,
               cmg.afirma_retptc_result,
               cmg.braf_flag,
               cmg.braf_variant,
               cmg.ras_flag,
               cmg.ras_subtype,
               cmg.ret_flag,
               cmg.ret_fusion_flag,
               cmg.tert_flag,
               cmg.ntrk_flag,
               cmg.eif1ax_flag,
               cmg.tp53_flag,
               cmg.pax8_pparg_flag,
               cmg.fusion_flag,
               cmg.high_risk_marker_flag,
               cmg.inadequate_flag,
               cmg.cancelled_flag,
               cmg.overall_result_class,
               cmg.molecular_confidence,
               COALESCE(
                   array_length(cmg.gene_mutations_variants),
                   0
               ) AS n_mutations_episode,
               COALESCE(
                   array_length(cmg.gene_fusions_list),
                   0
               ) AS n_fusions_episode,
               (
                   COALESCE(array_length(cmg.gene_mutations_variants), 0)
                   + COALESCE(array_length(cmg.gene_fusions_list), 0)
               ) AS n_distinct_genes_episode,
               (cmg.fusion_flag IS TRUE
                OR cmg.ret_fusion_flag IS TRUE
                OR cmg.pax8_pparg_flag IS TRUE
                OR COALESCE(array_length(cmg.gene_fusions_list), 0) > 0) AS episode_has_fusion
          FROM canonical_molecular_genetics_v2 cmg
    """
    cmg = con.execute(cmg_sql).df()
    cmg["research_id"] = cmg["research_id"].astype(str)
    LOGGER.info("CMG_v2 episodes: %d (patients=%d)",
                len(cmg), cmg["research_id"].nunique())
    return cpm, cmg


def resolve_all(cpm: pd.DataFrame, cmg: pd.DataFrame) -> pd.DataFrame:
    tested = cpm[cpm["molecular_tested_confirmed"].fillna(False).astype(bool)].copy()
    LOGGER.info("Resolving %d tested patients ...", len(tested))

    by_rid: dict[str, pd.DataFrame] = {
        rid: g for rid, g in cmg.groupby("research_id", sort=False)
    }

    rows: list[PatientResolution] = []
    for _, cpm_row in tested.iterrows():
        rid = str(cpm_row["research_id"])
        eps = by_rid.get(rid, pd.DataFrame())
        rows.append(resolve_patient(rid, cpm_row, eps))

    df = pd.DataFrame([r.__dict__ for r in rows])
    return df


def build_braf_audit(cpm: pd.DataFrame, cmg: pd.DataFrame) -> pd.DataFrame:
    by_rid = {rid: g for rid, g in cmg.groupby("research_id", sort=False)}
    audits: list[BrafAudit] = []
    # BRAF audit covers anyone with ANY BRAF evidence, not just mol_tested
    for _, row in cpm.iterrows():
        eps = by_rid.get(str(row["research_id"]), pd.DataFrame())
        result = audit_braf(row, eps)
        if result is not None:
            audits.append(result)
    if not audits:
        return pd.DataFrame()
    return pd.DataFrame([a.__dict__ for a in audits])


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _value_counts(s: pd.Series) -> pd.DataFrame:
    out = s.value_counts(dropna=False).rename_axis("value").reset_index(name="n")
    out["value"] = out["value"].astype(str)
    return out


def build_summary(resolved: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for orig, sub in resolved.groupby("mol_platform_original", sort=False):
        for resolved_value, count in sub["mol_platform_resolved"].value_counts(dropna=False).items():
            rows.append({
                "mol_platform_original": str(orig),
                "mol_platform_resolved": str(resolved_value),
                "n": int(count),
            })
    return pd.DataFrame(rows).sort_values(
        ["mol_platform_original", "n"], ascending=[True, False]
    )


def write_report(
    out_dir: Path,
    cpm: pd.DataFrame,
    cmg: pd.DataFrame,
    resolved: pd.DataFrame,
    braf: pd.DataFrame,
    dual: pd.DataFrame,
) -> None:
    tested = cpm[cpm["molecular_tested_confirmed"].fillna(False).astype(bool)]
    n_tested = len(tested)
    cmg_pids = set(cmg["research_id"].astype(str))

    orig_counts = (
        tested["mol_platform"].fillna("(null)").value_counts().to_dict()
    )
    resolved_family_counts = resolved["mol_platform_family"].value_counts().to_dict()
    resolved_value_counts = resolved["mol_platform_resolved"].value_counts().to_dict()

    n_unknown_in = orig_counts.get("unknown", 0)
    n_unknown_out = int((resolved["mol_platform_resolved"] == "truly_unknown").sum())
    unknown_ids = set(
        tested.loc[tested["mol_platform"] == "unknown", "research_id"].astype(str)
    )
    n_unknown_no_episode = len(unknown_ids - cmg_pids)

    conf_counts = resolved["mol_platform_confidence"].value_counts().to_dict()

    afirma = resolved[resolved["mol_platform_family"] == "Afirma"]
    afirma_breakdown = afirma["mol_platform_resolved"].value_counts().to_dict()
    thyroseq = resolved[resolved["mol_platform_family"] == "ThyroSeq"]
    ts_breakdown = thyroseq["mol_platform_resolved"].value_counts().to_dict()
    dual_total = int((resolved["mol_platform_family"] == "ThyroSeq+Afirma").sum())

    braf_summary: dict[str, Any] = {}
    if not braf.empty:
        braf_summary = {
            "rows": int(len(braf)),
            "audit_positive": int(braf["braf_positive_audit"].sum()),
            "discordant_records": int(braf["braf_discordance_flag"].sum()),
            "tier_counts": braf["braf_audit_tier"].value_counts().to_dict(),
            "method_counts": braf["braf_method_audit"].value_counts().to_dict(),
        }

    def _fmt_dict(d: dict[Any, Any]) -> str:
        if not d:
            return "_(empty)_"
        return "\n".join(f"- `{k}`: {v}" for k, v in d.items())

    timestamp = dt.datetime.now().isoformat(timespec="seconds")

    md_path = out_dir / "molecular_data_quality_report.md"
    md_path.write_text(
        f"""# M028/M033 Molecular Platform Audit — Data Quality Report

Generated: {timestamp}
Source: `{PUBLICATION_DB}.canonical_patient_master` (10,871 patients) +
`canonical_molecular_genetics_v2` (1,384 episode rows).

## 1. Cohort

- Patients with `molecular_tested_confirmed = TRUE`: **{n_tested}**
- Patients with `mol_platform = 'unknown'` going in: **{n_unknown_in}**
- Patients still classified `truly_unknown` after resolution: **{n_unknown_out}**
  ({n_unknown_in - n_unknown_out} of {n_unknown_in} resolved, {(n_unknown_in - n_unknown_out) / max(n_unknown_in, 1):.1%})

## 2. Original `mol_platform` distribution

{_fmt_dict(orig_counts)}

## 3. Resolved `mol_platform_family` distribution

{_fmt_dict(resolved_family_counts)}

## 4. Resolved `mol_platform_resolved` distribution

{_fmt_dict(resolved_value_counts)}

## 5. Confidence distribution

{_fmt_dict(conf_counts)}

## 6. Afirma subtype breakdown (N={len(afirma)})

{_fmt_dict(afirma_breakdown)}

Approximate institutional GEC→GSC transition: **{AFIRMA_GSC_LAUNCH.isoformat()}**
(ambiguous window {AFIRMA_GSC_AMBIG_LO.isoformat()} – {AFIRMA_GSC_AMBIG_HI.isoformat()}).
Patients dated inside that window with no text marker are labelled
`Afirma_version_unknown`.

## 7. ThyroSeq version breakdown (N={len(thyroseq)})

{_fmt_dict(ts_breakdown)}

ThyroSeq v3 launch approximated as **{THYROSEQ_V3_LAUNCH.isoformat()}** with
an ambiguous window of {THYROSEQ_V3_AMBIG_LO.isoformat()} –
{THYROSEQ_V3_AMBIG_HI.isoformat()}. v3 reports >=80 distinct genes and includes
RNA fusions; v2 is DNA-only.

## 8. Dual-platform (ThyroSeq + Afirma) — N={dual_total}

{_fmt_dict({
    'rows in dual-platform analysis': len(dual),
    **(dual['concordance_status'].value_counts().to_dict() if not dual.empty else {})
})}

## 9. BRAF audit summary

{_fmt_dict(braf_summary)}

Tiers:
- `tier_1_molecular_ihc_concordant` — molecular and IHC both positive (highest confidence)
- `tier_2_single_source_confirmed` — molecular positive, no IHC done
- `tier_3_ihc_only` — IHC positive, no supporting molecular
- `tier_4_discordant` — molecular positive but IHC negative (or vice versa)
- `tier_4_inferred_or_uncertain` — heuristic / NLP-only signal
- `tier_5_negative_only` — IHC negative with no molecular positivity

## 10. Remaining gaps

- {n_unknown_out} patients remain `truly_unknown`. These typically have no
  episode in `canonical_molecular_genetics_v2`, no BRAF detection method, no
  IHC, no gene list, and no test date. Most are NLP-extracted molecular
  mentions without a structured panel record. They should be excluded from
  M028 utilization analyses or treated as a separate sensitivity stratum.
- Afirma_version_unknown / ThyroSeq_version_unknown rows fall in the
  GEC→GSC or v2→v3 transition window with no text markers. Manuscripts
  should report an institutional default (e.g. assume GSC after 2017-12 and
  v3 after 2018-01) and quantify sensitivity against the alternate label.
- {n_unknown_no_episode} of the {n_unknown_in} original `unknown` patients
  have no row in `canonical_molecular_genetics_v2`. For these, resolution
  relies entirely on CPM-internal flags (`mol_has_thyroseq`,
  `mol_has_afirma`, `braf_detection_method_v11`, `mol_first_test_date`).

## 11. M028 / M033 cohort outputs (manuscript_workspace)

- `manuscript_workspace.molecular_platform_resolved_v1`
- `manuscript_workspace.braf_audit_v1`
- `manuscript_workspace.cohort_m028_molecular_utilization_v1`
- `manuscript_workspace.cohort_m033_braf_outcomes_v1`
""",
        encoding="utf-8",
    )
    LOGGER.info("Wrote %s", md_path)


# ---------------------------------------------------------------------------
# MotherDuck upload
# ---------------------------------------------------------------------------

def upload_table(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    fqtn: str,
) -> None:
    schema, _, table = fqtn.partition(".")
    if not schema or not table:
        raise ValueError(f"Expected SCHEMA.TABLE, got '{fqtn}'")
    LOGGER.info("Uploading %s rows to %s ...", len(df), fqtn)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.register("_upload_df", df)
    con.execute(f"DROP TABLE IF EXISTS {fqtn}")
    con.execute(f"CREATE TABLE {fqtn} AS SELECT * FROM _upload_df")
    con.unregister("_upload_df")
    n = con.execute(f"SELECT COUNT(*) FROM {fqtn}").fetchone()[0]
    LOGGER.info("  -> %s now has %d rows", fqtn, n)


def build_m028_cohort(cpm: pd.DataFrame, resolved: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "research_id",
        "age_at_surgery",
        "sex",
        "surg_first_date",
        "is_malignant",
        "histology_final",
        "ajcc8_t_stage",
        "ajcc8_n_stage",
        "ajcc8_m_stage",
        "ajcc8_stage_group",
        "molecular_tested_confirmed",
        "molecular_eligible_flag",
        "molecular_risk_tier",
        "molecular_risk_calculable_flag",
        "molecular_data_confidence",
        "mol_platform",
        "mol_first_test_date",
        "mol_first_test_days_from_surg",
        "mol_n_tests",
        "mol_n_distinct_genes",
        "mol_has_fusion",
        "braf_positive_final",
        "ras_positive_final",
        "any_recurrence_flag",
        "structural_recurrence_flag",
        "biochemical_recurrence_flag",
        "time_to_recurrence_days",
        "first_recurrence_days_from_surg",
    ]
    base = cpm[cpm["molecular_tested_confirmed"].fillna(False).astype(bool)][keep].copy()
    out = base.merge(
        resolved[
            [
                "research_id",
                "mol_platform_family",
                "mol_platform_resolved",
                "mol_platform_version",
                "mol_platform_evidence",
                "mol_platform_confidence",
                "n_episodes_used",
                "n_episodes_distinct_resolved",
                "has_inadequate_episode",
            ]
        ],
        on="research_id",
        how="left",
    )
    return out


def build_m033_cohort(cpm: pd.DataFrame, braf: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "research_id",
        "age_at_surgery",
        "sex",
        "surg_first_date",
        "is_malignant",
        "histology_final",
        "ajcc8_t_stage",
        "ajcc8_n_stage",
        "ajcc8_m_stage",
        "ajcc8_stage_group",
        "molecular_tested_confirmed",
        "mol_platform",
        "braf_positive_final",
        "braf_variant",
        "braf_detection_method",
        "braf_detection_method_v11",
        "ihc_braf_result_v13",
        "ihc_braf_confidence_v13",
        "ras_positive_final",
        "any_recurrence_flag",
        "structural_recurrence_flag",
        "biochemical_recurrence_flag",
        "time_to_recurrence_days",
        "first_recurrence_days_from_surg",
    ]
    base = cpm[keep].copy()
    out = base.merge(braf, on="research_id", how="inner")
    return out


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip MotherDuck uploads (CSVs + report still written).",
    )
    parser.add_argument(
        "--out-dir",
        default=str(OUT_DIR),
        help=f"Output directory (default: {OUT_DIR}).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    token = get_token()
    if not token:
        LOGGER.error("MotherDuck token MISSING; refusing to run.")
        sys.exit(2)
    import os
    os.environ["motherduck_token"] = token
    LOGGER.info("MotherDuck token: SET (len=%d)", len(token))

    con = duckdb.connect(f"md:{PUBLICATION_DB}")
    LOGGER.info("Connected to %s", PUBLICATION_DB)

    cpm, cmg = load_data(con)

    resolved = resolve_all(cpm, cmg)
    LOGGER.info(
        "Resolved %d tested patients; family counts: %s",
        len(resolved),
        resolved["mol_platform_family"].value_counts().to_dict(),
    )

    dual = build_dual_platform_table(cmg, resolved)
    braf = build_braf_audit(cpm, cmg)
    summary = build_summary(resolved)

    afirma_slice = resolved[resolved["mol_platform_family"] == "Afirma"]
    thyroseq_slice = resolved[resolved["mol_platform_family"] == "ThyroSeq"]

    resolved.to_csv(out_dir / "molecular_platform_resolved.csv", index=False)
    afirma_slice.to_csv(out_dir / "afirma_subtypes_resolved.csv", index=False)
    thyroseq_slice.to_csv(out_dir / "thyroseq_versions_resolved.csv", index=False)
    dual.to_csv(out_dir / "dual_platform_analysis.csv", index=False)
    braf.to_csv(out_dir / "braf_audit.csv", index=False)
    summary.to_csv(out_dir / "platform_resolution_summary.csv", index=False)
    LOGGER.info("Wrote CSVs to %s", out_dir)

    write_report(out_dir, cpm, cmg, resolved, braf, dual)

    if args.dry_run:
        LOGGER.info("--dry-run set; skipping MotherDuck uploads.")
        return

    m028 = build_m028_cohort(cpm, resolved)
    m033 = build_m033_cohort(cpm, braf)

    upload_table(con, resolved, "manuscript_workspace.molecular_platform_resolved_v1")
    upload_table(con, braf, "manuscript_workspace.braf_audit_v1")
    upload_table(con, m028, "manuscript_workspace.cohort_m028_molecular_utilization_v1")
    upload_table(con, m033, "manuscript_workspace.cohort_m033_braf_outcomes_v1")
    LOGGER.info("MotherDuck upload complete.")


if __name__ == "__main__":
    main()
