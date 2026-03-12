"""
Extraction Audit Engine v2 — Source-Specific Variable Refinement
=================================================================
Extends v1 (extraction_audit_engine.py) with:
  - SourceClassifier: note_type → source_category with text-level heuristics
  - SourceWeightedClassifier: source-aware TP/FP classification
  - CrossSourceReconciler: per-patient multi-source agreement/contradiction detection
  - audit_and_refine_by_source(): generalized audit function for any variable

Source reliability hierarchy:
  path_report  (1.0) > op_note (0.9) > endocrine (0.8) > discharge (0.7)
  > imaging (0.7) > other (0.5) > h_p_consent (0.2)

Usage:
    from notes_extraction.extraction_audit_engine_v2 import audit_and_refine_by_source
    results = audit_and_refine_by_source("ete", con, sample_size=250)

CLI:
    .venv/bin/python notes_extraction/extraction_audit_engine_v2.py \
        --entity ete --md --sample 250
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import v1 components we reuse
from notes_extraction.extraction_audit_engine import (
    EntityClassifier,
    AuditResult,
    CONSENT_BOILERPLATE_PATTERNS,
)

# ---------------------------------------------------------------------------
# Source classification
# ---------------------------------------------------------------------------
NOTE_TYPE_TO_SOURCE = {
    "h_p": "h_p_consent",
    "history_summary": "h_p_consent",
    "other_history": "h_p_consent",
    "op_note": "op_note",
    "dc_sum": "discharge",
    "ed_note": "discharge",
    "endocrine_note": "endocrine",
    "other_notes": "other",
}

SOURCE_RELIABILITY: dict[str, float] = {
    "path_report": 1.0,
    "structured_db": 1.0,
    "op_note": 0.9,
    "endocrine": 0.8,
    "discharge": 0.7,
    "imaging": 0.7,
    "ct_report": 0.7,
    "us_report": 0.7,
    "other": 0.5,
    "h_p_consent": 0.2,
}

# Text patterns that indicate a note is a pathology report (even if note_type is ambiguous)
_PATH_REPORT_SIGNALS = [
    re.compile(r"\b(synoptic\s+report|surgical\s+pathology|final\s+diagnosis|histologic\s+type"
               r"|extrathyroidal\s+extension\s*:|margin\s+status\s*:|lymphovascular\s+invasion\s*:)\b",
               re.I),
]
_IMAGING_REPORT_SIGNALS = [
    re.compile(r"\b(CT\s+(?:neck|chest|scan)|computed\s+tomography|ultrasound\s+(?:thyroid|neck)"
               r"|MRI|magnetic\s+resonance|impression\s*:|findings\s*:|TIRADS)\b", re.I),
]
_OP_NOTE_SIGNALS = [
    re.compile(r"\b(operative\s+(?:note|report|findings?)|intraoperatively|surgeon\s*:|"
               r"procedure\s*:|hemostasis|Bovie|electrocautery|the\s+wound\s+was\s+closed)\b",
               re.I),
]

# ---------------------------------------------------------------------------
# Variable configs — source hierarchy and output columns per variable
# ---------------------------------------------------------------------------
VARIABLE_CONFIGS: dict[str, dict] = {
    "ete": {
        "label": "Extrathyroidal Extension",
        "source_hierarchy": ["path_report", "structured_db", "op_note",
                             "endocrine", "discharge", "imaging", "other"],
        "exclude_sources": ["h_p_consent"],
        "grade_values": ["gross", "microscopic", "present_ungraded", "suspected", "none"],
        "output_columns": ["ete_path_confirmed", "ete_op_note_observed",
                           "ete_imaging_suspected", "ete_overall_confirmed", "ete_grade"],
        "fp_patterns": [
            re.compile(r"\b(risk(?:s)?\s+(?:of|include)\s+[^.]*extrathyroidal)", re.I),
            re.compile(r"\b(discussed.*extrathyroidal\s+extension\s+risk)", re.I),
            re.compile(r"\b(extrathyroidal\s+extension\s+(?:risk|possible|may|concern|question))", re.I),
            re.compile(r"\b(cannot\s+exclude\s+(?:extrathyroidal|ETE))\b", re.I),
        ],
        "tp_patterns": [
            re.compile(r"\bextrathyroidal\s+extension\s*:\s*(?:present|yes|microscopic|gross|minimal|focal|extensive)\b", re.I),
            re.compile(r"\b(gross\s+extrathyroidal\s+extension)\b", re.I),
            re.compile(r"\b(microscopic\s+extrathyroidal\s+extension)\b", re.I),
            re.compile(r"\b(minimal\s+extrathyroidal\s+extension)\b", re.I),
            re.compile(r"\btumor\s+(?:extends?|invad\w+)\s+(?:into|beyond)\s+(?:the\s+)?(?:thyroid\s+capsule|perithyroidal|strap\s+muscle|trachea|esophagus)\b", re.I),
            re.compile(r"\bperithyroidal\s+(?:fat|soft\s+tissue)\s+(?:involved|invaded|infiltrated)\b", re.I),
        ],
        "grade_patterns": {
            "gross": [
                re.compile(r"\b(gross|extensive|macroscopic|pT4|T4[ab]|strap\s+muscle|trachea|esophagus|RLN|skeletal\s+muscle)\b", re.I),
            ],
            "microscopic": [
                re.compile(r"\b(minimal|microscopic|focal|perithyroidal\s+fat|single\s+focus|pT3b)\b", re.I),
            ],
        },
        "prompt_file": "prompts/ete_v1.txt",
    },
    "tumor_size": {
        "label": "Tumor Size",
        "source_hierarchy": ["path_report", "structured_db", "imaging", "other"],
        "exclude_sources": ["h_p_consent"],
        "output_columns": ["tumor_size_path_cm", "tumor_size_imaging_cm", "tumor_size_source"],
        "fp_patterns": [],
        "tp_patterns": [
            re.compile(r"\b(\d+(?:\.\d+)?)\s*(?:cm|mm)\b", re.I),
        ],
    },
    "margin_status": {
        "label": "Surgical Margin Status",
        "source_hierarchy": ["path_report", "structured_db"],
        "exclude_sources": ["h_p_consent", "imaging"],
        "output_columns": ["margin_status_refined", "closest_margin_mm", "margin_site"],
        "fp_patterns": [
            re.compile(r"\b(resection\s+margins?\s+(?:will\s+be|are\s+planned|goal))\b", re.I),
        ],
        "tp_patterns": [
            re.compile(r"\bmargin\s+status\s*:\s*(?:positive|negative|involved|close|clear)\b", re.I),
            re.compile(r"\b(?:negative|positive|involved)\s+(?:surgical\s+)?margins?\b", re.I),
            re.compile(r"\bclosest\s+margin\s*:\s*[\d.]+\s*mm\b", re.I),
        ],
        "grade_patterns": {
            "positive": [re.compile(r"\b(positive|involved|R1|R2)\b", re.I)],
            "negative": [re.compile(r"\b(negative|clear|free|R0)\b", re.I)],
            "close": [re.compile(r"\b(close|<\s*[\d.]+\s*mm)\b", re.I)],
        },
    },
    "vascular_invasion": {
        "label": "Vascular / Angioinvasion",
        "source_hierarchy": ["path_report", "structured_db"],
        "exclude_sources": ["h_p_consent", "imaging"],
        "output_columns": ["vascular_invasion_refined", "vascular_invasion_grade"],
        "fp_patterns": [
            re.compile(r"\b(risk(?:s)?\s+(?:of|include)\s+[^.]*(?:vascular|bleeding))\b", re.I),
        ],
        "tp_patterns": [
            re.compile(r"\b(vascular\s+invasion\s*:\s*(?:present|identified|focal|extensive|absent))\b", re.I),
            re.compile(r"\b(angiolymphatic\s+invasion)\b", re.I),
            re.compile(r"\b(lymphovascular\s+invasion)\b", re.I),
        ],
        "grade_patterns": {
            "focal": [re.compile(r"\b(focal|rare|occasional|limited)\b", re.I)],
            "extensive": [re.compile(r"\b(extensive|widespread|numerous|many)\b", re.I)],
            "present": [re.compile(r"\b(present|identified|seen)\b", re.I)],
            "absent": [re.compile(r"\b(absent|none|not\s+identified|not\s+seen)\b", re.I)],
        },
    },
    "perineural_invasion": {
        "label": "Perineural Invasion",
        "source_hierarchy": ["path_report", "structured_db"],
        "exclude_sources": ["h_p_consent"],
        "output_columns": ["perineural_invasion_refined"],
        "fp_patterns": [],
        "tp_patterns": [
            re.compile(r"\b(perineural\s+invasion\s*:\s*(?:present|identified|absent))\b", re.I),
            re.compile(r"\b(perineural\s+(?:invasion|infiltration|spread))\b", re.I),
        ],
    },
    "lvi": {
        "label": "Lymphovascular Invasion",
        "source_hierarchy": ["path_report", "structured_db"],
        "exclude_sources": ["h_p_consent"],
        "output_columns": ["lvi_refined"],
        "fp_patterns": [],
        "tp_patterns": [
            re.compile(r"\b(lymphovascular\s+invasion\s*:\s*(?:present|identified|absent))\b", re.I),
            re.compile(r"\b(lymphatic\s+invasion\s*:\s*(?:present|identified|absent))\b", re.I),
        ],
    },
    "braf_status": {
        "label": "BRAF / Molecular Markers",
        "source_hierarchy": ["structured_db", "endocrine", "discharge", "op_note", "other"],
        "exclude_sources": ["h_p_consent"],
        "output_columns": ["braf_tested", "braf_positive_refined",
                           "molecular_platform", "molecular_test_date"],
        "fp_patterns": [
            re.compile(r"\b(BRAF\s+(?:mutation\s+)?(?:risk|testing\s+(?:may|was\s+recommended|discussed|considered|available)))\b", re.I),
            re.compile(r"\b(if\s+BRAF|BRAF\s+(?:inhibitor|therapy|targeted))\b", re.I),
        ],
        "tp_patterns": [
            re.compile(r"\b(BRAF\s+V600E\s+(?:mutation\s+)?(?:detected|positive|found|identified|confirmed))\b", re.I),
            re.compile(r"\b(ThyroSeq|Afirma|molecular\s+testing)\s*:\s*BRAF\b", re.I),
            re.compile(r"\bBRAF\s+(?:mutation\s+)?(?:negative|not\s+detected|wild\s*type)\b", re.I),
        ],
    },
    "recurrence_site": {
        "label": "Recurrence Site and Detection Method",
        "source_hierarchy": ["structured_db", "imaging", "endocrine", "discharge"],
        "exclude_sources": ["h_p_consent"],
        "output_columns": ["recurrence_site_refined", "recurrence_detection_method",
                           "recurrence_confirmed"],
        "fp_patterns": [
            re.compile(r"\b(risk\s+(?:of|for)\s+recurrence)\b", re.I),
            re.compile(r"\b(recurrence\s+(?:risk|rate|surveillance|monitoring|follow.?up))\b", re.I),
            re.compile(r"\b(no\s+evidence\s+of\s+recurrence)\b", re.I),
        ],
        "tp_patterns": [
            re.compile(r"\b(recurrence\s+(?:identified|confirmed|detected|proven|biopsy))\b", re.I),
            re.compile(r"\b(structural\s+(?:recurrence|disease))\b", re.I),
            re.compile(r"\b(biochemical\s+recurrence)\b", re.I),
        ],
    },
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class SourcedMentionResult:
    """
    Source-attributed entity mention result.
    Mirrors EntityMentionResult fields (avoiding inheritance issues with
    required-then-default field ordering) and adds source-attribution fields.
    """
    # --- Mirror of EntityMentionResult required fields ---
    note_row_id: str = ""
    research_id: Any = None
    entity_value_norm: str = ""
    present_or_negated: str = "present"
    note_type: str = ""
    note_date: Optional[str] = None
    evidence_span: str = ""
    context_window: str = ""
    classification: str = ""
    evidence_strength: str = ""
    is_true_event: Optional[bool] = None
    tier_recommendation: int = 0
    rule_triggered: str = ""
    contradiction_flag: bool = False
    notes: str = ""
    # --- Source attribution (new in v2) ---
    source_category: str = "other"
    source_reliability_tier: int = 4
    source_reliability_score: float = 0.5
    evidence_type: str = "mentioned"  # confirmed / observed / suspected / mentioned
    ete_grade: Optional[str] = None   # gross / microscopic / present_ungraded / suspected / none
    normalized_value: Optional[str] = None  # entity-specific normalized value


@dataclass
class PatientSourceProfile:
    """Per-patient cross-source reconciled result."""
    research_id: Any
    entity_name: str
    source_mentions: list[dict] = field(default_factory=list)
    concordance_status: str = "single_source"  # concordant / discordant / single_source
    final_value: Optional[str] = None
    source_of_truth: Optional[str] = None  # which source category is authoritative
    contradiction_details: Optional[str] = None
    tier_summary: dict = field(default_factory=dict)  # {source_category: tier}


@dataclass
class SourcedAuditResult(AuditResult):
    """Extends AuditResult with source breakdown."""
    source_breakdown: dict = field(default_factory=dict)  # {source_category: {n, tp, fp}}
    cross_source_concordance_rate: float = 0.0
    discordant_patients: list[Any] = field(default_factory=list)
    recommended_columns: list[str] = field(default_factory=list)
    patient_profiles: list[PatientSourceProfile] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SourceClassifier
# ---------------------------------------------------------------------------
class SourceClassifier:
    """Classifies a note's source type with both note_type and text-level heuristics."""

    def classify(self, note_type: str, note_text: str = "") -> tuple[str, float]:
        """
        Returns (source_category, reliability_score).
        Uses note_type first, then text heuristics for 'other_notes'.
        """
        cat = NOTE_TYPE_TO_SOURCE.get(note_type, "other")

        # Text-level overrides for ambiguous note types
        if note_text and cat in ("other", "h_p_consent"):
            sample = note_text[:800]
            if any(p.search(sample) for p in _PATH_REPORT_SIGNALS):
                cat = "path_report"
            elif any(p.search(sample) for p in _IMAGING_REPORT_SIGNALS):
                cat = "imaging"
            elif any(p.search(sample) for p in _OP_NOTE_SIGNALS):
                cat = "op_note"

        score = SOURCE_RELIABILITY.get(cat, 0.5)
        return cat, score


# ---------------------------------------------------------------------------
# SourceWeightedClassifier
# ---------------------------------------------------------------------------
class SourceWeightedClassifier:
    """Classifies entity mentions with source attribution."""

    def __init__(self, entity_name: str):
        self.entity_name = entity_name
        self.config = VARIABLE_CONFIGS.get(entity_name, {})
        self.fp_patterns = self.config.get("fp_patterns", [])
        self.tp_patterns = self.config.get("tp_patterns", [])
        self.grade_patterns = self.config.get("grade_patterns", {})
        self.source_classifier = SourceClassifier()
        # Fallback to v1 entity classifier for entities with existing patterns
        try:
            self.v1_classifier = EntityClassifier(entity_name)
        except Exception:
            self.v1_classifier = None

    def _detect_grade(self, context: str) -> Optional[str]:
        """Detect value sub-grade (e.g. gross/microscopic for ETE)."""
        for grade, patterns in self.grade_patterns.items():
            for p in patterns:
                if p.search(context):
                    return grade
        return None

    def _is_consent_boilerplate(self, context: str) -> bool:
        return any(p.search(context) for p in CONSENT_BOILERPLATE_PATTERNS)

    def classify(self, mention: dict) -> SourcedMentionResult:
        """
        Classify a mention dict from DuckDB query results.
        Expected keys: note_row_id, research_id, entity_value_norm, present_or_negated,
                       note_type, note_date, evidence_span, context_window
        """
        note_type = mention.get("note_type", "other")
        context = mention.get("context_window", "") or mention.get("evidence_span", "")
        pos_neg = mention.get("present_or_negated", "present")

        source_cat, reliability = self.source_classifier.classify(note_type, context)
        source_tier = {
            "path_report": 1, "structured_db": 1,
            "op_note": 2, "endocrine": 2,
            "discharge": 3, "imaging": 3,
            "other": 4, "h_p_consent": 5,
        }.get(source_cat, 4)

        # Build base result
        base = dict(mention)
        base.setdefault("note_date", None)
        base.setdefault("evidence_span", "")

        result = SourcedMentionResult(
            note_row_id=mention.get("note_row_id", ""),
            research_id=mention.get("research_id"),
            entity_value_norm=mention.get("entity_value_norm", ""),
            present_or_negated=pos_neg,
            note_type=note_type,
            note_date=mention.get("note_date"),
            evidence_span=mention.get("evidence_span", ""),
            context_window=context,
            source_category=source_cat,
            source_reliability_tier=source_tier,
            source_reliability_score=reliability,
        )

        # Exclusion: negated
        if pos_neg == "negated":
            result.classification = "negated"
            result.is_true_event = False
            result.tier_recommendation = 0
            result.evidence_strength = "weak"
            return result

        # Exclusion: consent boilerplate (applies to all sources except path/structured)
        if source_cat == "h_p_consent" and self._is_consent_boilerplate(context):
            result.classification = "false_positive_consent"
            result.is_true_event = False
            result.tier_recommendation = 0
            result.evidence_strength = "weak"
            result.rule_triggered = "consent_boilerplate"
            return result

        # Entity-specific FP patterns
        for fp_p in self.fp_patterns:
            if fp_p.search(context):
                result.classification = "false_positive_discussion"
                result.is_true_event = False
                result.tier_recommendation = 0
                result.evidence_strength = "weak"
                result.rule_triggered = f"fp_pattern:{fp_p.pattern[:40]}"
                return result

        # Entity-specific TP patterns
        tp_match = any(tp_p.search(context) for tp_p in self.tp_patterns)
        grade = self._detect_grade(context)
        result.ete_grade = grade
        result.normalized_value = grade or mention.get("entity_value_norm", "")

        # Source-specific classification logic
        if source_cat in ("path_report", "structured_db"):
            result.classification = "true_positive_confirmed"
            result.is_true_event = True
            result.tier_recommendation = 1
            result.evidence_strength = "strong"
            result.evidence_type = "confirmed"
        elif source_cat == "op_note":
            if tp_match:
                result.classification = "true_positive_confirmed"
                result.is_true_event = True
                result.tier_recommendation = 2
                result.evidence_strength = "strong"
                result.evidence_type = "observed"
            else:
                result.classification = "uncertain"
                result.is_true_event = None
                result.tier_recommendation = 3
                result.evidence_strength = "moderate"
                result.evidence_type = "mentioned"
        elif source_cat in ("imaging", "ct_report", "us_report"):
            result.classification = "true_positive_suspected"
            result.is_true_event = None  # imaging = suspected only
            result.tier_recommendation = 3
            result.evidence_strength = "moderate"
            result.evidence_type = "suspected"
        elif source_cat in ("endocrine", "discharge"):
            if tp_match:
                result.classification = "true_positive_confirmed"
                result.is_true_event = True
                result.tier_recommendation = 2
                result.evidence_strength = "moderate"
                result.evidence_type = "confirmed"
            else:
                result.classification = "uncertain"
                result.is_true_event = None
                result.tier_recommendation = 3
                result.evidence_strength = "weak"
                result.evidence_type = "mentioned"
        elif source_cat == "h_p_consent":
            result.classification = "false_positive_consent"
            result.is_true_event = False
            result.tier_recommendation = 0
            result.evidence_strength = "weak"
            result.evidence_type = "mentioned"
        else:
            # Fallback: use v1 classifier if available
            if self.v1_classifier:
                try:
                    v1r = self.v1_classifier.classify(mention)
                    result.classification = v1r.classification
                    result.is_true_event = v1r.is_true_event
                    result.tier_recommendation = v1r.tier_recommendation
                    result.evidence_strength = v1r.evidence_strength
                    result.rule_triggered = v1r.rule_triggered
                except Exception:
                    pass
            if not result.classification:
                result.classification = "uncertain"
                result.tier_recommendation = 3
                result.evidence_strength = "weak"

        return result


# ---------------------------------------------------------------------------
# CrossSourceReconciler
# ---------------------------------------------------------------------------
class CrossSourceReconciler:
    """Detects agreement/contradiction across sources for the same patient."""

    def __init__(self, entity_name: str):
        self.entity_name = entity_name
        self.config = VARIABLE_CONFIGS.get(entity_name, {})
        self.hierarchy = self.config.get("source_hierarchy",
                                         ["path_report", "op_note", "discharge", "other"])

    def reconcile_patient(self, mentions: list[SourcedMentionResult]) -> PatientSourceProfile:
        """Reconcile all sourced mentions for one patient."""
        if not mentions:
            return PatientSourceProfile(research_id=None, entity_name=self.entity_name)

        rid = mentions[0].research_id
        profile = PatientSourceProfile(research_id=rid, entity_name=self.entity_name)
        profile.source_mentions = [
            {
                "source_category": m.source_category,
                "classification": m.classification,
                "evidence_type": m.evidence_type,
                "tier": m.tier_recommendation,
                "is_true_event": m.is_true_event,
                "grade": m.ete_grade,
                "normalized_value": m.normalized_value,
            }
            for m in mentions
        ]

        # Filter to true-positive mentions only
        tp_mentions = [m for m in mentions if m.is_true_event is True]
        suspected = [m for m in mentions if m.is_true_event is None
                     and m.classification != "false_positive_consent"
                     and m.classification != "false_positive_discussion"]

        # Find source of truth (highest in hierarchy among TPs)
        sot = None
        for preferred_source in self.hierarchy:
            for m in tp_mentions:
                if m.source_category == preferred_source:
                    sot = preferred_source
                    break
            if sot:
                break

        # Collect confirmed values per source
        source_values: dict[str, set] = {}
        for m in tp_mentions:
            val = m.normalized_value or m.ete_grade or m.entity_value_norm
            source_values.setdefault(m.source_category, set()).add(val)

        profile.source_of_truth = sot
        profile.tier_summary = {m.source_category: m.tier_recommendation for m in mentions}

        # Determine final_value from source of truth
        if sot and sot in source_values:
            vals = source_values[sot]
            profile.final_value = list(vals)[0] if len(vals) == 1 else "multiple"
        elif tp_mentions:
            profile.final_value = tp_mentions[0].normalized_value or tp_mentions[0].entity_value_norm
        elif suspected:
            profile.final_value = "suspected"
        else:
            profile.final_value = None

        # Concordance check
        if len(source_values) <= 1:
            profile.concordance_status = "single_source"
        else:
            all_vals = set()
            for vals in source_values.values():
                all_vals.update(vals)
            if len(all_vals) == 1:
                profile.concordance_status = "concordant"
            else:
                profile.concordance_status = "discordant"
                profile.contradiction_details = (
                    f"Sources disagree: {source_values}"
                )

        return profile


# ---------------------------------------------------------------------------
# Main audit function
# ---------------------------------------------------------------------------
def audit_and_refine_by_source(
    entity_name: str,
    con,
    sample_size: int = 250,
    focus_excel_cells: bool = True,
    verbose: bool = True,
) -> SourcedAuditResult:
    """
    Full source-attributed audit for a single variable.

    Queries positive NLP mentions + structured sources, classifies each by
    source category, reconciles cross-source patient profiles, and returns
    a SourcedAuditResult with source breakdown table and recommended columns.
    """
    config = VARIABLE_CONFIGS.get(entity_name)
    if not config:
        raise ValueError(f"Entity '{entity_name}' not in VARIABLE_CONFIGS. "
                         f"Available: {list(VARIABLE_CONFIGS.keys())}")

    if verbose:
        print(f"\n[audit_v2] === {config['label']} ({entity_name}) ===")

    classifier = SourceWeightedClassifier(entity_name)
    reconciler = CrossSourceReconciler(entity_name)

    # ---------------------------------------------------------------------------
    # Query NLP mentions by entity
    # ---------------------------------------------------------------------------
    # Map entity name to the right note_entities table
    TABLE_MAP = {
        "ete": "note_entities_staging",
        "braf_status": "note_entities_genetics",
        "vascular_invasion": "note_entities_procedures",
        "perineural_invasion": "note_entities_procedures",
        "lvi": "note_entities_procedures",
        "margin_status": "note_entities_procedures",
        "tumor_size": "note_entities_staging",
        "recurrence_site": "note_entities_problem_list",
    }

    # Entity value patterns to match in each table
    ENTITY_VALUE_MAP = {
        "ete": ["extrathyroidal_extension_detail", "imaging_ete", "gross_invasion",
                "ete_present", "gross_ete", "extrathyroidal_extension", "mentioned"],
        "braf_status": ["BRAF"],
        "vascular_invasion": ["vascular_invasion_detail", "angiolymphatic_invasion"],
        "perineural_invasion": ["perineural_invasion"],
        "lvi": ["lymphatic_invasion_detail", "lymphovascular_invasion"],
        "margin_status": ["margin_status"],
        "tumor_size": ["tumor_size"],
        "recurrence_site": ["recurrence", "structural_disease", "biochemical_recurrence"],
    }

    src_table = TABLE_MAP.get(entity_name, "note_entities_staging")
    entity_vals = ENTITY_VALUE_MAP.get(entity_name, [entity_name])
    val_list = ", ".join(f"'{v}'" for v in entity_vals)

    # Try to get NLP mentions
    nlp_df = pd.DataFrame()
    try:
        nlp_df = con.execute(f"""
            SELECT
                e.note_row_id, e.research_id,
                e.entity_value_norm, e.entity_type,
                e.present_or_negated, e.confidence,
                e.note_type, e.note_date,
                e.evidence_span,
                SUBSTRING(n.note_text,
                    GREATEST(1, e.evidence_start - 200),
                    400) AS context_window
            FROM {src_table} e
            JOIN clinical_notes_long n ON e.note_row_id = n.note_row_id
            WHERE e.present_or_negated = 'present'
              AND (e.entity_value_norm IN ({val_list})
                   OR e.entity_type IN ({val_list}))
            ORDER BY RANDOM()
            LIMIT {sample_size}
        """).fetchdf()
    except Exception as exc:
        if verbose:
            print(f"  [warn] NLP query failed: {exc}")

    if verbose:
        print(f"  NLP mentions sampled: {len(nlp_df)}")

    # ---------------------------------------------------------------------------
    # Structured source data for ETE
    # ---------------------------------------------------------------------------
    structured_df = pd.DataFrame()
    if entity_name == "ete":
        try:
            structured_df = con.execute("""
                SELECT
                    research_id,
                    tumor_1_extrathyroidal_extension AS entity_value_raw,
                    'structured_db' AS source_category,
                    1 AS tier,
                    1.0 AS reliability
                FROM path_synoptics
                WHERE tumor_1_extrathyroidal_extension IS NOT NULL
                  AND tumor_1_extrathyroidal_extension NOT IN ('', 'None')
                LIMIT 500
            """).fetchdf()
            if verbose:
                print(f"  Structured ETE rows: {len(structured_df)}")
        except Exception as exc:
            if verbose:
                print(f"  [warn] Structured ETE query failed: {exc}")

    elif entity_name == "braf_status":
        try:
            structured_df = con.execute("""
                SELECT research_id,
                       CASE WHEN LOWER(CAST(braf_positive AS VARCHAR)) = 'true' THEN 'positive'
                            ELSE 'negative' END AS entity_value_raw,
                       'structured_db' AS source_category,
                       1 AS tier,
                       1.0 AS reliability
                FROM recurrence_risk_features_mv
                WHERE braf_positive IS NOT NULL
                LIMIT 500
            """).fetchdf()
            if verbose:
                print(f"  Structured BRAF rows: {len(structured_df)}")
        except Exception as exc:
            if verbose:
                print(f"  [warn] Structured BRAF query: {exc}")

    # ---------------------------------------------------------------------------
    # Classify NLP mentions
    # ---------------------------------------------------------------------------
    sourced_results: list[SourcedMentionResult] = []
    for _, row in nlp_df.iterrows():
        mention = row.to_dict()
        mention["note_date"] = str(mention.get("note_date", "")) if mention.get("note_date") else None
        result = classifier.classify(mention)
        sourced_results.append(result)

    # ---------------------------------------------------------------------------
    # Compute source breakdown
    # ---------------------------------------------------------------------------
    source_breakdown: dict[str, dict] = {}
    for r in sourced_results:
        sc = r.source_category
        if sc not in source_breakdown:
            source_breakdown[sc] = {"n": 0, "tp": 0, "fp": 0, "uncertain": 0, "suspected": 0}
        source_breakdown[sc]["n"] += 1
        if r.is_true_event is True:
            source_breakdown[sc]["tp"] += 1
        elif r.is_true_event is False:
            source_breakdown[sc]["fp"] += 1
        elif r.classification == "true_positive_suspected":
            source_breakdown[sc]["suspected"] += 1
        else:
            source_breakdown[sc]["uncertain"] += 1

    # ---------------------------------------------------------------------------
    # Cross-source reconciliation per patient
    # ---------------------------------------------------------------------------
    patient_mentions: dict[Any, list[SourcedMentionResult]] = {}
    for r in sourced_results:
        patient_mentions.setdefault(r.research_id, []).append(r)

    patient_profiles: list[PatientSourceProfile] = []
    multi_source_patients = []
    for rid, mentions in patient_mentions.items():
        profile = reconciler.reconcile_patient(mentions)
        patient_profiles.append(profile)
        if profile.concordance_status in ("concordant", "discordant"):
            multi_source_patients.append(rid)

    discordant = [p for p in patient_profiles if p.concordance_status == "discordant"]

    # Concordance rate
    multi_source = [p for p in patient_profiles
                    if p.concordance_status != "single_source"]
    concordance_rate = (
        len([p for p in multi_source if p.concordance_status == "concordant"])
        / len(multi_source)
        if multi_source else 1.0
    )

    # ---------------------------------------------------------------------------
    # Overall precision estimate
    # ---------------------------------------------------------------------------
    total = len(sourced_results)
    tp_count = sum(1 for r in sourced_results if r.is_true_event is True)
    fp_count = sum(1 for r in sourced_results if r.is_true_event is False)
    uncertain_count = sum(1 for r in sourced_results
                          if r.is_true_event is None
                          and r.classification not in ("true_positive_suspected", "negated"))
    precision = tp_count / max(tp_count + fp_count, 1)

    # ---------------------------------------------------------------------------
    # Build result
    # ---------------------------------------------------------------------------
    result = SourcedAuditResult(
        entity_name=entity_name,
        total_present_mentions=total,
        sample_size=total,
        true_positive_count=tp_count,
        false_positive_count=fp_count,
        uncertain_count=uncertain_count,
        precision_estimate=precision,
        estimated_total_true_events=int(precision * total),
        estimated_total_false_positives=int((1 - precision) * total),
        common_failure_modes=[],
        structured_ground_truth_available=len(structured_df) > 0,
        structured_true_events=len(structured_df),
        nlp_missing_structured=0,
        entity_report_md="",  # filled below
        refined_prompt_needed=precision < 0.5,
        per_sample_results=sourced_results,
        source_breakdown=source_breakdown,
        cross_source_concordance_rate=concordance_rate,
        discordant_patients=[p.research_id for p in discordant],
        recommended_columns=config.get("output_columns", []),
        patient_profiles=patient_profiles,
    )

    # Generate report
    result.entity_report_md = _build_report(result, config, structured_df)

    if verbose:
        print(f"  Precision estimate: {precision:.1%}")
        print(f"  Source breakdown: {source_breakdown}")
        print(f"  Multi-source patients: {len(multi_source)}")
        print(f"  Discordant patients: {len(discordant)}")
        print(f"  Cross-source concordance: {concordance_rate:.1%}")

    return result


def _build_report(result: SourcedAuditResult, config: dict,
                  structured_df: pd.DataFrame) -> str:
    """Build markdown audit report for one variable."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Phase 4 Audit Report: {config['label']}",
        f"_Generated: {now}_",
        "",
        "## Summary",
        "",
        f"- **NLP mentions sampled:** {result.sample_size}",
        f"- **True positive:** {result.true_positive_count} ({result.precision_estimate:.1%} precision)",
        f"- **False positive:** {result.false_positive_count}",
        f"- **Uncertain:** {result.uncertain_count}",
        f"- **Structured rows available:** {result.structured_true_events}",
        f"- **Cross-source concordance rate:** {result.cross_source_concordance_rate:.1%}",
        f"- **Discordant patients:** {len(result.discordant_patients)}",
        "",
        "## Source Breakdown",
        "",
        "| Source | N | TP | FP | Suspected | Uncertain | Precision |",
        "|--------|---|----|----|-----------|-----------|-----------|",
    ]
    for src, counts in sorted(result.source_breakdown.items(),
                               key=lambda x: -x[1]["n"]):
        n = counts["n"]
        tp = counts["tp"]
        fp = counts["fp"]
        sus = counts.get("suspected", 0)
        unc = counts["uncertain"]
        prec = f"{100*tp/max(tp+fp,1):.0f}%" if tp + fp > 0 else "—"
        lines.append(f"| `{src}` | {n} | {tp} | {fp} | {sus} | {unc} | {prec} |")

    lines += [
        "",
        "## Structured Data Summary",
        "",
    ]
    if not structured_df.empty:
        val_dist = structured_df["entity_value_raw"].value_counts().head(15)
        lines.append("| Raw Value | Count |")
        lines.append("|-----------|-------|")
        for val, cnt in val_dist.items():
            lines.append(f"| `{val}` | {cnt} |")
    else:
        lines.append("_No structured data available for this variable._")

    lines += [
        "",
        "## Recommended Patient-Level Columns",
        "",
    ]
    for col in result.recommended_columns:
        lines.append(f"- `{col}`")

    lines += [
        "",
        "## Cross-Source Concordance",
        "",
        f"- Multi-source patients in sample: {len([p for p in result.patient_profiles if p.concordance_status != 'single_source'])}",
        f"- Concordant: {len([p for p in result.patient_profiles if p.concordance_status == 'concordant'])}",
        f"- Discordant: {len(result.discordant_patients)}",
    ]
    if result.discordant_patients:
        lines.append(f"- Example discordant IDs: {result.discordant_patients[:10]}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# ETE-specific SQL refinement pipeline
# ---------------------------------------------------------------------------
def build_ete_refined_sql(db_prefix: str = "") -> str:
    """Returns SQL to create extracted_ete_refined_v1 table with source-split columns."""
    p = db_prefix
    return f"""
CREATE OR REPLACE TABLE {p}extracted_ete_refined_v1 AS
WITH

-- Source 1: Structured pathology (gold standard)
path_ete AS (
    SELECT
        research_id,
        'path_report'                                                AS source_category,
        1                                                            AS source_tier,
        1.0                                                          AS source_reliability,
        tumor_1_extrathyroidal_extension                             AS raw_value,
        CASE
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,''))
                 IN ('','no','none','absent','not identified','not present','negative','false','n/a','c/a')
                 THEN 'none'
            WHEN regexp_matches(LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')),
                 'gross|extensive|macroscopic|pt4|t4[ab]?|strap.?muscle|trachea|esophag|skeletal')
                 THEN 'gross'
            WHEN regexp_matches(LOWER(COALESCE(tumor_1_extrathyroidal_extension,'')),
                 'minimal|microscop|focal|perithyroid|single.focus|pt3b')
                 THEN 'microscopic'
            WHEN LOWER(COALESCE(tumor_1_extrathyroidal_extension,''))
                 IN ('x','present','yes','yes;','true','yes, present','yes (present)',
                     'x','present_ungraded')
                 THEN 'present_ungraded'
            WHEN tumor_1_extrathyroidal_extension IS NOT NULL
                 AND TRIM(LOWER(tumor_1_extrathyroidal_extension)) <> ''
                 THEN 'present_ungraded'
            ELSE NULL
        END                                                          AS ete_grade,
        TRUE                                                         AS pathologically_confirmed,
        surg_date                                                    AS detection_date
    FROM path_synoptics
    WHERE tumor_1_extrathyroidal_extension IS NOT NULL
      AND LOWER(COALESCE(tumor_1_extrathyroidal_extension,''))
          NOT IN ('', 'n/a', 'na')
),

-- Source 2: Operative episode (gross_ete_flag from V2 operative extractor)
op_ete AS (
    SELECT
        o.research_id,
        'op_note'                                                    AS source_category,
        2                                                            AS source_tier,
        0.9                                                          AS source_reliability,
        'gross_ete_intraoperative'                                   AS raw_value,
        'gross'                                                      AS ete_grade,
        FALSE                                                        AS pathologically_confirmed,
        COALESCE(TRY_CAST(o.resolved_surgery_date AS DATE),
                 o.surgery_date_native)                              AS detection_date
    FROM operative_episode_detail_v2 o
    WHERE LOWER(CAST(o.gross_ete_flag AS VARCHAR)) = 'true'
),

-- Per-patient path source profile
path_per_patient AS (
    SELECT
        research_id,
        MIN(detection_date)                                          AS earliest_date,
        -- Prefer gross > microscopic > present_ungraded > none
        MAX(CASE ete_grade WHEN 'gross' THEN 4
                           WHEN 'microscopic' THEN 3
                           WHEN 'present_ungraded' THEN 2
                           WHEN 'none' THEN 1
                           ELSE 0 END)                              AS grade_rank,
        BOOL_OR(ete_grade NOT IN ('none'))                          AS has_ete,
        COUNT(*)                                                     AS n_path_records
    FROM path_ete
    GROUP BY research_id
),

-- Join grade rank back to grade label
path_grade AS (
    SELECT pp.research_id,
           pp.earliest_date,
           pp.has_ete,
           pp.n_path_records,
           CASE pp.grade_rank WHEN 4 THEN 'gross'
                              WHEN 3 THEN 'microscopic'
                              WHEN 2 THEN 'present_ungraded'
                              WHEN 1 THEN 'none'
                              ELSE NULL END                         AS ete_grade_path
    FROM path_per_patient pp
),

-- Per-patient op note profile
op_per_patient AS (
    SELECT research_id,
           TRUE                                                     AS op_ete_observed,
           MIN(detection_date)                                      AS op_detection_date
    FROM op_ete
    GROUP BY research_id
),

-- Full patient spine (all patients with any ETE signal)
spine AS (
    SELECT research_id FROM path_grade
    UNION
    SELECT research_id FROM op_per_patient
),

-- Final wide-format patient-level table
final AS (
    SELECT
        s.research_id,
        -- Path-confirmed columns
        COALESCE(pg.has_ete, FALSE)                                 AS ete_path_confirmed,
        pg.ete_grade_path                                           AS ete_grade_path,
        pg.n_path_records,

        -- Op note observed
        COALESCE(op.op_ete_observed, FALSE)                        AS ete_op_note_observed,

        -- Overall confirmed (path first, then op, then imaging)
        CASE
            WHEN pg.has_ete IS TRUE THEN TRUE
            WHEN op.op_ete_observed IS TRUE THEN TRUE
            ELSE FALSE
        END                                                         AS ete_overall_confirmed,

        -- Final grade (path takes precedence)
        COALESCE(pg.ete_grade_path,
                 CASE WHEN op.op_ete_observed THEN 'gross' ELSE NULL END)
                                                                    AS ete_grade,

        -- Source of truth
        CASE
            WHEN pg.has_ete IS TRUE THEN 'path_report'
            WHEN op.op_ete_observed IS TRUE THEN 'op_note'
            ELSE NULL
        END                                                         AS ete_source_of_truth,

        -- Concordance
        CASE
            WHEN pg.has_ete IS TRUE AND op.op_ete_observed IS TRUE THEN 'concordant'
            WHEN pg.has_ete IS TRUE AND op.op_ete_observed IS NULL THEN 'path_only'
            WHEN pg.has_ete IS NULL AND op.op_ete_observed IS TRUE THEN 'op_only'
            ELSE 'no_ete'
        END                                                         AS ete_concordance_status,

        CURRENT_TIMESTAMP                                           AS refined_at
    FROM spine s
    LEFT JOIN path_grade pg ON s.research_id = pg.research_id
    LEFT JOIN op_per_patient op ON s.research_id = op.research_id
)

SELECT * FROM final
ORDER BY research_id;
"""


def build_ete_summary_sql(db_prefix: str = "") -> str:
    """Returns SQL for vw_ete_by_source summary view."""
    p = db_prefix
    return f"""
CREATE OR REPLACE VIEW {p}vw_ete_by_source AS
SELECT
    -- Overall ETE rates
    COUNT(DISTINCT research_id)                                     AS total_patients_with_ete_signal,
    COUNT(DISTINCT CASE WHEN ete_path_confirmed THEN research_id END) AS path_confirmed_patients,
    COUNT(DISTINCT CASE WHEN ete_op_note_observed THEN research_id END) AS op_note_patients,
    COUNT(DISTINCT CASE WHEN ete_overall_confirmed THEN research_id END) AS overall_confirmed_patients,

    -- Grade distribution
    COUNT(DISTINCT CASE WHEN ete_grade = 'gross' THEN research_id END) AS gross_ete_patients,
    COUNT(DISTINCT CASE WHEN ete_grade = 'microscopic' THEN research_id END) AS microscopic_ete_patients,
    COUNT(DISTINCT CASE WHEN ete_grade = 'present_ungraded' THEN research_id END) AS ungraded_ete_patients,

    -- Concordance
    COUNT(DISTINCT CASE WHEN ete_concordance_status = 'concordant' THEN research_id END) AS concordant_patients,
    COUNT(DISTINCT CASE WHEN ete_concordance_status = 'path_only' THEN research_id END) AS path_only_patients,
    COUNT(DISTINCT CASE WHEN ete_concordance_status = 'op_only' THEN research_id END) AS op_only_patients
FROM extracted_ete_refined_v1;
"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _get_connection(use_md: bool, local_path: str = "thyroid_master.duckdb"):
    import duckdb
    if use_md:
        try:
            import toml
            secrets = toml.load(PROJECT_ROOT / ".streamlit/secrets.toml")
            token = secrets["MOTHERDUCK_TOKEN"]
        except Exception:
            import os
            token = os.environ.get("MOTHERDUCK_TOKEN", "")
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(PROJECT_ROOT / local_path))


def main():
    parser = argparse.ArgumentParser(description="Extraction Audit Engine v2")
    parser.add_argument("--entity", default="ete",
                        choices=list(VARIABLE_CONFIGS.keys()),
                        help="Variable to audit")
    parser.add_argument("--all", action="store_true", help="Audit all configured variables")
    parser.add_argument("--sample", type=int, default=250)
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default="notes_extraction")
    args = parser.parse_args()

    use_md = not args.local
    out_dir = PROJECT_ROOT / args.output_dir

    if args.dry_run:
        print(f"[dry-run] Would audit entity={args.entity} sample={args.sample}")
        return

    con = _get_connection(use_md)
    entities = list(VARIABLE_CONFIGS.keys()) if args.all else [args.entity]

    all_results = {}
    for entity in entities:
        try:
            result = audit_and_refine_by_source(entity, con, args.sample)
            all_results[entity] = result

            # Save report
            rpt_path = out_dir / f"audit_v2_{entity}.md"
            rpt_path.write_text(result.entity_report_md, encoding="utf-8")
            print(f"[audit_v2] Report: {rpt_path}")

            # Save per-sample results
            if result.per_sample_results:
                rows = []
                for r in result.per_sample_results:
                    row = {
                        "research_id": r.research_id,
                        "note_type": r.note_type,
                        "source_category": r.source_category,
                        "source_tier": r.source_reliability_tier,
                        "classification": r.classification,
                        "is_true_event": r.is_true_event,
                        "evidence_type": r.evidence_type,
                        "ete_grade": r.ete_grade,
                        "tier_recommendation": r.tier_recommendation,
                    }
                    rows.append(row)
                df_out = pd.DataFrame(rows)
                parquet_path = out_dir / f"audit_v2_{entity}_samples.parquet"
                df_out.to_parquet(parquet_path, index=False)
                print(f"[audit_v2] Parquet: {parquet_path}")

        except Exception as exc:
            print(f"[ERROR] {entity}: {exc}")

    print(f"\n[audit_v2] Completed {len(all_results)} entities")


if __name__ == "__main__":
    main()
