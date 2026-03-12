"""
Extraction Audit Engine — Phase 2 QA for all clinical complication entities.

Extends IntrinsicEvaluator with:
  - Per-entity context pattern libraries (chyle_leak, hypocalcemia, seroma,
    hypoparathyroidism, hematoma, wound_infection)
  - MissedEventDetector (synonym expansion + structured cross-check)
  - ExcelCellAnalyzer (multi-source note text handling)
  - batch audit_entity() returning a structured AuditResult dataframe
  - master_audit() running all entities sequentially

Usage:
    python -m notes_extraction.extraction_audit_engine --all --md
    python -m notes_extraction.extraction_audit_engine --entity hypocalcemia --md
    python -m notes_extraction.extraction_audit_engine --inventory-only --md
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Shared consent/risk-disclosure boilerplate signatures
# (same template appears in H&P and some op-notes)
# ---------------------------------------------------------------------------

CONSENT_BOILERPLATE_PATTERNS: list[re.Pattern] = [
    re.compile(
        r"scarring[,\s]+hypocalcemia[,\s]+hoarseness[,\s]+chyle\s+leak",
        re.I,
    ),
    re.compile(
        r"poor\s+(?:wound\s+)?healing[,\s]+scarring[,\s]+hypocalcemia",
        re.I,
    ),
    re.compile(
        r"(?:material\s+)?risks?\s+(?:for|of|include|associated)[^.]{0,80}"
        r"(?:hypocalcemia|chyle\s+leak|seroma|hematoma)",
        re.I,
    ),
    re.compile(
        r"informed\s+consent[^.]{0,60}(?:hypocalcemia|chyle\s+leak|seroma|hematoma)",
        re.I,
    ),
    re.compile(
        r"(?:discussed|counseled|apprised|consented)\s+(?:the\s+)?(?:patient|pt)[^.]{0,80}"
        r"(?:hypocalcemia|chyle\s+leak|seroma|hematoma)",
        re.I,
    ),
    re.compile(
        r"complications?[,\s]+(?:including\s+)?(?:but\s+not\s+limited\s+to[,\s]+)?"
        r"(?:[^.]{0,60})?(?:hypocalcemia|chyle\s+leak|seroma|hematoma)",
        re.I,
    ),
    re.compile(
        r"(?:surgical\s+)?(?:risks?|complications?)[,:\s]+[^.]{0,40}(?:death|heart\s+attack|bleeding)",
        re.I,
    ),
    # Specific consent list signature used in every H&P
    re.compile(
        r"numbness[,\s]+orodental\s+trauma[,\s]+fistula",
        re.I,
    ),
    re.compile(
        r"(?:poor\s+reaction\s+to\s+anesthesia|pulmonary\s+embolus|heart\s+attack)"
        r"[^.]{0,80}(?:hypocalcemia|seroma|hematoma|chyle)",
        re.I,
    ),
    re.compile(
        r"(?:neck\s+)?hematoma[,\s]+seroma[,\s]+poor\s+scarring",
        re.I,
    ),
    re.compile(
        r"(?:hemothorax|pneumothorax)[,\s]+hematoma",
        re.I,
    ),
]

EDUCATION_MONITORING_PATTERNS: list[re.Pattern] = [
    re.compile(
        r"(?:signs?\s+and\s+symptoms?\s+of|symptoms?\s+of)\s+hypocalcemia\s+"
        r"were\s+reviewed",
        re.I,
    ),
    re.compile(
        r"(?:given\s+instructions?|instructed)\s+(?:on\s+)?(?:what\s+to\s+do\s+)?should\s+"
        r"(?:they|the\s+patient)\s+develop",
        re.I,
    ),
    re.compile(
        r"indications?\s+to\s+(?:present|go|return)\s+to\s+(?:the\s+)?(?:ED|ER|emergency)",
        re.I,
    ),
    re.compile(
        r"(?:monitor|monitoring|check)\s+(?:calcium|ca\s+level|pth)\s+(?:closely|levels?)?",
        re.I,
    ),
    re.compile(
        r"(?:routine|prophylactic|empiric(?:al)?)\s+(?:calcium|calcitriol|tums)",
        re.I,
    ),
]

# ---------------------------------------------------------------------------
# Entity-specific pattern libraries
# ---------------------------------------------------------------------------

ENTITY_PATTERNS: dict[str, dict[str, list[re.Pattern]]] = {

    "chyle_leak": {
        "false_positive": [
            # The canonical Valsalva hemostasis check phrase
            re.compile(r"lack\s+of\s+a?\s*chyle\s+leak", re.I),
            re.compile(r"(?:confirm|check|rule\s+out|absence\s+of|no)\s+chyle\s+leak", re.I),
            re.compile(r"chyle\s+leak[^.]{0,40}(?:absent|none|not\s+(?:seen|observed|identified|noted|present))", re.I),
            re.compile(r"(?:prevent|preventing|ligate[^.]{0,30})\s+chyle\s+leak", re.I),
            re.compile(r"(?:hemostasis|valsalva)[^.]{0,60}chyle\s+leak", re.I),
            re.compile(r"chyle\s+leak[^.]{0,40}(?:hemostasis|valsalva)", re.I),
            re.compile(r"(?:avoid|avoiding|risk\s+of)\s+chyle\s+leak", re.I),
            re.compile(r"care\s+(?:was\s+)?taken[^.]{0,40}chyle", re.I),
        ],
        "true_positive": [
            re.compile(r"chyle\s+(?:was\s+)?(?:seen|observed|noted|found|present|leak(?:ing)?|output)\b", re.I),
            re.compile(r"(?:milky|cloudy|chylous)\s+(?:fluid|output|drainage|effusion)", re.I),
            re.compile(r"chyle\s+leak\s+(?:developed|occurred|present|confirmed|requiring)", re.I),
            re.compile(r"(?:persistent|significant|documented)\s+chyle", re.I),
            re.compile(r"chyle\s+leak\s+(?:managed|treated|repaired)", re.I),
            re.compile(r"(?:triglyceride|TG)\s+(?:level|positive)\s+in\s+drain", re.I),
        ],
    },

    "hypocalcemia": {
        "false_positive": [
            # Consent/risk boilerplate — will be caught by CONSENT_BOILERPLATE_PATTERNS too
            re.compile(r"risk\s+of\s+(?:temporary\s+or\s+permanent\s+)?hypocalcemia", re.I),
            re.compile(r"hypocalcemia\s+(?:and\s+hypoparathyroidism\s+[-–]\s+)?temporary\s+or\s+permanent", re.I),
            re.compile(r"(?:Bleeding|Pain|Infection)[,\s]+Hypocalcemia[,\s]+Hypothyroidism", re.I),
            re.compile(r"hypocalcemia[^.]{0,40}(?:temporary|transient)\s+or\s+permanent", re.I),
            # Education/monitoring
            re.compile(r"signs?\s+and\s+symptoms?\s+of\s+hypocalcemia\s+were\s+reviewed", re.I),
            re.compile(r"(?:given\s+)?instructions?[^.]{0,60}hypocalcemia", re.I),
            re.compile(r"(?:monitor|watch)\s+for\s+(?:signs?\s+of\s+)?hypocalcemia", re.I),
            re.compile(r"(?:calcium|ca)\s+(?:was|is|level[s]?)\s+(?:\d+\.?\d*)\s*(?:mg|meq|mmol)?", re.I),
            re.compile(r"hypocalcemia\s+(?:precautions?|teaching|education)", re.I),
        ],
        "true_positive": [
            re.compile(r"(?:post[\s-]?op(?:erative)?|developed|experienced)\s+hypocalcemia", re.I),
            re.compile(r"hypocalcemia\s+(?:noted|confirmed|documented|diagnosed|present)", re.I),
            re.compile(r"symptomatic\s+hypocalcemia", re.I),
            re.compile(r"(?:IV\s+calcium|calcium\s+gluconate|calcium\s+infusion)\s+(?:given|administered|required|started)", re.I),
            re.compile(r"(?:tetany|carpopedal\s+spasm|Chvostek|Trousseau)\s+(?:sign|positive|noted)", re.I),
            re.compile(r"perioral\s+(?:numbness|tingling)\s+(?:consistent|suggesting|due\s+to)", re.I),
            re.compile(r"(?:calcium|ca)\s+(?:dropped|fell|decreased)\s+to\s+\d", re.I),
            re.compile(r"hypocalcemia\s+(?:required|necessitated|treated\s+with)\s+(?:IV|calcium|calcitriol)", re.I),
            re.compile(r"(?:mild|moderate|severe)\s+(?:post[\s-]?op)?\s*hypocalcemia", re.I),
        ],
        "synonym_patterns": [
            # For missed-event detection in notes without the exact word
            re.compile(r"(?:low\s+calcium|calcium\s+(?:low|below\s+normal|deficien))", re.I),
            re.compile(r"(?:tetany|carpopedal\s+spasm|perioral\s+numbness|tingling\s+(?:fingers?|hands?))", re.I),
            re.compile(r"Chvostek|Trousseau", re.I),
            re.compile(r"(?:calcium\s+was|Ca\s+was)\s+[67]\.", re.I),
        ],
    },

    "seroma": {
        "false_positive": [
            # Embedded consent template — the canonical list
            re.compile(r"(?:chyle\s+leak|wound\s+healing)[,\s]+seroma[,\s]+numbness", re.I),
            re.compile(r"seroma[,\s]+numbness[,\s]+orodental", re.I),
            re.compile(r"(?:risk|complication)[^.]{0,60}seroma[^.]{0,20}(?:numbness|orodental|fistula)", re.I),
            re.compile(r"(?:poor\s+scarring|poor\s+healing)[^.]{0,80}seroma", re.I),
        ],
        "true_positive": [
            re.compile(r"(?:post[\s-]?op|postoperative)\s+seroma", re.I),
            re.compile(r"seroma\s+(?:noted|found|identified|drained|aspirated|expanding|developed|present)", re.I),
            re.compile(r"(?:drain(?:ing|ed)?|aspiration\s+of)\s+seroma", re.I),
            re.compile(r"(?:fluid\s+collection|collection\s+of\s+fluid)[^.]{0,40}(?:seromatous|seroma|aspirat)", re.I),
            re.compile(r"(?:persistent|enlarging|symptomatic)\s+seroma", re.I),
            re.compile(r"seroma\s+(?:requiring|managed|treated)", re.I),
        ],
    },

    "hematoma": {
        "false_positive": [
            re.compile(r"risk\s+of\s+(?:post[\s-]?op(?:erative)?\s+)?hematoma", re.I),
            re.compile(r"potential[^.]{0,40}hematoma[^.]{0,30}(?:occur|develop)", re.I),
            re.compile(r"(?:including|but\s+not\s+limited\s+to)[^.]{0,60}hematoma", re.I),
            re.compile(r"hematoma[,\s]+seroma[,\s]+(?:poor|scarring)", re.I),
            re.compile(r"(?:seroma|infection|bleeding)[,\s]+hematoma", re.I),
            re.compile(r"(?:subdural|epidural)\s+hematoma", re.I),  # not thyroid-related
        ],
        "true_positive": [
            re.compile(r"(?:neck|wound|surgical\s+site)\s+hematoma\s+(?:noted|found|requiring|evacuation|drained)", re.I),
            re.compile(r"hematoma\s+evacuation", re.I),
            re.compile(r"(?:returned|taken|brought)\s+(?:to\s+OR|back\s+to\s+OR)[^.]{0,60}hematoma", re.I),
            re.compile(r"(?:expanding|growing|enlarging)\s+(?:neck\s+)?hematoma", re.I),
            re.compile(r"(?:evacuated?|drained?|explored?)[^.]{0,30}hematoma", re.I),
            re.compile(r"(?:clotted\s+blood|old\s+hematoma|hematoma\s+in\s+(?:the\s+)?(?:bed|wound|resection))", re.I),
            re.compile(r"postoperative\s+(?:Diagnosis|dx)[^.]{0,60}hematoma", re.I),
        ],
    },

    "hypoparathyroidism": {
        "false_positive": [
            re.compile(r"hypocalcemia\s+and\s+hypoparathyroidism\s*[-–]?\s*temporary\s+or\s+permanent", re.I),
            re.compile(r"(?:risk|damage)\s+to\s+(?:the\s+)?parathyroid", re.I),
            re.compile(r"(?:temporary|transient)\s+hypoparathyroidism\s+(?:is\s+)?(?:a\s+)?risk", re.I),
            re.compile(r"(?:parathyroid\s+gland[s]?|PTH)\s+(?:injury|damage)\s+(?:risk|may|can)", re.I),
        ],
        "true_positive": [
            re.compile(r"(?:post[\s-]?op(?:erative)?)\s+hypoparathyroidism", re.I),
            re.compile(r"hypoparathyroidism\s+(?:confirmed|noted|diagnosed|documented|after|following)", re.I),
            re.compile(r"(?:permanent|persistent|chronic)\s+hypoparathyroidism", re.I),
            re.compile(r"PTH\s+(?:undetectable|low|<\s*\d+|very\s+low|suppressed)", re.I),
            re.compile(r"hypoparathyroidism\s+(?:s/p|after|following)\s+(?:thyroidectomy|thyroid)", re.I),
            re.compile(r"(?:calcium|calcitriol)\s+supplementation[^.]{0,60}hypoparathyroidism", re.I),
            re.compile(r"(?:idiopathic|surgical)\s+hypoparathyroidism", re.I),
        ],
    },

    "wound_infection": {
        "false_positive": [
            # SSI = Sliding Scale Insulin — the critical abbreviation collision
            re.compile(r"SSI\s+(?:\w+\s+)?(?:per\s+protocol|before\s+meals|for\s+glucose|for\s+DM|for\s+diabetes)", re.I),
            re.compile(r"(?:insulin|novolog|humalog|regular\s+insulin)[^.]{0,40}SSI", re.I),
            re.compile(r"SSI[^.]{0,40}(?:insulin|units|glucose|BG|blood\s+sugar)", re.I),
            re.compile(r"(?:Humulin|Lantus|NPH)[^.]{0,50}SSI", re.I),
            re.compile(r"basal[^.]{0,40}SSI\b", re.I),
            re.compile(r"SSI\s+as\s+BG[-–]", re.I),
            re.compile(r"(?:risk|complication)[^.]{0,60}wound\s+infection[^.]{0,30}(?:hoarseness|calcium|scarring)", re.I),
        ],
        "true_positive": [
            re.compile(r"wound\s+infection\s+(?:noted|found|confirmed|diagnosed|requiring|treated)", re.I),
            re.compile(r"(?:surgical\s+site|wound|incision)\s+infection\s+(?:with|requiring|managed)", re.I),
            re.compile(r"(?:cellulitis|abscess|dehiscence)[^.]{0,40}(?:wound|incision|surgical\s+site)", re.I),
            re.compile(r"(?:wound\s+vac|negative\s+pressure\s+wound|VAC\s+dressing)[^.]{0,60}infection", re.I),
            re.compile(r"infection\s+(?:of|at)\s+(?:the\s+)?(?:surgical\s+site|wound|incision)", re.I),
            re.compile(r"(?:drainage|purulent|erythema)[^.]{0,40}(?:wound|incision)", re.I),
        ],
    },
}

# Synonym patterns for missed-event detection
SYNONYM_PATTERNS: dict[str, list[re.Pattern]] = {
    "chyle_leak": [
        re.compile(r"chylous\s+(?:fluid|effusion|drainage|fistula)", re.I),
        re.compile(r"milky\s+(?:fluid|output|drainage)", re.I),
        re.compile(r"triglyceride[s]?\s+(?:in|from)\s+(?:drain|fluid)", re.I),
    ],
    "hypocalcemia": [
        re.compile(r"(?:low\s+calcium|calcium\s+(?:low|deficien))", re.I),
        re.compile(r"tetany|carpopedal\s+spasm|perioral\s+(?:numbness|tingling)", re.I),
        re.compile(r"Chvostek|Trousseau", re.I),
        re.compile(r"(?:Ca|calcium)\s+(?:was|is|=)\s+[67]\.\d", re.I),
        re.compile(r"IV\s+calcium\s+(?:given|administered|infus)", re.I),
    ],
    "seroma": [
        re.compile(r"(?:serous|seromatous)\s+(?:fluid|drainage|collection)", re.I),
        re.compile(r"fluid\s+collection\s+(?:at|near|around)\s+(?:the\s+)?(?:wound|incision|surgical)", re.I),
    ],
    "hematoma": [
        re.compile(r"(?:neck|wound)\s+(?:swelling|bleeding)\s+(?:requiring|with|that)", re.I),
        re.compile(r"returned\s+to\s+OR\s+for\s+(?:bleeding|hemorrhage|hemostasis)", re.I),
        re.compile(r"clotted\s+blood|blood\s+clot\s+in\s+(?:the\s+)?wound", re.I),
    ],
    "hypoparathyroidism": [
        re.compile(r"PTH\s+(?:undetectable|<\s*5|very\s+low|of\s+[0-9]+\b)", re.I),
        re.compile(r"parathyroid\s+(?:gland[s]?\s+)?(?:removed|resected|autotransplanted)", re.I),
    ],
    "wound_infection": [
        re.compile(r"(?:wound|incision)\s+(?:cellulitis|dehiscence|abscess|drainage)", re.I),
        re.compile(r"(?:SSI|surgical\s+site\s+infection)\s+(?:diagnosed|confirmed|treated)", re.I),
    ],
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class EntityMentionResult:
    """Evaluation result for a single entity mention."""
    note_row_id: str
    research_id: Any
    entity_value_norm: str
    present_or_negated: str
    note_type: str
    note_date: str
    evidence_span: str
    context_window: str

    classification: str  # true_positive | false_positive_consent | false_positive_opnote |
    #                       false_positive_monitoring | false_positive_negation |
    #                       false_positive_unrelated | uncertain
    evidence_strength: str  # strong | medium | weak | none
    is_true_event: bool
    tier_recommendation: int  # 0=exclude, 1=confirmed, 2=probable, 3=possible
    rule_triggered: str
    contradiction_flag: bool = False
    notes: str = ""


@dataclass
class AuditResult:
    """Aggregate result of auditing one entity."""
    entity_name: str
    total_present_mentions: int
    sample_size: int
    true_positive_count: int
    false_positive_count: int
    uncertain_count: int

    precision_estimate: float
    estimated_total_true_events: int
    estimated_total_false_positives: int

    common_failure_modes: list[str] = field(default_factory=list)
    structured_ground_truth_available: bool = False
    structured_true_events: int = 0
    nlp_missing_structured: int = 0

    entity_report_md: str = ""
    refined_prompt_needed: bool = False
    per_sample_results: Optional[pd.DataFrame] = None


# ---------------------------------------------------------------------------
# Core classifier
# ---------------------------------------------------------------------------

class EntityClassifier:
    """Classify individual entity mentions using entity-specific rules."""

    def __init__(self, entity_name: str) -> None:
        self.entity_name = entity_name
        self.fp_patterns = ENTITY_PATTERNS.get(entity_name, {}).get("false_positive", [])
        self.tp_patterns = ENTITY_PATTERNS.get(entity_name, {}).get("true_positive", [])

    def classify(self, mention: dict) -> EntityMentionResult:
        ctx = (mention.get("context_window") or "").strip()
        note_type = (mention.get("note_type") or "").lower()
        _ = (mention.get("evidence_span") or "").strip()  # stored in result dataclass

        # Priority 1: Already negated in source
        if mention.get("present_or_negated") == "negated":
            return self._make_result(mention, "false_positive_negation", "none", False, 0, "source_negated")

        # Priority 2: Consent/risk boilerplate template (applies to ALL entities)
        for pat in CONSENT_BOILERPLATE_PATTERNS:
            if pat.search(ctx):
                return self._make_result(mention, "false_positive_consent", "none", False, 0, f"consent_boilerplate:{pat.pattern[:60]}")

        # Priority 3: Education/monitoring (hypocalcemia specific)
        if self.entity_name == "hypocalcemia":
            for pat in EDUCATION_MONITORING_PATTERNS:
                if pat.search(ctx):
                    return self._make_result(mention, "false_positive_monitoring", "none", False, 0, f"education_monitoring:{pat.pattern[:60]}")

        # Priority 4: Entity-specific false-positive patterns
        for pat in self.fp_patterns:
            if pat.search(ctx):
                return self._make_result(mention, "false_positive_opnote", "none", False, 0, f"entity_fp:{pat.pattern[:60]}")

        # Priority 5: Strong true-positive signal
        for pat in self.tp_patterns:
            if pat.search(ctx):
                strength = "strong" if note_type not in ("h_p",) else "medium"
                return self._make_result(mention, "true_positive", strength, True, 1, f"entity_tp:{pat.pattern[:60]}")

        # Priority 6: Note type heuristics
        if note_type == "h_p":
            # H&P without TP signal → likely risk discussion or history
            return self._make_result(mention, "false_positive_consent", "none", False, 0, "hp_no_tp_signal")

        if note_type in ("endocrine_note",):
            # Endocrine follow-up notes → high prior probability of true event
            return self._make_result(mention, "true_positive", "medium", True, 2, "endocrine_note_high_prior")

        if note_type == "dc_sum":
            # Discharge summaries — moderate prior
            return self._make_result(mention, "uncertain", "weak", False, 3, "dc_sum_moderate_prior")

        if note_type == "op_note":
            # Op_note without any specific TP pattern → uncertain (residual consent template)
            if self.entity_name == "chyle_leak":
                # Special case: op_note chyle_leak is almost always "lack of"
                return self._make_result(mention, "false_positive_opnote", "none", False, 0, "opnote_chyle_assumed_lack_of")
            return self._make_result(mention, "uncertain", "weak", False, 3, "opnote_fallthrough")

        # Default: uncertain — needs review
        return self._make_result(mention, "uncertain", "weak", False, 3, "no_pattern_match")

    def _make_result(
        self,
        mention: dict,
        classification: str,
        evidence_strength: str,
        is_true_event: bool,
        tier: int,
        rule: str,
    ) -> EntityMentionResult:
        return EntityMentionResult(
            note_row_id=mention.get("note_row_id", ""),
            research_id=mention.get("research_id", ""),
            entity_value_norm=mention.get("entity_value_norm", ""),
            present_or_negated=mention.get("present_or_negated", ""),
            note_type=mention.get("note_type", ""),
            note_date=mention.get("note_date", ""),
            evidence_span=mention.get("evidence_span", ""),
            context_window=mention.get("context_window", ""),
            classification=classification,
            evidence_strength=evidence_strength,
            is_true_event=is_true_event,
            tier_recommendation=tier,
            rule_triggered=rule,
        )


# ---------------------------------------------------------------------------
# Missed-event detector
# ---------------------------------------------------------------------------

class MissedEventDetector:
    """Scan raw note text for clinical events that the regex extractor missed."""

    def __init__(self, entity_name: str) -> None:
        self.entity_name = entity_name
        self.synonyms = SYNONYM_PATTERNS.get(entity_name, [])

    def scan_note(self, note_text: str, research_id: Any = None) -> list[dict]:
        """Return list of potential missed mentions in note_text."""
        if not note_text:
            return []
        found = []
        for pat in self.synonyms:
            for m in pat.finditer(note_text):
                start = max(0, m.start() - 80)
                end = min(len(note_text), m.end() + 80)
                found.append({
                    "research_id": research_id,
                    "entity_name": self.entity_name,
                    "matched_text": m.group(0),
                    "context": note_text[start:end],
                    "char_start": m.start(),
                    "pattern": pat.pattern[:80],
                })
        return found

    def scan_batch(self, df: pd.DataFrame, text_col: str = "note_text",
                   id_col: str = "research_id") -> pd.DataFrame:
        """Scan a DataFrame of notes for missed events."""
        results = []
        for _, row in df.iterrows():
            found = self.scan_note(
                str(row.get(text_col, "") or ""),
                research_id=row.get(id_col),
            )
            results.extend(found)
        return pd.DataFrame(results) if results else pd.DataFrame(
            columns=["research_id", "entity_name", "matched_text", "context", "char_start", "pattern"]
        )


# ---------------------------------------------------------------------------
# Excel cell analyzer
# ---------------------------------------------------------------------------

class ExcelCellAnalyzer:
    """Analyze multi-line dictation blocks from Excel-sourced clinical events."""

    # Source column categories (from extracted_clinical_events_v4)
    HP_COLS = {"h_p_1", "h_p_2", "h_p_3", "h_p_4"}
    OPNOTE_COLS = {"opnote_1", "opnote_2", "opnote_3", "opnote_4"}
    DC_SUM_COLS = {"dc_sum_1", "dc_sum_2", "dc_sum_4"}

    def classify_source(self, source_column: str) -> str:
        """Map source_column to note type category."""
        sc = (source_column or "").lower()
        if sc in {c.lower() for c in self.HP_COLS}:
            return "h_p"
        if sc in {c.lower() for c in self.OPNOTE_COLS}:
            return "op_note"
        if sc in {c.lower() for c in self.DC_SUM_COLS}:
            return "dc_sum"
        if "endocrine" in sc or "fm_note" in sc:
            return "endocrine_note"
        if "history" in sc or "thyroid_cx" in sc:
            return "other_history"
        return "other"

    def extract_date_value_pairs(self, text: str) -> list[dict]:
        """Parse Tg/TSH/Ca panel text blocks into (date, value) pairs."""
        # Pattern: value/value (date) optional_notes
        date_re = re.compile(
            r"(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        )
        value_re = re.compile(r"(\d+\.?\d*)\s*(?:ng/ml|ng/dL|mIU/L|mg/dL)?", re.I)
        pairs = []
        for date_m in date_re.finditer(text):
            ctx_start = max(0, date_m.start() - 60)
            ctx_end = min(len(text), date_m.end() + 60)
            ctx = text[ctx_start:ctx_end]
            vals = value_re.findall(ctx)
            if vals:
                pairs.append({"date": date_m.group(0), "values": vals[:3], "context": ctx})
        return pairs

    def is_risk_list(self, text: str) -> bool:
        """Return True if the text is the standard surgical risk disclosure list."""
        for pat in CONSENT_BOILERPLATE_PATTERNS:
            if pat.search(text):
                return True
        return False


# ---------------------------------------------------------------------------
# Main audit function
# ---------------------------------------------------------------------------

def audit_entity(
    entity_name: str,
    con: Any,  # duckdb.DuckDBPyConnection
    sample_size: int = 200,
    cross_check_structured: bool = True,
    verbose: bool = True,
) -> AuditResult:
    """
    Run full intrinsic audit on a single entity type.

    Steps:
      1. Query sample of positive extractions with context windows
      2. Classify each mention with EntityClassifier
      3. Optionally cross-check against structured complications table
      4. Compute precision estimate and failure mode breakdown
      5. Return AuditResult

    Returns:
        AuditResult with per_sample_results DataFrame included
    """
    if verbose:
        print(f"\n{'='*60}")
        print(f"AUDITING: {entity_name}  (sample_size={sample_size})")
        print(f"{'='*60}")

    classifier = EntityClassifier(entity_name)

    # ------------------------------------------------------------------
    # 1. Sample positive extractions with 300-char context window
    # Sample from note_entities_complications first, then join for context
    # to avoid DuckDB post-join sampling reduction.
    # ------------------------------------------------------------------
    q = f"""
    WITH sampled_mentions AS (
        SELECT note_row_id, research_id, entity_value_norm, present_or_negated,
               note_type, note_date, evidence_span, evidence_start, evidence_end
        FROM note_entities_complications
        WHERE entity_value_norm = '{entity_name}'
            AND present_or_negated = 'present'
        USING SAMPLE {sample_size} ROWS
    )
    SELECT
        s.note_row_id,
        s.research_id,
        s.entity_value_norm,
        s.present_or_negated,
        s.note_type,
        LEFT(s.note_date, 10) AS note_date,
        s.evidence_span,
        s.evidence_start,
        s.evidence_end,
        SUBSTRING(cl.note_text, GREATEST(1, s.evidence_start - 150), 300) AS context_window
    FROM sampled_mentions s
    LEFT JOIN clinical_notes_long cl ON s.note_row_id = cl.note_row_id
    """
    try:
        df_sample = con.execute(q).df()
    except Exception as e:
        if verbose:
            print(f"  ERROR sampling {entity_name}: {e}")
        return AuditResult(
            entity_name=entity_name,
            total_present_mentions=0,
            sample_size=0,
            true_positive_count=0,
            false_positive_count=0,
            uncertain_count=0,
            precision_estimate=0.0,
            estimated_total_true_events=0,
            estimated_total_false_positives=0,
            entity_report_md=f"ERROR: {e}",
            refined_prompt_needed=True,
        )

    actual_sample = len(df_sample)
    if verbose:
        print(f"  Sampled {actual_sample} positive mentions")

    # ------------------------------------------------------------------
    # 2. Classify each mention
    # ------------------------------------------------------------------
    results: list[EntityMentionResult] = []
    for _, row in df_sample.iterrows():
        result = classifier.classify(row.to_dict())
        results.append(result)

    if results:
        results_df = pd.DataFrame([vars(r) for r in results])
    else:
        # Empty sample — build minimal placeholder result
        results_df = pd.DataFrame(columns=[
            "note_row_id", "research_id", "entity_value_norm", "present_or_negated",
            "note_type", "note_date", "evidence_span", "context_window",
            "classification", "evidence_strength", "is_true_event",
            "tier_recommendation", "rule_triggered", "contradiction_flag", "notes",
        ])

    # ------------------------------------------------------------------
    # 3. Aggregate statistics
    # ------------------------------------------------------------------
    if len(results_df) > 0:
        tp_count = int((results_df["is_true_event"] == True).sum())
        fp_count = int(results_df["classification"].str.startswith("false_positive").sum())
        uncertain_count = int((results_df["classification"] == "uncertain").sum())
    else:
        tp_count = fp_count = uncertain_count = 0

    precision = tp_count / actual_sample if actual_sample > 0 else 0.0

    # Get total present mentions for extrapolation
    total_q = f"""
    SELECT COUNT(*) FROM note_entities_complications
    WHERE entity_value_norm = '{entity_name}' AND present_or_negated = 'present'
    """
    total_mentions = con.execute(total_q).fetchone()[0]

    estimated_true = int(total_mentions * precision)
    estimated_fp = total_mentions - estimated_true

    # ------------------------------------------------------------------
    # 4. Failure mode breakdown
    # ------------------------------------------------------------------
    failure_modes = []
    if fp_count > 0 and len(results_df) > 0:
        fp_df = results_df[results_df["classification"].str.startswith("false_positive")]
        mode_counts = fp_df["classification"].value_counts().to_dict()
        extracted = fp_df["rule_triggered"].str.extract(r"^([^:]+)")
        if len(extracted) > 0 and 0 in extracted.columns:
            rule_counts = extracted[0].value_counts().head(5).to_dict()
        else:
            rule_counts = {}
        for mode, cnt in sorted(mode_counts.items(), key=lambda x: -x[1]):
            failure_modes.append(f"{mode}: {cnt}/{actual_sample} ({100*cnt/max(actual_sample,1):.0f}%)")
        for rule, cnt in sorted(rule_counts.items(), key=lambda x: -x[1]):
            failure_modes.append(f"  rule={rule}: {cnt}")

    # ------------------------------------------------------------------
    # 5. Structured cross-check (where available)
    # ------------------------------------------------------------------
    structured_true = 0
    nlp_missing = 0
    structured_available = False

    if cross_check_structured and entity_name in ("seroma", "hematoma"):
        structured_available = True
        q_struct = f"""
        SELECT c.research_id,
            COUNT(DISTINCT n.note_row_id) AS nlp_count
        FROM complications c
        LEFT JOIN note_entities_complications n
            ON c.research_id = CAST(n.research_id AS INT)
            AND n.entity_value_norm = '{entity_name}' AND n.present_or_negated = 'present'
        WHERE LOWER(COALESCE(CAST(c.{entity_name} AS VARCHAR), '')) = 'x'
        GROUP BY c.research_id
        """
        struct_df = con.execute(q_struct).df()
        structured_true = len(struct_df)
        nlp_missing = int((struct_df["nlp_count"] == 0).sum())
        if verbose:
            print(f"  Structured {entity_name}: {structured_true} documented, NLP missing {nlp_missing}")

    # ------------------------------------------------------------------
    # 6. Build markdown report
    # ------------------------------------------------------------------
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    report_lines = [
        f"## {entity_name} Audit Report — {ts}",
        "",
        f"**Sample size**: {actual_sample} of {total_mentions} present mentions",
        f"**Precision estimate**: {precision*100:.1f}% ({tp_count} true / {actual_sample} sampled)",
        f"**Estimated total true events**: ~{estimated_true} of {total_mentions} present mentions",
        f"**Estimated false positives**: ~{estimated_fp} ({100*(1-precision):.0f}%)",
        "",
        "### Classification Breakdown",
        "",
    ]
    for cls, cnt in results_df["classification"].value_counts().items():
        report_lines.append(f"- {cls}: {cnt} ({100*cnt/actual_sample:.0f}%)")

    report_lines += ["", "### Evidence Strength", ""]
    for es, cnt in results_df["evidence_strength"].value_counts().items():
        report_lines.append(f"- {es}: {cnt} ({100*cnt/actual_sample:.0f}%)")

    report_lines += ["", "### Common Failure Modes", ""]
    for fm in failure_modes:
        report_lines.append(f"- {fm}")

    if structured_available:
        report_lines += [
            "",
            "### Structured Ground Truth Cross-Check",
            "",
            f"- Structured-documented {entity_name} cases: {structured_true}",
            f"- NLP missed (0 NLP mentions): {nlp_missing} ({100*nlp_missing/max(structured_true,1):.0f}%)",
        ]

    report_lines += [
        "",
        "### Tier Distribution",
        "",
    ]
    for tier, cnt in sorted(results_df["tier_recommendation"].value_counts().items()):
        report_lines.append(f"- Tier {tier}: {cnt}")

    needs_refinement = precision < 0.85
    report_lines += [
        "",
        f"**Refinement required**: {'YES' if needs_refinement else 'NO'} "
        f"(threshold: 85%, actual: {precision*100:.1f}%)",
    ]

    report_md = "\n".join(report_lines)

    if verbose:
        print(f"\n{report_md}\n")

    return AuditResult(
        entity_name=entity_name,
        total_present_mentions=total_mentions,
        sample_size=actual_sample,
        true_positive_count=tp_count,
        false_positive_count=fp_count,
        uncertain_count=uncertain_count,
        precision_estimate=precision,
        estimated_total_true_events=estimated_true,
        estimated_total_false_positives=estimated_fp,
        common_failure_modes=failure_modes,
        structured_ground_truth_available=structured_available,
        structured_true_events=structured_true,
        nlp_missing_structured=nlp_missing,
        entity_report_md=report_md,
        refined_prompt_needed=needs_refinement,
        per_sample_results=results_df,
    )


def run_missed_data_sweep(
    con: Any,
    sample_patients: int = 500,
    entities: Optional[list[str]] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Scan raw clinical notes for entities that the regex extractor may have missed.

    Samples `sample_patients` patients who currently have ZERO extractions for each
    entity, then applies synonym patterns to their raw note text.

    Returns DataFrame of potential missed mentions.
    """
    if entities is None:
        entities = list(SYNONYM_PATTERNS.keys())

    if verbose:
        print(f"\n{'='*60}")
        print(f"MISSED-DATA SWEEP: {sample_patients} patients, entities={entities}")
        print(f"{'='*60}")

    all_missed: list[dict] = []

    for entity in entities:
        detector = MissedEventDetector(entity)
        if not detector.synonyms:
            continue

        # Patients with no NLP extraction for this entity
        q_no_nlp = f"""
        SELECT DISTINCT p.research_id
        FROM (SELECT DISTINCT CAST(research_id AS INT) AS research_id FROM path_synoptics) p
        WHERE p.research_id NOT IN (
            SELECT DISTINCT CAST(research_id AS INT)
            FROM note_entities_complications
            WHERE entity_value_norm = '{entity}' AND present_or_negated = 'present'
        )
        USING SAMPLE {min(sample_patients, 500)} ROWS
        """
        try:
            no_nlp_patients = [r[0] for r in con.execute(q_no_nlp).fetchall()]
        except Exception as e:
            if verbose:
                print(f"  Error getting no-NLP patients for {entity}: {e}")
            continue

        if not no_nlp_patients:
            continue

        ids_str = ",".join(str(i) for i in no_nlp_patients[:200])
        q_notes = f"""
        SELECT research_id, note_text, LEFT(note_date, 10) AS note_date, note_type
        FROM clinical_notes_long
        WHERE CAST(research_id AS INT) IN ({ids_str})
            AND note_text IS NOT NULL AND LENGTH(note_text) > 50
        LIMIT 2000
        """
        try:
            notes_df = con.execute(q_notes).df()
        except Exception as e:
            if verbose:
                print(f"  Error fetching notes for {entity}: {e}")
            continue

        missed_df = detector.scan_batch(notes_df, text_col="note_text", id_col="research_id")
        if len(missed_df) > 0:
            missed_df["entity_name"] = entity
            all_missed.append(missed_df)
            if verbose:
                print(f"  {entity}: found {len(missed_df)} potential missed mentions in {len(notes_df)} notes")

    if all_missed:
        return pd.concat(all_missed, ignore_index=True)
    return pd.DataFrame(
        columns=["research_id", "entity_name", "matched_text", "context", "char_start", "pattern"]
    )


def master_audit(
    con: Any,
    entities: Optional[list[str]] = None,
    sample_size: int = 200,
    run_missed_sweep: bool = True,
    output_dir: Optional[Path] = None,
    verbose: bool = True,
) -> dict[str, AuditResult]:
    """
    Run full audit across all (or specified) entities and write reports.

    Returns:
        Dict mapping entity_name -> AuditResult
    """
    if entities is None:
        entities = ["chyle_leak", "hypocalcemia", "seroma", "hypoparathyroidism",
                    "hematoma", "wound_infection"]

    if output_dir is None:
        output_dir = Path(__file__).parent

    results: dict[str, AuditResult] = {}
    per_entity_sizes = {
        "chyle_leak": 250,
        "hypocalcemia": 250,
        "seroma": 200,
        "hypoparathyroidism": 200,
        "hematoma": 200,
        "wound_infection": 19,  # audit all 19
    }

    for entity in entities:
        n = per_entity_sizes.get(entity, sample_size)
        result = audit_entity(entity, con, sample_size=n, verbose=verbose)
        results[entity] = result

        # Save per-entity report
        report_path = output_dir / f"audit_{entity}_2026.md"
        report_path.write_text(result.entity_report_md, encoding="utf-8")
        if result.per_sample_results is not None and len(result.per_sample_results) > 0:
            parquet_path = output_dir / f"audit_{entity}_2026.parquet"
            result.per_sample_results.to_parquet(parquet_path, index=False)

    if run_missed_sweep:
        sweep_df = run_missed_data_sweep(con, entities=entities, verbose=verbose)
        if len(sweep_df) > 0:
            sweep_path = output_dir / "missed_data_sweep_2026.parquet"
            sweep_df.to_parquet(sweep_path, index=False)
            if verbose:
                print(f"\nMissed data sweep: {len(sweep_df)} potential missed mentions")
                print(f"  Saved to {sweep_path}")

    return results


# ---------------------------------------------------------------------------
# Summary reporting
# ---------------------------------------------------------------------------

def build_summary_table(results: dict[str, AuditResult]) -> str:
    """Build a markdown table summarizing all entity audit results."""
    header = (
        "| Entity | Sample | True+ | False+ | Uncertain | Precision | Est True | Est FP | "
        "Refine? |\n"
        "|--------|--------|-------|--------|-----------|-----------|----------|--------|---------|"
    )
    rows = [header]
    for entity, r in results.items():
        rows.append(
            f"| {entity} | {r.sample_size} | {r.true_positive_count} | {r.false_positive_count} | "
            f"{r.uncertain_count} | {r.precision_estimate*100:.0f}% | {r.estimated_total_true_events} | "
            f"{r.estimated_total_false_positives} | {'YES' if r.refined_prompt_needed else 'no'} |"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _get_connection(use_md: bool, local_path: str = "thyroid_master.duckdb"):
    import duckdb
    import toml
    if use_md:
        token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(local_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extraction audit engine")
    parser.add_argument("--all", action="store_true", help="Audit all entities")
    parser.add_argument("--entity", type=str, default=None, help="Single entity to audit")
    parser.add_argument("--inventory-only", action="store_true", help="Print inventory and exit")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL, don't execute")
    parser.add_argument("--sample", type=int, default=200, help="Sample size per entity")
    parser.add_argument("--no-missed-sweep", action="store_true", help="Skip missed-data sweep")
    args = parser.parse_args()

    if args.inventory_only:
        inv_path = Path(__file__).parent / "extraction_inventory_2026.md"
        if inv_path.exists():
            print(inv_path.read_text())
        else:
            print(f"Inventory file not found: {inv_path}")
        return

    use_md = args.md or (not args.local)
    if args.dry_run:
        print("DRY RUN — would connect to", "MotherDuck" if use_md else "local DuckDB")
        return

    con = _get_connection(use_md)

    output_dir = Path(__file__).parent
    entities = None if args.all else ([args.entity] if args.entity else None)
    if entities is None and not args.all and not args.entity:
        # Default: audit all
        entities = None

    results = master_audit(
        con=con,
        entities=entities,
        sample_size=args.sample,
        run_missed_sweep=not args.no_missed_sweep,
        output_dir=output_dir,
        verbose=True,
    )

    print("\n" + "=" * 60)
    print("MASTER AUDIT SUMMARY")
    print("=" * 60)
    print(build_summary_table(results))

    # Overall data quality confidence
    if results:
        weighted_precision = (
            sum(r.precision_estimate * r.total_present_mentions for r in results.values())
            / sum(r.total_present_mentions for r in results.values())
            if sum(r.total_present_mentions for r in results.values()) > 0
            else 0.0
        )
        print(f"\nOverall weighted precision (before refinement): {weighted_precision*100:.1f}%")
        print(f"Entities needing refinement: {sum(1 for r in results.values() if r.refined_prompt_needed)}")

    con.close()


if __name__ == "__main__":
    main()
