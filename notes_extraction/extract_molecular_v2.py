"""
Deep molecular / genetic-testing detail parser.

Extends BaseExtractor to parse molecular testing reports and clinical
note text for platform, adequacy, classification, individual mutations,
copy-number alterations, fusions, LOH, classifier language, risk
probabilities, and Bethesda category mentions.  Produces EntityMatch
objects with entity_domain = "molecular_detail".
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notes_extraction.base import BaseExtractor, EntityMatch
from utils.text_helpers import extract_nearby_date


class MolecularDetailExtractor(BaseExtractor):
    """Extract detailed molecular testing entities from note text."""

    entity_domain = "molecular_detail"

    # ── static patterns: (regex, entity_type, norm_value, confidence) ─

    _PATTERNS: list[tuple[re.Pattern, str, str, float]] = [

        # ── molecular platforms ──────────────────────────────────────

        (re.compile(r"\bThyroSeq\s*(?:v\.?\s*)?3\b", re.I),
         "molecular_platform", "thyroseq_v3", 0.95),
        (re.compile(r"\bThyroSeq\s*(?:v\.?\s*)?2\b", re.I),
         "molecular_platform", "thyroseq_v2", 0.95),
        (re.compile(r"\bThyroSeq\s*(?:v\.?\s*)?1\b", re.I),
         "molecular_platform", "thyroseq_v1", 0.95),
        (re.compile(r"\bThyroSeq\b(?!\s*(?:v\.?\s*)?\d)", re.I),
         "molecular_platform", "thyroseq", 0.90),
        (re.compile(r"\bAfirma\s+GSC\b", re.I),
         "molecular_platform", "afirma_gsc", 0.95),
        (re.compile(r"\bAfirma\s+GEC\b", re.I),
         "molecular_platform", "afirma_gec", 0.95),
        (re.compile(r"\bAfirma\b(?!\s+G[SE]C)", re.I),
         "molecular_platform", "afirma", 0.90),
        (re.compile(r"\bThyGenX\b", re.I),
         "molecular_platform", "thygenx", 0.95),
        (re.compile(r"\bThyGen\s*NEXT\b", re.I),
         "molecular_platform", "thygennext", 0.95),
        (re.compile(r"\bRosetta\s*GX\s*Reveal\b", re.I),
         "molecular_platform", "rosetta_gx_reveal", 0.95),

        # ── specimen adequacy ────────────────────────────────────────

        (re.compile(
            r"\b(insufficient\s+(?:cellularity|specimen|material|sample))\b",
            re.I),
         "specimen_adequacy", "insufficient", 0.95),
        (re.compile(
            r"\b(inadequate\s+(?:specimen|sample|material))\b", re.I),
         "specimen_adequacy", "inadequate", 0.95),
        (re.compile(r"\b(low\s+cellularity)\b", re.I),
         "specimen_adequacy", "low_cellularity", 0.95),
        (re.compile(r"\b(non[\s-]*diagnostic)\b", re.I),
         "specimen_adequacy", "non_diagnostic", 0.95),
        (re.compile(r"\b(QNS)\b"),
         "specimen_adequacy", "quantity_not_sufficient", 0.95),
        (re.compile(r"\b(quantity\s+not\s+sufficient)\b", re.I),
         "specimen_adequacy", "quantity_not_sufficient", 0.95),
        (re.compile(
            r"\b((?:test(?:ing)?|specimen|sample)\s+(?:was\s+)?cancell?ed)\b",
            re.I),
         "specimen_adequacy", "cancelled", 0.90),

        # ── result classification ────────────────────────────────────

        (re.compile(
            r"\b(?:result|testing|analysis)\s*(?:is|was|:)\s*(positive)\b",
            re.I),
         "result_classification", "positive", 0.95),
        (re.compile(
            r"\b(?:result|testing|analysis)\s*(?:is|was|:)\s*(negative)\b",
            re.I),
         "result_classification", "negative", 0.95),
        (re.compile(
            r"\b(positive\s+(?:for|result))\b", re.I),
         "result_classification", "positive", 0.90),
        (re.compile(
            r"\b(negative\s+(?:for|result))\b", re.I),
         "result_classification", "negative", 0.90),
        (re.compile(
            r"\b(?:result|testing|analysis)\s*(?:is|was|:)\s*(suspicious)\b",
            re.I),
         "result_classification", "suspicious", 0.95),
        (re.compile(
            r"\b(?:result|testing)\s*(?:is|was|:)\s*(indeterminate)\b",
            re.I),
         "result_classification", "indeterminate", 0.95),
        (re.compile(
            r"\b(?:result|testing)\s*(?:is|was|:)\s*(benign)\b",
            re.I),
         "result_classification", "benign", 0.95),
        (re.compile(
            r"\b((?:mutation|alteration|variant)s?\s+detected)\b", re.I),
         "result_classification", "positive", 0.95),
        (re.compile(
            r"\b(no\s+(?:mutation|alteration|variant)s?\s+"
            r"(?:detected|identified|found))\b", re.I),
         "result_classification", "negative", 0.95),
        (re.compile(
            r"\b(clinically\s+significant\s+"
            r"(?:mutation|alteration|variant))\b", re.I),
         "result_classification", "positive", 0.90),

        # ── individual mutations (beyond GeneticsExtractor) ──────────

        (re.compile(r"\bEIF1AX\b", re.I),
         "mutation_eif1ax", "EIF1AX", 0.95),
        (re.compile(r"\bTP53\b", re.I),
         "mutation_tp53", "TP53", 0.95),
        (re.compile(r"\bPAX8[\s/]*PPARG\b", re.I),
         "mutation_pax8_pparg", "PAX8_PPARG", 0.95),
        (re.compile(r"\bDICER1\b", re.I),
         "mutation_dicer1", "DICER1", 0.95),
        (re.compile(r"\bCTNNB1\b", re.I),
         "mutation_ctnnb1", "CTNNB1", 0.95),
        (re.compile(r"\bPIK3CA\b", re.I),
         "mutation_pik3ca", "PIK3CA", 0.95),
        (re.compile(r"\bPTEN\b", re.I),
         "mutation_pten", "PTEN", 0.95),
        (re.compile(r"\bAKT1\b", re.I),
         "mutation_akt1", "AKT1", 0.95),
        (re.compile(r"\bTSHR\b", re.I),
         "mutation_tshr", "TSHR", 0.95),
        (re.compile(r"\bGNAS\b", re.I),
         "mutation_gnas", "GNAS", 0.95),

        # ── copy number alterations ──────────────────────────────────

        (re.compile(r"\b(copy\s+number\s+amplification)\b", re.I),
         "copy_number_alteration", "amplification", 0.95),
        (re.compile(r"\b(copy\s+number\s+deletion)\b", re.I),
         "copy_number_alteration", "deletion", 0.95),
        (re.compile(r"\b(copy\s+number\s+gain)\b", re.I),
         "copy_number_alteration", "gain", 0.95),
        (re.compile(r"\b(copy\s+number\s+loss)\b", re.I),
         "copy_number_alteration", "loss", 0.95),
        (re.compile(r"\b((?:gene|chromosomal)\s+amplification)\b", re.I),
         "copy_number_alteration", "amplification", 0.85),
        (re.compile(r"\b((?:gene|chromosomal)\s+deletion)\b", re.I),
         "copy_number_alteration", "deletion", 0.85),

        # ── loss of heterozygosity ───────────────────────────────────

        (re.compile(r"\b(loss\s+of\s+heterozygosity)\b", re.I),
         "loh", "loh", 0.95),
        (re.compile(r"\bLOH\b"),
         "loh", "loh", 0.95),

        # ── gene expression classifier results ──────────────────────

        (re.compile(
            r"\bGEC\s*(?:result\s*)?(?:is\s*|:\s*)(positive)\b", re.I),
         "classifier_result", "gec_positive", 0.95),
        (re.compile(
            r"\bGEC\s*(?:result\s*)?(?:is\s*|:\s*)(suspicious)\b", re.I),
         "classifier_result", "gec_suspicious", 0.95),
        (re.compile(
            r"\bGEC\s*(?:result\s*)?(?:is\s*|:\s*)(benign)\b", re.I),
         "classifier_result", "gec_benign", 0.95),
        (re.compile(
            r"\bGSC\s*(?:result\s*)?(?:is\s*|:\s*)(suspicious)\b", re.I),
         "classifier_result", "gsc_suspicious", 0.95),
        (re.compile(
            r"\bGSC\s*(?:result\s*)?(?:is\s*|:\s*)(benign)\b", re.I),
         "classifier_result", "gsc_benign", 0.95),
        (re.compile(
            r"\b((?:classifier|expression)\s+score\s*"
            r"(?:of\s*|[=:]\s*)?\d+(?:\.\d+)?)\b", re.I),
         "classifier_result", "classifier_score", 0.90),

        # ── risk probability (static phrases) ────────────────────────

        (re.compile(
            r"\b(high\s+(?:probability|risk|suspicion)\s+"
            r"(?:of|for)\s+malignancy)\b", re.I),
         "risk_probability", "high_risk_malignancy", 0.90),
        (re.compile(
            r"\b(low\s+(?:probability|risk|suspicion)\s+"
            r"(?:of|for)\s+malignancy)\b", re.I),
         "risk_probability", "low_risk_malignancy", 0.90),
        (re.compile(
            r"\b(intermediate\s+(?:probability|risk|suspicion))\b", re.I),
         "risk_probability", "intermediate_risk", 0.85),
        (re.compile(r"\b(high\s+risk)\b", re.I),
         "risk_probability", "high_risk", 0.70),
        (re.compile(r"\b(low\s+risk)\b", re.I),
         "risk_probability", "low_risk", 0.70),
    ]

    # ── dynamic patterns (need runtime normalization) ────────────────

    _FUSION_PAT = re.compile(
        r"\b([A-Z][A-Z0-9]{1,10})[\s]*[-/][\s]*([A-Z][A-Z0-9]{1,10})"
        r"\s+(?:fusion|rearrangement|translocation)\b",
        re.I,
    )

    _FUSION_KNOWN = re.compile(
        r"\b(RET[\s]*[-/][\s]*PTC[123]?"
        r"|PAX8[\s]*[-/][\s]*PPARG"
        r"|CCDC6[\s]*[-/][\s]*RET"
        r"|NCOA4[\s]*[-/][\s]*RET"
        r"|ETV6[\s]*[-/][\s]*NTRK3"
        r"|TPM3[\s]*[-/][\s]*NTRK1)\b",
        re.I,
    )

    _RISK_PCT = re.compile(
        r"\b(\d{1,3}(?:\.\d+)?)\s*%\s*"
        r"(?:probability|risk|chance|likelihood)\s*"
        r"(?:of\s+)?(?:malignancy|cancer|neoplasm)?\b",
        re.I,
    )

    _BETHESDA = re.compile(
        r"\bbethesda\s*(?:category|class(?:ification)?|cat\.?)?\s*"
        r"(VI|V|IV|III|II|I|[1-6])\b",
        re.I,
    )

    _BETHESDA_ROMAN: dict[str, str] = {
        "I": "1", "II": "2", "III": "3",
        "IV": "4", "V": "5", "VI": "6",
    }

    # ── extract ──────────────────────────────────────────────────────

    def extract(self, note_row_id, research_id, note_type, note_text, note_date=None):
        results: list[EntityMatch] = []
        seen: set[tuple[str, int]] = set()

        # ── static patterns ──────────────────────────────────────────
        for pat, etype, norm, conf in self._PATTERNS:
            for m in pat.finditer(note_text):
                key = (f"{etype}:{norm}", m.start())
                if key in seen:
                    continue
                seen.add(key)
                raw = m.group(1) if m.lastindex else m.group(0)
                results.append(EntityMatch(
                    research_id=research_id,
                    note_row_id=note_row_id,
                    note_type=note_type,
                    entity_type=etype,
                    entity_value_raw=raw,
                    entity_value_norm=norm,
                    present_or_negated=self.check_negation(note_text, m.start()),
                    confidence=conf,
                    evidence_span=m.group(0),
                    evidence_start=m.start(),
                    evidence_end=m.end(),
                    entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                    note_date=note_date,
                ))

        # ── gene fusions (general pattern) ───────────────────────────
        for m in self._FUSION_PAT.finditer(note_text):
            gene_a = m.group(1).upper()
            gene_b = m.group(2).upper()
            norm = f"{gene_a}-{gene_b}"
            key = (f"gene_fusion:{norm}", m.start())
            if key in seen:
                continue
            seen.add(key)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="gene_fusion",
                entity_value_raw=m.group(0),
                entity_value_norm=norm,
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        # ── gene fusions (known thyroid fusions without keyword) ─────
        for m in self._FUSION_KNOWN.finditer(note_text):
            raw = m.group(1)
            norm = re.sub(r"[\s/\-]+", "-", raw.upper())
            key = (f"gene_fusion:{norm}", m.start())
            if key in seen:
                continue
            seen.add(key)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="gene_fusion",
                entity_value_raw=raw,
                entity_value_norm=norm,
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        # ── percentage-based risk ────────────────────────────────────
        for m in self._RISK_PCT.finditer(note_text):
            pct = m.group(1)
            key = (f"risk_probability:{pct}%", m.start())
            if key in seen:
                continue
            seen.add(key)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="risk_probability",
                entity_value_raw=m.group(0),
                entity_value_norm=f"{pct}%",
                present_or_negated=self.check_negation(note_text, m.start()),
                confidence=0.85,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        # ── Bethesda category ────────────────────────────────────────
        for m in self._BETHESDA.finditer(note_text):
            raw_cat = m.group(1)
            num = self._BETHESDA_ROMAN.get(raw_cat.upper(), raw_cat)
            norm = f"bethesda_{num}"
            key = (f"bethesda_mention:{norm}", m.start())
            if key in seen:
                continue
            seen.add(key)
            results.append(EntityMatch(
                research_id=research_id,
                note_row_id=note_row_id,
                note_type=note_type,
                entity_type="bethesda_mention",
                entity_value_raw=m.group(0),
                entity_value_norm=norm,
                present_or_negated="present",
                confidence=0.95,
                evidence_span=m.group(0),
                evidence_start=m.start(),
                evidence_end=m.end(),
                entity_date=extract_nearby_date(note_text, m.start(), m.end()),
                note_date=note_date,
            ))

        return results
