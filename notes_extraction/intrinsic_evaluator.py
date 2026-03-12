"""
Intrinsic evaluation framework for NLP-extracted clinical entities.

Provides heuristic, temporal, and context-based evaluation of extracted
entity mentions without requiring external gold-standard labels.
Generalizable to any entity type (RLN injury, hypocalcemia, hematoma, etc.).

Usage:
    evaluator = IntrinsicEvaluator(entity_name="rln_injury")
    results = evaluator.evaluate_batch(df_mentions, df_context)
    report = evaluator.generate_report(results)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Context-classification patterns (case-insensitive)
# ---------------------------------------------------------------------------

RISK_DISCUSSION_PATTERNS: list[re.Pattern] = [
    re.compile(r"risk(?:s)?\s+(?:of|including|for|and benefits|associated)", re.I),
    re.compile(r"discussed\b.*(?:risk|complication|consent)", re.I),
    re.compile(r"counseled\b.*(?:risk|complication)", re.I),
    re.compile(r"apprised\b.*(?:risk|complication)", re.I),
    re.compile(r"potential\s+complication", re.I),
    re.compile(r"possibility\s+of\b.*(?:injur|paralys|pares)", re.I),
    re.compile(r"consent\s+(?:was\s+)?(?:obtained|signed|reviewed)", re.I),
    re.compile(r"informed\s+consent", re.I),
    re.compile(r"alternatives?\s+(?:were\s+)?discussed", re.I),
    re.compile(r"risk\s+for\s+tracheostomy", re.I),
]

PRESERVATION_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:identified|visualized)\s+and\s+(?:preserved|protected|intact)", re.I),
    re.compile(r"(?:nerve|RLN)\s+(?:was\s+)?(?:preserved|intact|protected|identified)", re.I),
    re.compile(r"no\s+(?:evidence\s+of\s+)?(?:injury|damage)\s+to\s+(?:the\s+)?(?:nerve|RLN)", re.I),
    re.compile(r"(?:nerve|RLN)\s+(?:stimulated|stim)\s+at\s+\d", re.I),
    re.compile(r"bilateral\s+RLN\s+(?:identified|preserved|intact)", re.I),
    re.compile(r"(?:both|bilateral)\s+(?:recurrent\s+laryngeal\s+)?nerves?\s+(?:were\s+)?(?:preserved|intact)", re.I),
    re.compile(r"good\s+(?:action\s+)?potentials?\b", re.I),
    re.compile(r"EMG\s+(?:signal|response)\s+(?:was\s+)?(?:normal|stable|intact)", re.I),
]

HISTORICAL_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:history|hx)\s+of\b.*(?:RLN|nerve\s+injur|cord\s+p(?:aralys|ares))", re.I),
    re.compile(r"(?:prior|previous|pre[\s-]?existing|pre[\s-]?operative)\s+.*(?:RLN|nerve\s+injur|cord\s+p(?:aralys|ares))", re.I),
    re.compile(r"(?:RLN|nerve)\s+injury\s+at\s+(?:outside|OSH|another)", re.I),
    re.compile(r"known\s+(?:RLN|nerve|vocal\s+cord)", re.I),
]

DIAGNOSIS_SECTION_PATTERNS: list[re.Pattern] = [
    re.compile(r"(?:pre[\s-]?op(?:erative)?|post[\s-]?op(?:erative)?)\s+diagnos[ie]s?\s*:", re.I),
    re.compile(r"(?:discharge|admitting|final)\s+diagnos[ie]s?\s*:", re.I),
    re.compile(r"assessment\s*(?:/\s*plan)?\s*:", re.I),
    re.compile(r"(?:active\s+)?problem\s+list\s*:", re.I),
    re.compile(r"complications?\s*:", re.I),
    re.compile(r"impression\s*:", re.I),
]

TRUE_INJURY_LANGUAGE: list[re.Pattern] = [
    re.compile(r"(?:noted|found|observed|discovered)\s+(?:to\s+have\s+)?(?:RLN|nerve|vocal\s+cord)", re.I),
    re.compile(r"(?:RLN|nerve)\s+(?:was\s+)?(?:injured|sacrificed|transected|divided|stretched|inadvertent)", re.I),
    re.compile(r"(?:postoperative|post[\s-]?op)\s+.*(?:hoarseness|dysphonia|vocal\s+cord)", re.I),
    re.compile(r"(?:vocal\s+cord|fold)\s+(?:was\s+)?(?:immobile|paralyz|paret|weak|fixed)", re.I),
    re.compile(r"(?:scope|laryngoscopy|flex)\s+show(?:s|ed)\b.*(?:paralys|pares|immobil)", re.I),
    re.compile(r"s/p\s+.*(?:thyroidectomy|surgery)\b.*(?:cord|RLN|nerve)", re.I),
    re.compile(r"(?:resolved|improving|improved|persistent|recovering)", re.I),
    re.compile(r"(?:bilateral|unilateral|left|right)\s+(?:vocal\s+)?(?:cord|fold)\s+(?:paralys|pares)", re.I),
]


@dataclass
class EvaluationResult:
    """Result of intrinsic evaluation for a single entity mention."""
    note_row_id: str
    research_id: int
    entity_value_norm: str
    original_assertion: str
    confidence_original: float

    classification: str  # rln_injury_confirmed, rln_risk_mentioned, rln_identified_preserved, rln_injury_historical, rln_injury_suspected
    evidence_strength: str  # strong, medium, weak, none
    is_true_injury: bool
    tier_recommendation: int  # 0=exclude, 1=confirmed, 2=probable, 3=possible

    rule_triggered: str
    context_snippet: str = ""
    contradiction_flag: bool = False
    notes: str = ""


class IntrinsicEvaluator:
    """
    Intrinsic (no-gold-label) evaluator for NLP-extracted clinical entities.

    Applies layered heuristic rules:
      1. Risk-discussion context detection
      2. Preservation/intact language detection
      3. Historical/pre-existing reference detection
      4. Temporal plausibility (same-day vs post-op)
      5. Diagnosis/assessment section detection
      6. Entity specificity grading
      7. Contradiction detection (same patient: injury + preserved)
    """

    def __init__(self, entity_name: str = "rln_injury"):
        self.entity_name = entity_name
        self._entity_family = self._resolve_entity_family(entity_name)

    @staticmethod
    def _resolve_entity_family(entity_name: str) -> set[str]:
        families = {
            "rln_injury": {"rln_injury", "vocal_cord_paralysis", "vocal_cord_paresis"},
            "hypocalcemia": {"hypocalcemia", "hypoparathyroidism"},
            "hematoma": {"hematoma"},
            "seroma": {"seroma"},
            "wound_infection": {"wound_infection"},
            "chyle_leak": {"chyle_leak"},
        }
        return families.get(entity_name, {entity_name})

    def evaluate_mention(
        self,
        note_row_id: str,
        research_id: int,
        entity_value_norm: str,
        present_or_negated: str,
        confidence: float,
        note_type: str,
        days_post_surgery: Optional[int],
        context_window: str,
    ) -> EvaluationResult:
        """Evaluate a single entity mention using layered heuristic rules."""
        ctx = context_window.lower() if context_window else ""
        base = dict(
            note_row_id=note_row_id,
            research_id=research_id,
            entity_value_norm=entity_value_norm,
            original_assertion=present_or_negated,
            confidence_original=confidence,
            context_snippet=context_window[:200] if context_window else "",
        )

        if present_or_negated == "negated":
            return EvaluationResult(
                **base, classification="negated", evidence_strength="none",
                is_true_injury=False, tier_recommendation=0,
                rule_triggered="negated_assertion",
            )

        if self._matches_any(ctx, RISK_DISCUSSION_PATTERNS):
            in_dx_section = self._is_in_diagnosis_section(context_window, entity_value_norm)
            if not in_dx_section:
                return EvaluationResult(
                    **base, classification=f"{self.entity_name}_risk_mentioned",
                    evidence_strength="none", is_true_injury=False,
                    tier_recommendation=0, rule_triggered="risk_discussion",
                )

        if self._matches_any(ctx, PRESERVATION_PATTERNS):
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_identified_preserved",
                evidence_strength="none", is_true_injury=False,
                tier_recommendation=0, rule_triggered="preservation_language",
            )

        if self._matches_any(ctx, HISTORICAL_PATTERNS):
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_historical",
                evidence_strength="weak", is_true_injury=False,
                tier_recommendation=0, rule_triggered="historical_reference",
            )

        is_same_day = days_post_surgery is not None and days_post_surgery == 0
        is_hp = note_type in ("h_p", "history_summary", "other_history")

        if is_same_day and is_hp and entity_value_norm == "rln_injury":
            has_true_injury_lang = self._matches_any(ctx, TRUE_INJURY_LANGUAGE)
            in_dx_section = self._is_in_diagnosis_section(context_window, entity_value_norm)
            if not (has_true_injury_lang or in_dx_section):
                return EvaluationResult(
                    **base, classification=f"{self.entity_name}_risk_mentioned",
                    evidence_strength="none", is_true_injury=False,
                    tier_recommendation=0,
                    rule_triggered="same_day_hp_generic_rln",
                )

        if self._matches_any(ctx, TRUE_INJURY_LANGUAGE):
            strength = "strong" if not is_same_day else "medium"
            in_dx = self._is_in_diagnosis_section(context_window, entity_value_norm)
            if in_dx:
                strength = "strong"
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_confirmed",
                evidence_strength=strength, is_true_injury=True,
                tier_recommendation=1 if strength == "strong" else 2,
                rule_triggered="true_injury_language",
            )

        in_dx_section = self._is_in_diagnosis_section(context_window, entity_value_norm)
        if in_dx_section:
            strength = "strong" if not is_same_day else "medium"
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_confirmed",
                evidence_strength=strength, is_true_injury=True,
                tier_recommendation=1 if strength == "strong" else 2,
                rule_triggered="diagnosis_section",
            )

        is_specific_entity = entity_value_norm in ("vocal_cord_paralysis", "vocal_cord_paresis")
        if is_specific_entity and not is_same_day:
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_confirmed",
                evidence_strength="medium", is_true_injury=True,
                tier_recommendation=2, rule_triggered="specific_entity_post_day0",
            )

        if is_same_day:
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_suspected",
                evidence_strength="weak", is_true_injury=False,
                tier_recommendation=0,
                rule_triggered="same_day_unclassified",
                notes="Same-day mention without clear injury/risk context",
            )

        if days_post_surgery is not None and days_post_surgery > 0:
            return EvaluationResult(
                **base, classification=f"{self.entity_name}_suspected",
                evidence_strength="weak", is_true_injury=False,
                tier_recommendation=3,
                rule_triggered="post_day0_generic_unclassified",
                notes="Post-op mention without clear injury or risk context",
            )

        return EvaluationResult(
            **base, classification=f"{self.entity_name}_suspected",
            evidence_strength="weak", is_true_injury=False,
            tier_recommendation=3, rule_triggered="fallthrough",
        )

    def evaluate_batch(
        self,
        df: pd.DataFrame,
        context_col: str = "context_window",
    ) -> pd.DataFrame:
        """
        Evaluate a batch of entity mentions.

        Required columns: note_row_id, research_id, entity_value_norm,
            present_or_negated, confidence, note_type, days_post_surgery, {context_col}
        """
        results = []
        for _, row in df.iterrows():
            r = self.evaluate_mention(
                note_row_id=str(row.get("note_row_id", "")),
                research_id=int(row.get("research_id", 0)),
                entity_value_norm=str(row.get("entity_value_norm", "")),
                present_or_negated=str(row.get("present_or_negated", "present")),
                confidence=float(row.get("confidence", 0.0) or 0.0),
                note_type=str(row.get("note_type", "")),
                days_post_surgery=int(row["days_post_surgery"]) if pd.notna(row.get("days_post_surgery")) else None,
                context_window=str(row.get(context_col, "") or ""),
            )
            results.append(r.__dict__)

        result_df = pd.DataFrame(results)
        result_df = self._detect_contradictions(result_df)
        return result_df

    def _detect_contradictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flag patients who have both 'injury' and 'preserved' mentions."""
        if df.empty:
            return df
        injury_patients = set(
            df.loc[df["classification"].str.contains("confirmed|suspected", na=False), "research_id"]
        )
        preserved_patients = set(
            df.loc[df["classification"].str.contains("preserved", na=False), "research_id"]
        )
        contradicted = injury_patients & preserved_patients
        df["contradiction_flag"] = df["research_id"].isin(contradicted)
        return df

    def generate_report(self, eval_df: pd.DataFrame) -> str:
        """Generate a markdown evaluation report."""
        total = len(eval_df)
        if total == 0:
            return f"# Intrinsic Evaluation Report: {self.entity_name}\n\nNo mentions evaluated."

        n_true = eval_df["is_true_injury"].sum()
        n_false = total - n_true
        precision_est = n_true / total if total > 0 else 0

        lines = [
            f"# Intrinsic Evaluation Report: {self.entity_name}",
            f"",
            f"**Date:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}",
            f"**Entity family:** {', '.join(sorted(self._entity_family))}",
            f"**Total mentions evaluated:** {total}",
            f"",
            f"## Estimated Precision (Current Tier 3)",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| True injuries (heuristic) | {n_true} |",
            f"| False positives (heuristic) | {n_false} |",
            f"| **Estimated precision** | **{precision_est:.1%}** |",
            f"",
            f"## Classification Breakdown",
            f"",
        ]

        cls_counts = eval_df["classification"].value_counts()
        lines.append("| Classification | Count | % |")
        lines.append("|---------------|-------|---|")
        for cls, cnt in cls_counts.items():
            lines.append(f"| {cls} | {cnt} | {100*cnt/total:.1f}% |")

        lines.extend(["", "## Rule Triggers", ""])
        rule_counts = eval_df["rule_triggered"].value_counts()
        lines.append("| Rule | Count | % |")
        lines.append("|------|-------|---|")
        for rule, cnt in rule_counts.items():
            lines.append(f"| {rule} | {cnt} | {100*cnt/total:.1f}% |")

        lines.extend(["", "## Tier Recommendations", ""])
        tier_counts = eval_df["tier_recommendation"].value_counts().sort_index()
        tier_labels = {0: "Exclude", 1: "Confirmed", 2: "Probable", 3: "Possible"}
        lines.append("| Tier | Label | Count | % |")
        lines.append("|------|-------|-------|---|")
        for tier, cnt in tier_counts.items():
            lines.append(f"| {tier} | {tier_labels.get(tier, '?')} | {cnt} | {100*cnt/total:.1f}% |")

        lines.extend(["", "## Evidence Strength Distribution", ""])
        str_counts = eval_df["evidence_strength"].value_counts()
        lines.append("| Strength | Count |")
        lines.append("|----------|-------|")
        for s, cnt in str_counts.items():
            lines.append(f"| {s} | {cnt} |")

        n_contradictions = eval_df["contradiction_flag"].sum()
        if n_contradictions > 0:
            lines.extend([
                "", "## Contradiction Flags", "",
                f"**{n_contradictions} mentions** from patients with both injury and preserved mentions.",
                "These require manual review.",
            ])

        lines.extend([
            "", "## Suggested New Entity Values", "",
            f"| Current | Suggested Replacement |",
            f"|---------|----------------------|",
            f"| rln_injury (in risk discussion) | {self.entity_name}_risk_mentioned |",
            f"| rln_injury (nerve preserved) | {self.entity_name}_identified_preserved |",
            f"| rln_injury (historical) | {self.entity_name}_historical |",
            f"| rln_injury (confirmed post-op) | {self.entity_name}_confirmed |",
            f"| rln_injury (suspected, weak) | {self.entity_name}_suspected |",
        ])

        return "\n".join(lines)

    @staticmethod
    def _matches_any(text: str, patterns: list[re.Pattern]) -> bool:
        return any(p.search(text) for p in patterns)

    @staticmethod
    def _is_in_diagnosis_section(context: str, entity_value: str) -> bool:
        """Check if the entity appears within a diagnosis/assessment/complication section header."""
        if not context:
            return False
        for pat in DIAGNOSIS_SECTION_PATTERNS:
            m = pat.search(context)
            if m:
                ev_pos = context.lower().find(entity_value.replace("_", " "))
                if ev_pos < 0:
                    ev_pos = context.lower().find(entity_value)
                if ev_pos >= 0 and ev_pos > m.start():
                    section_text = context[m.start():min(m.start() + 500, len(context))]
                    next_section = re.search(r"\n\s*\n|\n[A-Z][A-Za-z ]+:", section_text[20:])
                    if next_section is None or ev_pos - m.start() < next_section.start() + 20:
                        return True
        return False


def refine_extraction(
    con,
    entity_name: str = "rln_injury",
    sample_size: int = 200,
    context_window_chars: int = 300,
) -> tuple[pd.DataFrame, str]:
    """
    Run the full intrinsic evaluation pipeline for any entity type.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB/MotherDuck connection.
    entity_name : str
        Root entity name (e.g., "rln_injury", "hypocalcemia").
    sample_size : int
        Number of mentions to evaluate.
    context_window_chars : int
        Characters before/after the match to include for context.

    Returns
    -------
    eval_df : pd.DataFrame
        Evaluated mentions with classification columns.
    report : str
        Markdown evaluation report.
    """
    evaluator = IntrinsicEvaluator(entity_name=entity_name)
    entity_family = evaluator._entity_family
    entity_in_clause = ", ".join(f"'{e}'" for e in entity_family)

    sql = f"""
    WITH surgical_patients AS (
        SELECT CAST(research_id AS INT) AS research_id,
               MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
        GROUP BY 1
    )
    SELECT
        ne.note_row_id,
        CAST(ne.research_id AS INT) AS research_id,
        ne.entity_value_norm,
        ne.present_or_negated,
        COALESCE(TRY_CAST(ne.confidence AS DOUBLE), 0.9) AS confidence,
        ne.note_type,
        DATEDIFF('day', sp.first_surgery_date,
                 COALESCE(ne.inferred_event_date,
                          TRY_CAST(ne.entity_date AS DATE),
                          TRY_CAST(ne.note_date AS DATE))) AS days_post_surgery,
        ne.evidence_start,
        ne.evidence_end,
        SUBSTRING(cn.note_text,
                  GREATEST(1, CAST(ne.evidence_start AS INT) - {context_window_chars}),
                  LEAST({context_window_chars * 2 + 200},
                        LENGTH(cn.note_text) - GREATEST(0, CAST(ne.evidence_start AS INT) - {context_window_chars})))
            AS context_window
    FROM note_entities_complications ne
    JOIN surgical_patients sp
        ON CAST(ne.research_id AS INT) = sp.research_id
    LEFT JOIN clinical_notes_long cn
        ON ne.note_row_id = cn.note_row_id
    WHERE LOWER(ne.entity_value_norm) IN ({entity_in_clause})
      AND LOWER(COALESCE(ne.present_or_negated, '')) = 'present'
      AND (ne.confidence IS NULL OR TRY_CAST(ne.confidence AS DOUBLE) >= 0.65)
      AND COALESCE(ne.inferred_event_date,
                   TRY_CAST(ne.entity_date AS DATE),
                   TRY_CAST(ne.note_date AS DATE)) >= sp.first_surgery_date
    ORDER BY RANDOM()
    LIMIT {sample_size}
    """
    df = con.execute(sql).fetchdf()

    eval_df = evaluator.evaluate_batch(df)
    report = evaluator.generate_report(eval_df)
    return eval_df, report
