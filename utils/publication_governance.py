"""Publication vs rehearsal governance for manual review and promotion decisions.

``119_md_formalization_validate.py`` (``--release-mode``), ``126_final_master_release.py``
(CSV preflight), and audits share these rules. See ``docs/publication_governance_gate.md``.
"""
from __future__ import annotations

# Canonical automation placeholder from ``scripts/128_mrq_tier_policy_gate_build.py``.
MRQ_SYNTHETIC_PLACEHOLDER_EXACT = "SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF"

# Case-insensitive substring markers for equivalent rehearsal / non-signoff placeholders.
MRQ_SYNTHETIC_PLACEHOLDER_MARKERS: tuple[str, ...] = (
    "synthetic_automation_only",
    "not_manuscript_signoff",
    "automation_only_not_manuscript",
)


def normalize_verification_status_token(raw: object) -> str | None:
    """Return stripped status or None when value is blank / NaN-like."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    if low in ("nan", "none", "null"):
        return None
    return s


def is_mrq_synthetic_placeholder_verification_status(raw: object) -> bool:
    """True when ``verification_status`` is a rehearsal placeholder, not manuscript sign-off."""
    s = normalize_verification_status_token(raw)
    if s is None:
        return False
    sl = s.lower()
    if sl == MRQ_SYNTHETIC_PLACEHOLDER_EXACT.lower():
        return True
    return any(marker in sl for marker in MRQ_SYNTHETIC_PLACEHOLDER_MARKERS)


def mrq_synthetic_placeholder_where_sql(column: str = "verification_status") -> str:
    """DuckDB boolean expression: true when ``column`` is a blocked synthetic placeholder."""
    exact = MRQ_SYNTHETIC_PLACEHOLDER_EXACT.lower().replace("'", "''")
    likes: list[str] = [
        f"LOWER(TRIM(CAST({column} AS VARCHAR))) = '{exact}'",
    ]
    for m in MRQ_SYNTHETIC_PLACEHOLDER_MARKERS:
        esc = m.replace("'", "''")
        likes.append(f"LOWER(TRIM(CAST({column} AS VARCHAR))) LIKE '%{esc}%'")
    return "(" + " OR ".join(likes) + ")"


def sql_count_mrq_synthetic_rows() -> str:
    """Parameterized SQL returning a single scalar count."""
    where = mrq_synthetic_placeholder_where_sql()
    return f"SELECT COUNT(*) FROM qa.manual_review_queue WHERE {where}"


def sql_count_promotion_decisions_missing_batch() -> str:
    """Count rows in ``qa.promotion_review_decisions`` with NULL/blank ``decision_batch_id``."""
    return """
        SELECT COUNT(*) FROM qa.promotion_review_decisions
        WHERE decision_batch_id IS NULL
           OR TRIM(CAST(decision_batch_id AS VARCHAR)) = ''
    """.strip()
