"""
Shared contract constants for ``longitudinal_lab_canonical_v1`` (script 77 / validation).

Used by:
- ``tests/test_lab_canonical_contract_offline.py`` — CI-safe in-memory checks
- ``tests/test_lab_canonical.py`` — local ``thyroid_master.duckdb`` smoke tests (``local_db`` marker)
"""
from __future__ import annotations

REQUIRED_COLUMNS = [
    "research_id",
    "lab_date",
    "lab_date_status",
    "lab_name_raw",
    "lab_name_standardized",
    "analyte_group",
    "value_raw",
    "value_numeric",
    "unit_raw",
    "unit_standardized",
    "reference_range",
    "abnormal_flag",
    "is_censored",
    "source_table",
    "source_script",
    "ingestion_wave",
    "data_completeness_tier",
    "provenance_note",
]

ALLOWED_TIERS = {
    "current_structured",
    "current_nlp_partial",
    "future_institutional_required",
}

ALLOWED_DATE_STATUSES = {
    "exact_collection_date",
    "extracted_date",
    "unresolved_date",
    None,
}

PLAUSIBILITY_BOUNDS = {
    "thyroglobulin": (0, 100_000),
    "anti_thyroglobulin": (0, 10_000),
    "pth": (0.5, 500),
    "calcium_total": (4, 15),
    "calcium_ionized": (0.5, 2.0),
    "tsh": (0.01, 200),
    "free_t4": (0.1, 10),
    "free_t3": (0.5, 20),
    "vitamin_d": (1, 200),
    "albumin": (0.5, 7),
    "phosphorus": (0.5, 15),
    "magnesium": (0.3, 10),
    "calcitonin": (0, 50_000),
    "cea": (0, 5_000),
}

POPULATED_ANALYTES = {"thyroglobulin", "anti_thyroglobulin", "pth", "calcium_total", "calcium_ionized"}
FUTURE_ANALYTES = {"tsh", "free_t4", "free_t3", "vitamin_d", "albumin", "phosphorus", "magnesium", "calcitonin", "cea"}
