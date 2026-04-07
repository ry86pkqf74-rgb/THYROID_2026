"""Stable paths for MotherDuck-related repo artifacts (no secrets)."""

from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Aggregate audit JSON from scripts/129_imaging_fna_linkage_mm_v1.py --md
IMAGING_FNA_MM_V1_AUDIT_JSON = _REPO_ROOT / "motherduck" / "exports" / "imaging_fna_linkage_mm_v1_audit.json"
