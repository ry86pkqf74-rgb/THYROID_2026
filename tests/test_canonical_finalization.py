"""Regression tests for canonical lakehouse finalization deliverables.

Verifies that key documentation, export bundles, and narrative alignment
exist and contain required sections. These tests run without MotherDuck
access (offline-safe).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
EXPORTS = ROOT / "exports"


class TestManuscriptDataStartHere:
    """MANUSCRIPT_DATA_START_HERE.md must exist with required sections."""

    path = ROOT / "MANUSCRIPT_DATA_START_HERE.md"

    def test_exists(self):
        assert self.path.exists(), "MANUSCRIPT_DATA_START_HERE.md missing at repo root"

    def test_required_sections(self):
        text = self.path.read_text(encoding="utf-8")
        for heading in [
            "Live source of truth",
            "Analyst-facing tables and views",
            "Row count citation rule",
            "Reviewer status caveat",
            "Can we start manuscripts now",
            "Files to ignore",
        ]:
            assert heading.lower() in text.lower(), f"Missing section: {heading}"

    def test_links_to_contract(self):
        text = self.path.read_text(encoding="utf-8")
        assert "final_source_of_truth_contract.md" in text


class TestFinalSourceOfTruthContract:
    """docs/final_source_of_truth_contract.md must contain canonical scope and claims."""

    path = DOCS / "final_source_of_truth_contract.md"

    def test_exists(self):
        assert self.path.exists(), "Contract doc missing"

    def test_canonical_scope_inventory(self):
        text = self.path.read_text(encoding="utf-8")
        assert "canonical scope inventory" in text.lower()

    def test_allowed_disallowed_claims(self):
        text = self.path.read_text(encoding="utf-8")
        assert "allowed" in text.lower() and "disallowed" in text.lower()

    def test_validation_definitions(self):
        text = self.path.read_text(encoding="utf-8")
        assert "technically validated" in text.lower()
        assert "human-reviewed" in text.lower() or "manuscript-grade" in text.lower()

    def test_links_to_start_here(self):
        text = self.path.read_text(encoding="utf-8")
        assert "MANUSCRIPT_DATA_START_HERE.md" in text


class TestHistoricalLabeling:
    """Pre-2026-04-11 evidence packs should carry supersession banners."""

    candidates = [
        ROOT / "studies" / "20260407_tier_final_master_release" / "EVIDENCE_PACK.md",
        ROOT / "studies" / "20260409_final_master_release" / "EVIDENCE_PACK.md",
        ROOT / "studies" / "manuscript_blocker_rebaseline_20260408T144500Z" / "report.md",
    ]

    @pytest.mark.parametrize("path", candidates, ids=[str(p.relative_to(ROOT)) for p in candidates])
    def test_banner_present(self, path: Path):
        if not path.exists():
            pytest.skip(f"{path.name} not present")
        text = path.read_text(encoding="utf-8")
        assert "HISTORICAL / SUPERSEDED" in text, f"Missing supersession banner in {path}"


class TestManifestStaleness:
    """Checked-in LATEST_MANIFEST.json must declare itself historical."""

    path = EXPORTS / "release_manifests" / "LATEST_MANIFEST.json"

    def test_exists(self):
        if not self.path.exists():
            pytest.skip("No LATEST_MANIFEST.json")

    def test_role_historical(self):
        if not self.path.exists():
            pytest.skip("No LATEST_MANIFEST.json")
        m = json.loads(self.path.read_text(encoding="utf-8"))
        role = m.get("role", "")
        assert "historical" in role.lower(), f"Manifest role should be historical, got: {role}"


class TestCanonicalExportBundle:
    """Full canonical export bundle should exist with required files."""

    def _find_bundle(self) -> Path | None:
        for d in sorted(EXPORTS.glob("full_canonical_release_*"), reverse=True):
            if d.is_dir():
                return d
        return None

    def test_bundle_exists(self):
        bundle = self._find_bundle()
        assert bundle is not None, "No full_canonical_release_* directory in exports/"

    def test_required_files(self):
        bundle = self._find_bundle()
        if bundle is None:
            pytest.skip("No bundle")
        required = [
            "canonical_counts.json",
            "lineage_completeness.json",
            "duplicate_audit.csv",
            "release_manifest_latest.json",
            "schema_inventory.json",
            "validation_summary.json",
            "manifest.json",
            "checksums.json",
        ]
        for fn in required:
            assert (bundle / fn).exists(), f"Missing {fn} in {bundle.name}"

    def test_manifest_governance_status(self):
        bundle = self._find_bundle()
        if bundle is None:
            pytest.skip("No bundle")
        m = json.loads((bundle / "manifest.json").read_text())
        assert "governance_status" in m
        assert "blocker" in m["governance_status"].lower() or "pass" in m["governance_status"].lower()


class TestNarrativePointers:
    """Top-level docs should all point to MANUSCRIPT_DATA_START_HERE.md."""

    docs_with_pointer = [
        ROOT / "README.md",
        ROOT / "truth_sync_summary.md",
        ROOT / "docs" / "REPO_STATUS.md",
    ]

    @pytest.mark.parametrize("path", docs_with_pointer, ids=[str(p.relative_to(ROOT)) for p in docs_with_pointer])
    def test_pointer_present(self, path: Path):
        if not path.exists():
            pytest.skip(f"{path.name} not present")
        text = path.read_text(encoding="utf-8")
        assert "MANUSCRIPT_DATA_START_HERE" in text, f"Missing pointer in {path.name}"
