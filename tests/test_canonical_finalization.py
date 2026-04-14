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
            "schema_inventory.json",
            "validation_summary.json",
        ]
        for fn in required:
            assert (bundle / fn).exists(), f"Missing {fn} in {bundle.name}"

    def test_canonical_counts_populated(self):
        bundle = self._find_bundle()
        if bundle is None:
            pytest.skip("No bundle")
        counts = json.loads((bundle / "canonical_counts.json").read_text())
        assert counts.get("canonical_extracted_fact_long_v2", 0) > 0
        assert counts.get("master_fact_long_verified_v1", 0) > 0


class TestReviewGrainDisclosure:
    """Contract and start-here must disclose review grain."""

    def test_contract_discloses_grain(self):
        text = (DOCS / "final_source_of_truth_contract.md").read_text(encoding="utf-8")
        assert "research_id_domain" in text or "research_id, domain" in text

    def test_start_here_discloses_grain(self):
        text = (ROOT / "MANUSCRIPT_DATA_START_HERE.md").read_text(encoding="utf-8")
        assert "research_id, domain" in text or "research_id_domain" in text or "per-fact" in text.lower()


class TestFinalizationStudy:
    """Canonical finalization study folder must contain key artifacts."""

    def _find_study(self) -> Path | None:
        for d in sorted(ROOT.joinpath("studies").glob("canonical_finalization_*"), reverse=True):
            if d.is_dir():
                return d
        return None

    def test_study_exists(self):
        assert self._find_study() is not None

    def test_final_handoff(self):
        s = self._find_study()
        if s is None:
            pytest.skip("No study")
        handoff = s / "FINAL_HANDOFF.md"
        assert handoff.exists()
        text = handoff.read_text(encoding="utf-8")
        assert "single ssot" in text.lower() or "not yet a single ssot" in text.lower()

    def test_final_state_json(self):
        s = self._find_study()
        if s is None:
            pytest.skip("No study")
        fstate = s / "final_state.json"
        assert fstate.exists()
        data = json.loads(fstate.read_text())
        assert "final_status" in data
        assert "parity" in data

    def test_before_state(self):
        s = self._find_study()
        if s is None:
            pytest.skip("No study")
        assert (s / "before_state").is_dir()
        assert (s / "artifacts").is_dir()


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
