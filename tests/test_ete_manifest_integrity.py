"""Integrity checks for the ETE manuscript numeric manifest.

Guards the claim-to-source-artifact contract. If any frozen source
artifact is modified without a coordinated manifest update, these tests
fail, forcing the manifest to be regenerated or the change reverted.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
MANIFEST = REPO / "artifacts" / "ete_manuscript_numeric_manifest.json"


@pytest.fixture(scope="module")
def manifest() -> dict:
    assert MANIFEST.exists(), f"missing manifest: {MANIFEST}"
    return json.loads(MANIFEST.read_text())


def _sha256(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_manifest_schema_is_recognized(manifest: dict) -> None:
    assert manifest["schema_version"] == "1.0"
    assert "frozen_source_artifacts" in manifest
    assert "numeric_claims" in manifest
    assert "policy" in manifest


def test_manifest_has_claims(manifest: dict) -> None:
    claims = manifest["numeric_claims"]
    assert isinstance(claims, list)
    assert len(claims) >= 20, "expected at least 20 pinned numeric claims"
    for c in claims:
        assert "claim_id" in c and "value" in c and "source_ref" in c, c


def test_frozen_source_shas_match_manifest(manifest: dict) -> None:
    mismatches = []
    for key, entry in manifest["frozen_source_artifacts"].items():
        p = REPO / entry["path"]
        if not p.exists():
            mismatches.append(f"{key}: path missing on disk ({entry['path']})")
            continue
        actual = _sha256(p)
        if actual != entry["sha256"]:
            mismatches.append(
                f"{key}: sha mismatch, manifest={entry['sha256']} disk={actual}"
            )
    assert not mismatches, (
        "Frozen source artifacts diverged from manifest. Regenerate the "
        "manuscript numeric manifest after inspecting changes:\n  "
        + "\n  ".join(mismatches)
    )


def test_psm_anchor_policy_is_encoded(manifest: dict) -> None:
    pol = manifest["policy"]
    assert pol["psm_anchor"].startswith("711"), pol
    assert pol["live_reanalysis_claim_allowed"] is False
    assert pol["export_source"].lower().startswith("branch a")
    assert pol["ajcc7_mapping"].strip().startswith("T3b -> T3")


def test_anchor_claims_are_marked(manifest: dict) -> None:
    anchors = [c for c in manifest["numeric_claims"] if c.get("policy") == "anchor"]
    ids = {c["claim_id"] for c in anchors}
    assert "psm.pairs_frozen" in ids
    assert "psm.or_frozen" in ids
    assert "psm.fisher_p_frozen" in ids
