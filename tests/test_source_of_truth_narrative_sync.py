"""Regression: top-level SSOT narrative must not drift across key docs."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PHRASES_FILE = ROOT / "config" / "source_of_truth_required_phrases.txt"
DOCS = [
    ROOT / "README.md",
    ROOT / "docs" / "REPO_STATUS.md",
    ROOT / "truth_sync_summary.md",
    ROOT / "docs" / "final_source_of_truth_contract.md",
]


def _required_phrases() -> list[str]:
    text = PHRASES_FILE.read_text(encoding="utf-8")
    out: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


@pytest.mark.parametrize("path", DOCS, ids=[p.name for p in DOCS])
def test_required_phrases_present(path: Path) -> None:
    assert path.is_file(), f"missing {path}"
    blob = path.read_text(encoding="utf-8")
    missing = [p for p in _required_phrases() if p not in blob]
    assert not missing, f"{path.relative_to(ROOT)} missing phrases: {missing}"


def test_truth_sync_headline_matches_readme_contract_pointer() -> None:
    """truth_sync_summary must reference the same contract as README."""
    ts = (ROOT / "truth_sync_summary.md").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "final_source_of_truth_contract.md" in ts
    assert "final_source_of_truth_contract.md" in readme
