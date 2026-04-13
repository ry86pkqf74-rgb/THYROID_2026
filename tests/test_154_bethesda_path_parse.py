"""Unit tests for scripts/154_fna_cytology_bethesda_from_path_text.parse rules."""

from importlib import util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_PATH = ROOT / "scripts" / "154_fna_cytology_bethesda_from_path_text.py"
_spec = util.spec_from_file_location("s154", _PATH)
assert _spec and _spec.loader
_mod = util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
extract = _mod.extract_bethesda_from_path_text


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Positive for malignancy (Bethesda category VI).", 6),
        ("Bethesda Category III: AUS", 3),
        ("bethesda category 2", 2),
        ("something cytopathology category IV something", 4),
        ("no category here", None),
        ("", None),
    ],
)
def test_extract_bethesda_from_path_text(text: str, expected: int | None) -> None:
    assert extract(text) == expected
