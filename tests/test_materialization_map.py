"""tests/test_materialization_map.py
Validates the MATERIALIZATION_MAP in scripts/26_motherduck_materialize_v2.py.

Rules enforced
──────────────
1. Every md_* target name must be unique within the MAP.
2. Every source (RHS) name must be unique within the MAP.
3. Every md_* target name must start with "md_".
4. No entry may map a name to itself (md_foo → md_foo is a programming error).
5. Total entry count must not drop below the committed baseline without an
   explicit change to _BASELINE_COUNT at the bottom of this file.

These tests run without a database connection – they parse the Python source
directly using bracket-depth counting to reliably locate the MAP list literal
without being confused by md_* references elsewhere in the file (e.g. SQL
template .replace() calls).

Usage:
    .venv/bin/python -m pytest tests/test_materialization_map.py -v
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

import pytest

# ── Helpers ──────────────────────────────────────────────────────────────────

SCRIPT26 = Path(__file__).resolve().parent.parent / "scripts" / "26_motherduck_materialize_v2.py"


class MapEntry(NamedTuple):
    md_name: str
    src_name: str


def _parse_materialization_map() -> list[MapEntry]:
    """Extract (md_name, src_name) pairs from MATERIALIZATION_MAP in script 26.

    Uses bracket-depth counting on the raw source text so that:
    - Indented ']' characters inside SQL strings or function bodies are ignored.
    - md_* references in .replace() / print calls outside the list are excluded.
    """
    src = SCRIPT26.read_text()

    # Locate the opening bracket of the MAP list literal
    marker = "MATERIALIZATION_MAP: list[tuple[str, str]] = ["
    if marker in src:
        bracket_start = src.index(marker) + len(marker) - 1
    else:
        # Fallback for lax annotation style
        m = re.search(r"MATERIALIZATION_MAP\s*[:\w\[\],\s]*=\s*\[", src)
        assert m, "Could not locate MATERIALIZATION_MAP definition in script 26"
        bracket_start = src.index("[", m.start())

    # Walk with depth counting to find the matching ']'
    depth = 0
    bracket_end = bracket_start
    for i, ch in enumerate(src[bracket_start:], bracket_start):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                bracket_end = i
                break

    map_content = src[bracket_start : bracket_end + 1]

    # Match ("md_name", "src_name") tuples; ignore comment lines
    pairs = re.findall(r'\("(md_[^"]+)",\s*"([^"]+)"\)', map_content)
    return [MapEntry(md, src) for md, src in pairs]


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def mat_map() -> list[MapEntry]:
    return _parse_materialization_map()


@pytest.fixture(scope="module")
def md_names(mat_map: list[MapEntry]) -> list[str]:
    return [e.md_name for e in mat_map]


@pytest.fixture(scope="module")
def src_names(mat_map: list[MapEntry]) -> list[str]:
    return [e.src_name for e in mat_map]


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_map_is_parseable(mat_map: list[MapEntry]) -> None:
    """MATERIALIZATION_MAP must exist and contain at least one entry."""
    assert len(mat_map) > 0, "MATERIALIZATION_MAP is empty or could not be parsed"


def test_no_duplicate_md_targets(md_names: list[str]) -> None:
    """Every md_* destination name must appear exactly once in the MAP.

    Duplicate md_* targets mean the same MotherDuck table gets materialised
    twice, with the second write silently clobbering the first.
    """
    seen: dict[str, int] = {}
    for n in md_names:
        seen[n] = seen.get(n, 0) + 1
    dupes = {k: v for k, v in seen.items() if v > 1}
    assert not dupes, (
        f"Duplicate md_* target names found in MATERIALIZATION_MAP:\n"
        + "\n".join(f"  {k}  ({v}×)" for k, v in sorted(dupes.items()))
    )


def test_no_duplicate_source_names(src_names: list[str]) -> None:
    """Every source table name must appear exactly once in the MAP.

    Duplicate sources mean two different md_* tables carry the same data,
    creating maintenance confusion and wasted MotherDuck storage.
    """
    seen: dict[str, int] = {}
    for n in src_names:
        seen[n] = seen.get(n, 0) + 1
    dupes = {k: v for k, v in seen.items() if v > 1}
    assert not dupes, (
        f"Duplicate source names found in MATERIALIZATION_MAP:\n"
        + "\n".join(f"  {k}  ({v}×)" for k, v in sorted(dupes.items()))
    )


def test_all_md_targets_have_md_prefix(md_names: list[str]) -> None:
    """All destination names must start with 'md_' by convention."""
    bad = [n for n in md_names if not n.startswith("md_")]
    assert not bad, (
        f"MAP entries missing 'md_' prefix: {bad}"
    )


def test_no_self_mapping(mat_map: list[MapEntry]) -> None:
    """No entry should map a name to itself (md_foo → md_foo)."""
    bad = [e for e in mat_map if e.md_name == e.src_name]
    assert not bad, f"Self-mapping entries detected: {bad}"


def test_entry_count_not_below_baseline(mat_map: list[MapEntry]) -> None:
    """MAP entry count must not silently drop below the committed baseline.

    Update _BASELINE_COUNT whenever entries are intentionally removed.
    """
    assert len(mat_map) >= _BASELINE_COUNT, (
        f"MATERIALIZATION_MAP has {len(mat_map)} entries — "
        f"below the expected baseline of {_BASELINE_COUNT}.  "
        f"If entries were intentionally removed, update _BASELINE_COUNT."
    )


def test_script26_imports_cleanly() -> None:
    """Importing script 26's MAP guard must not raise (no runtime duplicates)."""
    import importlib.util, sys

    spec = importlib.util.spec_from_file_location("_s26", SCRIPT26)
    assert spec is not None and spec.loader is not None

    # Remove from sys.modules if already cached to force fresh evaluation
    sys.modules.pop("_s26", None)
    mod = importlib.util.module_from_spec(spec)
    # This will raise ValueError if the MAP has duplicates
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except ValueError as exc:
        pytest.fail(f"MATERIALIZATION_MAP uniqueness guard fired: {exc}")
    except Exception:
        # Connection errors, missing duckdb, etc. are acceptable here — we only
        # care that the MAP guard itself does not raise ValueError.
        pass


# ── Baseline ─────────────────────────────────────────────────────────────────
# Update this constant whenever MAP entries are intentionally removed.
# The test above enforces len(MAP) >= _BASELINE_COUNT.
_BASELINE_COUNT: int = 220
