"""Regression tests: v2 LLM domain fan-out (no collapse into generic `llm`) and validator discovery."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load_validation_module():
    spec = importlib.util.spec_from_file_location(
        "llm_val_111",
        ROOT / "scripts" / "111_llm_extraction_validation.py",
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_v2_llm_only_domains_have_unique_parquet_stems() -> None:
    """Each v2 domain that is LLM-only must map to its own parquet stem (no accidental sharing)."""
    from llm_extraction.registry import load_registry

    reg = load_registry()
    llm_only_v2: list[tuple[str, str]] = []
    for name, spec in reg.v2_domains.items():
        if "llm" not in spec.extractors:
            continue
        if "regex" in spec.extractors:
            continue
        llm_only_v2.append((name, spec.parquet_stem))

    stems = [s for _, s in llm_only_v2]
    assert len(stems) == len(set(stems)), (
        "Duplicate parquet_stem among v2 LLM-only domains — outputs would overwrite: "
        f"{[(n, st) for n, st in llm_only_v2 if stems.count(st) > 1]}"
    )
    for name, stem in llm_only_v2:
        assert stem != "note_entities_llm", (
            f"Domain {name!r} must not use legacy merged stem note_entities_llm; got {stem!r}"
        )
        assert stem.startswith("note_entities_llm_") and len(stem) > len("note_entities_llm_"), (
            f"Expected per-domain stem note_entities_llm_<domain> for {name!r}; got {stem!r}"
        )


def test_run_llm_for_domain_passes_domain_and_sets_entity_domain() -> None:
    """run_llm_for_domain must call LLMExtractor.extract(..., domain=...) and stamp entity_domain."""
    from llm_extraction.registry import load_registry
    from llm_extraction.run_extraction import run_llm_for_domain

    reg = load_registry()
    spec = reg.resolve_domain("imaging")
    domain_name = "imaging"

    call_kw: dict = {}

    class _CapturingLLM:
        available = True
        _provider = "test"

        def extract(self, *args, **kwargs):
            call_kw.update(kwargs)
            # Minimal EntityMatch-like path: return empty to avoid BaseExtractor validation noise
            return []

    notes_df = pd.DataFrame(
        [
            {
                "note_row_id": "n1",
                "research_id": 1001,
                "note_type": "h_p",
                "note_text": "TI-RADS 4 nodule discussed.",
                "note_date": "2020-01-01",
            }
        ]
    )

    run_llm_for_domain(
        notes_df,
        _CapturingLLM(),  # type: ignore[arg-type]
        domain_name,
        spec,
        extraction_run_id="test-run",
        max_workers=1,
    )
    assert call_kw.get("domain") == domain_name


def test_run_llm_for_domain_stamps_entity_domain_on_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    from llm_extraction.base import EntityMatch
    from llm_extraction.registry import load_registry
    from llm_extraction.run_extraction import run_llm_for_domain

    reg = load_registry()
    spec = reg.resolve_domain("labs")

    match = EntityMatch(
        research_id=2002,
        note_row_id="row-labs",
        note_type="h_p",
        entity_type="lab",
        entity_value_raw="TSH 2.1",
        entity_value_norm="tsh 2.1",
        present_or_negated="present",
        confidence=0.9,
        evidence_span="TSH 2.1",
        evidence_start=0,
        evidence_end=7,
        entity_date=None,
        note_date=None,
        extraction_method="llm_github_models",
        extracted_at="2026-01-01T00:00:00+00:00",
        date_confidence=0.0,
        source_line=None,
        chunk_index=0,
        chunk_char_start=0,
        chunk_char_end=100,
        evidence_global_start=0,
        evidence_global_end=7,
        raw_response_sha256="0" * 64,
        verification_status="verified_substring",
        verification_step="substring_ok",
        extractor_name="LLMExtractor",
        model_name="test",
        model_version="test",
        prompt_version="test",
        verifier_name=None,
        verifier_version=None,
    )

    class _StubLLM:
        available = True
        _provider = "test"

        def extract(self, *args, **kwargs):
            return [match]

    notes_df = pd.DataFrame(
        [
            {
                "note_row_id": "row-labs",
                "research_id": 2002,
                "note_type": "h_p",
                "note_text": "TSH 2.1 mIU/L",
                "note_date": None,
            }
        ]
    )

    rows = run_llm_for_domain(
        notes_df,
        _StubLLM(),  # type: ignore[arg-type]
        "labs",
        spec,
        extraction_run_id="stamp-test",
        max_workers=1,
    )
    assert len(rows) == 1
    assert rows[0]["entity_domain"] == "labs"


def test_111_resolve_input_path_for_domain_matches_registry() -> None:
    mod = _load_validation_module()
    path = mod._resolve_input_path_for_domain("imaging")
    assert path.name == "note_entities_llm_imaging.parquet"
    assert path.parent.name == "processed"


def test_111_argparse_rejects_domain_with_input() -> None:
    mod = _load_validation_module()
    old = sys.argv
    try:
        sys.argv = [
            "111_llm_extraction_validation.py",
            "--domain",
            "imaging",
            "--input",
            "processed/foo.parquet",
        ]
        with pytest.raises(SystemExit):
            mod.parse_args()
    finally:
        sys.argv = old


def test_111_argparse_accepts_all_llm_domains_flag() -> None:
    mod = _load_validation_module()
    old = sys.argv
    try:
        sys.argv = ["111_llm_extraction_validation.py", "--all-llm-domains", "--run-label", "pytest_cli"]
        args = mod.parse_args()
        assert args.all_llm_domains is True
        assert args.run_label == "pytest_cli"
    finally:
        sys.argv = old


def test_domain_to_file_includes_per_domain_llm_stems_not_only_legacy_llm() -> None:
    """Registry map must expose per-domain v2 stems; legacy `llm` bucket remains for v1 audit path."""
    from llm_extraction.run_extraction import DOMAIN_TO_FILE

    assert DOMAIN_TO_FILE.get("imaging") == "note_entities_llm_imaging"
    assert DOMAIN_TO_FILE.get("labs") == "note_entities_llm_labs"
    assert DOMAIN_TO_FILE.get("llm") == "note_entities_llm"
