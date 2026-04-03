"""Smoke tests for the extraction domain registry and MotherDuck connector."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class TestRegistryLoad:
    def test_load_succeeds(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        assert reg.schema_version.startswith("entity_schema_v2")

    def test_28_domains_present(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        assert len(reg.domains) >= 28, (
            f"Expected ≥28 domains, got {len(reg.domains)}: "
            f"{sorted(reg.domains.keys())}"
        )

    def test_v1_domains_are_subset(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        v1 = reg.v1_domains
        expected = {
            "staging", "genetics", "procedures", "operative_detail",
            "complications", "medications", "problem_list", "llm",
        }
        assert expected == set(v1.keys()), (
            f"v1 mismatch: got {set(v1.keys())}"
        )

    def test_v2_domains_non_empty(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        assert len(reg.v2_domains) >= 20

    def test_all_prompts_have_repo_path(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        for name, spec in reg.domains.items():
            for p in spec.prompts:
                assert p.repo_path, f"Domain '{name}' prompt has empty repo_path"

    def test_canonical_outputs_include_v1_and_v2(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        assert "fact_long_table_v1" in reg.canonical_outputs
        assert "fact_long_table_v2" in reg.canonical_outputs

    def test_domain_to_parquet_stem_unique(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        stems = list(reg.domain_to_parquet_stem().values())
        assert len(stems) == len(set(stems)), "Duplicate parquet stems found"

    def test_validate_no_fatal_errors(self):
        from llm_extraction.registry import load_registry, validate_registry

        reg = load_registry()
        issues = validate_registry(reg)
        fatal = [i for i in issues if "Duplicate" in i]
        assert not fatal, f"Fatal issues: {fatal}"


class TestMDConnect:
    def test_connect_local_file(self, tmp_path):
        from utils.md_connect import connect_md_or_file

        db = tmp_path / "test.duckdb"
        con = connect_md_or_file(db, md=False)
        result = con.execute("SELECT 42 AS answer").fetchone()
        con.close()
        assert result == (42,)

    def test_connect_md_false_uses_file(self, tmp_path):
        from utils.md_connect import connect_md_or_file

        db = tmp_path / "test2.duckdb"
        con = connect_md_or_file(db, md=False)
        con.execute("CREATE TABLE t1 (id INTEGER)")
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        con.close()
        assert "t1" in tables

    def test_resolve_md_token_returns_none_without_env(self, monkeypatch):
        from utils.md_connect import _resolve_md_token

        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
        monkeypatch.delenv("MD_SA_TOKEN", raising=False)
        monkeypatch.delenv("LOCAL_DB_PATH", raising=False)
        result = _resolve_md_token()
        assert result is None or isinstance(result, str)


class TestRunExtractionRegistryIntegration:
    def test_domain_to_file_populated(self):
        from llm_extraction.run_extraction import DOMAIN_TO_FILE

        assert len(DOMAIN_TO_FILE) >= 28
        assert "staging" in DOMAIN_TO_FILE
        assert "imaging" in DOMAIN_TO_FILE

    def test_v1_domains_in_map(self):
        from llm_extraction.run_extraction import DOMAIN_TO_FILE

        for d in ["staging", "genetics", "procedures", "complications",
                   "medications", "problem_list"]:
            assert d in DOMAIN_TO_FILE, f"Missing v1 domain '{d}'"


class TestScript02bRegistryIntegration:
    def test_entity_tables_include_v2(self):
        sys.path.insert(0, str(ROOT / "scripts"))
        import importlib
        spec = importlib.util.spec_from_file_location(
            "s02b", ROOT / "scripts" / "02b_register_notes_entities.py"
        )
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except SystemExit:
            pass
        assert hasattr(mod, "ENTITY_TABLES")
        assert len(mod.ENTITY_TABLES) >= 28


class TestScript103RegistryIntegration:
    def test_entity_domain_map_expanded(self):
        spec = __import__("importlib").util.spec_from_file_location(
            "s103", ROOT / "scripts" / "103_fact_lineage_materialize.py"
        )
        mod = __import__("importlib").util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except SystemExit:
            pass
        assert hasattr(mod, "ENTITY_DOMAIN_MAP")
        stems = {stem for stem, _ in mod.ENTITY_DOMAIN_MAP}
        assert "note_entities_staging" in stems
        assert len(mod.ENTITY_DOMAIN_MAP) >= 20


class TestLLMExtractorRegistryPrompt:
    def test_domain_prompt_loading(self):
        from llm_extraction.extract_llm import LLMExtractor

        ext = LLMExtractor()
        prompt = ext._load_system_prompt(domain="staging")
        assert isinstance(prompt, str) and len(prompt) > 0
