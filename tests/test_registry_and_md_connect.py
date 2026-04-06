"""Smoke tests for the extraction domain registry and MotherDuck connector."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Registry loading and structure
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegistryLoad:
    def test_load_succeeds(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        assert reg.schema_version.startswith("entity_schema_v3")

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


# ═══════════════════════════════════════════════════════════════════════════════
# New v3 registry fields
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegistryV3Fields:
    def test_note_scope_on_all_domains(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        valid = {"all", "op_note", "path_report"}
        for name, spec in reg.domains.items():
            assert spec.note_scope in valid, (
                f"Domain '{name}' has invalid note_scope '{spec.note_scope}'"
            )

    def test_canonical_target_populated(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        for name, spec in reg.domains.items():
            assert spec.canonical_target, (
                f"Domain '{name}' missing canonical_target"
            )

    def test_linkage_anchor_family_valid(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        valid = {
            "pathology", "molecular", "operative", "imaging", "rai",
            "followup", "demographics", "audit",
        }
        for name, spec in reg.domains.items():
            assert spec.linkage_anchor_family in valid, (
                f"Domain '{name}' has invalid linkage_anchor_family "
                f"'{spec.linkage_anchor_family}'"
            )

    def test_dedupe_key_is_nonempty_list(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        for name, spec in reg.domains.items():
            assert isinstance(spec.dedupe_key, list) and len(spec.dedupe_key) >= 2, (
                f"Domain '{name}' dedupe_key must be a list with ≥2 elements"
            )

    def test_qa_tier_valid(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        valid = {"critical", "standard", "informational", "debug"}
        for name, spec in reg.domains.items():
            assert spec.qa_tier in valid, (
                f"Domain '{name}' has invalid qa_tier '{spec.qa_tier}'"
            )

    def test_operative_scoped_domains(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        op_domains = {
            name for name, spec in reg.domains.items()
            if spec.is_operative_scoped
        }
        assert "operative_detail" in op_domains

    def test_path_report_scoped_domains(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        path_domains = {
            name for name, spec in reg.domains.items()
            if spec.is_path_report_scoped
        }
        assert "pathology" in path_domains
        assert "vascular_invasion" in path_domains

    def test_domains_for_note_scope(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        op_note_domains = reg.domains_for_note_scope("op_note")
        for spec in op_note_domains.values():
            assert spec.note_scope in ("op_note", "all")

    def test_domains_by_qa_tier(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        critical = reg.domains_by_qa_tier("critical")
        assert "staging" in critical
        assert "complications" in critical

    def test_domains_by_linkage_family(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        pathology_fam = reg.domains_by_linkage_family("pathology")
        assert "staging" in pathology_fam


# ═══════════════════════════════════════════════════════════════════════════════
# Registry validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestRegistryValidation:
    def test_validate_returns_clean(self):
        from llm_extraction.registry import load_registry, validate_registry

        reg = load_registry()
        issues = validate_registry(reg)
        fatal = [
            i for i in issues
            if "Duplicate" in i or "not found" in i or "invalid" in i.lower()
        ]
        assert not fatal, f"Validation failures: {fatal}"

    def test_all_prompt_files_exist(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        missing = []
        for name, spec in reg.domains.items():
            for p in spec.prompts:
                if not p.absolute_path.exists():
                    missing.append(f"{name}: {p.repo_path}")
        assert not missing, f"Missing prompt files: {missing}"

    def test_every_prompt_backed_domain_has_output_stem(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        for name, spec in reg.domains.items():
            if spec.prompts:
                assert spec.parquet_stem, (
                    f"Domain '{name}' has prompts but no parquet_stem"
                )

    def test_unknown_domain_fails_loudly(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        with pytest.raises(ValueError, match="Unknown extraction domain"):
            reg.resolve_domain("totally_fake_domain_xyz")

    def test_resolve_known_domain(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        spec = reg.resolve_domain("staging")
        assert spec.name == "staging"
        assert spec.parquet_stem == "note_entities_staging"

    def test_llm_domains_have_prompts(self):
        from llm_extraction.registry import load_registry

        reg = load_registry()
        for name, spec in reg.domains.items():
            if "llm" in spec.extractors:
                assert spec.prompts, (
                    f"Domain '{name}' has 'llm' extractor but no prompt files"
                )


# ═══════════════════════════════════════════════════════════════════════════════
# MotherDuck connector
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# run_extraction integration
# ═══════════════════════════════════════════════════════════════════════════════

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

    def test_v2_domains_in_map(self):
        from llm_extraction.run_extraction import DOMAIN_TO_FILE

        for d in ["imaging", "labs", "pathology", "recurrence",
                   "vascular_invasion", "airway_invasion"]:
            assert d in DOMAIN_TO_FILE, f"Missing v2 domain '{d}'"

    def test_all_stems_are_strings(self):
        from llm_extraction.run_extraction import DOMAIN_TO_FILE

        for domain, stem in DOMAIN_TO_FILE.items():
            assert isinstance(stem, str) and len(stem) > 0, (
                f"Invalid stem for domain '{domain}'"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Script integration (02b, 103)
# ═══════════════════════════════════════════════════════════════════════════════

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
        stems = {entry[0] for entry in mod.ENTITY_DOMAIN_MAP}
        assert "note_entities_staging" in stems
        assert len(mod.ENTITY_DOMAIN_MAP) >= 20


# ═══════════════════════════════════════════════════════════════════════════════
# LLM extractor domain-aware prompt
# ═══════════════════════════════════════════════════════════════════════════════

class TestLLMExtractorRegistryPrompt:
    def test_domain_prompt_loading(self):
        from llm_extraction.extract_llm import LLMExtractor

        ext = LLMExtractor()
        prompt = ext._load_system_prompt(domain="staging")
        assert isinstance(prompt, str) and len(prompt) > 0

    def test_domain_prompt_for_v2_domain(self):
        from llm_extraction.extract_llm import LLMExtractor

        ext = LLMExtractor()
        prompt = ext._load_system_prompt(domain="imaging")
        assert isinstance(prompt, str) and len(prompt) > 0

    def test_fallback_when_domain_unknown(self):
        from llm_extraction.extract_llm import LLMExtractor

        ext = LLMExtractor()
        prompt = ext._load_system_prompt(domain="nonexistent_domain_xyz")
        assert isinstance(prompt, str) and len(prompt) > 0
