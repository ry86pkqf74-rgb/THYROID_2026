"""Fleet / registry / sub-prompt parity tests.

Ensures the VastAI fleet DOMAIN_PROMPT maps, the run_extraction_split.py
DOMAIN_PROMPT, and the promotion-gate SUB_PROMPT_STEM_MAP all derive from
(or match) the single source of truth in config/extraction_domain_registry.yaml.

Usage:
    .venv/bin/python -m pytest tests/test_fleet_registry_parity.py -v
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PROMPTS_DIR = ROOT / "llm_extraction" / "prompts"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _extract_domain_prompt_dict(script_path: Path) -> dict[str, str]:
    """Parse DOMAIN_PROMPT = {...} from a Python script without executing it."""
    src = script_path.read_text(encoding="utf-8")
    match = re.search(r"DOMAIN_PROMPT\s*=\s*\{", src)
    assert match, f"DOMAIN_PROMPT not found in {script_path.name}"
    start = match.start()
    depth = 0
    brace_start = src.index("{", start)
    for i, ch in enumerate(src[brace_start:], brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                block = src[brace_start : i + 1]
                break
    pairs = re.findall(r'"([^"]+)"\s*:\s*"([^"]+)"', block)
    return dict(pairs)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def registry():
    from llm_extraction.registry import load_registry
    load_registry.cache_clear()
    return load_registry()


@pytest.fixture(scope="module")
def expected_fleet_map(registry):
    return registry.expected_fleet_prompt_map()


@pytest.fixture(scope="module")
def vastai_domain_prompt():
    return _extract_domain_prompt_dict(
        ROOT / "scripts" / "vastai" / "run_extraction_concurrent.py"
    )


@pytest.fixture(scope="module")
def split_domain_prompt():
    return _extract_domain_prompt_dict(
        ROOT / "scripts" / "run_extraction_split.py"
    )


# ── Fleet DOMAIN_PROMPT parity ───────────────────────────────────────────────

class TestFleetRegistryParity:

    def test_fleet_keys_are_subset_of_expected(
        self, vastai_domain_prompt, expected_fleet_map
    ):
        extra = set(vastai_domain_prompt) - set(expected_fleet_map)
        assert not extra, (
            f"VastAI DOMAIN_PROMPT has keys not in registry: {sorted(extra)}"
        )

    def test_no_missing_fleet_keys(
        self, vastai_domain_prompt, expected_fleet_map
    ):
        missing = set(expected_fleet_map) - set(vastai_domain_prompt)
        assert not missing, (
            f"VastAI DOMAIN_PROMPT is missing registry keys: {sorted(missing)}"
        )

    def test_fleet_prompt_filenames_match(
        self, vastai_domain_prompt, expected_fleet_map
    ):
        mismatches = []
        for key in sorted(set(vastai_domain_prompt) & set(expected_fleet_map)):
            if vastai_domain_prompt[key] != expected_fleet_map[key]:
                mismatches.append(
                    f"{key}: fleet={vastai_domain_prompt[key]} "
                    f"registry={expected_fleet_map[key]}"
                )
        assert not mismatches, (
            f"Fleet prompt file mismatches:\n"
            + "\n".join(f"  {m}" for m in mismatches)
        )

    def test_fleet_prompt_files_exist(self, vastai_domain_prompt):
        missing = []
        for domain, fname in vastai_domain_prompt.items():
            if not (PROMPTS_DIR / fname).exists():
                missing.append(f"{domain}: {fname}")
        assert not missing, (
            f"Fleet prompt files not on disk:\n"
            + "\n".join(f"  {m}" for m in missing)
        )


# ── run_extraction_split.py parity ───────────────────────────────────────────

class TestSplitScriptParity:

    def test_split_keys_are_subset_of_expected(
        self, split_domain_prompt, expected_fleet_map
    ):
        extra = set(split_domain_prompt) - set(expected_fleet_map)
        assert not extra, (
            f"run_extraction_split DOMAIN_PROMPT has keys not in registry: "
            f"{sorted(extra)}"
        )

    def test_split_prompt_filenames_match(
        self, split_domain_prompt, expected_fleet_map
    ):
        mismatches = []
        for key in sorted(set(split_domain_prompt) & set(expected_fleet_map)):
            if split_domain_prompt[key] != expected_fleet_map[key]:
                mismatches.append(
                    f"{key}: split={split_domain_prompt[key]} "
                    f"registry={expected_fleet_map[key]}"
                )
        assert not mismatches, (
            f"Split prompt file mismatches:\n"
            + "\n".join(f"  {m}" for m in mismatches)
        )


# ── Sub-prompt map parity ────────────────────────────────────────────────────

class TestSubPromptMapParity:

    def test_sub_prompt_parent_domains_exist(self, registry):
        for sp_name, sp in registry.sub_prompt_domains.items():
            assert sp.parent_domain in registry.domains, (
                f"Sub-prompt '{sp_name}' parent_domain '{sp.parent_domain}' "
                f"not found in registry domains"
            )

    def test_sub_prompt_stems_unique(self, registry):
        stems = [sp.parquet_stem for sp in registry.sub_prompt_domains.values()]
        assert len(stems) == len(set(stems)), (
            f"Duplicate sub-prompt parquet stems"
        )

    def test_sub_prompt_stems_dont_collide_with_domains(self, registry):
        domain_stems = set(registry.all_parquet_stems())
        for sp_name, sp in registry.sub_prompt_domains.items():
            assert sp.parquet_stem not in domain_stems, (
                f"Sub-prompt '{sp_name}' stem '{sp.parquet_stem}' "
                f"collides with a domain parquet_stem"
            )

    def test_sub_prompt_prompt_files_exist(self, registry):
        missing = []
        for sp_name, sp in registry.sub_prompt_domains.items():
            if not sp.prompt_absolute_path.exists():
                missing.append(f"{sp_name}: {sp.prompt_file}")
        assert not missing, (
            f"Sub-prompt prompt files not on disk:\n"
            + "\n".join(f"  {m}" for m in missing)
        )

    def test_gate_sub_prompt_map_matches_registry(self, registry):
        """The promotion gate must use the registry-derived sub-prompt map."""
        spec = importlib.util.spec_from_file_location(
            "s112", ROOT / "scripts" / "112_v2_domain_promotion_gate.py"
        )
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except (SystemExit, Exception):
            pass
        gate_map = getattr(mod, "SUB_PROMPT_STEM_MAP", {})
        registry_map = registry.sub_prompt_parent_map()
        assert gate_map == registry_map, (
            f"Gate SUB_PROMPT_STEM_MAP drifts from registry:\n"
            f"  gate={gate_map}\n"
            f"  registry={registry_map}"
        )


# ── Prompt file coverage ────────────────────────────────────────────────────

class TestPromptFileCoverage:

    def test_no_uncovered_prompt_files(self, registry):
        """Every prompt file in llm_extraction/prompts/ should be referenced
        by either a registry domain or a sub-prompt entry."""
        all_prompt_files = {
            p.name for p in PROMPTS_DIR.glob("*_extraction_v1.txt")
        }
        referenced = set()
        for spec in registry.domains.values():
            for p in spec.prompts:
                referenced.add(Path(p.repo_path).name)
        for sp in registry.sub_prompt_domains.values():
            referenced.add(Path(sp.prompt_file).name)
        uncovered = all_prompt_files - referenced
        assert not uncovered, (
            f"Prompt files not referenced by registry or sub-prompts: "
            f"{sorted(uncovered)}"
        )
