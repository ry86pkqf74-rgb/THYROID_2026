"""Load and validate the extraction domain registry (YAML → typed objects).

Usage::

    from llm_extraction.registry import load_registry, DomainSpec

    reg = load_registry()
    for name, spec in reg.domains.items():
        print(name, spec.parquet_stem, spec.tier)
"""
from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_YAML = _REPO_ROOT / "config" / "extraction_domain_registry.yaml"


@dataclass(frozen=True)
class PromptSpec:
    repo_path: str
    scope: str

    @property
    def absolute_path(self) -> Path:
        return _REPO_ROOT / "llm_extraction" / self.repo_path


@dataclass(frozen=True)
class DomainSpec:
    name: str
    parquet_stem: str
    tier: str
    extractors: list[str]
    prompts: list[PromptSpec]
    canonical_output: bool

    @property
    def parquet_filename(self) -> str:
        return f"{self.parquet_stem}.parquet"

    @property
    def duckdb_table(self) -> str:
        return self.parquet_stem

    @property
    def is_v1(self) -> bool:
        return self.tier.startswith("v1")

    @property
    def is_v2(self) -> bool:
        return self.tier == "v2"


@dataclass(frozen=True)
class CanonicalOutputSpec:
    key: str
    duckdb_table: str
    parquet_path: str
    materialize_script: str
    domains: str = "all"


@dataclass(frozen=True)
class Registry:
    schema_version: str
    domains: dict[str, DomainSpec]
    canonical_outputs: dict[str, CanonicalOutputSpec]
    llm_extraction_meta: dict[str, str]

    @property
    def v1_domains(self) -> dict[str, DomainSpec]:
        return {k: v for k, v in self.domains.items() if v.is_v1}

    @property
    def v2_domains(self) -> dict[str, DomainSpec]:
        return {k: v for k, v in self.domains.items() if v.is_v2}

    @property
    def canonical_domains(self) -> dict[str, DomainSpec]:
        return {k: v for k, v in self.domains.items() if v.canonical_output}

    def domain_to_parquet_stem(self) -> dict[str, str]:
        return {k: v.parquet_stem for k, v in self.domains.items()}

    def parquet_stem_to_domain(self) -> dict[str, str]:
        return {v.parquet_stem: k for k, v in self.domains.items()}

    def all_parquet_stems(self) -> list[str]:
        return [v.parquet_stem for v in self.domains.values()]

    def prompt_for_domain(self, domain: str) -> PromptSpec | None:
        spec = self.domains.get(domain)
        if spec and spec.prompts:
            return spec.prompts[0]
        return None


def _parse_domain(name: str, raw: dict[str, Any]) -> DomainSpec:
    prompts = [
        PromptSpec(repo_path=p["repo_path"], scope=p["scope"])
        for p in raw.get("prompts", [])
    ]
    return DomainSpec(
        name=name,
        parquet_stem=raw["parquet_stem"],
        tier=raw.get("tier", "v2"),
        extractors=raw.get("extractors", ["llm"]),
        prompts=prompts,
        canonical_output=raw.get("canonical_output", True),
    )


def _parse_canonical(key: str, raw: dict[str, Any]) -> CanonicalOutputSpec:
    return CanonicalOutputSpec(
        key=key,
        duckdb_table=raw["duckdb_table"],
        parquet_path=raw["parquet_path"],
        materialize_script=raw["materialize_script"],
        domains=raw.get("domains", "all"),
    )


@functools.lru_cache(maxsize=1)
def load_registry(yaml_path: Path | None = None) -> Registry:
    """Load and cache the extraction domain registry."""
    path = yaml_path or _DEFAULT_YAML
    with open(path) as f:
        raw = yaml.safe_load(f)

    domains = {
        name: _parse_domain(name, spec) for name, spec in raw["domains"].items()
    }

    canonical = {
        key: _parse_canonical(key, spec)
        for key, spec in raw.get("canonical_outputs", {}).items()
    }

    return Registry(
        schema_version=raw.get("schema_version", "unknown"),
        domains=domains,
        canonical_outputs=canonical,
        llm_extraction_meta=raw.get("llm_extraction", {}),
    )


def validate_registry(reg: Registry | None = None) -> list[str]:
    """Return a list of warnings/errors for the loaded registry."""
    if reg is None:
        reg = load_registry()
    issues: list[str] = []

    stems_seen: set[str] = set()
    for name, spec in reg.domains.items():
        if spec.parquet_stem in stems_seen:
            issues.append(f"Duplicate parquet_stem '{spec.parquet_stem}' in domain '{name}'")
        stems_seen.add(spec.parquet_stem)

        for p in spec.prompts:
            if not p.absolute_path.exists():
                issues.append(
                    f"Domain '{name}': prompt '{p.repo_path}' not found at {p.absolute_path}"
                )
    return issues
