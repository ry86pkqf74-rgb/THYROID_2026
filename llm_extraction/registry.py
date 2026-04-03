"""Load and validate the extraction domain registry (YAML → typed objects).

Usage::

    from llm_extraction.registry import load_registry, DomainSpec

    reg = load_registry()
    for name, spec in reg.domains.items():
        print(name, spec.parquet_stem, spec.tier)
"""
from __future__ import annotations

import functools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_YAML = _REPO_ROOT / "config" / "extraction_domain_registry.yaml"

_VALID_NOTE_SCOPES = frozenset({"all", "op_note", "path_report"})
_VALID_QA_TIERS = frozenset({"critical", "standard", "informational", "debug"})
_VALID_LINKAGE_FAMILIES = frozenset({
    "pathology", "molecular", "operative", "imaging", "rai",
    "followup", "demographics", "audit",
})


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
    note_scope: str = "all"
    canonical_target: str = ""
    linkage_anchor_family: str = "audit"
    dedupe_key: list[str] = field(default_factory=lambda: [
        "research_id", "note_row_id", "entity_type", "entity_value_norm",
    ])
    qa_tier: str = "standard"

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

    @property
    def is_operative_scoped(self) -> bool:
        return self.note_scope == "op_note"

    @property
    def is_path_report_scoped(self) -> bool:
        return self.note_scope == "path_report"


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

    def domains_for_note_scope(self, scope: str) -> dict[str, DomainSpec]:
        """Return domains whose note_scope matches the given scope or 'all'."""
        return {
            k: v for k, v in self.domains.items()
            if v.note_scope == scope or v.note_scope == "all"
        }

    def domains_by_qa_tier(self, tier: str) -> dict[str, DomainSpec]:
        return {k: v for k, v in self.domains.items() if v.qa_tier == tier}

    def domains_by_linkage_family(self, family: str) -> dict[str, DomainSpec]:
        return {
            k: v for k, v in self.domains.items()
            if v.linkage_anchor_family == family
        }

    def linkage_family_map(self) -> dict[str, list[str]]:
        """Return {family: [domain_name, ...]} for all canonical domains."""
        out: dict[str, list[str]] = {}
        for name, spec in self.canonical_domains.items():
            out.setdefault(spec.linkage_anchor_family, []).append(name)
        return out

    def generate_entity_summary_sql(self) -> str:
        """Build CREATE VIEW notes_entity_summary from all canonical domains."""
        unions: list[str] = []
        count_cases: list[str] = []
        for name, spec in self.canonical_domains.items():
            tbl = spec.parquet_stem
            unions.append(
                f"    SELECT research_id, '{name}' AS domain, "
                f"entity_value_norm, present_or_negated\n"
                f"    FROM {tbl}"
            )
            count_cases.append(
                f"    SUM(CASE WHEN domain = '{name}' THEN 1 ELSE 0 END) "
                f"AS n_{name}"
            )
        union_block = "\n    UNION ALL\n".join(unions)
        count_block = ",\n".join(count_cases)
        return (
            "CREATE OR REPLACE VIEW notes_entity_summary AS\n"
            "WITH all_entities AS (\n"
            f"{union_block}\n"
            ")\n"
            "SELECT\n"
            "    CAST(research_id AS VARCHAR) AS research_id,\n"
            "    COUNT(*) AS n_entities_total,\n"
            f"{count_block},\n"
            "    SUM(CASE WHEN present_or_negated = 'present' THEN 1 ELSE 0 END) AS n_present,\n"
            "    SUM(CASE WHEN present_or_negated = 'negated' THEN 1 ELSE 0 END) AS n_negated\n"
            "FROM all_entities\n"
            "GROUP BY research_id"
        )

    def resolve_domain(self, name: str) -> DomainSpec:
        """Look up a domain by name; raise ValueError for unknown domains."""
        if name not in self.domains:
            raise ValueError(
                f"Unknown extraction domain '{name}'. "
                f"Valid domains: {', '.join(sorted(self.domains))}"
            )
        return self.domains[name]


def _parse_domain(name: str, raw: dict[str, Any]) -> DomainSpec:
    prompts = [
        PromptSpec(repo_path=p["repo_path"], scope=p["scope"])
        for p in raw.get("prompts", [])
    ]
    dedupe_raw = raw.get("dedupe_key")
    if isinstance(dedupe_raw, list):
        dedupe_key = [str(k) for k in dedupe_raw]
    else:
        dedupe_key = ["research_id", "note_row_id", "entity_type", "entity_value_norm"]

    return DomainSpec(
        name=name,
        parquet_stem=raw["parquet_stem"],
        tier=raw.get("tier", "v2"),
        extractors=raw.get("extractors", ["llm"]),
        prompts=prompts,
        canonical_output=raw.get("canonical_output", True),
        note_scope=raw.get("note_scope", "all"),
        canonical_target=raw.get("canonical_target", raw["parquet_stem"]),
        linkage_anchor_family=raw.get("linkage_anchor_family", "audit"),
        dedupe_key=dedupe_key,
        qa_tier=raw.get("qa_tier", "standard"),
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

        if spec.note_scope not in _VALID_NOTE_SCOPES:
            issues.append(
                f"Domain '{name}': invalid note_scope '{spec.note_scope}' "
                f"(valid: {sorted(_VALID_NOTE_SCOPES)})"
            )

        if spec.qa_tier not in _VALID_QA_TIERS:
            issues.append(
                f"Domain '{name}': invalid qa_tier '{spec.qa_tier}' "
                f"(valid: {sorted(_VALID_QA_TIERS)})"
            )

        if spec.linkage_anchor_family not in _VALID_LINKAGE_FAMILIES:
            issues.append(
                f"Domain '{name}': invalid linkage_anchor_family "
                f"'{spec.linkage_anchor_family}' "
                f"(valid: {sorted(_VALID_LINKAGE_FAMILIES)})"
            )

        if not spec.canonical_target:
            issues.append(f"Domain '{name}': missing canonical_target")

        if "llm" in spec.extractors and not spec.prompts:
            issues.append(
                f"Domain '{name}': has 'llm' extractor but no prompt files defined"
            )

    return issues
