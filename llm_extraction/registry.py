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
class SubPromptSpec:
    """A child extraction key that maps to a parent registry domain."""
    name: str
    parent_domain: str
    prompt_file: str
    parquet_stem: str

    @property
    def prompt_absolute_path(self) -> Path:
        return _REPO_ROOT / "llm_extraction" / self.prompt_file


@dataclass(frozen=True)
class Registry:
    schema_version: str
    domains: dict[str, DomainSpec]
    canonical_outputs: dict[str, CanonicalOutputSpec]
    llm_extraction_meta: dict[str, str]
    sub_prompt_domains: dict[str, SubPromptSpec] = field(default_factory=dict)

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
        """Return the *first* prompt for a domain (backward-compat convenience).

        Prefer ``prompts_for_domain`` when the caller can handle multiple
        prompts — multi-prompt domains (e.g. genetics, recurrence) need all
        their prompts to be exercised for complete extraction.
        """
        spec = self.domains.get(domain)
        if spec and spec.prompts:
            return spec.prompts[0]
        return None

    def prompts_for_domain(self, domain: str) -> list[PromptSpec]:
        """Return *all* prompts for a domain (empty list if unknown/no prompts)."""
        spec = self.domains.get(domain)
        if spec and spec.prompts:
            return list(spec.prompts)
        return []

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

    def generate_entity_summary_sql(
        self,
        loaded_tables: set[str] | None = None,
    ) -> str:
        """Build CREATE VIEW notes_entity_summary from canonical domains.

        Parameters
        ----------
        loaded_tables:
            When provided, only domains whose ``parquet_stem`` is present in
            this set are included in the UNION ALL.  Pass the set of table
            names successfully loaded into DuckDB so the generated SQL does
            not reference tables that do not yet exist.  When ``None`` (the
            default), all canonical domains are included.
        """
        unions: list[str] = []
        count_cases: list[str] = []
        for name, spec in self.canonical_domains.items():
            tbl = spec.parquet_stem
            if loaded_tables is not None and tbl not in loaded_tables:
                continue
            unions.append(
                f"    SELECT research_id, '{name}' AS domain, "
                f"entity_value_norm, present_or_negated\n"
                f"    FROM {tbl}"
            )
            count_cases.append(
                f"    SUM(CASE WHEN domain = '{name}' THEN 1 ELSE 0 END) "
                f"AS n_{name}"
            )
        if not unions:
            return (
                "CREATE OR REPLACE VIEW notes_entity_summary AS "
                "SELECT NULL::VARCHAR AS research_id, 0 AS n_entities_total, "
                "0 AS n_present, 0 AS n_negated WHERE FALSE"
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

    def sub_prompt_parent_map(self) -> dict[str, str]:
        """Return ``{parquet_stem: parent_domain_name}`` for all sub-prompt domains."""
        return {
            sp.parquet_stem: sp.parent_domain
            for sp in self.sub_prompt_domains.values()
        }

    def expected_fleet_prompt_map(self) -> dict[str, str]:
        """Return the canonical ``{fleet_key: prompt_filename}`` map.

        Combines parent domain first-prompts with all sub-prompt entries so
        the VastAI fleet script can validate its own ``DOMAIN_PROMPT`` dict
        against the single source of truth.

        Domains whose *entire* prompt set is covered by sub-prompt children
        are excluded (the fleet runs children directly, not the parent).
        Audit-only domains (canonical_output=False) are also excluded.
        """
        sub_prompt_files = {
            Path(sp.prompt_file).name
            for sp in self.sub_prompt_domains.values()
        }
        result: dict[str, str] = {}
        for name, spec in self.domains.items():
            if not spec.prompts:
                continue
            if not spec.canonical_output:
                continue
            domain_prompt_files = {Path(p.repo_path).name for p in spec.prompts}
            if domain_prompt_files <= sub_prompt_files:
                continue
            prompt_file = Path(spec.prompts[0].repo_path).name
            result[name] = prompt_file
        for sp_name, sp in self.sub_prompt_domains.items():
            prompt_file = Path(sp.prompt_file).name
            result[sp_name] = prompt_file
        return result

    def classify_stem(self, stem: str) -> str:
        """Classify a parquet stem into a deterministic category.

        Returns one of:
          - ``"standalone"`` — direct 1:1 domain parquet
          - ``"child-enrichment"`` — sub-prompt output that rolls into a parent
          - ``"audit-only"`` — debug/audit artifact (canonical_output=False)
          - ``"alias"`` — stem not in registry but matches a known pattern
          - ``"unknown"`` — unrecognised stem
        """
        stem_to_domain = self.parquet_stem_to_domain()
        if stem in stem_to_domain:
            spec = self.domains[stem_to_domain[stem]]
            if not spec.canonical_output:
                return "audit-only"
            return "standalone"
        sub_map = self.sub_prompt_parent_map()
        if stem in sub_map:
            return "child-enrichment"
        return "unknown"

    def all_known_stems(self) -> set[str]:
        """Return every parquet stem the registry knows about (domains + sub-prompts)."""
        stems = set(self.all_parquet_stems())
        stems |= {sp.parquet_stem for sp in self.sub_prompt_domains.values()}
        return stems


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


def _parse_sub_prompt(name: str, raw: dict[str, Any]) -> SubPromptSpec:
    return SubPromptSpec(
        name=name,
        parent_domain=raw["parent_domain"],
        prompt_file=raw["prompt_file"],
        parquet_stem=raw["parquet_stem"],
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

    sub_prompts = {
        name: _parse_sub_prompt(name, spec)
        for name, spec in raw.get("sub_prompt_domains", {}).items()
    }

    return Registry(
        schema_version=raw.get("schema_version", "unknown"),
        domains=domains,
        canonical_outputs=canonical,
        llm_extraction_meta=raw.get("llm_extraction", {}),
        sub_prompt_domains=sub_prompts,
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

    for sp_name, sp in reg.sub_prompt_domains.items():
        if sp.parent_domain not in reg.domains:
            issues.append(
                f"Sub-prompt '{sp_name}': parent_domain '{sp.parent_domain}' "
                f"not found in registry domains"
            )
        if not sp.prompt_absolute_path.exists():
            issues.append(
                f"Sub-prompt '{sp_name}': prompt_file '{sp.prompt_file}' "
                f"not found at {sp.prompt_absolute_path}"
            )
        if sp.parquet_stem in stems_seen:
            issues.append(
                f"Sub-prompt '{sp_name}': parquet_stem '{sp.parquet_stem}' "
                f"collides with a domain parquet_stem"
            )

    return issues
