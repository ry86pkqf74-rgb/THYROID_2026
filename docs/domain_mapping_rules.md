# Domain Mapping Rules

**Updated:** 2026-04-06

## Single Source of Truth

`config/extraction_domain_registry.yaml` is the **sole authority** for the
extraction domain inventory.  Every downstream consumer -- run_extraction,
02b_register, 09b_fabric, 103_fact_lineage, 112_promotion_gate, and both
fleet scripts -- reads from this registry (or validates against it) rather
than maintaining independent domain lists.

## Domain Types

### Parent Domains (31 entries in `domains:`)

Each entry defines a canonical extraction output with a unique `parquet_stem`.
Domains are classified by tier:

| Tier | Count | Description |
|------|-------|-------------|
| v1   | 8     | Established domains with regex + LLM extractors. Parquets live in `processed/`. |
| v1_debug | 1 | The `llm` merged audit artifact. Not a canonical output. |
| v2   | 22    | New LLM-only domains. Parquets live in `processed/output/v2_parquets/`. |

### Sub-Prompt Domains (7 entries in `sub_prompt_domains:`)

Child extraction keys that map back to a parent domain.  Each sub-prompt:

- Has its own prompt file and produces its own parquet
- Rolls into the parent domain's canonical target at promotion time
- Is run by the VastAI fleet and `run_extraction_split.py` as an independent extraction key

| Sub-Prompt Key | Parent Domain | Prompt File |
|----------------|---------------|-------------|
| recurrence_detailed | recurrence | recurrence_detailed_extraction_v1.txt |
| complications_rln_laryngoscopy | complications | complications_rln_laryngoscopy_extraction_v1.txt |
| medication_management | medications | medication_management_extraction_v1.txt |
| operative_details | operative_detail | operative_details_extraction_v1.txt |
| operative_v2_enrichment | operative_detail | operative_v2_enrichment_extraction_v1.txt |
| parathyroid_per_gland | parathyroid_detail | parathyroid_per_gland_extraction_v1.txt |
| molecular_thyroseq_afirma | genetics | molecular_thyroseq_afirma_extraction_v1.txt |

## Fleet DOMAIN_PROMPT Map

The VastAI fleet script (`scripts/vastai/run_extraction_concurrent.py`) and
the split extraction script (`scripts/run_extraction_split.py`) each maintain
a `DOMAIN_PROMPT` dict mapping extraction keys to prompt filenames.

**The expected fleet map is derived from the registry** via
`Registry.expected_fleet_prompt_map()`, which:

1. Includes all canonical parent domains that have prompts
2. Excludes parent domains whose *entire* prompt set is covered by sub-prompt children (e.g. `operative_detail`, whose two prompts are both sub-prompt entries)
3. Excludes audit-only domains (`canonical_output=False`)
4. Includes all sub-prompt domain keys

Both fleet scripts perform import-time validation against the registry and
log warnings for any drift.

## Classification Taxonomy

Every parquet stem is classified into one of these categories:

| Classification | Meaning |
|---------------|---------|
| `standalone` | Direct 1:1 domain parquet. Canonical extraction output. |
| `child-enrichment` | Sub-prompt parquet that rolls into a parent domain. |
| `audit-only` | Debug/audit artifact. Not a promotion target. |
| `missing` | Registry domain with no parquet on disk. |
| `unclaimed` | On-disk parquet not referenced by any registry entry. |

## Promotion Gate Interpretation

The promotion gate (`scripts/112_v2_domain_promotion_gate.py`) uses the
registry-derived classification when evaluating G1 (domain completeness):

- **Truly missing:** v2 canonical domain with prompt files on disk but no
  parquet. Blocks promotion.
- **Deferred:** v2 domain whose prompt files don't exist yet. Does not block
  promotion.
- **Child-absent:** Sub-prompt parquet is missing but the parent domain
  parquet IS present. Informational only; does not block promotion.
- **Audit-only:** Non-canonical domain (e.g. merged `note_entities_llm`).
  Never blocks promotion.

## Canonical Outputs

Per-domain parquets are the canonical extraction outputs for promotion.
Merged audit artifacts (e.g. `note_entities_llm.parquet` from `--merge-audit`)
are **not** promotion truth.

## MotherDuck Stage Tables

V2 domain parquets map to `v2_stage.<parquet_stem>` tables in MotherDuck.
V1 domain parquets map to `main.<parquet_stem>` tables.  The promotion
gate's G8 check validates row parity between local parquets and the
MotherDuck stage tables.

## Adding a New Domain

1. Add the domain entry to `config/extraction_domain_registry.yaml` under `domains:`
2. If the domain has sub-prompts, add them under `sub_prompt_domains:`
3. Create the prompt file(s) under `llm_extraction/prompts/`
4. Add the domain to the fleet `DOMAIN_PROMPT` in both:
   - `scripts/vastai/run_extraction_concurrent.py`
   - `scripts/run_extraction_split.py`
5. Run `pytest tests/test_fleet_registry_parity.py` to verify parity
6. Run `.venv/bin/python llm_extraction/run_extraction.py --validate-only`

## CI Enforcement

The test suite `tests/test_fleet_registry_parity.py` enforces:

- Fleet DOMAIN_PROMPT keys match the registry's expected fleet map
- Prompt filenames match between fleet and registry
- All prompt files exist on disk
- Sub-prompt parent domains are valid
- No parquet stem collisions between domains and sub-prompts
- Every prompt file in `llm_extraction/prompts/` is referenced by the registry
