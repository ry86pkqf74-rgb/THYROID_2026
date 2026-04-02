# V2 Parquet Canonical Promotion Assessment — 2026-04-02

## Decision

Do not promote the current V2 LLM parquet set beyond `output/v2_parquets/` yet.

## What is true right now

1. The repo has a complete local V2 artifact set in `output/v2_parquets/`: 14 domain parquets plus `note_entities_llm_combined.parquet`.
2. Existing repo documentation already treats `output/v2_parquets/` as the active V2 export/staging location.
3. The local inventory is structurally complete at the file level, but provenance completeness is mixed across domains.
4. The accessible MotherDuck catalog is not usable as a reconciliation target from this session: DuckDB CLI can open `Thyroid 2026`, but `information_schema.tables` returns zero thyroid tables.
5. The Vast extraction fleet is still actively producing remaining tail domains, so freezing a higher-level canonical bundle now would lock in an incomplete moment.

## Evidence

- `output/v2_parquets/` contains:
  - `note_entities_llm_complications.parquet`
  - `note_entities_llm_genetics.parquet`
  - `note_entities_llm_imaging.parquet`
  - `note_entities_llm_labs.parquet`
  - `note_entities_llm_medications.parquet`
  - `note_entities_llm_operative_v2_enrichment.parquet`
  - `note_entities_llm_parathyroid_per_gland.parquet`
  - `note_entities_llm_pathology.parquet`
  - `note_entities_llm_physical_exam.parquet`
  - `note_entities_llm_problem_list.parquet`
  - `note_entities_llm_procedures.parquet`
  - `note_entities_llm_recurrence.parquet`
  - `note_entities_llm_staging.parquet`
  - `note_entities_llm_tirads_granular.parquet`
  - `note_entities_llm_combined.parquet`
- `docs/POWERBI_SETUP_SUMMARY.md` already labels `output/v2_parquets/` as `V2 Parquet exports`.
- `docs/vastai_extraction_fleet_2026-04-01.md` documents completed domains being copied into `output/v2_parquets/`.
- `studies/motherduck_v2_inventory_20260402.md` shows the local inventory is complete but MotherDuck table enumeration is empty in the visible thyroid catalog.

## Interpretation

`output/v2_parquets/` should be treated as the active working landing zone, not as the final canonical publication layer.

That means:

1. It is the correct place to continue collecting, repairing, and validating per-domain extraction outputs.
2. It is not yet the right place to freeze from for long-lived downstream consumers that expect a stable, fully reconciled bundle.
3. The current `combined` parquet should be treated as a convenience artifact, not as the authoritative canonical object.

## Promotion gate for later

Promote only after all of the following are true:

1. MotherDuck access exposes the real thyroid tables or share needed for parity checks.
2. Local V2 domain artifacts are provenance-normalized to the stricter shared schema.
3. Active tail domains have either completed or been intentionally excluded with manifest-level justification.
4. A dated manifest is generated for the exact promoted set, including row counts, schema checks, and source provenance status per domain.

## Recommended canonical target when ready

When the gate is met, create a dated export bundle under `exports/` rather than repurposing `output/v2_parquets/` itself.

Preferred shape:

- `exports/v2_llm_parquet_bundle_YYYYMMDD_HHMM/`
- one parquet per domain
- optional combined parquet only as a derived convenience artifact
- `manifest.json` with row counts, schema/provenance completeness, git SHA, and MotherDuck parity status

## Bottom line

No additional promotion action is warranted today.

The remaining safe action is to keep `output/v2_parquets/` as the live staging area, finish fleet extraction and reconciliation, restore a usable MotherDuck comparison target, and only then freeze a canonical export bundle under `exports/`.