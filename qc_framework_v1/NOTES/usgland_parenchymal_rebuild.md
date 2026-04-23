# USGLAND02 — Parenchymal-phenotype rebuild (LLM pass)

**Status**: pending (opened 2026-04-23, prompt 28)
**Scope**: `main.canonical_us_thyroid_gland_v2` — 13,578 rows, 100% parenchymal-phenotype NULL

## Target columns

Columns in `canonical_us_thyroid_gland_v2` that the v2 parser left entirely NULL:

- `background_echogenicity` (VARCHAR)
- `heterogeneity` (VARCHAR)
- `hashimoto_pattern` (VARCHAR)
- `vascularity_overall` (VARCHAR)
- `calcifications_parenchymal` (VARCHAR)
- `goiter_flag` (BOOLEAN)
- `pyramidal_present_flag` (BOOLEAN)
- `substernal_extension_flag` (BOOLEAN)

## Source text

`source_us_impression_text` (VARCHAR) — radiologist impression block.
`clinical_impression_text` (VARCHAR) — narrative impression where present.
Both fields are already on the gland table; no join required.

## Output plan

- **Patch table**: `manuscript_workspace.canonical_us_thyroid_gland_v2_parench_patch_v1`
  Columns: `(research_id, us_exam_id, background_echogenicity, heterogeneity,
  hashimoto_pattern, vascularity_overall, calcifications_parenchymal, goiter_flag,
  pyramidal_present_flag, substernal_extension_flag, llm_model, llm_run_ts, confidence)`.
- **Merge view** `manuscript_workspace.canonical_us_thyroid_gland_v2_merged` will
  COALESCE patch over canonical. Canonical rows NOT mutated (respect main-is-audit rule).

## Execution gate

Do not kick off the LLM pass until:
1. Cost estimate (tokens × run volume × model price) signed off.
2. Sample prompts reviewed on a 100-row draw.
3. Confidence thresholds agreed (below threshold → stays NULL, flagged `llm_low_conf`).

## Related

- USGLAND01 shell rows (6,785) are handled by migration 29 / queue.
- Same pattern will apply to TIR03 multi-nodule reparse (migration 26, qc_tir03_llm_candidates_v1).
- US LN 100% shell (USLN01, prompt 29 / migration 30) is a sibling LLM task.
