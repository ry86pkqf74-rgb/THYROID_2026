# Linkage specification — canonical nodule chain (v1)

## Source precedence

1. **Imaging spine:** `imaging_nodule_master_v1` (dated rows only). TI-RADS category uses reported category when present, else mapped from integer TI-RADS fields (`tirads_reported` / `tirads_acr_recalculated`).
2. **US → FNA:** `imaging_fna_linkage_mm_v1` **primary links only** (`is_primary_link IS TRUE`). DuckDB treats `= TRUE` on BOOLEAN differently from `IS TRUE` in some catalogs—this study uses `IS TRUE` for reliable matching. Rules otherwise mirror script 129 (specimen key match or 0–90d temporal guard + side/size checks).
3. **FNA metadata:** `fna_episode_master_v2` joined on `(research_id, fna_episode_id)` from the imaging row.
4. **FNA → molecular:** `fna_molecular_linkage_v3` with `score_rank = 1` + `molecular_test_episode_v2`.
5. **FNA → surgery:** `preop_surgery_linkage_v3` with `preop_type = 'fna'`, `score_rank = 1`.
6. **Surgery → pathology focus:** `surgery_pathology_linkage_v3` with `score_rank = 1`.
7. **Pathology attributes:** `tumor_episode_master_v2` on `(research_id, surgery_episode_id, tumor_ordinal)`.

Final histology and staging come from the tumor episode row (synoptic-first precedence is already baked into `tumor_episode_master_v2` in script 22).

## Join keys (strict)

- `research_id` everywhere.
- Imaging ↔ FNA: `nodule_id` + primary row from multimodal linkage table.
- FNA ↔ molecular / surgery: integer `fna_episode_id` / `preop_episode_id` per v3 linkage tables.
- Surgery ↔ tumor: `surgery_episode_id` + `tumor_ordinal`.

## Deterministic score components

Downstream segments reuse **published v3 scoring** from `scripts/49_enhanced_linkage_v3.py` (temporal, laterality where applicable, ambiguity penalties). This study does not introduce new fuzzy string matching.

## Conflict / precedence rules

- **Final surgical / synoptic pathology** (via `tumor_episode_master_v2`) overrides cytology and molecular result classes for the `final_pathology_label` derivation used in discordance checks.
- **Molecular** is expected to adjudicate **Bethesda III/IV** when missing molecular work is clinically material — rows with `bethesda_category IN (3,4)` and no `molecular_episode_id` are flagged for **manual review** (not auto-imputed).

## Manual review triggers

See `_MANUAL_REVIEW_PREDICATE` in `utils/canonical_nodule_linkage.py`. In summary:

- Multi-candidate imaging↔FNA sets without a primary link.
- Any v3 linkage with `n_candidates > 1` on FNA→molecular, preop→surgery, or surgery→pathology (even when `score_rank = 1`).
- Linked surgery episode is not the **earliest post-FNA** surgery episode (operative timeline guard).
- Bethesda III/IV without molecular attachment.

## NIFTP

- `niftp_flag` is true when final histology text contains `niftp` (case-insensitive).
- Rows contribute to **sensitivity analyses** and `discordance_summary` (`niftp_rows`); primary malignant counts should exclude NIFTP where indicated in methodology.

## Multi-surgery

- Preop linkage selects the v3 rank-1 surgery per FNA episode.
- **Earliest post-FNA surgery** is computed as `MIN(surgery_episode_id)` among operative episodes on/after the FNA date. If rank-1 differs, `manual_review_needed_flag` is set (possible upgrade / staging completeness handled after adjudication).

## Multifocality

- `multifocal_flag` from `tumor_episode_master_v2` (`multifocality_flag` or `number_of_tumors > 1`).

## Outputs

| Artifact | Description |
|----------|-------------|
| `canonical_nodule_linkage.*` | One row per dated imaging nodule (primary FNA path) |
| `candidate_match_pairs.*` | Multi-candidate imaging↔FNA rows |
| `manual_review_queue.*` | Policy-triggered review contexts (JSON, no note text) |
| `discordance_summary.*` | Aggregate discordance + NIFTP row counts |
| `linkage_qc_summary.*` | Raw table / v3 yields |
