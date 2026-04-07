# Specimen–genomics binding hardening (v1)

**Date:** 2026-04-07  
**Materializer:** [`scripts/140_md_specimen_genomics_binding.py`](../scripts/140_md_specimen_genomics_binding.py)  
**DDL:** [`scripts/sql/140_specimen_genomics_binding_ddl.sql`](../scripts/sql/140_specimen_genomics_binding_ddl.sql)  
**MotherDuck attribution:** `custom_user_agent='specimen_genomics_binding_v1'`

## 1) Assay-bearing sources and grain

| Source | Grain | Binding path | Notes |
|--------|--------|--------------|--------|
| `molecular_test_episode_v2` | One row per molecular episode | `fna_molecular_linkage_v3` (rank-1) → `preop_surgery_linkage_v3` (rank-1) → `specimen_tumor_focus_v1` (distinct focus/specimen counts) | Base spine; tier mapped from v3 `linkage_confidence_tier`. |
| `genetic_testing` | Excel assay rows (+ synthetic `gt_rn`) | Same spine after join to `molecular_test_episode_v2` on normalized platform equality | Optional table; stripped when absent. |
| `thyroseq_molecular_enrichment` | One enrichment row per ThyroSeq workbook row | Join to best ThyroSeq `molecular_test_episode_v2` per `source_row_hash`; `json_each` on `fusion_genes_json` and `allele_fractions_json` | Optional table; stripped when absent. `payload_explode_ord` from `row_number()` over `json_each.key` for stable ordinality. |
| `thyroseq_review_queue` | Review metadata | Not merged into assay fact table in this pass; remains upstream for operational QA. | |
| `tumor_episode_master_v2` | Tumor-long pathology | Used only indirectly via `surgery_pathology_linkage_v3` (already built from operative vs tumor episodes in v3 linkage scripts). | No new molecular→surgery shortcut. |

**Prior grain mismatch removed:** `min(specimen_focus_id)` aggregation over multifocal surgeries is replaced by per-`(research_id, surgery_episode_id)` distinct counts; multiple foci or multiple specimen IDs clear the ambiguous dimension and downgrade tier / queue QA.

## 2) Outputs

- **`main.specimen_genomic_assay_v1`** — columns include `path_surgery_id`, `tumor_ordinal`, `pathology_linkage_tier_raw`, normalized `linkage_confidence_tier`, `linkage_reason_codes`, `source_table`, `source_row_key`, `payload_explode_ord`, `payload_field`, legacy `fm_tier` / `preop_tier` / `binding_confidence_tier` / `review_flag`.
- **`qa.specimen_genomic_link_review_v1`** — rows where tier is `plausible_review` / `unresolved_review` or reason codes indicate chain / multifocal / pathology / ThyroSeq ambiguity.
- **`qa.val_specimen_genomic_binding_v1`** — checks from script 140 (`genomic_assay_id_unique`, optional `specimen_master_fk_when_present`, audit placeholder).

## 3) Tests

- [`tests/test_specimen_genomics_binding.py`](../../tests/test_specimen_genomics_binding.py): ThyroSeq + Afirma rows bind `specimen_id`; cross-tumor isolation; JSON explosion idempotency; multifocal → NULL focus + QA.

## 4) MotherDuck execution

This study folder was generated in an environment **without** `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN` in the shell. To materialize on MotherDuck:

```bash
cd THYROID_2026
.venv/bin/python scripts/140_md_specimen_genomics_binding.py --md
```

Or run full specimen + FHIR orchestrator (identity → FHIR tail → 140):

```bash
.venv/bin/python scripts/138_md_specimen_fhir_layer.py --md
```

## 5) Integration

- [`scripts/138_md_specimen_fhir_layer.py`](../../scripts/138_md_specimen_fhir_layer.py) now applies [`scripts/sql/138_specimen_fhir_tail_ddl.sql`](../../scripts/sql/138_specimen_fhir_tail_ddl.sql) (FHIR only), then calls `apply_specimen_genomics_binding`.
