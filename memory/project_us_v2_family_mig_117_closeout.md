# US v2 imaging family Protocol v2 close-out — mig_117

Date: 2026-04-29
Author: Logan Glosser (drafted by Copilot)

## Scope

Closed three US v2 imaging canonicals under Protocol v2:

- `main.canonical_us_nodule_v2`
- `main.canonical_us_thyroid_gland_v2`
- `main.canonical_us_lymph_node_v2`

## Migration

- Migration: `qc_framework_v1/migrations/117_us_v2_family_signoff.sql`
- Verification method: `multi_source_derivation_plus_domain_sanity`
- Batch: `mig_117_us_v2_family_signoff_20260429`
- Build lineage reviewed: Scripts 362, 364, 364b, 366, 374, 377, and 378

## Validation results

Live MotherDuck checks:

| Table | Rows | Patients | Natural key duplicate groups | exam_date type |
|---|---:|---:|---:|---|
| canonical_us_nodule_v2 | 37,579 | 6,523 | 0 | DATE |
| canonical_us_thyroid_gland_v2 | 13,578 | 10,859 | 0 | DATE |
| canonical_us_lymph_node_v2 | 6,801 | 4,077 | 0 | DATE |

Additional checks:

- TIRADS guard view: 37,579 rows, 0 band mismatches, 0 concordance mismatches.
- Nodule feature vocab checks for composition, echogenicity, shape, margins, and TR1-TR5 categories were clean.
- Gland lobe/isthmus/volume range checks were clean; 6,785 fallback shell rows remain expected.
- LN source modality and suspicion-level vocab checks were clean; 6,793 evidence-only shell rows remain expected.
- `canonical_fna_events_v1` has no `nodule_id` or direct US exam reference, so the direct FNA orphan-reference check is not applicable.

## Registry final state

`canonical_column_verification_registry_v1`:

- `canonical_us_nodule_v2`: 53 `verified` + 4 `na`
- `canonical_us_thyroid_gland_v2`: 28 `verified` + 4 `na`
- `canonical_us_lymph_node_v2`: 23 `verified` + 6 `na`

`canonical_table_signoff_registry_v1`:

- All three tables: `table_status = verified`
- `signoff_migration = qc_framework_v1/migrations/117_us_v2_family_signoff.sql`

## Carry-forwards

- `CF-117-US-EXAM-ID-PORTABILITY`: `us_exam_id` is not portable across all three v2 children. By `(research_id, exam_date)`, 8,825/9,006 multisource exam groups (97.99%) had multiple child `us_exam_id` values. Pairwise: gland vs LN 0% inconsistent; nodule vs gland/LN 99.96% inconsistent. This was approved as a documented CF; downstream exam-level logic must join by `(research_id, exam_date)` and prefer nodule `us_exam_id` when available.
- `CF-117-US-NODULE-RANGE`: 21 nodule rows have `size_cm_max > 20` and 484 rows have `prior_size_mm_max > 200`, inherited from upstream CUNC/INM sources and preserved source-faithfully.
- `CF-117-US-LATERALITY-RAW`: `canonical_us_nodule_v2.laterality` contains raw location phrases, not side-only normalized values.
- `CF-117-US-GLAND-PARENCHYMA`: gland parenchymal phenotype fields remain all NULL by known parser/source limitation.
- `CF-117-US-LN-SHELL`: 6,793/6,801 LN rows are evidence-only shell rows pending future US LN enrichment.
