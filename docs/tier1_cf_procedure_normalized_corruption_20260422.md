# Tier-1 Carry-Forward — `canonical_operative_events_v1.procedure_normalized` upstream corruption

**Filed**: 2026-04-22
**Discovered by**: Script 365 Phase-1 anchor probe (PSH/PMH/Meds remediation)
**Severity**: Tier-1 (do NOT loosen downstream case definitions; fix at source)
**Status**: open — pending upstream re-normalization

## Problem

`main.canonical_operative_events_v1.procedure_normalized` has only **3 distinct values**:

| procedure_normalized | rows |
|---|---:|
| `total_thyroidectomy` | 4,559 |
| `hemithyroidectomy` | 3,808 |
| `other` | 355 |

The `'other'` bucket is **populated with pathology / diagnosis strings**
rather than procedure names. Sample top-20 values from `procedure_raw` for
the `'other'` bucket:

- `Papillary Thyroid Carcinoma;` (41)
- `Papillary Thyroid Carcinoma` (14)
- `Medullary Thyroid Carcinoma;` (9)
- `Hurthle Cell Adenoma;` (5)
- `Non-toxic Multinodular goiter` (3)
- `Medullary Thyroid Carcinoma` (3)
- `Papillary Thyroid Carcinoma;metastatic;` (2)
- `Follicular Adenoma;` (2)
- `colloid cyst;` (1)
- `Indeterminate Nodule` (1)
- `insular thyroid cancer, recurrent;` (1)
- `thyroid cyst;` (1)
- ... (etc.)

These are pathology assertions, NOT procedure names. The upstream extractor
(or the source spreadsheet column it consumes) is putting the wrong field
into `procedure_raw`, and because the normaliser can't match a procedure
keyword it emits `'other'`.

## Downstream impact

When Script 365b probed `canonical_operative_events_v1` for a strict
thyroidectomy anchor (`procedure_normalized ILIKE '%thyroidect%' AND
surgery_date_native IS NOT NULL`), it found:

- **8,367 patients** with a strict match
- **2,504 patients (23.03% of CPM = 10,871)** with NO strict match

ALL 2,504 null-anchor patients DO have at least one dated operative event
in `canonical_operative_events_v1` — they just don't match the strict
regex because their `procedure_normalized = 'other'` (with pathology text
in `procedure_raw`).

## Mitigation in Script 365 (Phase 1)

Script 365 anchors `is_preexisting` and `days_from_first_thyroidectomy`
on a **HYBRID** anchor:

```sql
anchor_date = COALESCE(
    strict_thyroidectomy_date,        -- per CHANGE C, strict regex
    canonical_patient_master.first_surgery_date  -- fallback
)
anchor_source ∈ {'strict', 'first_surgery_fallback', NULL}
```

`is_preexisting = NULL` when `anchor_date IS NULL`.

The `anchor_source` column is carried on both events and rollup canonicals
so downstream queries can filter to strict-anchor-only patients if a
specific analysis requires that semantic precision.

The hybrid recovers all 2,504 patients (cohort definition implies their
first surgery IS the thyroidectomy — they're in the thyroid-cancer cohort
by selection), at the cost of trusting `first_surgery_date` for the 23%
where the procedure_normalized field is corrupted upstream.

## Fix at source — recommended Tier-1 work

1. **Identify the upstream extraction script** that populates
   `canonical_operative_events_v1.procedure_normalized`. Likely one of:
   - the v2 operative-pipeline (under `notes_extraction_new/`)
   - the canonical_operative_events_v1 builder (Script 327 or 341)
2. **Audit the source spreadsheet column** that feeds `procedure_raw`.
   The pathology strings suggest a `pathology_diagnosis` column was
   plumbed where `procedure_name` should have been, or two sheets were
   joined on the wrong key.
3. **Re-normalise procedure_normalized** with an expanded keyword set
   (lobectomy, isthmusectomy, completion, biopsy, etc.) so the pathology
   strings get parked in their own field.
4. **Re-run Script 365 Phase 1** and verify that `pct_strict` rises and
   `pct_first_surgery_fallback` drops correspondingly. The anchor_source
   distribution gate in Script 365's QA suite already surfaces this.

After the upstream fix lands, Script 365 can OPTIONALLY revert to a
strict-only anchor (drop the COALESCE fallback) for stricter semantics,
but the hybrid is benign and self-documenting via `anchor_source`.

## Related CFs

- **CF-1 (from 364)**: trigger-phrase-only extraction lacks surrounding
  context. Not the same defect, but the same general class:
  source-of-truth columns capturing the wrong substring.
- **CF-2 (from 363)**: LLM finding_status ladder ordering. Unrelated.

## Verification queries

```sql
-- Procedure_normalized distribution
SELECT procedure_normalized, COUNT(*)
FROM main.canonical_operative_events_v1
GROUP BY 1 ORDER BY 2 DESC;

-- 'Other'-bucket procedure_raw samples (the pathology pollution)
SELECT procedure_raw, COUNT(*)
FROM main.canonical_operative_events_v1
WHERE procedure_normalized = 'other'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- Anchor-source distribution per Script 365 rollup
SELECT anchor_source, COUNT(*)
FROM main.canonical_pmh_patient_rollup_v1
GROUP BY 1 ORDER BY 1;
```
