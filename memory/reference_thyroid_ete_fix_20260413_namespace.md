---
type: reference
description: MotherDuck DB holding frozen source-of-truth for 78 CPM cols populated via Scripts 212 + 215; do not delete without coordination
---

# `thyroid_ete_fix_20260413` — historical CPM source-of-truth namespace

## What it is

- **DB**: `thyroid_ete_fix_20260413` on MotherDuck. Still attached to
  this workspace.
- **Role**: holds the historical source data that populated **78 frozen
  CPM cols** on `thyroid_canonical_publication_v1_0.main.canonical_patient_master`.
- **Status**: **FROZEN**. No live rebuild pipeline reads from or writes
  to this DB on the publication side.

## Populated CPM col families (78 total — all frozen)

| Source script | Source DB | CPM col-family | n cols |
|---|---|---|---:|
| `scripts/212_nlp_entity_rollup.py` | this DB | `nlp_ne_problemlist_*` (`_has_data`, `_n_rows`) | 2 |
| `scripts/212_nlp_entity_rollup.py` | this DB | `nlp_ne_medications_*` (`_has_data`, `_n_rows`) | 2 |
| `scripts/215_deep_nlp_entity_integration.py` | this DB | `pmhx_nlp_*` (per-comorbidity BOOL + counts + first dates) | 59 |
| `scripts/215_deep_nlp_entity_integration.py` | this DB | `med_nlp_*` (per-drug BOOL + counts + dates) | 15 |

Both 212 and 215 hard-code `DB = "thyroid_ete_fix_20260413"` and target
`CANONICAL = "canonical_patient_master_v1"` (note: `_v1` suffix — not
the publication CPM directly).

## Promotion path (how the values reached publication CPM)

1. Scripts 212 + 215 ran against this DB at some prior cutover, writing
   the 78 cols onto `canonical_patient_master_v1` in this namespace.
2. The publication snapshot script (likely Script 271 — to be
   re-confirmed if a future cycle needs the exact source SHA) promoted
   the rows + cols into
   `thyroid_canonical_publication_v1_0.main.canonical_patient_master`.
3. The 78 cols have not been re-sourced since. They are frozen at
   their post-cutover values.

## Current state

- **No live rebuild pipeline** reads from this DB on the publication
  side. Phase 1 of the Script 365 remediation cascade superseded
  the data sources for new analyses (`canonical_pmh_*_v1` and
  `canonical_medications_*_v1`).
- **Script 365b chose Option-C** (skip CPM repoint) explicitly to
  preserve publication reproducibility. Repointing the 78 cols to
  the new tier-driven rollups would have OVERWRITTEN frozen cohort
  values with current canonical state, breaking back-compat.

## What if a future cycle wants refreshed values?

That's a **full publication re-cut**, not a Tier-2 repoint:

- Bump `canonical_version` on CPM (e.g. to a new
  `v1_1_*` line).
- Decide whether to re-extract from the publication DB (using the new
  rollup phenotype-BOOL triads) OR re-run 212/215 against this DB
  (which still has the source tables).
- Document the cohort-version transition explicitly so any prior
  analysis citing the frozen values can be reproduced from the
  archived snapshot.

## Operational guardrail

**DO NOT drop this DB** without first confirming no CPM col provenance
chain depends on it. The 78 frozen cols listed above are the explicit
dependency. If you're triaging legacy MotherDuck namespaces and this
one looks unused, it's NOT unused — it's the source-of-truth for the
listed CPM lineage. Re-confirm via:

```sql
SELECT column_name FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0'
  AND table_schema='main' AND table_name='canonical_patient_master'
  AND (column_name LIKE 'nlp_ne_problemlist_%'
       OR column_name LIKE 'nlp_ne_medications_%'
       OR column_name LIKE 'pmhx_nlp_%'
       OR column_name LIKE 'med_nlp_%');
```

## Related memory

- `project_script_365b_close_out.md` — context for the Option-C
  decision that left these cols frozen
- `feedback_cpm_frozen_at_publication.md` — the underlying rule
