# Canonical Layer Integrity Report — Addendum 2026-05-08

**Script:** `scripts/ops/348_any_metastasis_canonical_propagation.py`
**Status:** PENDING EXECUTION — this file will be overwritten with live metrics when
`ops/348 --apply` runs.

## Planned Change

Propagate `any_metastasis` to `pub_canonical.canonical_path_malignant_patient_rollup_v1`
using M-stage evidence from `pub_workspace.manuscript_cohort_v1_surgery_reconciled` (M1
flag) and distant-site recurrence evidence from `pub_canonical.canonical_recurrence_events_v1`.

## Linked Verification Check

- **VC ID:** VC-2026-05-08-M086-any-metastasis-zero-and-bethesda-reconcile
- **Airtable record:** recSbq9CeoduZIP6F
- **Base / Table:** THYROID_DATA_REGISTRY (appTGeB1jIizZbjnw) / tbl65mYqMWIGEQIBZ
- **Action (on apply):** lifecycle → Verified; status → Resolved

## Run Commands

```bash
# Validate first (no BQ mutations):
.venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --dry-run

# Apply (BQ MERGE + signoff update + docs):
.venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --apply

# Apply + Airtable DFL + VC narrow:
AIRTABLE_API_KEY=<key> .venv/bin/python scripts/ops/348_any_metastasis_canonical_propagation.py --apply --log-airtable
```

## Acceptance Criteria

- `any_metastasis = TRUE` count in range **1,800–2,100** (M1 + distant-recurrence-only)
- Row count of `canonical_path_malignant_patient_rollup_v1` unchanged at **4,022**
- `canonical_table_signoff_registry_v1` row for `canonical_path_malignant_patient_rollup_v1`
  shows new `build_ts`
- Verification Check `recSbq9CeoduZIP6F` lifecycle = Verified, status = Resolved
