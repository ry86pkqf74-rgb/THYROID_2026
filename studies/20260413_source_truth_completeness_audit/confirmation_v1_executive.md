# Confirmation v1 (deterministic joins + exhaustive linkage classification)

- Deployed (UTC): `2026-04-13T17:31:30.898061+00:00`

## Scoped answers (fail-closed global YES still blocked if any `unresolved_linkage_gap` is unacceptable)

### Q1 — COMPLETE corpus → `imaging_nodule_master_v1`

- `v_canonical_us_nodule_scope_v1`: **19891** rows; source_table = `raw_us_tirads_excel_v1`.
- Deterministic parity was already **19891/19891** in the 20260413 audit.

### Q2 — TI-RADS when ≥5 ACR fields populated

- Rows with sufficient features but **both** `tirads_reported` and `tirads_acr_recalculated` null: **0** (see `v_imaging_nodule_tirads_gap_v1`).

### Q3 — Imaging ↔ FNA classification

Every `imaging_nodule_master_v1` row appears in `v_imaging_nodule_linkage_classification_v1` with:

- `linked_to_fna` — primary multimodal link exists
- `no_eligible_fna` — documented reason (no patient FNA, US after surgery, all FNA before index US, or only FNA beyond 90d window)
- `unresolved_linkage_gap` — candidate FNA in 0–90d after index US but **no** `imaging_fna_linkage_mm_v1` primary row (requires algorithm/review follow-up)

- **Unresolved linkage gap total: 2094**

### Q4 — Lymph node (exam-level text)

- Structured per-level LN model is **not** in scope for v1; exam-level `lymph_node_assessment` remains the capture mechanism.

### Q5 — Bethesda

- Episodes with numeric Bethesda resolved (`bethesda_resolved_num`): **8072 / 8119**.
- Remaining rows carry `bethesda_unscorable_reason` in `v_fna_episode_bethesda_resolved_v1`.

## Views created

- `v_fna_episode_bethesda_resolved_v1`
- `v_imaging_nodule_linkage_classification_v1`
- `v_imaging_nodule_tirads_gap_v1`
- `v_canonical_us_nodule_scope_v1`

Machine-readable metrics: `studies/20260413_source_truth_completeness_audit/confirmation_v1_metrics.json`
