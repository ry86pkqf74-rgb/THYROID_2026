# Script 402 — Histologic classification audit (read-only sidecar)

- **Script:** `scripts/apply_histologic_classification_audit.py`
- **Target:** `manuscript_workspace.cpm_histologic_classification_audit_v1` in `thyroid_canonical_publication_v1_0`
- **Axes (8):** `PDTC_SCATTER`, `HURTHLE_SCATTER`, `DHGTC_CATALOG`, `PTC_VARIANT_DISCREPANCY`, `PTC_VARIANT_UNKNOWN`, `HVA_DATA_QUALITY`, `AGGRESSIVE_VARIANT_FLAGGED`, `GRADE_3_OR_4_CROSS_REF`
- **CPM:** read-only; no `canonical_patient_master` updates; no manual-review queue writes
- **Run:** Phase 0 → approve probe SHA256 → `--apply --i-approve=<sha> [--phase4]`
- **Probe / log:** `scripts/output/apply_histologic_classification_audit_probe.md`, `scripts/output/apply_histologic_classification_audit_run.log`
- **Details:** see repository chat and `CLOSE_OUT_402.md` after apply
