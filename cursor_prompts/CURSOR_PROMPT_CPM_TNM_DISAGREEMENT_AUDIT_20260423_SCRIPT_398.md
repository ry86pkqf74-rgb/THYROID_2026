# Script 398 — CPM T/N/M primary↔v2 disagreement audit (read-only sidecar)

Implements `scripts/apply_cpm_tnm_disagreement_audit.py`: materializes
`manuscript_workspace.cpm_tnm_cross_source_disagreements_v1` (4256 rows) and one
`main.__readme` row for `script_398`, with **no** `UPDATE` to
`main.canonical_patient_master`. Full requirement text lives in the chat prompt
and the script docstring.

**Phase 0:** `python3 scripts/apply_cpm_tnm_disagreement_audit.py --phase 0`  
**Apply (after approval):**  
`python3 scripts/apply_cpm_tnm_disagreement_audit.py --apply --i-approve=<PROBE_SHA256> --phase4`

Probe report: `scripts/output/apply_cpm_tnm_disagreement_audit_probe.md`  
Run log: `scripts/output/apply_cpm_tnm_disagreement_audit_run.log`  
Close-out: `cursor_prompts/CLOSE_OUT_398.md`
