# THYROID_2026 — release gate report

**Generated (UTC):** 2026-04-07T12:46:41Z
**Git SHA:** `97c8503345c8ad292a1006e3223cc6a930d0ee12`
**MotherDuck env:** `prod`
**Session hint:** `THYROID_2026`

## Decision: **FAIL**

**Summary:** PASS=12, HOLD=5, FAIL=5

### Checklist

| ID | Severity | Detail |
|----|----------|--------|
| `registry.integrity` | PASS | YAML load OK; prompt files exist; fleet DOMAIN_PROMPT matches registry. |
| `extraction.domain_completion` | PASS | All canonical v2 domains have non-empty v2_stage tables and load_inventory row_match. |
| `staging.inventory_all_match` | PASS | BOOL_AND(row_match) is true. |
| `canonical.keys.main` | PASS | No NULL research_id; no duplicate fact_id groups. |
| `canonical.presence.v2_stage` | HOLD | v2_stage canonical long not materialized — common when only domain tables and main promotion surface are retained; verify main integrity. |
| `quarantine.main` | HOLD | Quarantine has rows — review before promote. |
| `quarantine.v2_stage` | HOLD | v2_stage.canonical_fact_quarantine_v2 missing. |
| `qa.manual_review_queue` | PASS | No pending verification rows. |
| `qa.val_specimen_contract_v1` | PASS | No FAIL rows. |
| `qa.val_specimen_genomic_binding_v1` | PASS | No FAIL rows. |
| `qa.promotion_scorecard` | HOLD | View missing. |
| `canonical.extraction_run_id` | PASS | All rows carry extraction_run_id. |
| `tg.thyroglobulin_lab_canonical_v1` | PASS | Canonical lab table has 76971 rows. |
| `tg.tg_lab_review_queue_v1` | HOLD | 1035 review rows pending. |
| `mm.val_contract_required_join_keys_mm_v1` | PASS | Empty (strict-release OK). |
| `mm.val_nodes_invariant_mm_v1` | FAIL | 2957 blocker rows — multimodal gate fails. |
| `mm.val_multitumor_expansion_mm_v1` | PASS | Empty (strict-release OK). |
| `mm.val_side_lobe_mismatch_mm_v1` | FAIL | 36 blocker rows — multimodal gate fails. |
| `mm.val_preop_temporal_order_mm_v1` | FAIL | 3 blocker rows — multimodal gate fails. |
| `mm.val_ambiguous_multimodal_linkage_mm_v1` | FAIL | 3989 blocker rows — multimodal gate fails. |
| `mm.val_imaging_fna_contract_blockers_mm_v1` | FAIL | 3093 blocker rows — multimodal gate fails. |
| `labs.institutional_backlog` | PASS | No future_institutional_required rows. |

### Evidence (JSON)

Full structured evidence: [`release_gate_manifest.json`](release_gate_manifest.json).

### Operator next commands

1. `cd "/Users/ros/THyroid 2026/THYROID_2026" && export MOTHERDUCK_SESSION_HINT=THYROID_2026 && .venv/bin/python scripts/148_thyroid2026_release_gate.py --md --env prod`
2. `cd "/Users/ros/THyroid 2026/THYROID_2026" && .venv/bin/python scripts/116_md_stage_loader.py --md`
3. `cd "/Users/ros/THyroid 2026/THYROID_2026" && .venv/bin/python scripts/112_v2_domain_promotion_gate.py --motherduck-check`
4. `cd "/Users/ros/THyroid 2026/THYROID_2026" && .venv/bin/python scripts/119_md_formalization_validate.py --md`
5. `Fix FAIL evidence (duplicate keys, load_inventory mismatch, specimen FAIL rows, multimodal blockers) before any promotion.`