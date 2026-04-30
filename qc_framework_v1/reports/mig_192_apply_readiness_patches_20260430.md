# mig_192 — apply-readiness patches for mig_185 / mig_186 / mig_188

**Date:** 2026-04-30  
**Lane:** mig_192 / `apply_readiness_patches_for_185_186_188`  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Posture:** Authored SQL + runbook only; **no MotherDuck execution** from Cursor lane.

## §1 Verification findings (Cowork Path-C, HEAD `922f138` handoff)

| Migration | Finding | Severity |
| --- | --- | --- |
| **mig_185** (`b641989`) | `BEGIN TRANSACTION` / `COMMIT` wrappers violate MD MCP one-statement-per-call (v10 §3.3). | Fix: strip wrappers in **185b**. |
| **mig_186** (`65ba4d6`) | §F+§G set `verification_status = 'not_started'` on verified canonicals without updating `canonical_table_signoff_registry_v1` aggregates — breaks **gate3** (`n_verified + n_na ≠ n_columns_total`). | Fix: **186b** — keep verified; append CF notes + `verification_method` only. |
| **mig_186** | §D2 rollup uses `COUNT(*)` for `n_tumors_total` vs **185** `COUNT(DISTINCT grain)`. | Acceptable if **185b** runs **after** **186b** (ratified order). |
| **mig_188** (`8e2549c`) | T-stage buckets `no_primary…` / ambiguous omit explicit **`T0`** in `t_stage_ajcc8_resolved` (only implied via `ajcc_resolution_source`). | Fix: **188b** — store `'T0'` in §D CASE for those branches. |

## §2 Logan-ratified patches summary

| Deliverable | Replaces | Intent |
| --- | --- | --- |
| `185b_apply_rollup_only_patch_no_transaction_20260430.sql` | `185_apply_rollup_only_patch_20260430.sql` | No transaction wrapper; plain `INSERT … VALUES` for provenance with idempotency note; snapshot name suffix `mig185b`. |
| `186b_apply_RD_niftp_exclusion_no_gate3_break_20260430.sql` | `186_apply_RD_niftp_exclusion_ratified_20260430.sql` | §F: `derivation_re_derivation_post_niftp_exclusion` + CF note; §G: `spot_check_pending_115_edge_patients` + CF note; **§I gate3** probe **must return 0**. |
| `188b_mig184_v2_plus_r1c_with_explicit_T0_20260430.sql` | `188_mig184_v2_plus_r1c_ln_only_stage_rule_apply_20260430.sql` | Explicit **`T0`** for three NULL branches in event T-stage CASE; AJCC7 maps **`T0` → `T0`**; §J T0 cohort probes; new provenance `run_id` / `batch_id` for **188b**. |

## §3 Apply-order rationale

**188b → 186b → 185b → 187**

- Staging / AJCC resolution on full malignant events precedes NIFTP/UMP **DELETE**.
- **186b** temporarily leaves rollup `n_tumors_total` on `COUNT(*)` semantics; **185b** restores distinct-grain rollup + optional event grain flag.
- **187** + Script **366** + **171b** replay consume extended exam master; run last to avoid rebuilding LN v2 twice.

## §4 Risk register

| ID | Risk | Mitigation |
| --- | --- | --- |
| R1 | Double provenance `INSERT` on re-run | Path-C deletes `run_id` before **185b** / **186b** / **188b** inserts, or skip §E when row exists. |
| R2 | Gate3 drift after registry `UPDATE` | **186b §I** and runbook gate3 after each lane; halt if non-zero. |
| R3 | T0 + PM rollup `arg_max` ordering | Existing severity ordering; **stage_group** may remain **NULL** for T0 — intentional pending Logan ambiguous-bucket curation. |
| R4 | Archive table name drift vs first apply | **186b** / **185b** use new `*_pre_mig186b_*` / `*_pre_mig185b_*` archive names to avoid collisions with earlier attempts. |

---

## §5 Artifacts

- `qc_framework_v1/migrations/185b_apply_rollup_only_patch_no_transaction_20260430.sql`
- `qc_framework_v1/migrations/186b_apply_RD_niftp_exclusion_no_gate3_break_20260430.sql`
- `qc_framework_v1/migrations/188b_mig184_v2_plus_r1c_with_explicit_T0_20260430.sql`
- `qc_framework_v1/runbooks/COWORK_APPLY_RUNBOOK_188_186_185_187_20260430.md`
- `qc_framework_v1/reports/mig_192_apply_readiness_patches_20260430.md` (this file)

---

End of report.
