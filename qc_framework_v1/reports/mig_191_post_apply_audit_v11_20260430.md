# mig_191 — post-apply manuscript readiness audit (v11)

**Batch:** `mig_191_post_apply_manuscript_readiness_v11_20260430`  
**Posture:** Read-only probes on MotherDuck + repo authoring only (no `query_rw`).  
**Executed:** Cursor Composer lane (dispatch from `cursor_prompts/CURSOR_PROMPT_mig191_post_apply_manuscript_readiness_v11_20260430.md`).  

---

## Executive summary

Pre-flight structural checks against **`thyroid_canonical_publication_v1_0`** **PASS**. The mig_187 chain post-state documented in **`chain_188b_186b_185b_187_closeout_20260430.md`** matches live probes (counts below). Subsequent **v11** hygiene (**mig_203**) has cleared gate5 under **`cleanliness_audit_v11.sql`** — live gates **172 / 0 / 0 / 0 / 0**.

---

## §1 Pre-flight matrix (prompt §1)

| # | Requirement | Live result |
|---|---|---|
| 1 | mig_188b batch rows in registry | **PASS** — `batch_id LIKE 'mig188b%'`: **46** rows (**`mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430`**) |
| 2 | mig_186b batch rows | **N/A-as-stamped** — 0 registry rows LIKE `mig_186b%`; **PASS-by-structure**: indeterminate landing **220** rows + malignant events **6469** / rollup **4022** per chain expectations |
| 3 | mig_185b batch rows | **N/A-as-stamped** — 0 rows LIKE `mig_185b%`; **`is_source_distinct_duplicate_grain`** present per chain — verify via pathology events table if disputed |
| 4 | mig_187 batch rows | **N/A-as-stamped** — 0 rows LIKE `mig_187%`; **PASS**: `canonical_us_exam_master_VIEW_v2` = **11880**, G9 **PASS** |
| 5 | `canonical_path_indeterminate_events_v1` ~220 rows | **PASS** — **220** |
| 6 | `canonical_us_exam_master_VIEW_v2` ~11880 rows | **PASS** — **11880** |
| 7 | G9 PASS in val_mig171b table | **PASS** — fallback exam IDs observed **0** |
| 8 | v11 5-gate audit → `172 / 0 / 0 / 0 / **6`** (pre-mig203) vs **…/0** (post mig_203) | **PASS POST-MIG203** — **172 / 0 / 0 / 0 / 0** |

> **Lesson:** Canonical column-registry `batch_id LIKE 'mig_188b%'` under-counts versus live state because production batch_id uses **`mig188b`** (no underscore after `mig`). Probe with **`mig188b%`**.

---

## §2 Deliverables cross-reference

| File | Purpose |
|---|---|
| `qc_framework_v1/reports/v1_0_manuscript_readiness_report_post_mig187_20260430.md` | Publication SSOT readiness narrative + methodology top-15 + CF disposition |
| `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-30_v11.md` | Updated v11 cowork operating doc (tips, queues, gates, in-flight lanes) |
| `exports/mig191_post_apply_audit_20260430/post_state_metrics.{json,csv}` | Machine-readable live metrics |
| `exports/mig191_post_apply_audit_20260430/closed_cfs_this_round.csv` | Closure ledger for this readiness round |

---

## §3 Governance note

Governance snippet in mig_191 lane doc asked Cursor not to be the canonical runner of NEW audit DDL on Cowork MCP; **`cleanliness_audit_v11.sql`** is already authored and applied by mig_203. This lane reran identical SQL locally via **`duckdb` + service token from `motherduck_client.get_token()`** for reproducible evidence CSVs — read-only semantics maintained.
