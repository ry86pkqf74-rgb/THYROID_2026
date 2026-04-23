# Script 399 — Malignant NULL `stage_group` close-out (CF-397-1) — REVISED with queue-schema fix

**Runner:** `scripts/apply_malignant_null_stage_group_closeout.py`
**Close-out:** `cursor_prompts/CLOSE_OUT_399.md` (after apply)

**REVISION NOTE (2026-04-23, pre-apply):** Scope expanded to include a schema fix on `manuscript_workspace.cpm_stage_group_manual_review_v1`. The queue table was created by Script 395 with columns for `ajcc8_n_stage` and `ajcc8_m_stage` but NOT `ajcc8_t_stage`, forcing T context into the `reason` string. This script now:

1. `ALTER TABLE manuscript_workspace.cpm_stage_group_manual_review_v1 ADD COLUMN ajcc8_t_stage VARCHAR` (idempotent — skip if column already exists)
2. Backfill the 2 pre-existing queue rows (1404, 12198) with their CPM `ajcc8_t_stage` values (both NULL per current CPM state)
3. The 6 new queue INSERTs now include structured `ajcc8_t_stage` values: 4015=T2, 9600=T1b, 423=NULL, 924=T3b (primary; v2 disagreement T1a noted in reason), 6275=NULL, 6768=T1a
4. The 2 CPM UPDATEs (rid 111 → I, rid 106 → I) are unchanged
5. Snapshot in `archive_pub_v1_0` covers the 8 CPM rows touched (unchanged)

**Dependents safety check** (repo grep): `cpm_stage_group_manual_review_v1` is referenced only by Script 395 (writer, explicit column list in INSERT), Script 396 (docs reference only), and Script 399 (this writer). No views, no SELECT * consumers with strict schema assumptions. Additive column add is safe.

Phase 0 probe, `--i-approve` gate, schema ALTER + 2 backfill UPDATEs + 2 CPM UPDATEs + 6 queue INSERTs + snapshot + `__readme` provenance (`script_399`).

### Writes summary (revised)

**Write S-1 — Schema ALTER (idempotent):**
```sql
-- Skip if column already exists. DuckDB DDL:
ALTER TABLE manuscript_workspace.cpm_stage_group_manual_review_v1
  ADD COLUMN IF NOT EXISTS ajcc8_t_stage VARCHAR;
```
If `ADD COLUMN IF NOT EXISTS` is not supported in the target DuckDB version, guard via `information_schema.columns` pre-check.

**Writes S-2, S-3 — Queue backfill for existing 2 rows:**
```sql
-- rid 1404: CPM ajcc8_t_stage is NULL
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET ajcc8_t_stage = (SELECT ajcc8_t_stage FROM main.canonical_patient_master WHERE research_id='1404')
WHERE research_id='1404' AND source_script='395';

-- rid 12198: CPM ajcc8_t_stage is NULL
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET ajcc8_t_stage = (SELECT ajcc8_t_stage FROM main.canonical_patient_master WHERE research_id='12198')
WHERE research_id='12198' AND source_script='395';
```
Both backfills are expected to write NULL (since both PTC rows currently have CPM `ajcc8_t_stage IS NULL`). The UPDATE is idempotent and should execute even on NULL writes — this ensures the column is formally populated rather than NULL-by-default.

**Writes A-1, A-2 — CPM UPDATEs (unchanged from original prompt):**
- rid 111 DTC_NOS T1b N1a M0 age 28 → Stage I (DTC age<55 M0 rule)
- rid 106 MTC T1b N0 M0 age 60 → Stage I (MTC AJCC8 T1 N0 M0 rule)

**Writes B-1 through B-6 — Queue INSERTs (revised with ajcc8_t_stage column):**
```sql
INSERT INTO manuscript_workspace.cpm_stage_group_manual_review_v1
  (research_id, reason, path_stage_raw, gm_path_stage_raw,
   ajcc8_t_stage, ajcc8_n_stage, ajcc8_m_stage,
   age_at_surgery, diagnosis_primary, source_script, inserted_at)
VALUES
  ('4015',
   'mtc_t2_n1a_m0_rule_yields_iii_no_builder_or_path_corroboration',
   NULL, NULL, 'T2', 'N1a', 'M0',
   72, 'MTC', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('9600',
   'mtc_m1_ajcc8_rule_yields_ivc_but_builder_and_path_both_say_ivb_edition_adjudication_needed',
   'IVB', NULL, 'T1b', 'N0', 'M1',
   63, 'MTC', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('423',
   'mtc_t_null_cannot_derive_plus_builder_corrected_i_is_dtc_rule_misapplied_to_mtc_n1a_m0_row',
   NULL, NULL, NULL, 'N1a', 'M0',
   47, 'MTC', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('924',
   'mtc_multi_axis_primary_v2_disagreement_t3b_vs_t1a_n1a_vs_n1b_builder_and_path_both_i_no_combination_reconciles',
   'I', NULL, 'T3b', 'N1a', 'M0',
   33, 'MTC', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('6275',
   'other_malignant_staging_rules_undefined_t_null_n_disagreement_n0_vs_n1a',
   NULL, NULL, NULL, 'N0', 'M0',
   38, 'other_malignant', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP)),
  ('6768',
   'other_malignant_staging_rules_undefined_n_disagreement_n1a_vs_n0_path_ii',
   'II', NULL, 'T1a', 'N1a', 'M0',
   62, 'other_malignant', '399', CAST(CURRENT_TIMESTAMP AS TIMESTAMP));
```

Note: for rids 924, 6768 the `ajcc8_t_stage` stored is the CPM primary value. V2 disagreement context stays in `reason` string (v2 columns intentionally NOT added to queue schema to keep parity with N/M columns; CF-399-5 tracks adding v2 columns if needed later).

### New halt gates (added to H1–H8)

- **H9 — Queue schema pre-state:** either the column `ajcc8_t_stage` does NOT exist on `cpm_stage_group_manual_review_v1` (fresh ALTER path) OR it exists with type `VARCHAR` (idempotency-resume path). FAIL on any other schema shape.
- **H10 — Dependents safety (static):** no new references to the queue table's columns found in repo outside the known writers (395, 396, 399). Script should re-grep during Phase 0 and FAIL if unexpected references appear.

### New Phase 3 gates

- **P9 — Queue has T column:** `information_schema.columns` confirms `ajcc8_t_stage VARCHAR` on `cpm_stage_group_manual_review_v1`.
- **P10 — Queue rows correct:** after apply, all 8 queue rows have expected `ajcc8_t_stage`:
  - 1404 → NULL (backfilled from CPM NULL)
  - 12198 → NULL (backfilled from CPM NULL)
  - 4015 → 'T2'
  - 9600 → 'T1b'
  - 423 → NULL
  - 924 → 'T3b'
  - 6275 → NULL
  - 6768 → 'T1a'

### Idempotency update

Treat as applied iff all of:
1. Snapshot `archive_pub_v1_0.cpm_pre_malignant_null_stage_group_closeout_*` exists
2. `__readme script='script_399'` row exists
3. Queue table has `ajcc8_t_stage` column
4. rids 111 and 106 both have `ajcc8_stage_group='I'` in CPM
5. All 6 new queue rids (4015, 9600, 423, 924, 6275, 6768) present with `source_script='399'`
6. All 8 queue rows (including backfilled 1404, 12198) have `ajcc8_t_stage` populated per expectations above

If applied → NO-OP, Phase 3 verify only, no close-out overwrite.

### Close-out additions (`cursor_prompts/CLOSE_OUT_399.md`)

- Schema ALTER note: `ajcc8_t_stage VARCHAR` added to queue table
- Backfill summary: 1404 and 12198 both NULL
- New queue INSERTs with structured T values (not just text-encoded in reason)
- Updated CF list:
  - **CF-399-1** (unchanged): MTC rows possibly mis-staged under DTC rules — audit scope
  - **CF-399-2** (unchanged): AJCC8 MTC M1 edition authority for rid 9600
  - **CF-399-3** (unchanged): other_malignant staging framework spec
  - **CF-399-4** (unchanged): 1404/12198 chart-review from CF-395-1
  - **CF-399-5** (new): consider adding v2 and dominant columns to queue table for fuller disagreement context; currently encoded in reason strings for 924 / 6768

---

## Original prompt (unchanged sections below)

Phase 0 probe, `--i-approve` gate, 2 CPM `UPDATE`s (111, 106), 6 `manuscript_workspace.cpm_stage_group_manual_review_v1` inserts, `archive_pub_v1_0` snapshot (8 rows), and `__readme` provenance (`script_399`).

See project chat and probe report under `scripts/output/apply_malignant_null_stage_group_closeout_probe.md`.
