# mig_205 close-out — US gland v2 signoff registry inserts (retro governance for mig_194 Cursor-applied lane)

**Date:** 2026-04-30
**Lane:** Cowork-direct apply per Logan authorization (this round's "if you can safely and effectively do so you can do it too")
**Final HEAD:** TBD post-commit
**Predecessor lanes verified clean this round:** mig_191 + mig_193 + mig_198 (= mig_194 file) + mig_201 + mig_202 + mig_203 + mig_204
**5-gate audit (post-mig_205):** **174 / 0 / 0 / 0 / 0** ✓
**Cohort parity:** **10,871 / 10,871** ✓

---

## §1 Background — gap surfaced during this round's Path-C verification

mig_194 (Cursor-applied at commit `3cdb804`) built three new tables in `thyroid_canonical_publication_v1_0.main`:

| Table | Rows | Purpose |
|---|---:|---|
| `canonical_us_thyroid_gland_events_v2` | 13,578 | Tier-2 event-grain canonical (shell-only Option B) |
| `canonical_us_thyroid_gland_patient_rollup_v2` | 10,871 | Tier-2 patient-rollup spanning CPM spine |
| `val_mig194_canonical_us_thyroid_gland_shell_only_v1` | 10 | 10-gate validation table |

The 28 mig_194 batch_id rows in `canonical_column_verification_registry_v1` correctly closed CF-117-US-GLAND-PARENCHYMA at the **shell** table (`canonical_us_thyroid_gland_v2`) column level. **However, the three new derivative tables themselves were never registered** in `canonical_table_signoff_registry_v1` or `canonical_column_verification_registry_v1`. This left them functionally outside QC governance — they would not be counted in gate1 / gate3 / gate4 / gate5 audits.

mig_205 closes this governance gap by mirroring the mig_171b LN v2 family pattern (canonical_us_lymph_node_events_v2 + patient_rollup_v2 + val_mig171b).

---

## §2 What mig_205 did

| Block | Operation | Result |
|---|---|---|
| §A | Pre-snapshot `canonical_table_signoff_registry_v1` (183 rows) → `archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig205_20260430` | 183 rows archived ✓ |
| §B | INSERT 3 signoff_registry rows (events_v2 verified, rollup_v2 verified, val_mig194 na) | 3 rows inserted ✓ |
| §C | INSERT column-verification rows for events_v2 (38) + rollup_v2 (22) | 60 rows inserted ✓ |
| §D | INSERT column-verification rows for val_mig194 (7, all status='na') | 7 rows inserted ✓ |
| §E | Provenance row insert for `mig_205_us_gland_v2_signoff_registry_inserts_20260430` | 1 row ✓ |

**Total registry mutations:** 3 signoff_registry + 67 column_verification_registry + 1 provenance = 71 rows. No data table writes.

**Verification methods chosen** (mirrors mig_171b convention):
- `canonical_us_thyroid_gland_events_v2` cols → `derivation_vs_canonical_us_thyroid_gland_v2_shell_only_option_b`
- `canonical_us_thyroid_gland_patient_rollup_v2` cols → `derivation_vs_canonical_us_thyroid_gland_events_v2_cpm_spine`
- `val_mig194_*` cols → `helper_validation_table_na`

**Category assignment:**
- identifier: `research_id`, `us_exam_id`, `gland_event_id`, `source_row_id`, `source_report_id`
- provenance: `build_ts`, `build_migration`, `extracted_at`, `llm_model`, `source_modality`, `source_note_type`, `source_table`, `exam_id_source`, `exam_date_unavailable_fallback_flag`, `date_confidence`, `date_source_keyword`, `confidence`, `evidence_text`, `gland_entity_index_within_exam`
- temporal: `exam_date`, `first_us_gland_exam_date`, `last_us_gland_exam_date` (all DATE-typed; clean per `feedback_clinical_dates_calendar_only.md`)
- analytic: all remaining clinical/measurement cols

---

## §3 Pre/post state matrix

| Metric | Pre-mig_205 | Post-mig_205 | Δ |
|---|---:|---:|---:|
| signoff_registry total rows | 183 | 186 | +3 |
| gate1 (verified canonicals) | 172 | **174** | +2 |
| gate2 (verified w/ no signoff_migration) | 0 | 0 | — |
| gate3 (count mismatch on verified) | 0 | 0 | — |
| gate4 (verified w/o batch_id/method/by) | 0 | 0 | — |
| gate5 (date-type residual on verified) | 0 | 0 | — |
| mig_205 batch_id col rows | 0 | 67 | +67 |
| events_v2 row count | 13,578 | 13,578 | — |
| rollup_v2 patient parity | 10,871 | 10,871 | — |

---

## §4 Reusable patterns (1 new)

**Retro signoff via INSERT...SELECT FROM information_schema.columns** — When a Cursor lane creates new derivative tables but skips registry governance, a follow-up Cowork-direct retro-signoff lane can mirror the parent-family pattern (mig_171b for v2 ultrasound family) and use `INSERT INTO canonical_column_verification_registry_v1 SELECT FROM information_schema.columns WHERE table_name IN (...)`. This is robust against schema drift (auto-derives ordinal_position, data_type, column_name) and avoids 67-line VALUES blocks. CASE-based category assignment per the parent family's existing convention. Apply via `query_rw` block-by-block (MCP wrapper bans transactions).

---

## §5 Post-round publication state

After mig_205 + the 6 in-flight Cursor lanes that completed this round:

- **174/174** Tier-2 canonicals at `table_status='verified'` (was 172; +2 from this lane)
- **PM 1,606 v / 24 na / 0 not_started / 1,630 total** (mig_203 closed the 10 *_resolved gap)
- **5-gate audit: 174 / 0 / 0 / 0 / 0** (all gates fully clean)
- Cohort parity 10,871 / 10,871
- **CFs CLOSED this round:**
  - CF-117-US-GLAND-PARENCHYMA (mig_194 Option B; events_v2 + rollup_v2 now under governance via mig_205)
  - CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION (mig_202; Python source patched + Script 366 redeployed; live VIEW = 11,880/0 nulls/121 ln_nlp_only)
  - CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION (mig_203; v11 audit query w/ extended regex allowlist)
  - 4 disposition-C tags via mig_201 (mig_156b prm_high_risk + mig_163b any_recurrence + mig_160 lab_date_anchor + mig_154 margin_mm_retype)

---

## §6 Open items remaining for "fully verified to every CF"

| Item | Disposition | Owner |
|---|---|---|
| r1c bucket-3 (50 ambiguous PM-only-size events) | Logan adjudication of `r1c_disposition_ambiguous_pm_only.csv` (50 rows) | Logan |
| r1d adjudication CSV (387 candidate T4 events; 40 already caught by mig_188b §D) | Logan adjudication of `r1d_t4_invasion_post_mig188.csv` | Logan |
| r1e mixed-histology stage_group (168 events) | Logan adjudication of `r1e_mixed_histology_post_mig188.csv` | Logan |
| Methods section voice pass | Logan voice edit of `methods_section_starter.md` (~12 placeholders) | Logan |
| CF-117-US-EXAM-ID-PORTABILITY (US-nodule remaining; ~25 cols) | Future v2 lane (separate from US gland) | future round |
| 7 mid-tier CFs (mig_190 disposition B) | Manuscript supplementary appendix candidates; tag-only retain for trace | manuscript |

**No Cowork-direct apply work remains.** Manuscript readiness is fully ready for survival/recurrence/outcomes analyses.

---

End of mig_205 close-out.
