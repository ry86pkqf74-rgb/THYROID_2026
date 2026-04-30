# mig_172b apply close-out

**Date:** 2026-04-29 (very late evening)
**Cursor commit:** `53c3fdb` (SQL + audit + CSV rewrite Python — governance-compliant; no MD writes)
**Cowork apply:** Path-C executed 2026-04-29; data writes on 4 PM histology cols + 1 new map table
**Cursor prompt:** `cursor_prompts/CURSOR_PROMPT_mig172b_vocab_apply_post_mig178_20260429.md` (Cowork-authored)

---

## §1 Executive summary

Closed `CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES` and reaffirmed `CF-mig172-MTC-PTC-MIXED-REJECT` (Logan-rejected `mtc_ptc_mixed` already absent post-mig_178; mig_172b's CSV omits the rejected code). 4 PM histology cols normalized; distinct value cardinality reduced from 94 → 34 across the family.

---

## §2 Apply trace

| Step | Action | Result |
|---|---|---|
| §A | 4 pre-snapshots → archive_pub_v1_0 | 10,871 rows × 4 ✓ |
| §B | CTAS `main.histology_vocab_normalization_map_v1` | 96 rows |
| §B | Validation: 0 mtc_ptc_mixed; 0 unmapped raw values across all 4 cols | ✓ |
| §C.1 | UPDATE recurrence_histology | 440 rows |
| §C.2 | UPDATE recurrence_histology_v2 | 118 rows |
| §C.3 | UPDATE completion_prior_histology | 385 rows |
| §C.4 | UPDATE completion_histology_type | 188 rows |
| §D.1 | Registry note appendix on 4 col rows | 4 rows |
| §D.2 | cpm_built_at refresh | 10,871 rows |
| §D.3 | INSERT cpm_reconciliation_provenance_v1 | 1 row |

---

## §3 Post-state verification

| Gate | Expected | Actual | Status |
|---|---|---|---|
| recurrence_histology distinct | 11 | 11 | ✓ |
| recurrence_histology_v2 distinct | 8 | 8 | ✓ |
| completion_prior_histology distinct | 10 | 10 | ✓ |
| completion_histology_type distinct | 5 | 5 | ✓ |
| mtc_ptc_mixed in any target col | 0 | 0 | ✓ |
| Cohort parity | 10,871 / 10,871 | 10,871 / 10,871 | ✓ |
| null cpm_built_at after refresh | 0 | 0 | ✓ |
| 5-gate audit | 169 / 0 / 0 / 0 / 21 | 169 / 0 / 0 / 0 / 21 | ✓ unchanged |

---

## §4 Closures

- **CF-mig168-VOCAB-DRIFT-RECURRENCE-HISTOLOGY-42-VALUES** — closed (recurrence_histology 42→11 distinct values)
- **CF-mig172-MTC-PTC-MIXED-REJECT** — reaffirmed (Logan-rejected code absent from CSV; affected raw values mapped to `MTC | PTC` per mig_178 convention)

No new CFs opened.

---

## §5 Pre-snapshot inventory (recoverable)

- `archive_pub_v1_0.canonical_patient_master_recurrence_histology_pre_mig172b_20260429`
- `archive_pub_v1_0.canonical_patient_master_recurrence_histology_v2_pre_mig172b_20260429`
- `archive_pub_v1_0.canonical_patient_master_completion_prior_histology_pre_mig172b_20260429`
- `archive_pub_v1_0.canonical_patient_master_completion_histology_type_pre_mig172b_20260429`

---

End of close-out.
