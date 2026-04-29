# mig_179 + mig_179b apply close-out — canonical_invasion_events_v1 LVI rebuild

**Date:** 2026-04-29 (very late evening)
**Lane:** 68 / mig_177-events-rebuild + Cowork follow-up
**Commits:** mig_179 SQL/script/audit at `8d858b2` (Cursor); mig_179b notes-only at <commit-pending>
**Apply governance:** Cowork executed Path C; Cursor honored AGENTS-governance (no MD execution from Cursor)

---

## §1 Executive summary

The CAP synoptic combined-field LVI extractor bug (CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS) is now fixed. `canonical_invasion_events_v1` grew from 51,751 → **58,582 rows** by adding 6,831 supplemental `structured_mig179` rows extracted directly from `path_synoptics`. `canonical_invasion_patient_rollup_v1` was rebuilt from refreshed events (10,871 / 10,871 ✓ cohort parity preserved).

**Lymphatic_microscopic PRESENT:** 1,233 → **5,969 rows** / 780 → **989 patients** (+209 patients).
**Vascular_microscopic PRESENT:** 2,883 → **4,978 rows** / 1,109 → **1,178 patients** (+69 patients via combined-CAP duplication).
Capsular + perineural axes unchanged (2,136 / 360 PRESENT).

---

## §2 Path-C apply trace

### §2.1 Pre-flight verification (Cowork, before any query_rw)

- ✓ Schema: 20 cols match between live `canonical_invasion_events_v1` and `supplemental_events` SELECT
- ✓ Cohort: events table spans 10,871 distinct RIDs → POST_ROLLUP_ROW_INVARIANT will pass
- ✓ Pre-snapshot tables don't exist yet (first apply OK)
- ✓ Idempotency clause works: no current rows have `source_kind='structured_mig179'`
- ✓ Live MD baseline matches Cursor prompt — Cursor honored AGENTS-governance (no MD writes)
- ⚠️ TYPE BUG identified in §D INSERT: `feeds_master_columns_array` was cast as `NULL::VARCHAR` but schema is `VARCHAR[]` — patched in-flight to `NULL::VARCHAR[]`
- ⚠️ Pattern probe: 541 combined_lymphovasc + 204 angiolymphatic + 1 lymphangitic + 757 structured-tumor1 patients in path_synoptics raw text

### §2.2 Apply sequence (~11 query_rw calls executed)

| Step | Action | Result |
|---|---|---|
| §A.1 | Pre-snapshot events → archive | 51,751 rows ✓ |
| §A.2 | Pre-snapshot rollup → archive | 10,871 rows ✓ |
| §A.3 | Snapshot parity verify | PASS |
| §B.1 | Stage CTAS with supplemental LVI re-extract | 58,582 rows |
| §B.2 | CREATE OR REPLACE TABLE events FROM stage | 58,582 rows |
| §B.3 | DROP stage | success |
| §B.4 | COMMENT ON TABLE events | success |
| §C/E.1 | CREATE OR REPLACE TABLE rollup FROM events GROUP BY rid | 10,871 rows |
| §C/E.2 | COMMENT ON TABLE rollup | success |
| §D.1 | DELETE 2 detail_table_registry rows | 2 deleted |
| §D.2 | INSERT events row (patched type) | 1 inserted |
| §D.3 | INSERT rollup row (patched type) | 1 inserted |

### §2.3 Post-state gates (all PASS)

| Gate | Result | Threshold |
|---|---|---|
| POST_CPM_ROW_INVARIANT | 10,871 / 10,871 | =10,871 |
| POST_MIN_VASCULAR_PRESENT_GATE | 4,978 / 1,178 | ≥ 2,883 / 1,109 |
| POST_LYMPHATIC_GROWTH_GATE | 5,969 / 989 | > 1,233 / 780 |
| POST_ROLLUP_ROW_INVARIANT | 10,871 / 10,871 | =10,871 |
| POST_CAPSULAR_PERINEURAL_UNCHANGED | 2,136 / 360 | =2,136 / =360 |
| POST_ROLLUP_REDERIVED_FROM_EVENTS | 0 / 0 mismatches | =0 |

### §2.4 Dependent VIEW smoke test (PASS)

- `views_readable.invasion_events_VIEW_v1`: 58,582 rows ✓
- `views_readable.invasion_patient_rollup_VIEW_v1`: 10,871 rows ✓

DuckDB lazy view resolution handled the CREATE OR REPLACE TABLE transparently.

### §2.5 5-gate audit before/after

Unchanged: **169 / 0 / 0 / 0 / 21** — mig_179 introduced no governance debt. Both target canonicals remain `verified` (no schema change; per-column verification logic still holds).

---

## §3 mig_179b — CF closures (registry-only, Cowork-authored)

mig_179b appended closure notes on:
- 11 events col-rows (closes CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS, LYMPHATIC_PRESENT_SEPARATE_MISS, VOCAB-FOACL/EXTRENSIVE/INDETERMINENT/CA-X)
- 44 rollup col-rows (closes CF-mig177-ROLLUP-VASC-ALIAS-LVI)
- 2 signoff registry rows (build provenance + pre-snapshot pointers)

Pre-snapshot at `archive_pub_v1_0.canonical_column_verification_registry_pre_mig179b_20260429` (67 rows).

---

## §4 Carry-forwards (open)

- **CF-mig177-PM-VASC-ALIAS-LVI** (196 patients) — NOT closed by mig_179. Remains open until **mig_177b** PM `lvi_*` rederive lands.
- **CF-mig179-COMBINED-CAP-VASC-DUPLICATION** (NEW, informational) — mig_179 supplemental_events emits both `vascular_microscopic` AND `lymphatic_microscopic` for combined-CAP patterns. Existing extractor already emitted vasc structured rows; the supplemental adds duplicate vasc rows from text patterns. Net: vasc PRESENT row count grew ~70%. Patient-level rollup unaffected (BOOL_OR semantic). Future canonical_invasion_events_v2 rebuild may want to dedupe by (rid, finding_date, tumor_index, invasion_type).

---

## §5 Files committed in this lane

- `qc_framework_v1/migrations/179_canonical_invasion_events_v1_rebuild_lvi_20260429.sql` (Cursor)
- `qc_framework_v1/reports/mig_179_invasion_events_rebuild_audit_20260429.md` (Cursor)
- `scripts/363_invasion_canonical.py` (Cursor — 247 lines added for path_synoptics_lvi_reextract lane)
- `qc_framework_v1/migrations/179b_mig179_cf_closures_and_build_notes_20260429.sql` (Cowork)
- `qc_framework_v1/reports/mig_179_apply_closeout_20260429.md` (Cowork — this file)
- `cursor_prompts/CURSOR_PROMPT_mig180_pm_nlp_cluster_verify_apply_20260429.md` (Cowork — for next round)
- `cursor_prompts/CURSOR_PROMPT_mig172b_vocab_apply_post_mig178_20260429.md` (Cowork — for next round)

---

## §6 Next steps

1. **mig_177b** (Cowork-direct, ~6 query_rw) — PM `lvi_*` rederive from refreshed events; closes CF-mig177-PM-VASC-ALIAS-LVI (196 pts)
2. **§8 retro Path-C audits** of mig_178 / 173b / 163b — outstanding governance debt
3. **mig_180** (Cursor lane) — PM `nlp_*` cluster verify+apply (~116 cols)
4. **mig_172b** (Cursor lane) — vocab CSV rewrite + apply for recurrence + completion family
5. **mig_171b** (awaiting Logan ratification) — canonical_us_lymph_node_v2 build
6. **mig_174b** (awaiting Logan ratification) — cnln_img_laterality per-side BOOLEAN apply
7. **mig_160** (Cowork-direct, structural) — global clinical-date retype; closes ~190 col-impact CFs + gate5 21→0

---

End of close-out.
