# mig_160b apply close-out — PM date cols retype (gate5 25 → 6)

**Date:** 2026-04-30
**Lane:** mig_160b / pm_date_cols_retype_close_gate5
**Cowork applied:** 2026-04-30 (Cowork-direct; Logan green-light on file from v10)
**Predecessor:** mig_160 (`16a9833`) — closed 21 clinical-date cols on Tier-2 canonicals + path_malignant + recurrence + frozen rollup + ETE.

---

## §1 Executive summary

- 21 PM cols retyped (18 clinical-date VARCHAR/TIMESTAMP → DATE; 2 audit TIMESTAMP-WITH-TZ → TIMESTAMP; the 1 still-VARCHAR `gm_rai_date_confidence` left alone — misnamed, actually a confidence string).
- 5-gate audit: 172 / 0 / 0 / 0 / **6** (was 25 pre-mig_160b).
- Cohort parity: 10,871 / 10,871 ✓
- PM `table_status='verified'` preserved; 1,596 v / 24 na / 0 not_started; total 1,620 unchanged.
- Pre-snapshot landed at `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre_mig160b_date_retype_20260430` (10,871 rows).
- Provenance row added to `manuscript_workspace.cpm_reconciliation_provenance_v1` for run_id `mig_160b_pm_date_cols_retype_20260430`.

---

## §2 Cols retyped

### §2.1 ISO VARCHAR → DATE (4)

| col | n_iso_pre | n_null_pre |
|---|---:|---:|
| cnln_earliest_date | 603 | 10,268 |
| cnln_img_first_date | 134 | 10,737 |
| cnln_img_last_date | 166 | 10,705 |
| cnln_surg_first_date | 484 | 10,387 |

USING `TRY_CAST(NULLIF(col, '') AS DATE)` to handle empty-string trap.

### §2.2 MM/DD/YYYY VARCHAR → DATE (7)

| col | n_mmdd_pre | n_null_pre |
|---|---:|---:|
| cnln_latest_date | 849 | 10,022 |
| cnln_surg_last_date | 721 | 10,150 |
| nsqip_admission_date | 1,035 | 9,836 |
| nsqip_discharge_date | 1,261 | 9,610 |
| nsqip_first_readmission_date | 29 | 10,842 |
| nsqip_operation_date | 1,261 | 9,610 |
| ops_surg_date | 8,731 | 2,140 |

USING `TRY_STRPTIME(col, '%m/%d/%Y')::DATE`.

### §2.3 TIMESTAMP → DATE (8)

`first_recurrence_date`, `first_surgery_date`, `last_contact_date`, `mol_first_test_date`, `mol_test_date`, `rai_first_date`, `recurrence_date`, `surg_first_date`. USING `CAST(col AS DATE)`.

### §2.4 TIMESTAMP-WITH-TZ → TIMESTAMP (2)

`resolved_at`, `rollup_built_at`. USING `CAST(col AS TIMESTAMP)`. Drops TZ silently per `reference_duckdb_timestamp_tz` to avoid the silent pytz-dep trap. These remain audit/build stamps; not retyped to DATE because their semantics include time-of-day.

---

## §3 Cols left unchanged (5 audit + 1 misnamed)

These trip gate5 due to audit-query allowlist gaps, not data issues. Each is correctly typed for its semantics.

| col | data_type | rationale |
|---|---|---|
| cpm_built_at | TIMESTAMP | Build/audit stamp; correctly TIMESTAMP. Not in audit allowlist (allowlist has `built_at` but not `cpm_built_at`). |
| gm_path_stage_raw_derived_at | TIMESTAMP | Derivation/audit stamp; correctly TIMESTAMP. Not in audit allowlist (`_derived_at` pattern unrecognized). |
| path_stage_raw_derived_at | TIMESTAMP | Same as above. |
| resolved_at | TIMESTAMP | After §2.4 retype. Audit stamp; correctly TIMESTAMP. Not in audit allowlist (`_resolved_at` pattern unrecognized). |
| rollup_built_at | TIMESTAMP | After §2.4 retype. Build stamp; correctly TIMESTAMP. Not in audit allowlist. |
| gm_rai_date_confidence | VARCHAR | NOT a date column. Holds confidence floats `0.5`, `0.6`, etc. Name contains `date` substring matching the audit regex. Misnamed; correct fix is rename to `gm_rai_date_resolution_confidence_score` or add allowlist exclusion. |

---

## §4 Carry-forward: audit allowlist extension for v11

To get gate5 to **true 0**, update the audit query allowlist to include:

- Suffix patterns: `_built_at`, `_derived_at`, `_resolved_at` (these are conventionally audit/build stamps in this lakehouse)
- Substring exclusion: `_confidence` (confidence-score cols are not dates regardless of name)

OR rename `gm_rai_date_confidence` → `gm_rai_date_resolution_confidence_score` (or similar) and add the 5 audit cols to the explicit allowlist VALUES list.

CF tag: `CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION` (informational; does not block manuscript).

---

## §5 Pre/post 5-gate audit

| Gate | Pre | Post | Threshold |
|---|---:|---:|---:|
| gate1 (verified canonicals) | 172 | 172 | ≥ 169 |
| gate2 (verified without signoff_migration) | 0 | 0 | 0 |
| gate3 (verified arithmetic check) | 0 | 0 | 0 |
| gate4 (verified cols missing metadata) | 0 | 0 | 0 |
| gate5 (date type residual) | 25 | **6** | 0 (target) |

---

## §6 Registry note appendix

21 col rows in `canonical_column_verification_registry_v1` (table_name='canonical_patient_master') had this note appended:

> | mig_160b 2026-04-30: VARCHAR/TIMESTAMP/TIMESTAMP_TZ retyped per feedback_clinical_dates_calendar_only + reference_duckdb_timestamp_tz; gate5 close-out.

CFs partially or fully closed:
- CF-90-DATE-FORMAT (38 cols total; 25 PM cols closed here; 13 elsewhere were closed by mig_160)
- CF-mig137-PM-MOL-DATE-RETYPE (25 cols incl. mol_*; subset closed; remainder is on canonical_molecular_genetics_v2 already)
- CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE (first_surgery_date + nsqip + ops_surg_date)
- CF-mig133-PM-CNCLN-DATE-PARSE (6 cnln_* cols)
- CF-mig142 (rai_first_date)
- CF-mig157 (first_recurrence_date, last_contact_date)
- CF-mig155-RESOLVED-LAYER-VERSION-DEGENERATE / CF-mig155-DATE-RETYPE-CLEAR (resolved_at)
- CF-mig138 (recurrence_date)

---

## §7 Reusable patterns

1. **Empty-string trap on VARCHAR → DATE retype.** `CAST(col AS DATE)` fails on empty strings even when the col is otherwise pure ISO format. Always wrap with `TRY_CAST(NULLIF(col, '') AS DATE)` for VARCHAR retypes.
2. **Format probe BEFORE retype.** Use a per-col regex match (`'^\d{4}-\d{2}-\d{2}$'` vs `'^\d{2}/\d{2}/\d{4}$'`) on each VARCHAR to confirm internal consistency. Mixed-format cols would need branching `CASE WHEN regex THEN ... ELSE TRY_STRPTIME(...) END` logic.
3. **TZ-strip vs DATE-truncate.** Audit/build stamps that are TIMESTAMP TZ should be retyped to TIMESTAMP (no TZ), not to DATE — the time-of-day component is meaningful for build provenance. Clinical-date cols should be DATE.
4. **Audit-allowlist extension as v11 follow-up.** When gate5 residuals are correctly-typed audit cols rather than data-quality issues, the proper fix is audit query allowlist extension, not data mutation. Document the gap; don't force gate5 to 0 by mis-typing audit stamps.

---

## §8 Cowork apply trace

All 4 phases ran via `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query_rw` (one statement per call):

1. §A pre-snapshot — 1 statement; 10,871 rows landed.
2. §B–§E ALTER COLUMN — 21 statements; all succeeded after switching from `CAST` to `TRY_CAST(NULLIF(col,''))` for the 4 ISO VARCHAR cols (initial CAST failed on empty strings).
3. §F registry update — 1 statement; 21 rows updated.
4. §G provenance insert — 1 statement; 1 row inserted.
5. §H post-state probe — gate5 = 6.

No transactions used (MD MCP wrapper is one-statement-per-call).

---

End of close-out.
