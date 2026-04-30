# Cowork Verification Suite — thyroid_canonical_publication_v1_0
**Last updated:** 2026-04-30 (post-mig_206 close-out, HEAD `8e8642b` or later)
**Purpose:** Standalone series of MotherDuck queries any future Cowork session can run to verify publication state + surface any drift or new issues. Each query has an expected result and a pass/fail criterion. **Read-only — no `query_rw` calls.**

---

## §0 First message to paste into a fresh Cowork chat (verbatim)

> Please run the verification suite at `/Users/ros/THyroid 2026/qc_framework_v1/queries/cowork_verification_suite_20260430.md` against MotherDuck DB `thyroid_canonical_publication_v1_0`. Execute each numbered §1 through §15 query in order, compare the actual result to the **EXPECTED** value, and produce a pass/fail report. Any FAIL should be surfaced with the actual vs expected delta + a hypothesis of what changed. If everything passes, the publication state is unchanged from the 2026-04-30 v11 round close-out (HEAD `8e8642b`). Report total pass/fail count at the end.

---

## §1 5-gate cleanliness audit (v11 query with extended allowlist)

```sql
WITH verified_tables AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
  WHERE table_status='verified' AND table_name LIKE 'canonical_%'
),
audit_allowlist AS (
  SELECT col_name FROM (VALUES
    ('build_ts'),('built_at'),('extracted_at'),('llm_build_ts'),('llm_extracted_at'),
    ('verified_ts'),('signed_off_ts'),('registered_ts'),('updated_at'),('created_at'),
    ('promoted_at'),('completed_at'),('started_at'),('ended_at'),('ingested_at_utc'),
    ('ingestion_date'),('lab_datetime'),
    ('cpm_built_at'),('rollup_built_at'),('resolved_at'),('reclassified_at')
  ) v(col_name)
)
SELECT
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified') AS gate1,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 WHERE table_status='verified' AND signoff_migration IS NULL) AS gate2,
  (SELECT COUNT(*) FROM main.canonical_table_signoff_registry_v1 t WHERE t.table_status='verified' AND (t.n_verified+t.n_na<>t.n_columns_total OR t.n_not_started<>0 OR COALESCE(t.n_failed,0)<>0)) AS gate3,
  (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 r JOIN main.canonical_table_signoff_registry_v1 t USING (schema_name,table_name) WHERE t.table_status='verified' AND r.verification_status='verified' AND (r.verified_by IS NULL OR r.batch_id IS NULL OR r.verification_method IS NULL)) AS gate4,
  (SELECT COUNT(*) FROM information_schema.columns c
     JOIN verified_tables v ON c.table_name=v.table_name
     LEFT JOIN main.canonical_column_verification_registry_v1 r ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
   WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
     AND c.column_name NOT IN (SELECT col_name FROM audit_allowlist)
     AND NOT regexp_matches(c.column_name, '_built_at$')
     AND NOT regexp_matches(c.column_name, '_derived_at$')
     AND NOT regexp_matches(c.column_name, '_resolved_at$')
     AND NOT regexp_matches(c.column_name, '_confidence$')
     AND c.column_name NOT LIKE '%_status' AND c.column_name NOT LIKE '%_source'
     AND c.column_name NOT LIKE '%_keyword' AND c.column_name NOT LIKE '%_raw'
     AND COALESCE(r.verification_status,'unknown')!='na'
     AND (c.data_type IN ('TIMESTAMP','TIMESTAMP WITH TIME ZONE')
          OR (c.data_type='VARCHAR' AND (regexp_matches(c.column_name,'(^|_)dates?(_|$)') OR regexp_matches(c.column_name,'(^|_)dt(_|$)'))))
  ) AS gate5;
```

**EXPECTED:** `gate1=174, gate2=0, gate3=0, gate4=0, gate5=0`
**FAIL CRITERIA:** Any value ≠ expected. gate1 may grow if new canonicals were verified post-handoff (+ is OK; -2 is a regression).

---

## §2 Cohort parity invariant

```sql
SELECT
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_patient_master) AS cpm_pts,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_thyroid_gland_patient_rollup_v2) AS us_gland_v2_pts,
  (SELECT COUNT(DISTINCT research_id) FROM main.canonical_us_lymph_node_patient_rollup_v2) AS us_ln_v2_pts;
```

**EXPECTED:** `cpm_pts=10871, us_gland_v2_pts=10871, us_ln_v2_pts=10871`
**FAIL CRITERIA:** Any ≠ 10,871. Cohort parity is a hard invariant.

---

## §3 PM backbone signoff math

```sql
SELECT n_verified, n_na, n_not_started, n_columns_total, table_status, signoff_migration
FROM main.canonical_table_signoff_registry_v1
WHERE table_name='canonical_patient_master';
```

**EXPECTED:** `n_verified=1606, n_na=24, n_not_started=0, n_columns_total=1630, table_status='verified'`
**FAIL CRITERIA:** Any field mismatch. n_verified+n_na must = n_columns_total.

---

## §4 AJCC resolution coverage on path_malignant_events_v1

```sql
SELECT
  COUNT(*) AS total_events,
  SUM(CASE WHEN ajcc_resolution_source IS NULL THEN 1 ELSE 0 END) AS n_null_source,
  SUM(CASE WHEN t_stage_ajcc8_resolved IS NOT NULL THEN 1 ELSE 0 END) AS n_t_resolved
FROM main.canonical_path_malignant_events_v1;
```

**EXPECTED:** `total_events=6469, n_null_source=0, n_t_resolved=6467` (2 NULL = niftp_excluded edge case)
**FAIL CRITERIA:** total_events drifts from 6,469 (mig_186b excluded 220 from original 6,689). n_null_source > 0 means a new event was inserted without AJCC resolution.

---

## §5 ajcc_resolution_source distribution

```sql
SELECT ajcc_resolution_source, COUNT(*) AS n_events, COUNT(DISTINCT research_id) AS n_pts
FROM main.canonical_path_malignant_events_v1
GROUP BY 1 ORDER BY n_events DESC;
```

**EXPECTED (event counts):**
| ajcc_resolution_source | n_events | n_pts |
|---|---:|---:|
| coalesce_size_greatest_dimension_cm_tumor_size_cm_per_surgery | 6,310 | 3,938 |
| prior_thy_recurrence_T_from_prior_path | 54 | 42 |
| ambiguous_pm_size_only_logan_pending | 50 | 42 |
| anaplastic_default_T4 | 25 | 25 |
| canonical_invasion_events_v1 | 15 | 9 |
| no_primary_at_this_surgery_pT0_unstaged | 13 | 9 |
| niftp_excluded | 2 | 2 |

**FAIL CRITERIA:** New source labels appear (signal a re-derivation lane ran), or counts drift outside ±5%.

---

## §6 Mixed-histology stage_group_resolved coverage

```sql
SELECT
  CASE WHEN ajcc8_stage_group_resolved IS NULL THEN '0_NULL_stage_group' ELSE 'has_stage_group' END AS bucket,
  COUNT(*) AS n
FROM main.canonical_patient_master
WHERE histologic_types_all LIKE '%|%'
GROUP BY 1;
```

**EXPECTED:** `has_stage_group=271, 0_NULL_stage_group=5`
**FAIL CRITERIA:** 0_NULL_stage_group > 5. The 5 expected NULL: 2 FTUMP|NIFTP (non-malignant per WHO 2017) + 3 PTC|FC with Nx (correctly uncalculable).

---

## §7 Invasion-evidence T-stage distribution (rule-conservatism check)

```sql
WITH r1d_pts AS (
  SELECT DISTINCT cie.research_id
  FROM main.canonical_invasion_events_v1 cie
  WHERE cie.invasion_type IN ('airway','tracheal','laryngeal','recurrent_laryngeal_nerve','rln','prevertebral','mediastinal','carotid','esophageal','gross_ete')
    AND cie.finding_status='present'
)
SELECT
  CASE
    WHEN pm.ajcc8_t_stage_resolved LIKE 'T4%' THEN '1_T4_resolved'
    WHEN pm.ajcc8_t_stage_resolved LIKE 'T3%' THEN '2_T3_lower'
    WHEN pm.ajcc8_t_stage_resolved LIKE 'T2%' THEN '3_T2'
    WHEN pm.ajcc8_t_stage_resolved LIKE 'T1%' THEN '4_T1'
    WHEN pm.ajcc8_t_stage_resolved IN ('T0','TX','Tx') THEN '5_T0_TX'
    WHEN pm.ajcc8_t_stage_resolved IS NULL THEN '6_NULL'
    ELSE '7_OTHER'
  END AS bucket,
  COUNT(*) AS n_pts
FROM r1d_pts r JOIN main.canonical_patient_master pm USING (research_id)
GROUP BY 1 ORDER BY 1;
```

**EXPECTED:** `1_T4_resolved=20, 2_T3_lower=799, 3_T2=63, 4_T1=106, 5_T0_TX=3, 6_NULL=78`
**FAIL CRITERIA:** Total ≠ 1,069 distinct invasion-evidence patients. The 78 NULL must all be `is_malignant=FALSE` (non-malignant cohort, no T-stage); confirm with §7b probe.

### §7b Confirm 78 NULL are non-malignant
```sql
WITH r1d_pts AS (
  SELECT DISTINCT cie.research_id FROM main.canonical_invasion_events_v1 cie
  WHERE cie.invasion_type IN ('airway','tracheal','laryngeal','recurrent_laryngeal_nerve','rln','prevertebral','mediastinal','carotid','esophageal','gross_ete')
    AND cie.finding_status='present'
)
SELECT pm.is_malignant, COUNT(*) AS n_pts
FROM r1d_pts r JOIN main.canonical_patient_master pm USING (research_id)
WHERE pm.ajcc8_t_stage_resolved IS NULL
GROUP BY 1;
```

**EXPECTED:** `is_malignant=FALSE → 78 pts` (single row)

---

## §8 NIFTP exclusion math

```sql
SELECT
  (SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1) AS post_exclusion_events,
  (SELECT COUNT(*) FROM main.canonical_path_indeterminate_events_v1) AS niftp_landing_events,
  (SELECT COUNT(*) FROM main.canonical_path_malignant_patient_rollup_v1) AS post_exclusion_pts;
```

**EXPECTED:** `post_exclusion_events=6469, niftp_landing_events=220, post_exclusion_pts=4022`
**FAIL CRITERIA:** Any drift signals re-deriv lane.

---

## §9 Source-distinct duplicate flag

```sql
SELECT
  is_source_distinct_duplicate_grain,
  COUNT(*) AS n_events
FROM main.canonical_path_malignant_events_v1
GROUP BY 1;
```

**EXPECTED:** `TRUE=525, FALSE/NULL=5944` (525 source-distinct dups preserved per mig_185b)
**FAIL CRITERIA:** TRUE count drifts from 525.

---

## §10 Exam master VIEW state (mig_187 R-A + mig_202 fix)

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE exam_date IS NULL) AS null_exam_dates,
  COUNT(*) FILTER (WHERE exam_id_source='ln_nlp_only') AS ln_nlp_only_count
FROM main.canonical_us_exam_master_VIEW_v2;
```

**EXPECTED:** `total_rows=11880, null_exam_dates=0, ln_nlp_only_count=121`
**FAIL CRITERIA:** Any drift. `null_exam_dates>0` indicates Script 366 regression (CF-mig187).

---

## §11 LN exam_id_source distribution

```sql
SELECT exam_id_source, COUNT(*) AS n FROM main.canonical_us_lymph_node_events_v2 GROUP BY 1;
```

**EXPECTED:** `exam_master_reused=6973` (single row; G9 PASS — no fallback IDs)
**FAIL CRITERIA:** Any value other than `exam_master_reused`. fallback IDs > 0 means LN-NLP integration broke.

---

## §12 Governance gap detector — tables in main not in signoff_registry

```sql
WITH all_main_tables AS (
  SELECT table_name FROM information_schema.tables
  WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_type='BASE TABLE'
    AND (table_name LIKE 'canonical_%' OR table_name LIKE 'val_%')
),
in_registry AS (
  SELECT table_name FROM main.canonical_table_signoff_registry_v1
)
SELECT t.table_name AS ungoverned_table
FROM all_main_tables t LEFT JOIN in_registry r USING (table_name)
WHERE r.table_name IS NULL
ORDER BY 1;
```

**EXPECTED:** 0 rows — every canonical_*/val_* in main has a signoff_registry row
**FAIL CRITERIA:** Any rows. New tables created without governance need a retro-signoff lane (per mig_205 pattern).

---

## §13 Provenance row presence for major migs

```sql
SELECT run_id, started_at
FROM manuscript_workspace.cpm_reconciliation_provenance_v1
WHERE run_id IN (
  'mig_201_disposition_c_cf_closure_apply_20260430',
  'mig_203_gate5_zero_audit_allowlist_extension_20260430',
  'mig194_us_thyroid_gland_shell_only_option_B_20260430',
  'mig_205_us_gland_v2_signoff_registry_inserts_20260430'
)
ORDER BY started_at DESC;
```

**EXPECTED:** All 4 rows present
**FAIL CRITERIA:** Any missing → provenance log lost.

---

## §14 Clinical date type sanity (per `feedback_clinical_dates_calendar_only.md`)

```sql
SELECT c.table_name, c.column_name, c.data_type
FROM information_schema.columns c
JOIN main.canonical_table_signoff_registry_v1 t USING (table_name)
WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
  AND t.table_status='verified'
  AND c.column_name SIMILAR TO '(.+_date|date_.+|surgery_date|fna_date|path_date|exam_date|finding_date|first_.+_date|last_.+_date|recurrence_date|first_followup_date|last_followup_date)'
  AND c.data_type NOT IN ('DATE')
  AND c.column_name NOT IN ('build_ts','build_migration','extracted_at','llm_extracted_at','registered_ts','signed_off_ts','verified_ts','ingestion_date','validated_at')
  AND NOT regexp_matches(c.column_name, '_at$')
  AND NOT regexp_matches(c.column_name, '_ts$')
ORDER BY c.table_name, c.column_name LIMIT 30;
```

**EXPECTED:** 0 rows — all clinical date cols on verified canonicals are DATE-typed
**FAIL CRITERIA:** Any rows = a TIMESTAMP/VARCHAR clinical date col slipped in. mig_160 / mig_160b retyped these; new ones need same treatment.

---

## §15 Recent registry write activity (post-handoff lane detector)

```sql
SELECT batch_id, COUNT(*) AS n_rows, MAX(verified_ts) AS latest
FROM main.canonical_column_verification_registry_v1
WHERE verified_ts > (NOW() - INTERVAL '7 days')
GROUP BY 1 ORDER BY MAX(verified_ts) DESC LIMIT 20;
```

**EXPECTED:** Most recent batch_id is `mig_205_us_gland_v2_signoff_registry_inserts_20260430` (or later if a new lane has run since handoff). All listed batch_ids should match a known committed migration.
**FAIL CRITERIA:** An unknown batch_id surfaces → either a Cursor parallel-lane race (apply without governance) or a phantom entry. Investigate via `SELECT * FROM main.canonical_column_verification_registry_v1 WHERE batch_id='<unknown>' LIMIT 5`.

---

## Final pass/fail report

After running §1–§15, produce a table:

| § | Check | Expected | Actual | Status |
|---|---|---|---|---|
| 1 | 5-gate audit | 174/0/0/0/0 | … | PASS/FAIL |
| 2 | Cohort parity | 10871/10871/10871 | … | PASS/FAIL |
| … | | | | |

**Overall:** `n_pass / 15 checks PASS`. Any FAIL → surface to Logan with hypothesis (most common: a new lane ran between sessions that updated counts).

---

## Reference: HEAD `8e8642b` baseline (2026-04-30 v11 round close-out)

If actual results match expected, the publication state is **identical** to baseline. Manuscript readiness = READY. No outstanding apply work; r1c/r1d/r1e investigation closed by mig_206 (rule-driven resolution accepted; CSVs at `exports/mig193_r1_adjudication_post_mig188_20260430/` available for optional spot-check).

---

End of verification suite.
