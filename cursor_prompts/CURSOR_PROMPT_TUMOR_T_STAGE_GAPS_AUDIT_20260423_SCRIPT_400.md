# CURSOR COMPOSER 2.0 PROMPT — Script 400 — Thyroid tumor T-stage gap audit log (read-only, 9-row sidecar)

**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck cloud DuckDB)
**Auth:** `.env.motherduck` → `MOTHERDUCK_TOKEN`
**Mode:** Phase-gated runner (Phase 0 probe → plan-approval → `--apply` → Phase 3 verify → Phase 4 commit+tag+push)
**Verbal-gate formalization:** `--i-approve=<probe_report_sha256>` required for `--apply`
**CPM invariant:** `main.canonical_patient_master` row count = **10,871** (must hold throughout — this script does NOT write to CPM)
**Repository:** `/Users/loganglosser/THYROID_2026`
**Script path:** `scripts/apply_tumor_t_stage_gaps_audit.py` (new)
**Tag-prefix:** `v1_0-tumor-t-stage-gaps-audit-`
**Close-out path:** `cursor_prompts/CLOSE_OUT_400.md`
**Run-log path:** `scripts/output/apply_tumor_t_stage_gaps_audit_run.log`
**Probe-report path:** `scripts/output/apply_tumor_t_stage_gaps_audit_probe.md`
**Target new table:** `manuscript_workspace.cpm_tumor_t_stage_gaps_v1`

---

## Context — structured log of T-NULL thyroid tumors

Post-399, the cohort of non-benign thyroid tumors lacking `ajcc8_t_stage` splits into three categories:

| Category | Count | Rids | Handling |
|----------|-------|------|----------|
| QUEUED_395 | 2 | 1404, 12198 | PTC, NULL stage_group, path_raw='III', AJCC-edition unknown → chart-review |
| QUEUED_399 | 2 | 423, 6275 | MTC / other_malignant, NULL stage_group, framework/authority issues |
| STAGED_AGE_STRATIFIED_T_DATA_GAP | 2 | 21 (FTC), 1337 (PTC) | age<55 + M0 + DTC → Stage I regardless of T; stage_group correct; T column is data gap only |
| BORDERLINE_DIAGNOSIS_NOT_STAGEABLE_BUILDER_CORRECTED_BUG | 3 | 7593 (FTUMP), 11609 (NIFTP), 12188 (NIFTP) | NIFTP/FTUMP are NOT AJCC8-stageable per 2016 WHO/AJCC reclassification; `_corrected='I'` and `path='I'` for these rows is a builder and path-data bug |
| **Total** | **9** | — | — |

Benign diagnoses (multinodular_goiter, follicular_adenoma, graves_disease, hashimotos_thyroiditis, colloid_nodule, etc. — ~6687 T-NULL rows) are **intentionally excluded**. AJCC8 staging doesn't apply to benigns; their T-NULL state is by design, not a gap.

This script produces a structured audit log as a read-only sidecar. Zero writes to CPM. Zero new queue inserts (the 4 already-queued rows stay where they are; this log cross-references them for completeness).

---

## Target table schema

```sql
CREATE TABLE manuscript_workspace.cpm_tumor_t_stage_gaps_v1 (
  research_id            VARCHAR NOT NULL,
  diagnosis_primary      VARCHAR NOT NULL,
  age_at_surgery         BIGINT,
  ajcc8_t_stage          VARCHAR,              -- always NULL by definition of inclusion
  ajcc8_t_stage_v2       VARCHAR,
  dominant_tumor_ajcc8_t_stage VARCHAR,
  ajcc8_n_stage          VARCHAR,
  ajcc8_m_stage          VARCHAR,
  current_stage_group    VARCHAR,              -- may be NULL or populated (for age-stratified rows)
  stage_group_corrected  VARCHAR,              -- ajcc8_stage_group_corrected at snapshot time
  path_stage_raw         VARCHAR,
  resolution_status      VARCHAR NOT NULL,     -- one of 4 categories above
  resolution_status_note VARCHAR,              -- free-text detail per row
  snapshot_ts            TIMESTAMP NOT NULL,
  PRIMARY KEY (research_id)
);
```

One row per research_id. PRIMARY KEY enforces uniqueness — a patient appears at most once in this log regardless of how many T-related issues their row has.

---

## Planned writes

### Write A — CREATE TABLE (schema above)

### Writes B-1 through B-9 — INSERT one row per T-NULL tumor

All 9 rows selected directly from `main.canonical_patient_master` with a CASE expression computing `resolution_status`:

```sql
INSERT INTO manuscript_workspace.cpm_tumor_t_stage_gaps_v1
SELECT
  research_id,
  diagnosis_primary,
  age_at_surgery,
  ajcc8_t_stage,
  ajcc8_t_stage_v2,
  dominant_tumor_ajcc8_t_stage,
  ajcc8_n_stage,
  ajcc8_m_stage,
  ajcc8_stage_group AS current_stage_group,
  ajcc8_stage_group_corrected AS stage_group_corrected,
  path_stage_raw,
  CASE
    WHEN research_id IN ('1404','12198')
      THEN 'QUEUED_395'
    WHEN research_id IN ('423','6275')
      THEN 'QUEUED_399'
    WHEN research_id IN ('21','1337') AND ajcc8_stage_group IS NOT NULL
      THEN 'STAGED_AGE_STRATIFIED_T_DATA_GAP'
    WHEN diagnosis_primary IN ('NIFTP','FTUMP')
      THEN 'BORDERLINE_DIAGNOSIS_NOT_STAGEABLE_BUILDER_CORRECTED_BUG'
    ELSE 'UNCLASSIFIED'  -- should never hit; sentinel for H-gate failure
  END AS resolution_status,
  CASE
    WHEN research_id = '1404' THEN 'ptc_t_null_path_iii_ajcc_edition_unknown_chart_review'
    WHEN research_id = '12198' THEN 'ptc_t_null_path_iii_ajcc_edition_unknown_chart_review'
    WHEN research_id = '423' THEN 'mtc_t_null_plus_corrected_i_dtc_rule_misapplied_to_mtc_n1a_m0'
    WHEN research_id = '6275' THEN 'other_malignant_t_null_staging_framework_undefined'
    WHEN research_id = '21' THEN 'ftc_age_43_m0_stage_i_via_dtc_age_stratified_rule_t_is_pure_data_gap'
    WHEN research_id = '1337' THEN 'ptc_age_26_m0_stage_i_via_dtc_age_stratified_rule_t_is_pure_data_gap'
    WHEN research_id = '7593' THEN 'ftump_not_cancer_per_ajcc8_but_corrected_i_and_path_i_populated_builder_bug'
    WHEN research_id = '11609' THEN 'niftp_not_cancer_per_ajcc8_but_corrected_i_and_path_i_populated_builder_bug'
    WHEN research_id = '12188' THEN 'niftp_not_cancer_per_ajcc8_but_corrected_i_populated_no_path_builder_bug'
  END AS resolution_status_note,
  CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS snapshot_ts
FROM main.canonical_patient_master
WHERE ajcc8_t_stage IS NULL
  AND diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant','NIFTP','FTUMP');
```

Expected row count after insert: **9**.

### Write C — `__readme` provenance row

`script='script_400'`, `script_name='apply_tumor_t_stage_gaps_audit.py'`, `run_timestamp=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, content including:
- rows inserted: 9 total by category (QUEUED_395=2, QUEUED_399=2, STAGED_AGE_STRATIFIED=2, BORDERLINE_BUILDER_BUG=3)
- target table FQN: `manuscript_workspace.cpm_tumor_t_stage_gaps_v1`
- probe SHA256 consumed
- zero CPM primary column mutations
- note: benign diagnoses intentionally excluded

**Explicitly NOT doing:**
- No writes to `main.canonical_patient_master`
- No new rows in `manuscript_workspace.cpm_stage_group_manual_review_v1` (QUEUED_395/QUEUED_399 rows are cross-referenced only)
- No modification to `_corrected`, `path_stage_raw`, or any CPM column
- No benign diagnosis rows in the log
- No builder fix (that's a separate CF / code change)

---

## Script requirements (`scripts/apply_tumor_t_stage_gaps_audit.py`)

Copy Script 398's runner skeleton (read-only sidecar pattern, no CPM snapshot needed). Inherit every runner behavior:

- Stable probe hash (no timestamps in hashed region; `---HASH-BOUNDARY---` footer)
- `--i-approve=<sha256>` required for `--apply` (exit 3 mismatch, exit 5 missing)
- `--phase4` NO-OP-safe
- `FORCE_ADD_PATTERNS = [r"scripts/output/.*_run\.log$"]` for the run-log `-f`
- `git push origin HEAD` before tag push
- Close-out write AFTER idempotency check
- Exit 4 on row-count drift (if target table exists with wrong row count)

### Halt gates (Phase 0; all must PASS)

- **H1 — Scope lock:** `SELECT COUNT(*) FROM cpm WHERE ajcc8_t_stage IS NULL AND diagnosis_primary IN (<tumor_allowlist>)` = 9. FAIL if ≠ 9.
- **H2 — Per-category count lock:**
  - `QUEUED_395` (rids 1404, 12198) = 2
  - `QUEUED_399` (rids 423, 6275) = 2
  - `STAGED_AGE_STRATIFIED_T_DATA_GAP` (rids 21, 1337 where `ajcc8_stage_group IS NOT NULL`) = 2
  - `BORDERLINE_DIAGNOSIS_...BUG` (NIFTP/FTUMP rows = 7593, 11609, 12188) = 3
  - Sum = 9, and no 'UNCLASSIFIED' sentinel hits
- **H3 — Target table does NOT exist:** `cpm_tumor_t_stage_gaps_v1` absent from `manuscript_workspace` (first-time create). FAIL if present (idempotency path handles NO-OP; this is for fresh-create verification).
- **H4 — CPM invariant:** count = 10,871.
- **H5 — No CPM UPDATEs in apply SQL:** static grep the generated SQL for `UPDATE main.canonical_patient_master` and `UPDATE main.` → must be 0 matches.
- **H6 — manuscript_workspace schema exists** (should; used by 395/398/399).
- **H7 — Queue cross-ref integrity:** the 4 rows marked QUEUED_395/QUEUED_399 MUST currently exist in `manuscript_workspace.cpm_stage_group_manual_review_v1` — verify 1404, 12198 present with source_script='395'; 423, 6275 present with source_script='399'. FAIL if any cross-reference is missing or stale.
- **H8 — Benign exclusion:** verify zero benign rows appear in the write predicate. Query: `SELECT COUNT(*) FROM cpm WHERE ajcc8_t_stage IS NULL AND diagnosis_primary NOT IN (tumor_allowlist)` returns ~6687 — these are NOT in scope. The insert SELECT must use the INclusive filter (tumor_allowlist), not the exclusive filter.
- **H9 — Age-stratified corroboration for STAGED rows:**
  - rid 21: age_at_surgery=43 (<55), `ajcc8_m_stage='M0'`, `ajcc8_stage_group='I'`, diagnosis='FTC' — matches DTC age-stratified rule
  - rid 1337: age_at_surgery=26 (<55), `ajcc8_m_stage='M0'`, `ajcc8_stage_group='I'`, diagnosis='PTC' — matches DTC age-stratified rule
  - FAIL if either row's state doesn't match.

### Idempotency

Treat as applied iff all three:
1. `manuscript_workspace.cpm_tumor_t_stage_gaps_v1` exists
2. Its row count = 9
3. `__readme script='script_400'` row exists

If applied → NO-OP, Phase 3 verify, exit 0. Target-table drift (exists but row count ≠ 9) → exit 4 with "audit table drifted; manual investigation required".

### Phase 3 post-state gates

- P1 — CPM still 10,871 (no main.* mutations)
- P2 — Target table exists with PRIMARY KEY `(research_id)`
- P3 — Row count = 9
- P4 — Per-category count: `QUEUED_395=2, QUEUED_399=2, STAGED_AGE_STRATIFIED_T_DATA_GAP=2, BORDERLINE_DIAGNOSIS_NOT_STAGEABLE_BUILDER_CORRECTED_BUG=3`
- P5 — Zero 'UNCLASSIFIED' rows
- P6 — `__readme script='script_400'` count = 1
- P7 — All 4 QUEUED_*s cross-reference back to `cpm_stage_group_manual_review_v1` (INNER JOIN yields 4 matches)
- P8 — CPM diff: zero primary-column changes in CPM during this script's transaction

---

## Execution plan for Composer

1. Create `scripts/apply_tumor_t_stage_gaps_audit.py` from 398's skeleton (read-only sidecar), adapted per above.
2. Run Phase 0 probe:
   ```
   python3 scripts/apply_tumor_t_stage_gaps_audit.py --phase 0
   ```
   Emit probe markdown + SHA256. Print SHA.
3. Pause at plan-approval gate. Post H1–H9 verdicts + per-category counts back to user.
4. On approval:
   ```
   python3 scripts/apply_tumor_t_stage_gaps_audit.py --apply \
     --i-approve=<sha256_from_step_2> --phase4
   ```
5. Phase 4 surgical git-add (5 paths):
   - `scripts/apply_tumor_t_stage_gaps_audit.py`
   - `scripts/output/apply_tumor_t_stage_gaps_audit_probe.md`
   - `scripts/output/apply_tumor_t_stage_gaps_audit_run.log` (`-f` via `FORCE_ADD_PATTERNS`)
   - `cursor_prompts/CURSOR_PROMPT_TUMOR_T_STAGE_GAPS_AUDIT_20260423_SCRIPT_400.md`
   - `cursor_prompts/CLOSE_OUT_400.md`
6. Commit message: `Script 400: thyroid tumor T-stage gaps audit (9 rows, read-only sidecar)`
7. Tag: `v1_0-tumor-t-stage-gaps-audit-<YYYYMMDD_HHMMSS>`
8. Push: `git push origin HEAD` then `git push origin <tag>`
9. Write close-out AFTER idempotency check clears.

---

## Close-out contents (`cursor_prompts/CLOSE_OUT_400.md`)

- Commit SHA, tag, UTC timestamp
- Probe SHA256 (consumed)
- Halt-gate verdict table (H1–H9)
- 9-row per-category breakdown
- Cross-reference integrity confirmation (4 rows QUEUED_* trace back to `cpm_stage_group_manual_review_v1`)
- Zero CPM writes confirmation (H5 + P8)
- Target table FQN
- CF-400 followups:
  - **CF-400-1:** Builder bug — `ajcc8_stage_group_corrected` is being populated for NIFTP and FTUMP rows (7593, 11609, 12188) which are NOT AJCC8-stageable per 2016 WHO/AJCC reclassification. Builder should skip `_corrected` computation for borderline/non-cancer diagnoses. Potential Script 401 scope.
  - **CF-400-2:** Clinical chart-review of the 3 borderline rows — decide whether `_corrected` and `path_stage_raw` should be NULLed in CPM (data correction) or left as-is with annotation.
  - **CF-400-3:** Builder enhancement — the 2 age-stratified staged rows (21 FTC, 1337 PTC) have correct stage_group but NULL T. Consider whether T can be recovered from pathology source, or whether this is an acceptable permanent data gap.
  - **CF-400-4:** Benign T-NULL audit — optionally extend the log to include ~6687 benign rows, clearly marked as "out-of-scope-for-staging-benign-diagnosis" for pipeline completeness tracking.

---

## Verbal gate — confirm before Composer begins

Reply with:
- **"Approved. Run Phase 0, return SHA256 for `--i-approve`."** — standard path
- **"Hold — narrow:"** (e.g., "log only the 3 UNRESOLVED borderline rows; the 4 queued rows don't need re-logging since they're already in the manual-review queue")
- **"Hold — broaden:"** (e.g., "include benign rows too", or "also NULL out `_corrected` and `path_stage_raw` for the 3 builder-bug rows in the same script" — that would become a data-modifying rather than read-only script)
- **"Hold — restructure:"** (e.g., "make it a VIEW instead of materialized TABLE", or "route the 3 borderline rows to `cpm_stage_group_manual_review_v1` as well with a new borderline reason code")
