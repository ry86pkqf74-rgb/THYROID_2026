# Cursor Composer Dispatch — mig_304: M037 cohort filter audit (1,766 patient gap)

**Generated:** 2026-05-04 by Cowork at HEAD `d4bdebd`.
**Lane:** mig_304 — M037 inclusion-flow CSV (Cowork round 21) shows: 10,871 PUB → 4,019 malig → 4,000 LN-staged (N0/N1a/N1b/Nx assigned) → **2,234 final M037 cohort**. The 1,766-patient gap between LN-staged-known (4,000) and final cohort (2,234) means the cohort view applies additional filters that aren't documented. Audit + add to M037 README.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** 30 min.
**Severity:** LOW (documentation hygiene). Doesn't break manuscript but reviewers may ask.
**Closes:** CF-M037-COHORT-FILTER-UNDOCUMENTED (newly opened).

---

## §0 — First message

> mig_304 dispatch. Audit `manuscript_workspace.cohort_m037_ln_metastasis_v1` view definition to identify the additional filters that drop 1,766 patients between LN-staged-known (4,000) and final cohort (2,234). Document each filter in M037 README + supplement Methods. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

## §1 — Audit

```sql
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m037_ln_metastasis_v1';
```

For each WHERE clause filter beyond `is_malignant=TRUE` and `ajcc8_n_stage IS NOT NULL`, identify:
- The criterion (e.g., "complete-case on tumor size")
- Number of patients dropped (re-run the inclusion waterfall)

## §2 — Document

Update `M037_submission_package_v1_0/00_README.md` and `03_supplement.docx` Methods §Cohort with the full filter waterfall:

```
Step 1: PUB v1.1 = 10,871
Step 2: Malignant histology = 4,019 (-6,852)
Step 3: AJCC8 N-stage assigned = 4,000 (-19)
Step 4: <additional filter A> = X (-Y)
Step 5: <additional filter B> = Z (-W)
...
Final M037 cohort = 2,234
```

## §3 — Update inclusion-flow CSV

Cowork has a 4-step waterfall in `snowflake_trial/reports/inclusion_flows/M037_inclusion_flow.csv`. Extend to N steps with the discovered filters.

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_304', CURRENT_TIMESTAMP, 'cursor_composer_mig304',
 'mig_304: M037 cohort filter audit. Documented N additional filters that drop 1,766 patients between LN-staged-known (4,000) and final cohort (2,234). Updated M037 README + supplement Methods + inclusion_flows CSV. Closes CF-M037-COHORT-FILTER-UNDOCUMENTED.');
```

## §5 — Surgical git add

```
M037_submission_package_v1_0/00_README.md
M037_submission_package_v1_0/03_supplement.docx
snowflake_trial/reports/inclusion_flows/M037_inclusion_flow.csv
qc_framework_v1/migrations/304_m037_filter_audit_20260504.sql
scripts/output/mig_304_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_304_M037_COHORT_FILTER_AUDIT_20260504.md
```

---

**End of mig_304 dispatch.**
