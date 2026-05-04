# Cursor Composer Dispatch — mig_279: Registry audit for mig_253/256/258/259 (apply state vs SQL files)

**Generated:** 2026-05-03 by Cowork at HEAD `2cfa535`.
**Lane:** mig_279 — Four mig SQL files exist in `qc_framework_v1/migrations/` but have NO corresponding row in `main.signoff_migration`. Determine for each whether the mig was applied (and just missed signoff) or never applied (and we need to either re-apply or formally retire). Backfill or re-apply per Protocol v2.

**SQL files in scope:**
- `253_surg_procedure_type_fill_20260501.sql`
- `256_cohort_complication_temporality_propagation_20260502.sql`
- `258_m044_surgery_date_lineage_flags_20260501.sql`
- `259_ln_status_source_cf_mig258_20260501.sql`

**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs per-mig adjudication (apply state probe + decision).
**Estimated runtime:** 30-45 min.
**Closes:** CF-mig253-REGISTRY-GAP + CF-mig256-REGISTRY-GAP + CF-mig258-REGISTRY-GAP + CF-mig259-REGISTRY-GAP.

---

## §0 — First message to paste into Cursor Chat

> mig_279 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_279_REGISTRY_AUDIT_253_256_258_259_20260503.md` end-to-end. For each of the 4 migs, read its SQL file, probe MD to check whether the target columns/views/rows exist in the expected end-state, then surface a per-mig disposition table to me before applying anything.
>
> MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Per-mig probe + disposition

For each mig:

### mig_253 (surg_procedure_type_fill)

```sql
-- Probe: is the target column populated as the SQL expected?
-- Read 253_surg_procedure_type_fill_20260501.sql to find the target col + expected count
-- Then probe MD:
SELECT
  COUNT(*) AS n_total,
  COUNT(<target_col>) AS n_filled,
  COUNT_IF(<target_col> = '<expected_value>') AS n_expected_value
FROM main.<target_table>;
```

Disposition:
- **APPLIED + needs backfill signoff** → §2 INSERT signoff_migration
- **NOT APPLIED + still needed** → §3 re-apply per Protocol v2 (pre-snapshot + UPDATE + signoff)
- **SUPERSEDED** → §4 mark mig SQL file as `253_DEPRECATED_no_op_see_<successor>.sql` + register a no-op signoff

### mig_256 (cohort_complication_temporality_propagation)

Same probe pattern. Cohort views in scope: `manuscript_workspace.cohort_*`. Check whether complication temporality cols (`onset_class`, `permanence_class`, `detection_date_inferred`) propagated.

```sql
SELECT table_name, column_name FROM information_schema.columns
WHERE table_schema='manuscript_workspace'
  AND column_name IN ('onset_class','permanence_class','detection_date_inferred')
ORDER BY table_name, column_name;
```

If present in M044/M037/M025/M032 cohort views → APPLIED. If absent → NOT APPLIED.

### mig_258 (m044_surgery_date_lineage_flags)

This one is **almost certainly applied** — the commit `bc2d013` ("M044: cohort surgery-date lineage flags (mig_258), validation gate, refit models") landed 2026-05-01 + the handoff §6 references mig_258 LN status_source rule as live. Probe:

```sql
SELECT column_name FROM information_schema.columns
WHERE table_schema='manuscript_workspace' AND table_name LIKE 'cohort_m044%'
  AND column_name ILIKE '%lineage%' OR column_name ILIKE '%surg_date_source%';
```

If lineage cols exist → backfill signoff.

### mig_259 (ln_status_source_cf_mig258)

Handoff §6 explicitly references `ln_status_source` with bucket counts: `both=1126 / staging=1509 / NULL=8236`. Almost certainly applied. Probe:

```sql
SELECT ln_status_source, COUNT(*) FROM main.canonical_patient_master
GROUP BY ln_status_source ORDER BY 2 DESC;
```

Expected: `both=1126`, `staging=1509`, `NULL=8236`. If matches → backfill signoff.

---

## §2 — Backfill signoff (for APPLIED migs)

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_<NNN>', CURRENT_TIMESTAMP, 'cursor_composer_mig279_backfill',
 '<one-line summary of what mig did, with verification probe results inline>');
```

---

## §3 — Re-apply (for NOT-APPLIED migs that are still needed)

Standard Protocol v2:
1. Pre-snapshot to `"Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_mig<NNN>_20260503`
2. Apply the SQL UPDATE/CREATE
3. Verify counts
4. INSERT signoff_migration with actual apply timestamp

---

## §4 — Mark superseded (for SUPERSEDED migs)

```bash
git mv qc_framework_v1/migrations/<NNN>_<name>.sql qc_framework_v1/migrations/<NNN>_DEPRECATED_no_op_see_<successor>.sql
```

Plus signoff_migration row noting the supersession.

---

## §5 — Disposition table (Cursor surfaces to Logan)

| mig | apply state | disposition | action |
|---|---|---|---|
| 253 | ? | ? | ? |
| 256 | ? | ? | ? |
| 258 | likely applied | backfill | §2 |
| 259 | likely applied | backfill | §2 |

Surface this filled-in table to Logan via chat before any DML / file moves.

---

## §6 — Surgical git add

```
qc_framework_v1/migrations/279_registry_audit_253_256_258_259_20260503.sql
qc_framework_v1/migrations/<NNN>_DEPRECATED_*.sql  (if any superseded)
scripts/output/mig_279_disposition_report.md
scripts/output/mig_279_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_279_REGISTRY_AUDIT_253_256_258_259_20260503.md
```

Commit message:
```
chore(md): mig_279 registry audit + backfill for mig_253/256/258/259

- mig_253: <disposition>
- mig_256: <disposition>
- mig_258: <disposition>
- mig_259: <disposition>
- Closes CF-mig253/256/258/259-REGISTRY-GAP
```

---

**End of mig_279 dispatch.**
