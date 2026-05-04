# Cursor Composer Dispatch — mig_287: Smoking taxonomy normalization (mig_281 follow-up)

**Generated:** 2026-05-04 by Cowork.
**Lane:** mig_287 — Cowork sensitivity analysis on M044 surfaced that the AI_CLASSIFY smoking output has 6+ distinct values for what should be a 3-level enum: `current`, `current smoker`, `former`, `former smoker`, `never`, `never smoker`, `quit smoking`. This breaks `C(smoking_combined)` factor encoding in regressions. Normalize CPM `pmhx_nlp_smoking_status` + the underlying `note_entities_llm_pmhx` extracted_value to a clean 3-level enum: `current` / `former` / `never`.
**Recommended agent:** **Cursor Composer** — mechanical UPDATE.
**Estimated runtime:** 20 min.
**Closes:** CF-mig281-SMOKING-TAXONOMY-DIRTY (newly opened).

---

## §0 — First message to paste into Cursor Composer

> mig_287 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_287_SMOKING_TAXONOMY_NORM_20260504.md`. Normalize smoking taxonomy. MotherDuck DB is `thyroid_canonical_publication_v1_0`. Pre-snapshot to archive before UPDATE.

---

## §1 — Pre-task probe

```sql
-- 1.1 Distinct values currently on CPM
SELECT pmhx_nlp_smoking_status, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE pmhx_nlp_smoking_status IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- 1.2 Distinct values in note_entities_llm_pmhx
SELECT extracted_value, COUNT(*) AS n
FROM main.note_entities_llm_pmhx
WHERE category = 'smoking' AND extracted_value IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;
```

Expected mappings (per mig_281 SF AI_CLASSIFY output observed by Cowork):
- `current_smoker` / `current smoker` / `current` → `current`
- `former_smoker` / `former smoker` / `former` / `quit smoking` → `former`
- `never_smoker` / `never smoker` / `never` → `never`
- `unknown_or_not_mentioned` → NULL

---

## §2 — Apply

### §2a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_smoking_pre_mig287_20260504 AS
SELECT research_id, pmhx_nlp_smoking_status FROM main.canonical_patient_master;

CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_pmhx_smoking_pre_mig287_20260504 AS
SELECT * FROM main.note_entities_llm_pmhx WHERE category = 'smoking';
```

### §2b — UPDATE CPM enum

```sql
UPDATE main.canonical_patient_master
SET pmhx_nlp_smoking_status = CASE
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('current','current_smoker','current smoker') THEN 'current'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('former','former_smoker','former smoker','quit smoking','quit_smoking','ex-smoker','ex_smoker') THEN 'former'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('never','never_smoker','never smoker','non-smoker','non_smoker') THEN 'never'
  WHEN LOWER(pmhx_nlp_smoking_status) IN ('unknown_or_not_mentioned','unknown','not mentioned','nan') THEN NULL
  ELSE pmhx_nlp_smoking_status  -- preserve any unrecognized values; surface to Logan
END
WHERE pmhx_nlp_smoking_status IS NOT NULL;
```

### §2c — UPDATE underlying note_entities_llm_pmhx

```sql
UPDATE main.note_entities_llm_pmhx
SET extracted_value = CASE
  WHEN LOWER(extracted_value) IN ('current','current_smoker','current smoker') THEN 'current'
  WHEN LOWER(extracted_value) IN ('former','former_smoker','former smoker','quit smoking','quit_smoking','ex-smoker','ex_smoker') THEN 'former'
  WHEN LOWER(extracted_value) IN ('never','never_smoker','never smoker','non-smoker','non_smoker') THEN 'never'
  WHEN LOWER(extracted_value) IN ('unknown_or_not_mentioned','unknown','not mentioned','nan') THEN NULL
  ELSE extracted_value
END
WHERE category = 'smoking' AND extracted_value IS NOT NULL;
```

### §2d — Verify

```sql
-- Should be only 3 values + NULL
SELECT pmhx_nlp_smoking_status, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE pmhx_nlp_smoking_status IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;
-- Expected: current ~ 200-300, former ~ 500-700, never ~ 2200-2400, total ~ 3000
```

If anything outside {current, former, never} survives → surface to Logan.

### §2e — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_287', CURRENT_TIMESTAMP, 'cursor_composer_mig287',
 'mig_287: Smoking taxonomy normalization. UPDATE pmhx_nlp_smoking_status on CPM + note_entities_llm_pmhx category=smoking to clean 3-level enum (current/former/never + NULL). Closes CF-mig281-SMOKING-TAXONOMY-DIRTY.');
```

---

## §3 — Surgical git add

```
qc_framework_v1/migrations/287_smoking_taxonomy_norm_20260504.sql
scripts/output/mig_287_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_287_SMOKING_TAXONOMY_NORM_20260504.md
```

---

**End of mig_287 dispatch.**
