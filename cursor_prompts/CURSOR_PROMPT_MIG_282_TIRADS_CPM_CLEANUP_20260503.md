# Cursor Composer Dispatch — mig_282: CPM `nlp_tirads_max_category` cleanup

**Generated:** 2026-05-03 by Cowork at HEAD `1284973`.
**Lane:** mig_282 — `canonical_patient_master.nlp_tirads_max_category` (VARCHAR) currently contains 345 distinct values, the vast majority of which are free-text dump (anatomic descriptions like "right_lobe_mid", sizes like "1.2 x 0.9 x 1.0 cm", shape descriptors like "wider_than_tall", pattern descriptors like "spongiform"). Only ~28 patients have clean TR1-TR6 categorical values. This breaks any direct CPM consumer of TIRADS; cohort views work around it via cleaning logic.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs prompt walking through the source-of-truth selection (NLP raw vs SF AI_CLASSIFY re-extract vs ACR canonical lookup); apply is mechanical.
**Estimated runtime:** 60-90 min.
**Triggered by:** Cowork audit during M025 v1.0 re-verify 2026-05-04.
**Severity:** MED. M025 manuscript currently uses cohort_view-cleaned values (works), but breaks any downstream NLP-based covariate join.
**Closes:** CF-M025-CPM-TIRADS-COL-DIRTY.

---

## §0 — First message to paste into Cursor Chat

> mig_282 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_282_TIRADS_CPM_CLEANUP_20260503.md`. The CPM col `nlp_tirads_max_category` is dirty. Decide source-of-truth (look at how `manuscript_workspace.cohort_m025_tirads_performance_v1` resolves clean values), then rebuild the col on CPM with proper TR0-TR5 enum (or NULL for unparseable). MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Pre-task probes

```sql
-- 1.1 Confirm dirty state on CPM (~345 distinct values)
SELECT COUNT(DISTINCT nlp_tirads_max_category) AS n_distinct_values FROM main.canonical_patient_master;

-- 1.2 Sample of dirty values
SELECT nlp_tirads_max_category, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE nlp_tirads_max_category IS NOT NULL
  AND nlp_tirads_max_category NOT REGEXP '^TR[0-5]$'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- 1.3 How does cohort_m025 view derive its clean tirads category?
SELECT view_definition FROM information_schema.views
WHERE table_schema='manuscript_workspace' AND table_name='cohort_m025_tirads_performance_v1';

-- 1.4 Upstream NLP source for tirads (note_entities_llm_imaging? imaging_features?)
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND (table_name ILIKE '%tirads%' OR table_name ILIKE '%imaging_features%')
ORDER BY table_name;
```

---

## §2 — Decision: source-of-truth

3 options, pick simplest that works:

### Option A — Mirror cohort_m025 view's derivation
If `cohort_m025_tirads_performance_v1` already has a clean derivation (e.g., regex on raw NLP text, or a join to a lookup), apply the same logic on CPM via `UPDATE`.

### Option B — SF AI_CLASSIFY re-extraction
If raw notes have TIRADS info but the NLP wasn't extracting cleanly, do a SF AI_CLASSIFY pass over the imaging notes and promote like mig_281. Higher effort.

### Option C — Drop the col, replace with `tirads_resolved` enum
Add a new col `tirads_resolved` (VARCHAR — 'TR1','TR2','TR3','TR4','TR5','TR0_unknown') and populate from cohort_view derivation. Leave `nlp_tirads_max_category` in place but mark deprecated in the audit/registry.

**Cowork recommends Option C** — least disruptive to existing code; clean enum for new consumers.

---

## §3 — Apply (Option C path)

### §3a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_tirads_pre_mig282_20260503 AS
SELECT research_id, nlp_tirads_max_category FROM main.canonical_patient_master;
```

### §3b — Add new col

```sql
ALTER TABLE main.canonical_patient_master ADD COLUMN tirads_resolved VARCHAR;
```

### §3c — Populate from cohort_m025 view derivation

```sql
-- (after probing the view's derivation logic in §1.3)
UPDATE main.canonical_patient_master pm
SET tirads_resolved = (
  SELECT cm.tirads_category  -- or whichever clean col the view exposes
  FROM manuscript_workspace.cohort_m025_tirads_performance_v1 cm
  WHERE cm.research_id = pm.research_id
);
```

### §3d — Verify enum

```sql
SELECT tirads_resolved, COUNT(*) AS n,
       COUNT_IF(is_malignant) AS n_malig,
       ROUND(100.0*COUNT_IF(is_malignant)/COUNT(*), 1) AS pct_malig
FROM main.canonical_patient_master
GROUP BY 1 ORDER BY 1;
-- Expected: TR1-TR5 buckets + NULL; ~3,375 non-NULL total
```

### §3e — Registry signoff + col deprecation note

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_282', CURRENT_TIMESTAMP, 'cursor_composer_mig282',
 'mig_282: Added tirads_resolved enum col on canonical_patient_master (TR1-TR5 + NULL). Populated from manuscript_workspace.cohort_m025_tirads_performance_v1 derivation. Legacy nlp_tirads_max_category retained for backward compatibility but marked DEPRECATED — 345 distinct dirty values incl free-text dump. Closes CF-M025-CPM-TIRADS-COL-DIRTY. n_resolved=NN; TR1=NN/TR2=NN/TR3=NN/TR4=NN/TR5=NN.');
```

---

## §4 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-M025-CPM-TIRADS-COL-DIRTY | **CLOSED on apply** | New `tirads_resolved` enum available |
| CF-mig282-LEGACY-NLP-COL | **OPEN** | `nlp_tirads_max_category` retained for backward compat; future mig may DROP it after consumer audit |

---

## §5 — Surgical git add

```
qc_framework_v1/migrations/282_tirads_cpm_cleanup_20260503.sql
scripts/output/mig_282_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_282_TIRADS_CPM_CLEANUP_20260503.md
```

Commit message:
```
fix(md): mig_282 CPM tirads_resolved enum (cleanup nlp_tirads_max_category dirty col)

- Added canonical_patient_master.tirads_resolved (VARCHAR enum: TR1-TR5 + NULL)
- Populated from manuscript_workspace.cohort_m025_tirads_performance_v1 derivation
- Legacy nlp_tirads_max_category retained but marked DEPRECATED (345 dirty distinct values)
- Closes CF-M025-CPM-TIRADS-COL-DIRTY
```

---

**End of mig_282 dispatch.**
