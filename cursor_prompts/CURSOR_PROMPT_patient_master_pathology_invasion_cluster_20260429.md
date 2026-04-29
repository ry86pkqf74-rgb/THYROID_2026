# Cursor Agent Task — `canonical_patient_master` PATHOLOGY-INVASION CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_142b)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting (current tip `55de468`)
**Estimated effort:** 2-3 hours (~38 cols)
**Run order:** Lane 43 of next 4-prompt batch (mig_154)

---

## 0. Cleanliness & safety preamble (MUST READ)

This batch was scoped after Cowork found systematic agent-QA misses in mig_135 / mig_141 / mig_142 / mig_144 / mig_145 / mig_147 / mig_148 / mig_151:
- BOOLEAN cohort sweeps that only flagged FALSE-dominant degeneracy (missed TRUE-only and TRUE/0/NULL presence flags).
- VARCHAR-with-embedded-units measurement cols left un-retyped.
- TIMESTAMP/VARCHAR dates left un-retyped.
- `verification_method` strings naming archived/legacy tables not in live `main` schema (mig_151 named `note_entities_medications` and `note_entities_llm_rad_treatment` — both archive-only).

**Your prompts must NOT repeat these mistakes.** Required guards in this lane (every lane in this batch):
1. Before claiming `<verification_method>` against any table, run `SELECT 1 FROM information_schema.tables WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main' AND table_name='<X>'`. If the table does not live in `main`, you MUST EITHER (a) use the live canonical successor, (b) name the archive snapshot explicitly with `_archive_<snapshot>` suffix, OR (c) reclassify the col to `na` with `helper_<placeholder>_pending_real_extraction`. Never name a non-existent live table.
2. **AGENTS governance** — Do NOT execute writes against `thyroid_canonical_publication_v1_0`. Commit and push the SQL file; Logan applies via Cowork after independent verification.
3. **Cohort-uniformity sweep both directions** on every BOOLEAN flipped (template in §2d).
4. Pre-snapshot the registry rows you'll mutate to `"Thyroid 2026 UPdated".archive_pub_v1_0` (template in §3).
5. Surgical git add — explicit paths only; never `-A`.

---

## 1. Goal

Verify the **pathology invasion + margin cluster** — 38 unverified cols on `canonical_patient_master` covering capsular invasion, vascular invasion, perineural invasion (PNI), lymphovascular invasion (LVI), margin status, and IHC BRAF.

### 1a. Pre-flight probe (must return exactly 38)

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'capsular_any_present_path','capsular_invasion_refined','capsular_invasion_v6','capsular_ordinal_worst',
    'closest_margin_mm','closest_margin_mm_max','closest_margin_mm_min',
    'ihc_braf_confidence_v13','ihc_braf_note_type_v13','ihc_braf_result_v13',
    'lvi_any_present_path','lvi_grade','lvi_ordinal_worst',
    'margin_all_uninvolved','margin_involved_any','margin_ord_worst','margin_r_class_v10',
    'margin_r_classification','margin_status','margin_status_final','margin_status_final_source','margin_status_true',
    'perineural_invasion','pni_any_present_path','pni_positive','pni_refined_v6',
    'vasc_confidence_final_v13','vasc_grade','vasc_grade_final_v13','vasc_source_final_v13','vasc_vessel_count_v13',
    'vascular_invasion_final','vascular_invasion_grade','vascular_vessel_count','vascular_who_2022_grade',
    'vi_any_present_path','vi_ordinal_worst','vi_vessels_max'
  )
ORDER BY column_name;
```

Confirm count is **exactly 38**. All must be `verification_status='not_started'`.

### 1b. Sub-clusters

- **Capsular invasion (4 cols):** capsular_any_present_path, capsular_invasion_refined, capsular_invasion_v6, capsular_ordinal_worst
- **Vascular invasion canonical (12 cols):** vasc_confidence_final_v13, vasc_grade, vasc_grade_final_v13, vasc_source_final_v13, vasc_vessel_count_v13, vascular_invasion_final, vascular_invasion_grade, vascular_vessel_count, vascular_who_2022_grade, vi_any_present_path, vi_ordinal_worst, vi_vessels_max
- **PNI (4 cols):** perineural_invasion, pni_any_present_path, pni_positive, pni_refined_v6
- **LVI (3 cols):** lvi_any_present_path, lvi_grade, lvi_ordinal_worst
- **Margin (12 cols):** margin_all_uninvolved, margin_involved_any, margin_ord_worst, margin_r_class_v10, margin_r_classification, margin_status, margin_status_final, margin_status_final_source, margin_status_true, margin_truth_*, closest_margin_mm, closest_margin_mm_max, closest_margin_mm_min
- **IHC BRAF (3 cols):** ihc_braf_confidence_v13, ihc_braf_note_type_v13, ihc_braf_result_v13

---

## 2. Methodology

### 2a. SSOT pointers (verify each lives in `main` first!)

- `canonical_invasion_events_v1` + `canonical_invasion_patient_rollup_v1` — the verified invasion family (see `project_invasion_family_signoff_2026-04-28.md`). This is the primary SSOT for capsular/vascular/PNI/LVI rollups.
- `canonical_path_malignant_events_v1` + `canonical_path_malignant_patient_rollup_v1` — verified pathology family. Margin status and ordinal_worst pattern mirror `path_malignant`'s patterns.
- `canonical_path_synoptics_*` if present in `main` — synoptic-grade source for margin distances.
- `canonical_molecular_genetics_v2` — for IHC BRAF cross-validation.

Pre-check: `SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND (table_name LIKE 'canonical_invasion%' OR table_name LIKE 'canonical_path%' OR table_name LIKE 'canonical_molecular%') ORDER BY 1;`

### 2b. Per-sub-cluster derivation rules

**Capsular** — re-derive from `canonical_invasion_events_v1` filtered to `invasion_type='capsular'`. `*_any_present_path` = BOOL_OR(present); `*_ordinal_worst` = MAX over an ordinal ladder (none → focal → minor → extensive).

**Vascular** — re-derive from `canonical_invasion_events_v1` filtered to `invasion_type='vascular'`. `vascular_who_2022_grade` follows WHO 2022 ladder (none → focal → angioinvasion). `vascular_vessel_count` = MAX(vessel_count) at patient grain; verify against the `vasc_vessel_count_v13` confidence-versioned counterpart. **Critical:** Findings primary, staging follows findings (per `feedback_findings_vs_staging.md`).

**PNI** — `canonical_invasion_events_v1` filtered to `invasion_type='perineural'`. `pni_positive` BOOL_OR; `pni_refined_v6` is a v6-cleaned categorical (verify the cleaning rule from the rebuild script).

**LVI** — `canonical_invasion_events_v1` filtered to `invasion_type='lymphovascular'`. Note: distinct from vi_* (which is union of vascular+lymphatic in some legacy views). Verify lvi_any_present_path is intersection logic, not union.

**Margin** — `canonical_path_malignant_events_v1` margin_status with ordinal ladder (uninvolved → close → involved → R1 → R2). `closest_margin_mm` from synoptic structured field, possibly VARCHAR-with-units. `margin_status_final` is the resolved-after-tie-breaker col; `margin_status_final_source` documents which feed won.

**IHC BRAF** — `note_entities_llm_pathology` + `canonical_molecular_genetics_v2`. v13 confidence-versioned cols are the latest IHC interpretation pass.

### 2c. ⚠️ VARCHAR-with-units retype audit

`closest_margin_mm` family — likely DOUBLE already since the suffix is `_mm`, but confirm. If VARCHAR with embedded units (e.g. `"2.3 mm"`), apply mig_144b pattern (in-place ALTER COLUMN with TRY_CAST + pre-snapshot). Open `CF-mig154-MARGIN-MM-VARCHAR-RETYPE` only if you ship the retype within this lane; otherwise reclassify col to `na` and open the CF for a downstream lane.

### 2d. ⚠️ Cohort-uniformity sweep (REQUIRED — both directions)

For every BOOLEAN col flipped, run this template:

```sql
SELECT
  '<col>' AS col,
  SUM(CASE WHEN <col> THEN 1 ELSE 0 END) AS t,
  SUM(CASE WHEN NOT <col> THEN 1 ELSE 0 END) AS f,
  SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END) AS n
FROM main.canonical_patient_master;
```

Decision rules:
- **TRUE-count = 0 AND FALSE > 0** → Type-B/C placeholder. Reclassify col verified→na with method `<helper>_pending_real_extraction`. Open `CF-mig154-COHORT-UNIFORM-FALSE-<col>`.
- **FALSE-count = 0 AND TRUE > 0** → Type-A presence flag (FALSE structurally impossible by design). Keep verified with appendix `CF-mig154-COHORT-NEAR-UNIFORM-TRUE-<col>` informational note.
- **(TRUE-rate > 99% OR < 1%) AND not Type-A** → investigate clinical plausibility against published rates (capsular ~15-25%, vascular ~10-30%, PNI ~3-15%, LVI ~5-20%, margin involved ~10-25% for thyroid cancer). If degenerate, `na`; if real cohort invariance, keep with informational CF.

Cross-check expected rates:
- `vascular_invasion_final` should be 10-30% TRUE in malignant cohort
- `pni_positive` should be 3-15% TRUE
- `margin_involved_any` should be 10-25% TRUE

### 2e. Calendar-only date check

No `*_date` cols in this set, but `*_at` provenance timestamps OK as TIMESTAMP per allowlist.

### 2f. Cross-source spot-check (REQUIRED)

Pick 5 random rids with `vascular_invasion_final=TRUE`. For each: SELECT supporting `canonical_invasion_events_v1` rows. Manually verify that the WHO 2022 grade ladder, vessel_count, and confidence string round-trip cleanly from events→PM. Same for 5 rids with `pni_positive=TRUE` and 5 rids with `margin_involved_any=TRUE`. Document derivation chain in the migration header.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/154_patient_master_pathology_invasion_cluster_signoff_20260429.sql`

```
batch_id = 'mig_154_patient_master_pathology_invasion_cluster_20260429'
verified_by = 'logan'
verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
verification_method options:
  derivation_vs_canonical_invasion_events_v1
  derivation_vs_canonical_path_malignant_events_v1
  derivation_vs_canonical_path_malignant_patient_rollup_v1
  cross_validate_vs_canonical_molecular_genetics_v2 (IHC BRAF only)
  internal_consistency_<rule> (e.g., margin_status_final = COALESCE(refined, true) — passthrough)
  partial_signal_supplanted_by_<authoritative>_canonical (for legacy-shadow cols)
  helper_<placeholder>_pending_real_extraction (for Type-B/C reclassifications)
```

Sub-block layout (one UPDATE per methodology+sub-cluster):
- 154a — Capsular (4 cols)
- 154b — Vascular (12 cols)
- 154c — PNI (4 cols)
- 154d — LVI (3 cols)
- 154e — Margin (12 cols)
- 154f — IHC BRAF (3 cols) — verify upstream lives in `main` before naming methodology
- 154g — Resync `canonical_table_signoff_registry_v1` for `canonical_patient_master`

### 3a. Pre-snapshot block (top of file, COMMITS first)

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig154_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig154_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name='canonical_patient_master' AND column_name IN (<the 38 cols>);
```

### 3b. Per-block UPDATE template

```sql
UPDATE main.canonical_column_verification_registry_v1
SET verification_status='verified',
    verified_by='logan',
    verification_method='<method>',
    batch_id='mig_154_patient_master_pathology_invasion_cluster_20260429',
    verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
    notes=COALESCE(notes,'') || ' | mig_154 <sub-cluster>: <derivation summary> | <CF tags if any>'
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND verification_status='not_started'
  AND column_name IN ( <cols> );
```

### 3c. Final signoff resync (idempotent count rebuild)

Same pattern as mig_149g. Update `n_columns_total / n_verified / n_not_started / n_failed / n_na / table_status / signoff_migration` from live registry.

---

## 4. Required CFs to enumerate (even if zero rows)

Each CF below MUST be present-or-explicitly-clear in the migration header:

- `CF-mig154-COHORT-UNIFORM-FALSE-<col>` — list each near-uniform-FALSE col reclassified to `na`
- `CF-mig154-COHORT-NEAR-UNIFORM-TRUE-<col>` — list each TRUE-only presence flag kept verified
- `CF-mig154-MARGIN-MM-VARCHAR-RETYPE` — open OR clear
- `CF-mig154-IHC-BRAF-MOLECULAR-CROSSCHECK` — drift between `ihc_braf_result_v13` and `canonical_molecular_genetics_v2.braf` calls
- `CF-mig154-INVASION-FAMILY-LINEAGE` — note that this lane consumes `canonical_invasion_events_v1` (verified 2026-04-28 per `project_invasion_family_signoff_2026-04-28.md`)
- `CF-mig154-DATE-RETYPE-CLEAR` — confirm no `*_date` col in scope (only provenance `_at`)

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

DO NOT run UPDATEs against `thyroid_canonical_publication_v1_0`. Stop at "SQL file committed + pushed". Logan + Cowork will apply.

After Logan applies, post-state checks:

```sql
-- Count flipped
SELECT verification_status, COUNT(*) FROM main.canonical_column_verification_registry_v1
WHERE batch_id='mig_154_patient_master_pathology_invasion_cluster_20260429' GROUP BY 1;
-- Expect: verified=<X>, na=<38-X>

-- 5-gate audit (full query in Cowork handoff doc §11)
-- Expect: gate1+1 if PM flips, else +0; gate2..4=0; gate5 unchanged

-- Cohort-uniformity sweep on every BOOLEAN flipped (Cowork will repeat independently)
```

---

## 6. Git workflow

```bash
# After SQL file is written:
git add qc_framework_v1/migrations/154_patient_master_pathology_invasion_cluster_signoff_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_154 CPM pathology-invasion cluster sign-off (38 cols)"
git push origin main
```

Surgical add only — never `git add -A` or `git add scripts/output/`.

---

## 7. Done definition

- [ ] Pre-flight probe returns exactly 38
- [ ] All 38 cols flipped (verified or na, no failed)
- [ ] Methodology distribution clean (no `_misc` / `_passthrough` placeholders unless explicitly justified)
- [ ] Cohort-uniformity sweep documented for every BOOLEAN
- [ ] All required CFs enumerated (open or clear)
- [ ] Pre-snapshot table created in archive_pub_v1_0
- [ ] No `verification_method` strings name tables that don't live in `main`
- [ ] SQL file committed + pushed; NO MD writes from agent
- [ ] Migration header contains 5-spot-check evidence trace (rids + values)
