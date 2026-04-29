# Cursor Agent Task — `rai_treatment_episode_v2` Tier-2 SIGN-OFF (RAI upstream unblocker for mig_142)

**Generated:** 2026-04-29 (Cowork session)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~25 cols)
**Run order:** Lane 38 — **PRIORITY: this unblocks mig_142 PM RAI cluster.**

---

## 1. Goal

Sign off the **`main.rai_treatment_episode_v2`** Tier-2 canonical (event-grain RAI treatment episodes — 1,857 rows / 862 distinct patients). Currently `not_started` in `canonical_table_signoff_registry_v1`; 25 of 32 cols are `not_started` and 7 are `na` (auto_identifier_skip / auto_provenance_skip). After this lane closes, the PM RAI cluster (mig_142, 51 cols) can run.

The prior mig_142 attempt **correctly stopped** per Protocol v2 upstream gate (CF-mig142-RAI-UPSTREAM-PENDING) — do NOT re-attempt the PM RAI lane until this one closes.

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='rai_treatment_episode_v2'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='rai_treatment_episode_v2' AND verification_status='na'
  )
ORDER BY column_name;
```

Confirm count is **exactly 25** before proceeding (32 total − 7 already na).

The 25 not_started cols by sub-cluster:

- **RAI assertion + term normalization** (5): `rai_assertion_status`, `rai_term_normalized`, `rai_mention_raw`, `rai_intent`, `rai_confidence`
- **Dose chain** (5): `dose_mci`, `dose_confidence`, `dose_source`, `dose_missingness_reason`, `dose_text_raw`
- **Date chain** (5): `rai_date_native`, `resolved_rai_date`, `note_date_parsed`, `date_confidence`, `date_status`
- **Avidity / scan flags** (3): `iodine_avidity_flag`, `post_therapy_scan_flag`, `pre_scan_flag`
- **Stimulated labs** (2): `stimulated_tg`, `stimulated_tsh`
- **Adjudication + linkage** (5): `adjudication_status`, `completion_status`, `surgery_link_score_v3`, `scan_findings_raw`, `source_note_type`

---

## 2. Methodology — extraction-faithfulness vs Tier-1 LLM source

Tier-1 SSOT: `main.note_entities_llm_rai_detailed` (already verified, 23/23 cols `na` as raw-LLM-mirror exempt — not a verifier target, but is the source-of-truth for extracted values).

Pattern reference: `feedback_extraction_faithfulness_llm_canonical.md` + `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (similar Tier-2 LLM-derived table) + `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern #9).

### 2a. Per-col verification map

For each col, the standard pattern is:

```sql
-- Re-derive the col fresh from upstream WHERE error=0
WITH source AS (
  SELECT research_id, json_extract(extracted_payload, '$.<jsonpath>') AS reproduced_val
  FROM main.note_entities_llm_rai_detailed
  WHERE error = 0
)
SELECT COUNT(*) AS rows_with_drift
FROM main.rai_treatment_episode_v2 t
LEFT JOIN source s ON ... -- match key
WHERE t.<col> IS DISTINCT FROM s.reproduced_val;
```

If the col carries a deterministic transformation (e.g., `dose_mci` parsed from text → numeric), verify the transformation logic against the raw text in `dose_text_raw` for a sample.

Specific cols:

- `rai_assertion_status` / `rai_term_normalized` / `rai_intent` → categorical extraction (controlled vocab); cross-check that values come from a known SSOT enum
- `rai_mention_raw` → raw text passthrough; extraction-faithful by design (no transformation)
- `rai_confidence` → numeric per-LLM-call score
- `dose_mci` → numeric from `dose_text_raw` parsing; verify the parser handles "150 mCi", "150mCi", "0.150 GBq", etc.
- `dose_confidence` / `dose_source` → metadata about parsing
- `dose_missingness_reason` → categorical reason for NULL dose (e.g., 'unparseable_text', 'document_not_available', 'pre_treatment_planning')
- `rai_date_native` (TIMESTAMP), `resolved_rai_date` (TIMESTAMP), `note_date_parsed` (TIMESTAMP) — see §2c calendar policy
- `date_confidence` → integer score
- `date_status` → categorical (e.g., 'parsed', 'inferred', 'unknown')
- `iodine_avidity_flag` / `post_therapy_scan_flag` / `pre_scan_flag` → BOOL extracted from note text
- `stimulated_tg` / `stimulated_tsh` → numeric; should match `canonical_labs_tg_v1` / `canonical_labs_tsh_v1` for the same encounter date when available
- `adjudication_status` / `completion_status` → categorical workflow status
- `surgery_link_score_v3` → numeric link-confidence from algorithm v3
- `scan_findings_raw` (INTEGER per registry — but col name suggests text — verify schema-vs-data alignment)
- `source_note_type` → from upstream note metadata (e.g., 'op_note', 'clinic', 'imaging')

### 2b. ⚠️ Episode-grain key probe (per `feedback_mention_grain_partition_probe.md`)

Before any aggregation, confirm `rai_episode_id` is a true unique key per row:

```sql
SELECT COUNT(*) AS n_rows, COUNT(DISTINCT rai_episode_id) AS n_distinct_keys,
       COUNT(*) - COUNT(DISTINCT rai_episode_id) AS dup_count
FROM main.rai_treatment_episode_v2;
```

Expect: 1,857 rows / 1,857 distinct (zero dups). If non-zero dup, document and check.

### 2c. ⚠️ Calendar-only date types (Logan-ratified)

Per `feedback_clinical_dates_calendar_only.md`: clinical event dates MUST be DATE not TIMESTAMP. The 3 TIMESTAMP cols in this table are:

- `rai_date_native` — TIMESTAMP, semantic = clinical RAI treatment date → should be DATE
- `resolved_rai_date` — TIMESTAMP, semantic = clinical RAI treatment date → should be DATE
- `note_date_parsed` — TIMESTAMP, semantic = note authoring date (provenance) → MAY remain TIMESTAMP if treated as audit timestamp; otherwise DATE

**Recommended action:** verify these against the calendar policy. If clinical-semantic, open `CF-mig148-RAI-DATE-RETYPE` and queue with the date-retype batch. If the policy clearly says retype now, do it within this lane via ALTER COLUMN ... TYPE DATE (TIMESTAMP → DATE is lossy on time-of-day; pre-snapshot to archive_pub_v1_0 first).

### 2d. ⚠️ Cohort-uniformity sanity check

For BOOLEAN cols (`iodine_avidity_flag`, `post_therapy_scan_flag`, `pre_scan_flag`):

```sql
SELECT
  SUM(CASE WHEN iodine_avidity_flag THEN 1 ELSE 0 END) AS avid_TRUE,
  SUM(CASE WHEN post_therapy_scan_flag THEN 1 ELSE 0 END) AS post_tx_TRUE,
  SUM(CASE WHEN pre_scan_flag THEN 1 ELSE 0 END) AS pre_scan_TRUE,
  COUNT(*) AS total_episodes
FROM main.rai_treatment_episode_v2;
```

Expected:
- `iodine_avidity_flag` → moderate TRUE rate (~30-50% of episodes)
- `post_therapy_scan_flag` → high (most therapy episodes have post-tx scans)
- `pre_scan_flag` → variable (depends on whether dosimetry was used)

Flag any near-uniform-TRUE OR near-uniform-FALSE.

### 2e. ⚠️ Cross-validate stimulated_tg / stimulated_tsh against labs canonical

`stimulated_tg` and `stimulated_tsh` should match `canonical_labs_tg_v1` (Tg+TgAb shared per `project_lab_consolidation_script_347_closeout.md`) and `canonical_labs_tsh_v1` for the same encounter date / research_id. Open `CF-mig148-STIM-LAB-LINKAGE` if drift > 5%.

### 2f. Sign-off SQL

File: `qc_framework_v1/migrations/148_rai_treatment_episode_v2_signoff_20260429.sql`

Two stages:

**Stage 1**: Per-col registry update for the 25 not_started cols → `verified` with batch_id and method.
**Stage 2**: Table-level signoff in `canonical_table_signoff_registry_v1`:

```sql
UPDATE main.canonical_table_signoff_registry_v1
SET table_status = 'verified',
    n_verified = 25,
    n_na = 7,
    n_not_started = 0,
    n_failed = 0,
    n_columns_total = 32,
    signoff_migration = 'qc_framework_v1/migrations/148_rai_treatment_episode_v2_signoff_20260429.sql',
    last_verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
WHERE table_name = 'rai_treatment_episode_v2';
```

```
batch_id = 'mig_148_rai_treatment_episode_v2_signoff_20260429'
verification_method options:
  - 'extraction_faithfulness_vs_note_entities_llm_rai_detailed'
  - 'json_extract_passthrough_per_path'
  - 'numeric_dose_parser_logic_check'
  - 'date_parse_logic_check'
  - 'cross_validate_vs_canonical_labs_tg_v1'
  - 'cross_validate_vs_canonical_labs_tsh_v1'
  - 'patient_level_episode_grain_aggregate'
  - 'auto_categorical_skip_with_vocab_check'
```

---

## 3. Acceptance gates

- 25 not_started cols flipped to verified (+ 7 already na = 32 total)
- Table-level signoff registry flips: `table_status='verified'`, `n_verified=25`, `n_na=7`, total=32
- gate 1 in 5-gate audit ticks **87 → 88** (this is the immediate effect)
- gate 4 = 0 (every verified row has full metadata)
- All 3 TIMESTAMP date cols handled (retyped or CF-queued)
- Cohort-uniformity sweep clean on 3 BOOLEAN cols
- Cross-validation against canonical_labs_tg_v1 / canonical_labs_tsh_v1 for stimulated cols

---

## 4. Don't touch (active parallel lanes)

- ETE (mig_140 LANDED), Survival (mig_141 LANDED), Small-clusters (mig_143 LANDED), RAI PM cluster (mig_142 BLOCKED — DO NOT RETRY until this lane closes)
- US/CT/MRI+PET/Nucmed prompts (mig_144-147) — these are PM-side and may run in parallel; do NOT touch any imaging-side cols
- After this lane closes, mig_142 RAI PM cluster can be re-run by the agent that originally stopped it

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `feedback_mention_grain_partition_probe.md`
- Auto-memory: `project_op_procedure_codes_mig_118_closeout.md` (hybrid pattern #9, Tier-2 LLM-derived precedent)
- Auto-memory: `project_lab_consolidation_script_347_closeout.md`
- Auto-memory: `reference_2digit_year_convention.md`
- Repo: `qc_framework_v1/migrations/118_operative_procedure_codes_signoff_20260429.sql` (similar Tier-2 LLM-derived table)
- Repo: `cursor_prompts/CURSOR_PROMPT_patient_master_rai_cluster_20260429.md` (the BLOCKED downstream lane that this unblocks)

---

## 6. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Single commit closing rai_treatment_episode_v2
- Surgical git add (just the migration file + maybe a small probe script if needed)
- DuckDB `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts
- Explicit `WHERE verification_status='not_started'` so re-runs are no-ops

---

## 7. If something unexpected surfaces

- Tier-1 source `note_entities_llm_rai_detailed` is exempt (registry-seeded as raw-LLM-mirror) — extraction-faithfulness from THIS table is the SSOT chain, do not need to verify it separately
- 1,857 rows but only 862 distinct rids — episode-grain is correct; do NOT collapse to patient grain in this lane (PM RAI cluster mig_142 will do that)
- TIMESTAMP date retyping is in scope OR queued — Logan-ratified policy says clinical dates MUST be DATE; if you queue rather than retype, document why in CF-mig148-RAI-DATE-RETYPE
- `dose_mci` parser logic is opaque → STOP, ask Logan; the parser SSOT is likely in `scripts/365_psh_pmh_meds.py` or `scripts/410-412_*` (RAI parsing scripts)
- `surgery_link_score_v3` algorithm undocumented → STOP, ask Logan
- More than 5 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 38 — RAI upstream unblocker for mig_142.

After this lane closes:
1. PM RAI cluster (mig_142) can be re-launched — same prompt as before (`cursor_prompts/CURSOR_PROMPT_patient_master_rai_cluster_20260429.md`); the upstream gate will now PASS.
2. gate 1 of 5-gate audit ticks 87 → 88 (rai_treatment_episode_v2 becomes the 88th verified canonical).
