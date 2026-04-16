# Canonical Changelog

---

## v1_0 — 2026-04-16

**Type:** baseline (initial publication)

**MotherDuck DB:** `thyroid_canonical_publication_v1_0`

**Built by:** `scripts/223_ingest_and_publish.py` (pre-versioning), renamed to
`thyroid_canonical_publication_v1_0` on 2026-04-16 via
`scripts/225_promote_canonical_version.py` versioning convention adoption.

**Contents:**

- **10,871** thyroid surgery patients (research_id 100% populated, zero nulls)
- **1,377** columns in `canonical_patient_master` (100% commented)
- **110 tables** total:
  - 1 patient master (`canonical_patient_master`)
  - 15 patient-level summaries
  - 58 episode-level tables
  - 30 NLP entity tables
  - 3 data dictionaries
  - 2 utility tables (`__readme`, `data_dictionary_parquet_v221`)
- 6 newly-ingested tables (not present in source prior to Script 223):
  - `mri_imaging` — 715 MRI exams (PHI-scrubbed)
  - `nsqip_enrichment` — 1,275 perioperative records (DOB removed)
  - `nsqip_patient_summary` — 1,261 NSQIP summaries (DOB removed)
  - `patient_completion_oed_path_linkage_v1` — 11,506 completion linkage rows
  - `thyroid_weights` — 10,001 gland weight records (DOB + path text removed)
  - `thyroid_sizes` — 11,675 standardized size records

**Script 221c gap-fix state (verified pre-build):**

| Invariant | Value |
|-----------|-------|
| Total patients | 10,871 |
| `null research_id` | 0 |
| `followup_years > 0` | 4,038 |
| `prm_first_fna_date IS NOT NULL` | 5,212 |
| `first_tg_date IS NOT NULL` | 2,721 |

**Built from:**
- Source DB: `"Thyroid 2026 UPdated"` on eras MotherDuck account
- Script 221c gap fixes applied before build

**Known gaps (will be addressed in v1_1 when data lands):**
- Lab pull pending: TSH/PTH/Ca/VitD baseline values not yet ingested
- US exam dates incomplete: ~4,082 patients missing baseline US date
- Molecular test dates incomplete: ~809 patients
- `followup_years = 0` for 6,833 patients (still in active follow-up or very early cohort)

---

## Canonical v1_0 finalization run (Scripts 237–247, started 2026-04-16)

Post-baseline fixup pass driven by coworker data-quality review. See
`CURSOR_PROMPT_224_CANONICAL_FIXES.md` for the source spec. Every script
carries a backup to `"Thyroid 2026 UPdated".archive_pub_v1_0` (where a
destructive op exists) and an assertion block. Scripts without a
pre-change backup are explicitly no-ops on data (documentation only).

### Script 237 — Document imaging↔FNA size concordance gap (no-op on data)
- **Type:** documentation-only (no row counts or cell values change).
- **Why:** `imaging_fna_linkage_v3.fna_size_cm` has no independent source in
  the canonical DB. The only candidate backfill path
  (`imaging_nodule_long_v2.size_cm_max` via `nodule_id`) is the same source
  `img_size_cm` already uses (verified: 9,911/9,911 byte-identical), so any
  derived `size_score` would be tautologically 1.0. Preserving the flat 0.5
  fallback is the correct v1_0 behavior until an independent FNA-side size
  extractor is built.
- **Changes:**
  - `COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm` / `.size_score`
    with v1_0 design intent + v1_1 TODO.
  - `UPDATE manuscript_workspace.detail_table_registry_v1` description for
    `imaging_fna_linkage_v3` to surface the gap.
  - `INSERT` 2 provisional rows into `data_dictionary_v240` (for
    `fna_size_cm` and `size_score`).
- **Assertions (10/10 PASS):** row counts unchanged; `canonical_patient_master`
  at 10,871; comments persisted; registry description updated; dictionary rows
  exactly 1 each.
- **Follow-up for v1_1:** build a targeted NLP pass over
  `note_entities_llm_us_nodule_dynamics` / `note_entities_llm_tirads_granular`
  to extract FNA-era nodule sizes, then re-run the scoring.

### Script 238 — Populate `serial_imaging_us` (one row per US exam, ≥2-exam pts)
- **Change:** populated the previously-empty `serial_imaging_us` shell using
  its existing 6-column schema. No new columns added.
- **Source:** `ultrasound_reports` (ultrasound_date, source_us_impression,
  clinical_impression) filtered to patients with ≥2 US exams in
  `ultrasound_reports`, hydrated with `imaging_nodule_master_v1` (largest
  `max_dimension_cm` per exam → `dominant_nodule_size_on_us`; its
  `location_raw` → `dominant_nodule_location`). No `dominant_nodule_flag`
  exists on `imaging_nodule_master_v1`; largest-dimension is the operational
  proxy.
- **Row counts:** 4,162 exams / 1,443 patients. 130 exams without nodule
  size (~3.1% miss rate — no matching `imaging_nodule_master_v1` row for
  the (research_id, ultrasound_date) pair).
- **Deliberate omissions:** TI-RADS trajectory (source broken —
  `imaging_nodule_long_v2.tirads_score` is 100% NULL; defer to v1_1).
  Per-patient summary columns (first/last/interval/n_us_exams) —
  `n_us_exams` already lives in CPM via a broader imaging source.
- **Assertions (9/9 PASS):**
  - serial_imaging_us was empty pre-script (0 rows)
  - `ultrasound_reports` baseline = 6,793 exams / 4,074 pts
  - 1,443 patients have ≥2 US exams (spec)
  - populated table: COUNT > 0, distinct pts = 1,443, rows = 4,162
  - ≥95% of exam rows have `dominant_nodule_size_on_us` (actual: ~96.9%)
  - for every patient in serial_imaging_us, CPM.n_us_exams ≥ local count
    (CPM counts the broader imaging source; it's a strict superset)
  - canonical_patient_master still 10,871 rows
- **COMMENTs added:** table + two columns with v1_0 design intent.
- **Follow-up for v1_1:** add TI-RADS trajectory after an independent
  per-exam TI-RADS source is built (imaging_nodule_long_v2 rebuild is
  tentatively part of Script 246).

### Script 239 — Fix `rai_benign_histology_recovery_v234` (decision: DROP)
- **Investigation:** ran the recovery-candidate query under three RAI
  criteria against `canonical_benign_diagnosis_v1`, `path_synoptics`
  (tumor 1..5), and `canonical_malignant_diagnosis_v1`:
  - RAI strict (`rai_assertion_status` ∈ {definite_received,
    likely_received}): **0** candidates
  - RAI any (any non-negated episode): **0** candidates
  - CPM `rai_received_reconciled=TRUE`: **0** candidates
- **Root cause:** NOT a Script 234 bug — the canonical benign /
  malignant diagnosis tables partition patients correctly. Everyone with
  path_synoptics malignancy who received RAI is already captured in
  `canonical_malignant_diagnosis_v1`. The 0-row outcome is the truthful
  state of the data.
- **Decision gate triggered:** per the finalization spec, an empty
  recovery table is deleted, not left as a TODO.
- **Actions:**
  - Archived empty shell (0 rows, schema preserved) to
    `"Thyroid 2026 UPdated".archive_pub_v1_0.rai_benign_histology_recovery_v234_pre239_backup_<ts>`
    for auditability.
  - Dropped `thyroid_canonical_publication_v1_0.main.rai_benign_histology_recovery_v234`.
  - Removed row from `__readme` and `manuscript_workspace.detail_table_registry_v1`.
- **Assertions (5/5 PASS):**
  - Live table does not exist in canonical
  - No `__readme` row for the table
  - No registry row for the table
  - `canonical_patient_master` unchanged at 10,871
  - ≥1 archive copy present in `archive_pub_v1_0`
- **If v1_1 re-classification surfaces new `rai_assertion_status` values
  (definite_received / planned / historical, which don't exist today),
  re-enable the populate branch** (`--force-populate`) in the stubbed
  Phase 2a.

### Script 240 — Deprecate broken CPM columns (rename + view sweep)
- **Renamed** on `canonical_patient_master`:
  - `imaging_nodule_size_cm` → `deprecated__imaging_nodule_size_cm`
    (broken per-patient aggregation: 44.8% MAX / 31.5% MIN / 15.1% MEAN
    across 3,439 patients; superseded by `dominant_nodule_size_cm`).
  - `tumor_size_cm` → `deprecated__tumor_size_cm`
    (byte-identical duplicate of `path_tumor_size_cm` across 4,130/4,130
    populated rows; superseded by `path_tumor_size_cm`).
  Both columns preserved — not dropped. `COMMENT ON COLUMN` carries a
  `DEPRECATED 2026-04-16 (Script 240): ... Will be removed in v1_1.`
  marker on each.
- **View sweep:** 65 views total in `manuscript_workspace`. 55 reference
  the renamed columns. Pre-flight classified them by FROM source:
  - **32 views reference CPM directly** → rewritten. Replacement:
    `deprecated__X AS X` (preserves output column surface for
    downstream consumers).
  - **23 views are downstream** (FROM another `manuscript_workspace`
    view) → NOT rewritten. Their upstream view's `AS X` alias
    re-exposes the original name, so they continue to work without
    editing.
- **Backups** (before any write):
  - `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre240_v2_backup_<ts>`
    (full row copy, 10,871 rows).
  - `"Thyroid 2026 UPdated".archive_pub_v1_0._view_ddl_snapshot_pre240_<ts>`
    (55 rows: the pre-rewrite DDL for every affected view, for rollback).
- **Mid-run correction:** first attempt rewrote all 55 affected views
  indiscriminately and failed at the 27th view when a downstream view
  tried to select `deprecated__tumor_size_cm` from a base view that only
  exposes `tumor_size_cm` (correctly). Rolled back cleanly from the
  snapshot, re-ran with the FROM-source split. No residual state.
- **Assertions (12/12 PASS):**
  - Both `deprecated__*` columns present in CPM; original names gone.
  - COMMENT carries DEPRECATED marker for each.
  - CPM row count unchanged at 10,871.
  - CPM column count unchanged at 1,505 (rename is metadata-only).
  - Full view-compile sweep: 65/65 pass.
  - All 55 affected views preserve their original column surface
    (original names still in `information_schema.columns`).
  - Both archive artifacts present in `archive_pub_v1_0`.
- **Follow-up for v1_1:** DROP the `deprecated__*` columns once all
  downstream views have been updated to use the replacement columns
  directly. The registered `canonical_tumor_characteristics_v1` work in
  Script 245 and its CPM-feed path will also help retire
  `deprecated__tumor_size_cm`.

### Script 241 — Build `path_size_adjudication_v241` (review artifact)
- **Created** `thyroid_canonical_publication_v1_0.main.path_size_adjudication_v241`
  with 96 rows (union of two criteria):
  - 68 patients where `ABS(path_tumor_size_cm - tumor_size_cm_max) > 2cm`
  - 37 patients with `path_tumor_size_cm > 10cm` (anatomically implausible)
- **Breakdown (rule × priority):**
  - `outlier_manual_review_required` / HIGH: **37** (path >10cm;
    proposed value = NULL)
  - `multifocal_use_rollup_max` / MEDIUM: **45** (`n_foci_path > 1`;
    proposed value = `tumor_size_cm_max`)
  - `unifocal_retain_path_size` / MEDIUM: **14** (unifocal with >2cm
    discrepancy; proposed value = `path_tumor_size_cm`)
- **Columns:** `research_id`, `path_tumor_size_cm`, `tumor_size_cm_max`,
  `n_foci_path` (from `specimen_tumor_focus_v1`), `n_tumors_path` (from
  `patient_tumor_rollup_v1`), `proposed_path_tumor_size_cm_adjudicated`,
  `adjudication_rule`, `review_priority`.
- **NOT applied to `canonical_patient_master`** — per spec, this is a
  review artifact only. Clinician sign-off required before any proposed
  values flow back.
- **Registry entry** added in `manuscript_workspace.detail_table_registry_v1`
  with `domain = Pathology/Adjudication` and `feeds_master_columns =
  'TODO: clinician sign-off; will feed deprecated__tumor_size_cm /
  path_tumor_size_cm manual-review queue in v1_1'`. Script 247 will
  resolve the TODO to a non-TODO description before the lock.
- **Assertions (9/9 PASS):** source columns present; baseline outlier
  counts match (68, 37); table row count in bracket [60, 120]; registry
  has exactly one row for the new table; CPM unchanged at 10,871; no
  NULL `research_id`; all rows carry a valid rule + priority from the
  closed sets.
- **Follow-up for v1_1:** clinician review → apply adjudicated values
  back to a new CPM column (e.g., `path_tumor_size_cm_adjudicated`) and
  retire `deprecated__tumor_size_cm`.

### Script 242 — Reconcile manuscript views: rai_received_flag → rai_received_reconciled
- **Invariant (verified pre-run):** `rai_received_reconciled` is a strict
  superset of `rai_received_flag`:
  - CPM rai_received_flag = TRUE:       **583** patients
  - CPM rai_received_reconciled = TRUE: **862** patients
  - legacy-only (TRUE legacy, FALSE reconciled): **0**
  - reconciled-only (FALSE legacy, TRUE reconciled): **279**
  862 also matches `rai_treatment_episode_v2` distinct patients exactly.
- **View classification (15 views reference `rai_received_flag`):**
  - **11 direct-CPM views** → rewritten. Replacements: SELECT-list
    context → `rai_received_reconciled AS rai_received_flag`
    (preserves output surface). Filter context → `rai_received_reconciled`
    (bare rename).
  - **4 downstream views** (FROM another view) → NOT rewritten. Upstream
    AS alias keeps output stable.
- **Filter-context views** (2 — bare `rai_received_flag = TRUE` in WHERE):
  - `cohort_m019_rai_outcomes_v1`: pre-rewrite size = 583-universe →
    post = **862** rows.
  - `cohort_m081_rai_resistant_v1`: pre-rewrite size = 583-universe →
    post = **862** rows.
  (Strict superset — row counts can only grow, matching spec.)
- **Backup:** `"Thyroid 2026 UPdated".archive_pub_v1_0._view_ddl_snapshot_pre242_<ts>`
  (15 rows: pre-rewrite DDL for every affected view).
- **Assertions (7/7 PASS):**
  - CPM row count unchanged at 10,871
  - all 11 rewritten views compile
  - full sweep: 65/65 manuscript_workspace views compile
  - no view lost rows (post ≥ pre for every affected view)
  - output surface preserved (`rai_received_flag` column still present
    on all 11 rewritten views via AS alias)
  - archive snapshot present in `archive_pub_v1_0`
  - CPM superset invariant: reconciled(862) ≥ legacy(583)
- **Follow-up for v1_1:** consider fully deprecating CPM
  `rai_received_flag` (parallel to Script 240 pattern). Today the column
  is kept because downstream `rai_received_flag`-as-output-name
  consumers still read it via the view surface.

### Script 243 — Extend `data_dictionary_v240` with status + replacement_column_name
- **Schema extension:** added two columns to `data_dictionary_v240`:
  - `status VARCHAR` — domain `{authoritative, deprecated, provisional,
    recovery_pending}`
  - `replacement_column_name VARCHAR` — points from deprecated entries
    to their replacement
- **Backup:** `"Thyroid 2026 UPdated".archive_pub_v1_0.data_dictionary_v240_pre243_backup_20260416`
  (copy of pre-script dictionary, pre-schema-extension).
- **Row updates (folding in 237-242 changes):**
  - Removed 2 stale rows for `tumor_size_cm` and `imaging_nodule_size_cm`
    (renamed to `deprecated__*` in Script 240; no longer exist on CPM).
  - Removed 0 rows for `rai_benign_histology_recovery_v234`
    (already absent post-Script 239; defensive DELETE).
  - Added 2 deprecated rows: `deprecated__tumor_size_cm` →
    `path_tumor_size_cm`; `deprecated__imaging_nodule_size_cm` →
    `dominant_nodule_size_cm`.
  - Marked Script 237's 2 provisional rows (`fna_size_cm`, `size_score`)
    explicitly as `status='provisional'`.
  - Added 5 authoritative rows for `serial_imaging_us.*` (Script 238).
  - Added 7 provisional rows for `path_size_adjudication_v241.*`
    (Script 241).
- **Final status breakdown:**
  - `authoritative`: **1,491**
  - `provisional`:   **9** (`fna_size_cm`, `size_score`, and the 7
    `path_size_adjudication_v241.*` columns)
  - `deprecated`:    **2** (`deprecated__tumor_size_cm`,
    `deprecated__imaging_nodule_size_cm`)
  - Total: **1,502** rows (was 1,490 pre-script; net +12 after
    removing 2 stale + adding 14).
- **Assertions (12/12 PASS):**
  - archive table row count matches pre-script source
  - both new columns present on dictionary
  - every `deprecated__*` CPM column has exactly one dictionary row
    with `status='deprecated'` and non-null `replacement_column_name`
  - no rows with `status IS NULL`
  - status values all in the domain set
  - no stale rows for the old column names
  - no `rai_benign_histology_recovery_v234` rows
  - path_size_adjudication_v241.* has exactly 7 provisional rows
  - serial_imaging_us.* has exactly 5 authoritative rows
  - archive backup present
  - canonical_patient_master unchanged at 10,871
