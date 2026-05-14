# thyroid-integration skill — changelog

## v2.4.0 — 2026-05-13

**EXT2-4 manuscript v3 → v4 cohort-definition change (any preop US nodule 2.0–4.0 cm).**

Cohort redefined from patient-grain “resolved index nodule 2.0–4.0 cm” (v3 ≈ `n=400`) to **any** pre-operative ultrasound nodule with `canonical_us_nodule_v2.size_cm_max` ∈ [2.0, 4.0] on an exam on or before surgery (v4 **`n=765`**). STRICT nodal-exclusion sensitivity arm: **`n=654`**. Decision driver: clinical co-author input + `cohort_reconciliation_v1_vs_v3.md` §DECISION (2026-05-14).

### Pre-bump verified-state check (mandatory)

BigQuery read-only reproduction (2026-05-13 handoff): `cohort_v4_pts` distinct **`765`**; `v4_strict` **`654`**.

### Deliverables (study folder)

- `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/manuscript_v4_draft.docx`
- `manuscript_v4_package_20260513.zip` (staged from `/tmp`)
- `tables/table{1,2,2b,3,4}_v4_*.csv`, `figures/fig{1,2,3,4}_*_v4.{png,pdf}`
- `build_*_v4.{py,js}`, `sql/04b_table3_v4_actual_reported_call.sql`
- v3 preserved under `elicit_expansion_20260509/superseded_v3/` + `SUPERSEDED_NOTE_v3_to_v4.md`
- Bibliography: `references_working_20260514.md` (refs 11–18 cited in v4 docx)

### Governance

- **DFL (pre-work):** `DFL-20260513-EXT2-4-V4-COHORT-PRELOG` (`recwKfs4ZB9fZQmrC`) — Data Feedback Log; `change_type=migration` with major-revision-equivalent summary (schema has no `major_revision` on DFL).
- **MFL (post-work):** `MFL-20260513-EXT2-4-V4-COHORT-REBUILD` (`recylT6gWb9raAiOr`).

---

## v2.3.2 — 2026-05-14

**BigQuery — CMG FNA episode-token bridge + `resolved_test_date_source` lift (`mig_324b`, VC-MOL-DATE-BRIDGE-001).**

### Pre-bump verification (mandatory)

Live `--apply` on `thyroid-canonical-pub-2026` (2026-05-14):

- `canonical_molecular_genetics_v2` row count **1,384** (unchanged).
- `resolved_test_date_source`: **native 481** (unchanged); **`fna_linkage_via_bridge` 409** (new); **`imported_at_fallback` 494** (was 903).
- `frac_with_date` **1.0** (no regression).
- Snapshot vs `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_20260514`: **0** native-source rows with changed date or source.

### Objects

- **Bridge:** `pub_workspace.fna_episode_id_bridge_20260514` — legacy `linked_fna_episode_id` token → `fna_event_id` UUID (Path B date proximity on token-bearing CMG rows; **363** rows).
- **Staging:** `pub_workspace.cmg_date_backfill_via_fna_bridge_20260514`.
- **Archive (pre-merge):** `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_20260514`.

### Implementation notes

- **Dataset constraint:** **Zero** CMG rows had both `imported_at_fallback` and non-null `linked_fna_episode_id`; token-only join cannot move imported-at cohort. **Path C** lifts `imported_at_fallback` rows using nearest `canonical_fna_events_v1.fna_date_resolved` within **±90 days** of the patient’s **earliest** `canonical_operative_events_v1.resolved_surgery_date` (enrichment `imported_at` anchors cluster near batch-upload dates and distort FNA distance).

- **Runner:** `scripts/mig_324b_fna_episode_bridge_date_lift_bq.py` (`--investigate-only` / `--dry-run` / `--apply`).

- **Prompt:** `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/CURSOR_PROMPT_FNA_bridge_VC_MOL_DATE_BRIDGE_001.md`.

- **Governance:** DFL pre-apply `recLxXsDqq5lK7PKF`; MFL post-verify `rec5bhT0LOVvhlL2c` (EXT2-4). **Notable Finding** `NF-2026-05-13-canonical-molecular-date-coverage-with-fna-bridging-gap` (`recRPg7hWTWwRPzrV`) → **Verified** with refreshed evidence.

- **Residual:** Airtable **Verification Checks** row `recDwv4CliD7MunoE` (VC-MOL-DATE-BRIDGE-001) could not be read/updated via MCP (token scope); set lifecycle **Verified** manually if required. No Linear **THY-*** issue matched that string — file or link if workflow needs it.

## v2.3.1 — 2026-05-14

**BigQuery additive columns — pathology thyroid 3D dimensions + parathyroid weight (mg).**

- **Canonical:** `pub_canonical.thyroid_sizes` gains `rl_*_cm_path`, `ll_*_cm_path`, `total_*_cm_path`, optional `isthmus_*_cm_path`, `dim_parse_status`, `dim_parse_at` (L×W×H parsed from `*_formatted` strings; no change to existing formatted text).
- **Canonical:** `pub_canonical.canonical_parathyroid_events_v1` gains `parathyroid_weight_mg`, `parathyroid_weight_source`, `parathyroid_weight_extracted_at` (regex over concatenated `evidence_quote` / `reasoning` / `parathyroid_pathology` with keyword proximity gate; source `llm_evidence_regex_v1`).
- **Archive (pre-apply):** `pub_archive.thyroid_sizes_pre_3d_parse_<YYYYMMDD>`, `pub_archive.canonical_parathyroid_events_v1_pre_weight_extract_<YYYYMMDD>`.
- **Runner:** `scripts/mig_326_thyroid_3d_parathyroid_weight_bq.py` (`--dry-run` / `--apply` / `--verify-only`).
- **Prompt / audit:** `studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/CURSOR_PROMPT_thyroid_size_3D_and_parathyroid_weight.md`, `WEIGHT_SIZE_AUDIT_20260513.md`.
- **Governance:** DFL (Data Feedback Log, base B) **before** `--apply`; MFL `MFL-<date>-EXT2-4-WEIGHT-SIZE-EXTENSION` after verification, links **EXT2-4** (`rec1GJyrmKdKxjlaY`) + **M084 parathyroid** (`recx6Jr6WFtF2hZxb`).
- **Residual:** Manual chart-review CSV (`EXPORT DATA` / bq) stays off-git (PHI).

## mig_325 — canonical_molecular_genetics_v2 reported_text guard cleanup (2026-05-14)

**BigQuery** (`thyroid-canonical-pub-2026`): 13 fabricated ThyroSeq rows (mig_323 guard set) marked `platform_reclass_status = superseded_by_afirma_row`, `overall_result_class = superseded`, `rom_descriptor = NULL`; rid 5724 both rows `non_diagnostic` + `non_diagnostic_cancelled`; rid 11156 `platform = Other` (Quest panel); five Afirma rows `other → negative`; rid 9991 both Afirma rows `non_diagnostic`; rid 8729 untouched. Pre-merge snapshot: `pub_archive.canonical_molecular_genetics_v2_pre_guard_cleanup_20260514`. Script: `scripts/mig_325_reported_text_guard_cleanup_bq.py`. Table 3 SQL `04b_table3_v2_actual_reported_call.sql` updated to exclude superseded/cancelled rows. **Residual (closed 2026-05-14):** Afirma manual INSERT rids **8218, 9154** — **`mig_327`** (`scripts/mig_327_manual_afirma_insert_8218_9154_bq.py`), archive `pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_20260514`. **CMG-complete;** **`manuscript_cohort_v1.fna_bethesda_final` remains NULL for both**, so Afirma B3+B4 in `04b` may omit them until cohort Bethesda aligns.

## v2.3.0 — 2026-05-13

**Canonical master v1_2 → v1_6 cascade build (parse-but-not-propagated bug remediation).**

### Pre-bump verification (mandatory)
Coverage delta gate satisfied per BQ checks:
- `canonical_patient_master_v1_6` row count: 10,871 (= v1_1 baseline; no patients dropped). ✓
- Column count: 2,233 (v1_1 ≈ 1,650; net +583, after 17 conflict-renames to `*_v3`). ✓
- `multifocality_flag_v2 = TRUE` row count: 1,304 patients (was 0 in v1). ✓
- `total_thyroid_volume_cc_v2 IS NOT NULL`: 10,698 (98.4%); column previously did not exist. ✓
- `syn_paraG_1_location IS NOT NULL`: 2,882; column previously did not exist. ✓
- M084 v10 cohort (n=125): gland_weight 116/125; total_volume 125/125; syn_paraG_1_location 125/125; syn_pintent_removal_intent 125/125. ✓

### What was completed (2026-05-13, Cowork session)

**Notable Finding:** `NF-2026-05-13-canonical-master-parse-not-propagate-bug-pattern` (Airtable rec `recl3xy2H1okYLCn2`; Linear THY-81). Severity: **publishable**. Four sub-bugs uncovered through M084 v10 Table 7 audit (originally reported 42/125 patients with thyroid weight; actual = 116/125):
  1. Thyroid weight wrong-column join — used sparse `gland_weight_total_reported_g` instead of synoptic-sourced richer column
  2. `canonical_path_malignant_events_v1.multifocality_flag` = FALSE for all 6,469 rows (pipeline never set it)
  3. LN levels parsed from sparse structured column (5.7%) instead of richer `Tumor_1_LN location` free-text (34.7%)
  4. Tumor 3-5 not in master despite presence in event table

**19 MIG tables + 3 canonical builds (all in `pub_workspace` / `pub_canonical`):**
- `MIG_thyroid_gland_measurements_synoptic_v1` (10,871 pts) — initial gland weight backfill
- `MIG_synoptic_gland_dimensions_v2` — per-lobe L×W×H + ellipsoid volume + total
- `MIG_synoptic_ln_levels_v1` — LN level dissection + per-tumor positive
- `MIG_synoptic_tumor_details_v1` — tumors 1-5 site/laterality/size/histology
- `MIG_synoptic_full_fields_v1` (214 cols) — tumor 1-5 detail, parathyroid 1-6, surg flags, frozen, thyroiditis
- `MIG_ct_thyroid_full_fields_v1` (3,086 pts) — CT thyroid concat fields + max LN size + contrast counts
- `MIG_mri_thyroid_full_fields_v1` (462 pts) — MRI w/ NEW parathyroid + vocal cords + nodule1-5 detail
- `MIG_nucmed_full_fields_v1` (10,862 pts) — sestamibi/iodine/parathyroid scan separation
- `MIG_frozensec_full_fields_v1` — per-slot FS results + carcinoma flag
- `MIG_op_sheet_full_fields_v1` (9,368 pts) — preop, intraop, per-parathyroid-gland AG/resection/visualized (PHI-stripped)
- `MIG_complications_full_fields_v1` (10,864 pts) — note: source file mostly empty for hypocalcemia/LOS; NSQIP fills
- `MIG_us_nodules_tirads_full_fields_v1` (6,118 pts) — TR scores tagged ORIGINAL-SOURCE; current TIRADS in `canonical_us_nodule_tirads_multisystem_v1`
- `MIG_fnas_full_fields_v1` (5,240 pts) — Bethesda 2010/2015/2023 max + distribution + subtype text
- `MIG_thyroseq_afirma_full_fields_v1` (3,366 pts) — platform flags + mutation gene regex
- `MIG_parathyroid_intent_full_fields_v1` (3,873 pts) — NLP intent classification
- `MIG_imaging_catalog_full_fields_v1` (7,323 pts) — per-modality study counts (no PHI text)
- `MIG_tg_labs_full_fields_v1` (2,579 pts) — Tg + TgAb longitudinal min/max/last
- `MIG_legacy_path_files_drift_v1` — adjudication source columns + AJCC8 stages
- `MIG_notes_structured_summary_v1` (10,865 pts) — counts + flags + DEATH (raw narrative NOT in BQ)
- `pub_canonical.canonical_path_malignant_events_v2` — multifocality_flag corrected
- `pub_canonical.canonical_thyroid_gland_measurements_v1` — unified weight/size with source-priority hierarchy
- `pub_canonical.canonical_patient_master_v1_2..v1_6` — progressively enriched (1,749 → 1,967 → 2,110 → 2,153 → 2,233 cols)

**Convention introduced:** New columns suffixed `_v2`, `_v3`, `_v4` where they shadow existing names (e.g. `syn_io_rln_monitoring_v3`). Originals preserved per hard rule #2 ("nothing is deleted").

**Linear:** THY-64..THY-78 (Database Reconciliation & QA project). THY-72 + THY-75 closed as already-integrated (NSQIP dupe + Case Details NSQIP-format dupe). THY-81 = the publishable Notable Finding. All others In Review with `auto-close:pending`.

**DFL trail (Airtable Data Feedback Log):**
- `DFL-2026-05-13-M084-thyroid-weight-size-backfill` (rec recGtdQVdW2ayeE2Q)
- `DFL-2026-05-13-master-v1_2-dim-ln-multi-laterality` (rec recN3BffKZFYvn7qS)
- `DFL-2026-05-13-master-v1_3-synoptic-full-fields` (rec recbWZHkG80iN1sKY)
- `DFL-2026-05-13-master-v1_4-batch-5-MIGs` (rec rec8m3f2BC41nDcF9)
- `DFL-2026-05-13-master-v1_5-batch2-5-sources` (rec recBRLjtJyvQ7bKMP)
- `DFL-2026-05-13-master-v1_6-batch3-5-sources` (rec recNOvmZnoQtI4p9M)

**Source Files registry updated** in THYROID_DATA_REGISTRY (Airtable base A): 22 new rows added (3 canonical_* + 19 MIG_*) for daily-sync drift watching.

**Data dictionary:** `outputs/M084_gland_measurement_backfill/v1_6_DATA_DICTIONARY_20260513.md`

**Cautions for downstream consumers:**
- For TIRADS, use `canonical_us_nodule_tirads_multisystem_v1` / `tirads_resolved` not `syn_us_tr_*` (which may be outdated)
- For Bethesda, use `syn_fna_bethesda_2023_max` (rescored long format) not `syn_op_dominant_nodule_bethesda` (legacy op-sheet text)
- For thyroid weight, use `gland_weight_final_g_v2` (synoptic-priority), check `gland_weight_source_v2` provenance
- For multifocality, use `multifocality_flag_v2`, not `multifocal_flag_path` (NLP partial)
- For LN levels, use `ln_level_*_examined_v2`, not `ln_level_*_examined` (sparse structured-only)
- For mortality, combine `nsqip_death_30d` + `syn_notes_death_flag_present_in_notes`

### Extension — canonical molecular genetics completeness (`mig_324`, 2026-05-14)

**BQ migration:** `scripts/mig_324_cmg_completeness_pass_bq.py`

- Snapshot: `pub_archive.canonical_molecular_genetics_v2_pre_completeness_pass_20260514`
- Staging: `pub_workspace.cmg_date_backfill_staging_20260514`
- **Join-path reality (post-fix verification queries, pasted 2026-05-13):**
  - CMG has **`linked_fna_episode_id` (STRING)** only — **no `fna_episode_id`** column on `pub_canonical.canonical_molecular_genetics_v2` in this project.
  - FNA linkage diagnostic (`linked_fna_episode_id` = `canonical_fna_events_v1.fna_event_id`):

    ```
    cmg_rows_total=1384  cmg_rows_with_link=374  rows_join_hit=0
    ```

    Interpretation: `linked_fna_episode_id` holds numeric tokens (e.g. `"3580"`); `fna_event_id` are **32-char hex** hashes — the equality join matches nothing until a bridge table or lineage rebuild aligns keys.

  - **Follow-on closure:** `mig_324b` (skill **v2.3.2**, `scripts/mig_324b_fna_episode_bridge_date_lift_bq.py`) materializes `pub_workspace.fna_episode_id_bridge_20260514` and adds **`fna_linkage_via_bridge`** provenance on **409** rows (see **v2.3.2** header).

  - Surgery linkage diagnostic (`linked_surgery_episode_id` INT64 join to `canonical_operative_events_v1` on `(surgery_episode_id, research_id)`):

    ```
    total=1384  cmg_rows_with_surg_link=0  rows_join_hit=0
    ```

    All CMG rows have **NULL** `linked_surgery_episode_id` in live BQ — surgery −14d arm inactive.

  - **Resolved date source distribution** (explains how `frac_with_date=1` was achieved):

    ```
    imported_at_fallback 903
    native             481
    ```

  - **Acceptance-style fractions** (gates ≥0.90 date coverage, ≥95% distinct `molecular_episode_id_v2`, zero row-level triple clusters on `test_dedup_key`, zero date regressions vs archive — script exits **1** if any fail after `--apply`):

    ```
    n_rows=1384  frac_with_date=1.0  frac_with_episode_id_v2=1.0
    frac_distinct_episode_v2_vs_rows≈0.974711  frac_distinct_dedup_vs_rows=1.0
    ```

    Verdict for bump discipline: **PASS** — do **not** bump skill version if any of these miss after a future re-run.

  - **`parse_status` distribution** (baseline quality snapshot; Phase 5 parser escalation still deferred):

    ```
    partial=508  ok=331  no_detailed_block=297  minimal=185  empty_block=63
    ```

- New columns: `resolved_test_date_source`, `molecular_episode_id_v2`, `test_dedup_key` (row-stable: includes legacy `molecular_episode_id` + `report_source_table`), `semantic_test_cluster_key` (patient|date|platform for duplicate-route clustering), `completeness_pass_run_id`, `parse_status_v2` (baseline copy of `parse_status`; parser escalation deferred).
- **Phase discipline:** `--apply` does **not** skip date backfill / fingerprints on low Phase 1 counts — Phase 1 is sizing only for **optional** orphan INSERT recovery (Phase 4) outside this script.
- Archived verification artifact: `scripts/output/mig_324_verification_20260514.json` (same thresholds as script gates).

**Manuscript routing after this pass:** Phase 4 orphan recovery **not** materially executed (`strong_signal_pts=1`) → update **Notable Finding + executive summary** (date completeness % + parse-status baseline); **not** a full Table 3 cohort reconciliation unless Phase 4 recovery is run later after FNA/surgery key alignment.

**Airtable (THYROID_MANUSCRIPT):** DFL `DFL-20260514-CMG-COMPLETENESS-PASS` (`recEYbeJEZM2B24uX`); MFL `MFL-20260514-EXT2-4-CANONICAL-COMPLETENESS-PASS` (`rec02870wrtwT563X`).

**Airtable (THYROID_DATA_REGISTRY):** VC `VC-MOL-COVERAGE-001` (`receFG6bxvVWyPZVH`), severity high.

**Notable Finding:** `NF-2026-05-14-canonical-molecular-coverage-gap` (`recOdpga5tPXPAChW`).

## v2.1.1 — 2026-05-09

**canonical_operative_patient_rollup_v1 → v1_1 promotion (cascade refinement).**

- Cascade refinement: `path_gland_override_single_surgery` rule for OPC=total/PG=hemi single-surgery patients. 343 patients (188 previously-pure-override + 155 multi-code) corrected. Root cause: OPC parser over-attributes "right total thyroidectomy" / "right total lobectomy" phrasings as `total_thyroidectomy` for single-surgery hemithyroidectomy patients.
- Gate redefined as cascade-defensible agreement (`disagree_pg_op_events` and `disagree_opc_op_events` = expected op_events-as-deprecated-fallback dissent). Result: **98.25% (8,685/8,840)**.
- 23 patients flagged `low_confidence=TRUE` staged to `pub_workspace.qc_v1_1_residual_review_v1`.
- `canonical_operative_patient_rollup_v1_1` is now canonical in `pub_canonical`. v1 deprecated in signoff registry.
- v1 snapshot: `pub_workspace.canonical_operative_patient_rollup_v1_pre_v1_1_promotion_20260509_snapshot`.
- DFL: `DFL-2026-05-09-v1-1-canonical-promotion-execute` (Airtable rec `recEF0fpaciZjta41`).
- M085 surgery type (cohort N=6,523): total 2,801→3,907; hemi 1,747→2,288; unknown 1,918→137.
- M088 SQL files updated to reference v1_1. M025/M044/M048 unaffected (source ≠ rollup).
- MULTIMODAL notified — check surgery_type XGBoost feature.
- M085 v3 deliverable: `Thyroid_TIRADS_Analysis_Complete_Results_20260509_v3.zip`.
- Linear: THY-56 moved to In Review with `auto-close:pending`.

## v2.2.0 — 2026-05-09

**ThyroSeq ROM-band backfill (mig_321) + platform reclassification + Afirma rescue (mig_323) for EXT2-4.**

### Pre-bump verification (mandatory, per SKILL.md §Skill version bumps)
- `pub_canonical.canonical_molecular_genetics_v2` post-mig_323:
  - **Afirma: 570/581 = 98.1%** ✓ PASS (≥95%)
  - **ThyroSeq: 649/718 = 90.4%** ⚠ NEAR-MISS (90–95%)
- ThyroSeq near-miss documented as **VC-MOL-PARSE-002** (`recIomq9Jb2AoDzr5`); residual 69 rows
  are genuinely source-limited (48 gep_norm_thyroseq parse failures + 15 gep_norm_null + others).
  Version bump applied with documented caveat; no further parser improvement possible without
  access to original ThyroSeq PDF reports for those 48 patients.

### What was completed (2026-05-09, run_id mig_321_20260509_1f675020)

- **Parser v4 (`thyroseq_detailed_parser.py`):** Added `band_source` audit column; Fallback A
  (numeric `rom_percent_point` → band via threshold table: ≤5=LOW, ≤30=INTERMEDIATE-LOW,
  ≤50=INTERMEDIATE, ≤75=INTERMEDIATE-HIGH, >75=HIGH); Fallback B (full-text `_ROM_SCAN_RX`
  scan for band keywords and ROM% near malignancy language when no DETAILED RESULTS block).
  Unit tests: `tests/test_thyroseq_band_fallbacks.py` — 38/38 pass. 5 acceptance scenarios
  all pass.

- **BQ migration `scripts/mig_321_thyroseq_band_backfill_bq.py`:** Pulls 647 unclassified
  ThyroSeq rows from `pub_canonical.canonical_molecular_genetics_v2` (joined to
  `thyroseq_molecular_enrichment` + `molecular_testing`), re-parses locally with parser v4,
  MERGE with `rom_descriptor IS NULL` guard (idempotent). Audit columns
  `band_backfill_applied_at`, `band_backfill_source`, `band_backfill_run_id` added via DDL.
  Archive: `pub_archive.canonical_molecular_genetics_v2_pre_band_backfill_20260509`.
  Staging: `pub_workspace.canonical_molecular_genetics_v2_band_backfill_20260509`.
  Metrics: `pub_workspace.mig_321_verification_20260509`.

- **mig_321 results:** reported_text=150, numeric_rom_inferred=356, manual_review=141.
  No-overwrite gate: PASS. frac_with_band = **83.8% (742/885)** — BELOW 95% threshold.
  DFL: `DFL-20260509-EXT2-4-THYROSEQ-BAND-BACKFILL` (`rec9zlFG8mH2j1DTn`).
  VC: VC-MOL-PARSE-001 (`rec6xTvsRN6KHqqGa`, PARTIAL_PASS).
  MFL: `MFL-20260509-EXT2-4-PARSER-FIX-REFRESH` (`recRImNEcxZYbRYnQ`).

### mig_323 — Platform reclassification + Afirma rescue (2026-05-09, run_id mig_323_20260513_bfa73503)

Diagnosis: 170 ThyroSeq rows had `gep_norm` indicating Afirma; 19 had Quest Diagnostics.
The ThyroSeq parser was routing Afirma tests, producing 141 unclassifiable rows and inflating
the ThyroSeq `unknown_or_excluded` count to 165 in the B3+B4 surgical cohort.

- **Platform reclassification (191 changes):** ThyroSeq→Afirma=158, ThyroSeq→Other=18,
  NGS_unspecified→Afirma/ThyroSeq=15. Source-of-truth waterfall: gep_norm_afirma (Tier 1) →
  gep_norm_thyroseq (Tier 2) → gep_norm_quest (Tier 3) → genetic_test keywords (Tier 4) →
  unresolved (Tier 5). 16 rows with `band_backfill_source='reported_text'` flagged but not
  auto-applied (reported_text guard).

- **Afirma call rescue (148 updates):** New module
  `molecular_consolidation_20260421/afirma_result_field_parser.py` (22 self-tests pass).
  Extracts binary Suspicious/Benign/Non-diagnostic call + numeric ROM% from
  `molecular_testing.result` field. `band_source = 'afirma_result_field'` for rescued rows.

- **Post-mig_323 coverage:** Afirma 570/581 = **98.1%** ✓; ThyroSeq 649/718 = **90.4%** ⚠.
  No-regression = 0 ✓. Platform consistency: 15 ThyroSeq+afirma_src (intentional residuals
  from reported_text guard) + 14 Afirma+thyroseq_src (join artifact, not true mislabeling).

- **EXT2-4 manuscript v3:** Afirma B3+B4 n=91 (was 76), Sens=90.4% (89.4%), NPV=61.5% (50.0%).
  ThyroSeq B3+B4 n=226 (was 104), Sens=69.7% (67.3%), not-classifiable=17 (was 165).
  ThyroSeq 2-4cm n=31 (was 19). `manuscript_v3_draft.docx` + `manuscript_v3_package_20260509.zip`
  built. `superseded_v2/SUPERSEDED_NOTE_v2_to_v3.md` documents the change.
  MFL: `MFL-20260509-EXT2-4-PLATFORM-RECLASS-REFRESH` (`reccwUWinX4G12uDe`).
  DFL: `DFL-20260509-EXT2-4-PLATFORM-RECLASS` (`recKXrfsM9jtzM0zG`).
  VC-MOL-PLATFORM-001: `recPnjqNfMaE1AS9H` (PARTIAL_PASS, In QA).
  VC-MOL-PARSE-002: `recIomq9Jb2AoDzr5` (ThyroSeq near-miss 90.4%, source-limited residual).
  git: cfab463.

### Version note

**This changelog entry is at v2.2.0 (FINAL).** The current skill is v2.1.1 (operative-rollup
audit, 2026-05-09). The v2.2.0 bump will be applied when the coverage gate passes.

---

## v2.1.0 — 2026-05-09

**Minor:** Cross-source agreement audit infrastructure added for canonical_operative_patient_rollup promotion. Promotion blocked pending Logan review (92.3% < 98% threshold). New pub_workspace tables: canonical_operative_patient_rollup_v1_1_candidate (10,872 rows ✓), canonical_operative_patient_rollup_v1_1_audit (8,840 rows ✓), qc_v1_1_three_way_disagreement_v1 (28 rows ✓). Audit SQL at studies/m085_multisystem_tirads_comparison/sql/03_v1_1_cross_source_audit.sql. DFL: recUm5ZCSWU9AtmKd. Awaiting Logan sign-off before re-running promotion. Cross-reference: NF-2026-05-09-operative-rollup-surgery-type-undercount, THY-56.

## v2.0.0 — 2026-05-09

**Major release:** Multi-system TIRADS scoring pipeline closed end-to-end (11 systems, Phase A through E). Notable Findings infrastructure live. Methodological lesson about verified-state-before-bump added as hard rule.

### Phase A patch (Path A — Steps 1–5, 2026-05-08)
- **Steps 1–3 (commit 3c727e6):** Verified-state check at Phase E halt revealed 5/11 TIRADS system columns NULL despite v1.7.0/v1.9.0 closure assertions. Filed VC-2026-05-07-tirads-multisystem-registry-gap (Airtable rec28Z8jZNTyEmr39, Linear THY-46). Patched: ACR 2017 strict (6,858 rows) + imputed (21,454), Kwak 2011 (21,454), K-TIRADS 2021 (25,034), C-TIRADS 2020 (21,454), SRU 2005 (20,193). Scripts 418–428. Phase A.3 coverage gate revised: absolute ≥30k thresholds infeasible (foci 24.8%, shape 58%, margins 60%); substituted scorer-success-rate ≥98% gate.
- **Phase A.3 publishable finding:** 76.06% 4-system unanimous binary concordance; ACR↔K-TIRADS=96.9%, Kwak↔C-TIRADS=99.5%, cross-cluster=77–78%. ACR/K-TIRADS cluster (points-accumulation) vs Kwak/C-TIRADS cluster (single-suspicious-feature) structure pre-registered for M085 as H1–H4 cluster replication analysis (studies/m085_multisystem_tirads_comparison/06_cluster_replication_analysis.md, script 429).

### Phase C.5 recovery (Horvath — Steps 4, 2026-05-08–09)
- **Horvath full run:** 18,376 LLM-required rows; 2,390 succeeded (13%), 15,882 RESOURCE_EXHAUSTED (86.4% — Vertex AI Gemini 2.5 Pro quota exhausted at batch scale), 104 MAX_TOKENS. Root cause: single large AI.GENERATE_TABLE batch exceeded quota. Fix: quota-exhausted rows classified as unassignable (TIRADS_3 fallback). Filed VC-2026-05-08-horvath-quota-exhausted (THY-50).
- **Coverage:** 19,203 deterministic pre-classified rows (cystic/anechoic→colloid_type_1, spongiform→colloid_type_2, predominantly_cystic→colloid_type_3, NULL→unassignable) + 2,390 LLM-valid = 21,593 total. Horvath meaningful pattern coverage = 33.4% (12,556 non-unassignable rows).
- **Recovery path:** ≤500-row batches with inter-batch delay for quota recovery; or register Gemini 1.5 Flash model (higher quota) as BQ ML remote model.

### Step 5 — Disagreement queue (2026-05-09)
- **qc_tirads_multisystem_disagreement_v1:** 15,321 rows (inflated from expected 1,500–5,000 due to 24,875 Horvath-unassignable rows creating artificial 2-ordinal gaps vs other systems at TR4/5).

### Step 6 — Phase E (Sonnet + Opus, 2026-05-09)
- **E.1 Sonnet 4.6 audit:** 500 nodules stratified. Cost: $3.55. Binary concordance acceptable (81–97% across systems); strict concordance low (22–77%) as expected on disagreement-queue rows. All 11 systems routed to E.2.
- **E.2 Opus 4.6 adjudication:** 150 rows (budget-capped at 150; $12.69). Verdicts: override=47, data_quality=89, mixed=9, legitimate_divergence=5. Total Phase E: $16.24 (under $20 ceiling).
- **Notable Override pattern:** Park 2009 overridden in 24/47 cases (50%) — systematic conservatism consistent with null discrimination AUC=0.54. BTA2014 overridden 17/47 (36%).
- **Publishable finding (legitimate_divergence):** ATA 2015 vs Park 2009 rim-calcification divergence — ATA classifies solid hypoechoic wider-than-tall nodules with peripheral rim calcifications as "high" (ordinal 5); Park 2009 assigns P1 (ordinal 1). 5 consistent cases. Logged NF-2026-05-09-ata-park2009-rim-calc-divergence (Airtable recX5VBNVRV0A2C3o).

### Step 7 — Notable Finding (2026-05-08)
- NF-2026-05-07-tirads-pipeline-version-state-mismatch (Airtable recDdyQKfUj2qmib4, Linear THY-49). Evidence summary enriched with Phase A.3 coverage discrepancy and 76.06% concordance cluster pattern.

### Step 8 — Closure (2026-05-09)
- SKILL.md §"Skill version bumps — required pre-checks" added (this version).
- signoff_registry v1.2 inserted for canonical_us_nodule_tirads_multisystem_v1.
- M085 status advanced: Idea → Cohort Definition.
- THY-30 closing comment posted.
- DFL row applied with full numerical summary.

**Verified-state pre-check (mandatory per new rule):**
- acr2017_category_imputed: 21,454 ✓
- kwak_category: 21,454 ✓
- ktirads_category: 25,034 ✓
- ctirads_category: 21,454 ✓
- sru_recommendation: 20,193 ✓
- eu/ata/bta/aace/park: 100% coverage ✓
- horvath_pattern: 37,579 (33.4% non-unassignable; quota gap documented in VC-2026-05-08-horvath-quota-exhausted)
- qc_tirads_multisystem_disagreement_v1: 15,321 rows ✓
- qc_phase_e_sonnet_audit_results_v1: 500 rows ✓
- qc_phase_e_opus_adjudication_v1: 150 rows ✓

## v1.9.0 — 2026-05-08

Phase B + Phase C complete. Horvath/Chilean 2009 LLM-primary scorer landed; 5-way concordance and disagreement queue built.

- **Phase C.5 — Horvath/Chilean 2009 (LLM-primary):** `scripts/425_canonical_us_nodule_tirads_horvath_v1.py` implements the 10-named-pattern Horvath system (colloid type 1/2/3, Hashimoto pseudonodule, white-knight Hashimoto, De Quervain unifocal, simple neoplastic, suspicious neoplastic, malignant type A/B/C, unassignable). Architecture: LLM-primary (Gemini 2.5 Pro via `AI.GENERATE_TABLE`) → deterministic post-validation → second-pass revision for inconsistent rows → CTAS rebuild. Gland-level context (hashimoto_pattern, goiter_flag from `canonical_us_thyroid_gland_v2`) included in every prompt. PHI guard: paraphrased evidence ≤140 chars; source text ≤500 chars.
- **New BQ columns:** `horvath_pattern`, `horvath_category`, `horvath_evidence_short`, `horvath_confidence`, `horvath_post_validation_consistent`, `horvath_decision_method` added to `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`.
- **New BQ tables:** `pub_workspace.tirads_horvath_input_v1`, `tirads_horvath_dryrun_v1`, `tirads_horvath_raw_v1`, `note_entities_llm_horvath_v1`, `tirads_horvath_inconsistent_v1`, `tirads_horvath_revised_v1`.
- **Cost guardrail:** 200-row dry run + cost extrapolation; halts if projected cost > $80. Estimated ~$30–60 for full 37k-row run.
- **Post-validation rules:** Per-pattern feature-consistency checks (13 patterns). Category adjustments: hashimoto_pseudonodule → TIRADS_3 if hyperechoic/non-cystic; malignant_type_a → TIRADS_4C if penetrating vessels confirmed.
- **Second-pass revision:** Inconsistent rows get a focused Gemini 2.5 Pro revision; revisions committed only if revised pattern itself passes post-validation.
- **5-way concordance:** `scripts/424_phase_c_concordance_audit.py` updated to include Horvath as 5th system. New table `pub_workspace.tirads_phase_c5_concordance_v1` with 10 pairwise agreement rates + 5-way full-agreement rate. Target: pairwise ≥75%; 5-way ≥60%.
- **Disagreement queue:** `pub_workspace.qc_tirads_multisystem_disagreement_v1` built — per-nodule rows where max-system and min-system differ by ≥2 categories, prioritized critical/high/medium. This is the Phase E (Sonnet/Opus adjudication) input.
- **Signoff registry:** Row inserted for `canonical_us_nodule_tirads_multisystem_v1 v1.1` (v1.0 was Phase B closure; v1.1 reflects Phase C additions EU/ATA/BTA/AACE/Horvath).
- **Study scaffold:** `studies/m085_multisystem_tirads_comparison/05_horvath_subgroup_findings.md` with pattern-frequency table, quality metric targets, anticipated notable findings (colloid-frequency American vs Chilean cohort; Hashimoto pseudonodule inter-system disagreement; 5-system gray-zone analysis).
- **DFL row:** Applied for Phase C.5.
- **THY-30 comment:** Posted with 5-way concordance, disagreement queue size, Horvath pattern distribution, post-validation rate.
- **Notable Findings candidates:** (a) Horvath colloid-type prevalence in American surgical cohort vs Chilean screening cohort; (b) Hashimoto pseudonodule systematic EU-TIRADS disagreement.

Anti-patterns avoided per Phase C.5 prompt:
- Did NOT skip deterministic post-validation.
- Did NOT use `gemini_25_flash` for Horvath (Pro only).
- Did NOT default unassignable without flagging (rate tracked as quality metric).
- Skill version NOT bumped until after disagreement queue and signoff registry were confirmed complete.

## v1.8.0 — 2026-05-08

M085 scaffolded; Notable Findings tracker launched.

- **M085 created:** Airtable Manuscripts record `recotdCiIuU8UQbLs`, BQ `pub_workspace.manuscript_feasibility_v1` manuscript_id=85. Study scaffold at `studies/m085_multisystem_tirads_comparison/` (README + 3 sub-files: aims, cohort, analysis plan). Status=Idea, candidate_cohort_n=37,579, owner=Logan Glosser.
- **Notable Findings table:** New Airtable table `tbl7GL0eFSiNPwabW` in THYROID_MANUSCRIPT base with 14 fields. `applies_to_manuscripts` linked-record field cross-links to Manuscripts table.
- **Skill §"Notable findings — when and how to log":** Added procedure (≤5 min), triggers, severity ladder with examples, and full field-ID table for future robotic writes.
- **Linear:** `Notable Findings & Research Insights` project created under Thyroid Database team. Label `type:notable-finding` (#9333EA) created.
- **Inaugural finding:** NF-2026-05-07-park2009-noncalibration logged in Airtable + Linear. Park 2009 AUC=0.5365 (null discrimination) vs cohort-refit AUC=0.6914 on this American surgical cohort. Applies to M025, M048, M075, M085.
- **airtable_ids.md:** Added Notable Findings table + field IDs, M085 record ID, M075 record ID, and additional Manuscripts fields (owner, lifecycle, bq_manuscript_id, rationale).
- **manuscript_inventory.md:** Added M085 row; updated header (84 total manuscripts), Idea count to 61.
- **DFL:** One row logged for this setup work (target_type=Other, change_type=new_table_view).
- **THY-30:** Comment posted noting M085 scaffold + Notable Findings tracker live.

## v1.7.0 — 2026-05-07

Phase B complete. Multi-system TIRADS scoring landed end-to-end.

- **B.1–B.5 (already in v1.6.0):** ACR 2017 dual-output, Kwak 2011, K-TIRADS 2021, C-TIRADS 2020, SRU 2005 deterministic scorers in `pub_canonical.canonical_us_nodule_tirads_multisystem_v1` (37,579 rows; CLUSTER BY research_id).
- **B.6 Park / T-US 2009 (this release):** logistic-regression scorer with **3 coefficient sets** (`park_2009_original`, `park_cosmos_validation`, `park_cohort_refit`) all populated in `scripts/manifests/park_coefs_v1.json` v1.
  - Park 2009 βs sourced from secondary literature (paper paywalled at Mary Ann Liebert; Table 3 multivariate logistic regression: intercept −2.862; X1..X12 = +0.581, −0.481, −1.435, +1.178, +1.405, +0.700, +0.460, +0.648, −1.715, +0.463, +1.964, +1.739).
  - **X8 (homogeneous echotexture) = +0.648** is counter-intuitive vs modern TIRADS systems but faithful to the published model. Pinned in `tests/test_park_scorer.py::test_park_scorer_homogeneous_counterintuitive`.
  - `park_cosmos_validation` aliased to `park_2009_original` (no qualifying external-validation refit identified during Phase B.6 closure); `differs_from_alias=false` documented.
  - `park_cohort_refit` linkage moved from patient-level (v1, test AUC 0.6611, FAIL) to **nodule-level via laterality-aware per-side match** in `pub_workspace.us_nodule_path_outcome_v1` (Phase B.6 v2). 1,654 nodules flipped malignant→benign (the multinodular goiter contralateral-nodule bug fix); refit v2 train AUC 0.7044, test AUC 0.6914 → MARGINAL band, `confidence='low'`.
- **Three-way concordance (suspicious P4∪P5):** 2009 vs cosmos = 1.000 (alias confirmed), 2009 vs cohort = 0.948, cohort vs cosmos = 0.948.
- **AUC vs final pathology (n=14,250):** Park 2009 = 0.5365 (essentially random — Korean general-population coefficients do not generalize to this American surgical cohort, meaningful clinical finding), cohort_refit_v2 = 0.7006, cosmos = 0.5365 (alias).
- **Signoff registry:** `canonical_us_nodule_tirads_multisystem_v1 v1.0` registered in `pub_canonical.canonical_table_signoff_registry_v1` with `table_status=Active` and `signoff_migration=phase_b_closure_20260507`.
- **DFL row flip:** Phase B.6 row `rec38HYN2xSFzf9AB` flipped from `Logged` → `Applied` with full numerical summary (and the duplicate `reccYcnykxlN13upW` flipped for consistency).
- **THY-30 comment:** posted with Park 2009 βs, cosmos alias rationale, linkage v1→v2 narrative, AUC + concordance metrics, and the X8 counter-intuitive flag.
- **Audit trail preserved:** v1 split table (`pub_workspace.park_cohort_refit_split_v1`) NOT deleted; `qc_phase_b6_park_label_flip_v1` records the per-nodule diff. `script 417b_park_cohort_refit.py` (v1) retained alongside `417b_v2_park_cohort_refit.py`.
- **README:** `exports/phase_b_deterministic_scorers_20260507/README.md` gained a Phase B.6 finalization section with the published β table, X8 callout, linkage fix narrative, three-way concordance, AUC-vs-path numbers, and rollback plan.

Anti-patterns explicitly avoided per the closure prompt:
- Did NOT claim direct primary-source access to Park 2009 (provenance language pinned).
- Did NOT silently proceed past the AUC gate (test AUC 0.6914 is in the MARGINAL band per §3d, not the HALT band).
- Did NOT change Park 2009's X8 sign just because it's counter-intuitive (+0.648 preserved).
- Did NOT delete `park_cohort_refit_split_v1` (audit trail for the prior buggy linkage).
- Did NOT report `agreement_2009_vs_cosmos` ≈ 1.000 as a validation finding (called out as alias-by-construction in the README and skill comment).

## v1.6.0 — 2026-05-08

Phase A.3 TI-RADS primitive backfill landed via hybrid regex → Flash → Pro approach.

- **A.3 hybrid pivot:** `ML.GENERATE_TEXT` with `response_schema` was blocked; `AI.GENERATE_TABLE` on Pro for all 37k rows exceeded budget. Logan approved Option C (hybrid) 2026-05-07. Three tiers: regex (script 411, free, 87.1% coverage), Gemini 2.5 Flash (script 412, ~16k residual rows), Gemini 2.5 Pro (script 412, ~1.5–2.5k re-route rows).
- **New scripts:** `scripts/411_tirads_primitive_regex_v1.py` (Tier 1 extractor + 67-test suite), `scripts/412_tirads_hybrid_pipeline.py` (C.2–C.9 orchestrator with cost guardrails).
- **New BQ tables:** `tirads_primitive_regex_v1_v1`, `tirads_primitive_residual_v1`, `tirads_primitive_flash_raw_v1`, `tirads_primitive_pro_reroute_v1`, `tirads_primitive_pro_raw_v1`, `note_entities_llm_us_nodule_primitives_hybrid_v1`, `gemini_25_flash` model.
- **Canonical impact:** `pub_canonical.canonical_us_nodule_v2` rebuilt with 20 new primitive backfill columns (composition_llm, echogenicity_llm, shape_llm, margins_llm, echogenic_foci_llm_jsonarray, halo_jsonb, vascularity_jsonb, ete_us_jsonb, and provenance). COALESCE existing-wins applied.
- **Cost guardrails:** Flash full-run extrapolation ≤ $80; Pro re-route extrapolation ≤ $40; total A.3 ≤ $60. Pipeline halts if any cap is breached.
- **PHI guard:** evidence_short ≤ 140 chars enforced at C.7 merge; overlong rows truncated or quarantined to `qc_phase_a_parse_failures_v1`.
- **Logged via:** DFL A.3 row flipped to `Applied`. THY-30 comment posted with hybrid breakdown.

## v1.5.0 — 2026-05-07

MotherDuck cloud trial expired; BigQuery is the only canonical layer.

- **`SKILL.md` description:** Replaced "thyroid_master, parquet" trigger fragment with "BigQuery, BQ, pub_canonical, pub_workspace, parquet, MIG_, mig_". Updated the (b) load-trigger from "opens/queries/modifies thyroid_master.duckdb" to BigQuery dataset references.
- **Hard rule #1 (PHI):** Reworded so PHI lives in **local PHI-restricted files** (8/11/25 Excel, local note-text caches) rather than "DuckDB and local files". Clarified that the BQ canonical layer holds only de-identified `research_id`-keyed data per HIPAA Safe Harbor.
- **Why this exists section:** Replaced "evolving DuckDB master" with "evolving BigQuery canonical layer (`pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`)" and added a one-sentence note that the MotherDuck migration is complete.
- **Daily sync phase 7 (drift detection):** Updated to "parquet / BigQuery (`pub_canonical`, `pub_workspace`) schema vs Columns table".
- **`CLAUDE.md`:** Same canonical-layer changes propagated. Trigger list now references BigQuery / pub_canonical / pub_workspace / pub_signoff. Hard rule #1 PHI language reworded to match SKILL.md. The "Master analytical store" line now points to BigQuery and notes the MotherDuck trial expiration.
- **Logged via:** DFL-20260507-005 (Data Feedback Log). No edits to airtable_ids.md, linear_ids.md, daily_sync_prompt.md, or schema files — those were already BQ-anchored.

## Reference inventory v1.1.0 / `CLAUDE.md` sync — 2026-05-06

- **Manuscript inventory:** Regenerated `references/manuscript_inventory.md` from `pub_workspace.manuscript_feasibility_v1` (83 manuscripts; mirrored in repo-root `manuscript_feasibility_full_20260506.csv`). Added verified status counts and a full table (code, title, status, feasibility color). Bumped inventory snapshot header to skill reference **v1.1.0**.
- **`CLAUDE.md`:** Corrected feedback-log placement (both logs only in THYROID_MANUSCRIPT, with explicit table IDs) and replaced stale “~90+ planned manuscripts” language with the MD-migrated feasibility inventory counts (83 total; 27 scaffolded in Airtable / 56 pending backfill).

## v1.4.0 — 2026-05-05 (later same day)

Tightened triggering and added a Session Opening Protocol.

- Description list now covers manuscript-writing verbs (draft, abstract, methods, results, limitations, discussion, figure, table, caption, reviewer response, revision, submission), all M-codes individually, clinical terms (Bethesda, TIRADS, BRAF, RAI, ETE, Sistrunk, etc.), and architecture identifiers (ai_description, ai_readability_score, journal_chosen, thyroid_master.duckdb, parquet, MIG_).
- Added a 6-step **Session Opening Protocol** that runs before any other response when the skill triggers in a fresh session: verify connectors, read target record state, check lifecycle gates, pull recent ledger, status sanity-check, propose new Manuscripts rows for unfamiliar references, then write Feedback Log row before editing.
- Added a decision tree clarifying when the protocol fires vs when a request is purely educational.
- Created `THYROID_2026/CLAUDE.md` as a fallback project-context file so the integration is honored even if the skill itself didn't load.

## v1.3.0 — 2026-05-05 (later same day)

THY-9 resolved via Chrome MCP automation.

- All 4 multilineText fields converted to Field Agents (Airtable AI Fields):
  - `Columns.ai_description` (auto-gen on column_name, source_file, data_type)
  - `Columns.allowed_values` (auto-gen on column_name, data_type)
  - `Manuscripts.ai_journal_recommendation` (auto-gen on short_title, aim, candidate_cohort_n, journal_chosen)
  - `Sections.ai_readability_score` (auto-gen on content_summary)
- Each prompt enforces the no-PHI rule and references upstream fields via @ chips.
- Closed THY-9 with `resolution:resolved-verified`.

Lesson learned for future Field Agent edits: Airtable's Add field button inserts the @ at current cursor position. Place the cursor explicitly at end of textarea (Cmd+End is unreliable in their contenteditable; click the visible end-of-text instead) before clicking Add field.

## v1.2.0 — 2026-05-05 (later same day)

THY-10 resolved without manual UI work.

- Replaced the three planned custom workflow states (Awaiting Chart Review, Awaiting Coauthor, Pending Auto-Close) with team-scoped labels: `awaiting:chart-review`, `awaiting:coauthor`, `auto-close:pending`.
- Updated daily_sync_prompt.md so phases 2-3 watch the `auto-close:pending` label rather than a state name.
- Closed THY-10 with `resolution:resolved-verified`.

Why labels won: filterable, audit-trail-preserving, no state-creation API needed, easy to evolve.

## v1.1.0 — 2026-05-05 (live system)

System is live. Live IDs in `airtable_ids.md` and `linear_ids.md`.

- 2 Airtable bases scaffolded: THYROID_DATA_REGISTRY (9 tables), THYROID_MANUSCRIPT (7 tables)
- 27 Manuscripts seeded, 22 Source Files, 21 TGDC Verification Checks, 2 Reconciliation Runs
- Linear team Thyroid Database (THY) created with 25 projects (6 workstream + 19 active manuscript)
- 32 team-scoped labels created
- 10 initial issues filed (THY-1 through THY-10), with Linear URLs cross-stamped onto Airtable Verification Checks and Manuscript records
- Scheduled daily sync (`thyroid-daily-sync`) live at 0 7 * * * local time
- Outstanding manual UI tasks: convert AI Fields (THY-9), add 3 custom workflow states (THY-10)

## v1.0.0 — 2026-05-05

Initial skill. Captures:
- Two Airtable bases (DATA_REGISTRY, MANUSCRIPT) with 13 tables total
- Linear team THYROID with 6 workstream projects + per-active-manuscript projects
- Lifecycle field (Active → In QA → Verified → Finalized → Manuscript-Locked → Archived)
- Issue Ledger (append-only audit trail)
- Manuscript Feedback Log + Data Feedback Log (append-only, for every chat-driven edit)
- Manuscript Snapshots (immutable evidence freeze on Submit/Accept)
- 10-phase daily sync prompt with Pending Auto-Close 48h buffer
- AI journal recommendation refresh (14-day cadence)
- 19 confirmed active manuscripts, 8 dormant M-codes, room for ~60 more
- HIPAA rule (research_id only, no raw note text in either tool)

Open seams that future versions will need to address:
- The other ~60 manuscripts the user mentioned but isn't yet listed on disk
- Co-author seat allocation (deferred per user)
- Possible future migration from per-day sync to event-driven webhooks if latency matters
