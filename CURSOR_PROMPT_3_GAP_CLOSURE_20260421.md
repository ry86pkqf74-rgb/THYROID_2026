# Cursor Prompt 3 — Verified Gap Closure (SQL-only, no new LLM extraction)

**Date:** 2026-04-21
**Author:** handoff from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Runs after:** `CURSOR_PROMPT_COMPREHENSIVE_V1_0_CLEANUP_20260420.md` (Scripts 288–303) AND `CURSOR_PROMPT_TIER2_AND_VERIFICATION_20260420.md` (Scripts 304–326). Execute after both land on `main`.
**Companion:** `RUNPOD_EXTRACTION_PROMPT_20260421.md` — covers GPU/extraction work that is NOT in this prompt (re-extract 3 stale domains, run TIRADS re-extraction queue). Those are kicked off from a separate Cowork chat because they need H200 time, not SQL.

## Scope (one sentence)

Close the seven SQL-solvable gaps that the 2026-04-20 verification audit confirmed against live MotherDuck: operative re-op rebuild, TIRADS v2 Gaps A+B, vocal-cord complication tiering, calcium denominator recovery from LLM labs, path_stage extension, rai_scan_findings backfill, and op_esophageal_inv_any derivation from already-extracted airway-invasion JSON.

## Why this prompt is separate from the others

- Prompt 1 (288–303) did conservative CPM backfills + built US/genetics masters.
- Prompt 2 (304–326) builds Tier 2 typed tables + the side-by-side Excel/LLM/source-text verification layer.
- Prompt 3 (327–336, this one) uses ONLY existing MotherDuck tables to close verified coverage gaps. No new LLM extractions, no re-parses of tables Prompt 2 produces. Every script here should run in <10 minutes.

## Operating constraints (same as prior prompts)

1. PHI safety: no patient text in stdout. `research_id` only.
2. Never overwrite non-NULL — backfill where NULL unless explicitly replacing a broken column.
3. Archive before drop/replace using `archive_pub_v1_0."<name>_preNNN_<UTCZ>"` convention, log to `manuscript_workspace.archive_move_log_v1`.
4. Reference-safety check before archiving: enumerate views/tables that reference it; abort if non-archive references exist.
5. One script = one scope.
6. Env: `scripts/_md_connect.py::connect_locked()`.
7. Every aggregate flag dated and source-linked (Constraint 7 from Prompt 2 applies here too). Bare booleans without `_first_date` + `_first_note_id` + `_first_evidence_text` companions are rejected.
8. **The timestamp column on every `note_entities_llm_*` table is `extracted_at`, NOT `extraction_timestamp`** — the other Claude session's prompt had this wrong.

---

## Script 327 — Operative episode detail v2 REBUILD (P0)

**Problem (verified):** `canonical_patient_master.n_surgeries_v2` distribution is (1:10,133) (2:698) (3:31) (4:7) (5:1) (6:1) → **738 patients have ≥2 surgeries**. `operative_episode_detail_v2` has 9,371 rows for 9,368 RIDs but **only 3 patients with ≥2 rows in the detail table**. So ~735 re-operation episodes are silently missing. Any per-episode analysis on revisions, completions, or lateral-neck dissections currently cannot be done.

**Sources (verified exist):**
- `main.note_entities_operative_detail`: 12,151 rows / 4,032 RIDs, 48 cols. Per-RID distribution: (1:681) (2:1,305) (3:922) (4:472) (5:251) (6:161) (7:109) (8:58) (9:38) (10:16) — has multi-event coverage.
- `main.note_entities_procedures`: 21,942 rows / 4,723 RIDs, 48 cols. Richer procedure coverage.
- `main.operative_episode_detail_v2` (current, 9,371 rows): use as starting point for the FIRST episode; derive 2nd–6th episodes from the note_entities tables.

**Target grain:** one row per (research_id, surgery_episode_id). `surgery_episode_id` = deterministic hash of (research_id, ordinal, canonical_operative_date).

**Rebuild plan:**

1. **Preserve existing v2 data.** Archive `operative_episode_detail_v2` → `archive_pub_v1_0.operative_episode_detail_v2_pre327_<UTCZ>`.
2. **Derive operative dates per RID** from `note_entities_operative_detail` where `entity_type = 'operative_date'`. Deduplicate dates within ±7 days as "same surgery". Order ascending → `surgery_ordinal ∈ 1..N`.
3. **Cross-validate against `canonical_patient_master.n_surgeries_v2`**: every RID in CPM with `n_surgeries_v2 = k` must produce exactly `k` distinct episodes in the rebuild (within ±1 tolerance for edge cases logged to `manuscript_workspace.operative_rebuild_mismatch_v1`).
4. **Populate per-episode fields** from `note_entities_operative_detail` + `note_entities_procedures` joined on (research_id, note_date ∈ operative_date ± 7d):
   - `surgery_type` (total thyroidectomy / lobectomy / completion / revision / neck_dissection_only)
   - `surgical_approach` (open / transoral / robotic — from entity_type='approach')
   - `cnd_flag`, `lnd_left_flag`, `lnd_right_flag`, `lnd_bilateral_flag`
   - `rln_identified_flag`, `rln_stimulation_used_flag`
   - `pt_n_identified`, `pt_n_preserved`, `pt_n_autotransplanted`, `pt_n_removed` (pull from `parathyroid_patient_wide_v1` built in Prompt 2 Script 307; fallback to `note_entities_operative_detail` where entity_type='parathyroid_*')
   - `frozen_section_performed_flag`, `frozen_section_result_primary` (from `frozen_section_event_v1` built in Prompt 2 Script 304; fallback to operative note entities)
   - `estimated_blood_loss_ml`, `operative_time_min` (entity_type matches)
   - `complications_documented_flag` (any complication entity on this op date)
   - Source provenance: `source_note_id`, `operative_note_date`, `rebuild_confidence` ∈ {high, medium, low} (high = all fields from structured entity; medium = some imputation; low = only operative date, other fields NULL).
5. **Write to `main.operative_episode_detail_v2`** (replace the archived version with the rebuild).
6. **Re-derive downstream rollups** — any `n_completion_thyroidectomy`, `any_lateral_neck_dissection`, `revision_surgery_flag` columns in CPM must be re-aggregated from the new detail table. List affected CPM columns in the script's docstring; only update where CURRENT CPM value ≠ NEW rollup AND NEW is non-null (log overwrites to `cpm_backfill_log_v1`).

**Invariants:**
- `COUNT(DISTINCT research_id) FROM operative_episode_detail_v2` ≥ 10,800 (should cover ~10,871 minus a small set with no operative notes).
- Per-RID row count distribution matches `n_surgeries_v2` distribution within 2% relative error at each bucket.
- No CPM row count change (still 10,871).

**Script:** `scripts/327_operative_episode_detail_v2_rebuild.py`. Commit immediately after CPM invariants pass.

---

## Script 328 — TIRADS v2 Gap A cast fix

**Problem (verified):** `tirads_v2_nodules_raw` has 3,021 distinct RIDs. `cpm.tirads_v2_worst_category` is populated for only 2,465. Delta = **556 patients** lost to `research_id` VARCHAR↔BIGINT silent join failure (documented pattern in memory).

**Fix:** rebuild `tirads_v2_nodule_patient_rollup_v1` with explicit `CAST(research_id AS VARCHAR)` on both sides of every join. Confirm `COUNT(DISTINCT research_id) = 3021`. Then backfill `cpm.tirads_v2_worst_category`, `cpm.tirads_v2_max_points`, `cpm.tirads_v2_n_nodules_scored` where NULL for those 556 RIDs.

**Invariants:**
- `cpm.tirads_v2_worst_category` nonnull ≥ 3,021 (up from 2,465).
- No existing CPM value changed — log each backfill to `cpm_backfill_log_v1` with `pre_value=NULL, post_value=<new>`.

**Script:** `scripts/328_tirads_v2_gap_a_cast_fix.py`.

---

## Script 329 — TIRADS v2 Gap B report-level re-roll

**Problem (verified):** `tirads_v2_report_patient_rollup_v1` covers 4,073 distinct RIDs, but CPM exposes the report-level columns for only 2,465 (and worse for `tirads_v2_any_suspicious_ln_on_us` which is stuck at 1,498). That's 1,608–2,575 patients with report-level TIRADS signal not represented in CPM.

**Fix:** re-roll the following CPM columns from `tirads_v2_report_patient_rollup_v1` (with the same VARCHAR cast as 328), backfilling NULL-only:

- `tirads_v2_any_fna_recommended` (target: 4,073)
- `tirads_v2_any_suspicious_ln_on_us` (target: 4,073 — currently 1,498, huge gap)
- `tirads_v2_any_ete_on_us` (target: 4,073)
- `tirads_v2_any_interval_growth` (target: 4,073)
- `tirads_v2_shortest_followup_months` (new column if not present; NULL-safe MIN across per-report rows)
- `tirads_v2_any_biopsy_recommended_date` + `_first_note_id` + `_first_evidence_text` (Constraint 7 companions — build these by joining on `tirads_v2_reports_raw.note_id` + `clinical_notes_long`)

**Invariants:** each listed column nonnull ≥ 4,073 for the first three; `_any_suspicious_ln_on_us` must be ≥ 4,073 (jump of 2,575).

**Script:** `scripts/329_tirads_v2_gap_b_report_reroll.py`.

---

## Script 330 — Vocal-cord complication tiering extension

**Problem (verified):** `cpm.comp_vc_paralysis_evidence_tier` = 0 nonnull. `cpm.comp_vc_paresis_evidence_tier` = 0 nonnull. The tier-1/2/3 logic was only ever wired for hypocalcemia, hypopara, and rln_injury. Raw evidence rows exist in `complication_phenotype_v1` (88 VC paralysis, 71 VC paresis — per the 2026-04-20 report).

**Fix:**

1. Read `scripts/build_complication_phenotype_v1.py` (or whichever script built the hypocalcemia tiering) to understand the tier-1/2/3 rule set.
2. Extend to VC paralysis + VC paresis with the same rules, using evidence from:
   - `complication_phenotype_v1` rows where `complication_type IN ('vocal_cord_paralysis','vocal_cord_paresis')`
   - `note_entities_llm_functional_outcomes` (parsed to `functional_outcomes_event_v1` in Prompt 2 Script 310) where `fo_domain = 'voice'` and `fo_severity ∈ ('moderate','severe')`
   - `complication_patient_summary_v1`
3. Write `comp_vc_paralysis_evidence_tier` ∈ {tier_1_confirmed, tier_2_probable, tier_3_possible, tier_4_resolved, NULL_no_evidence} to CPM where NULL.
4. Same for `comp_vc_paresis_evidence_tier`.
5. Populate companion columns per Constraint 7: `comp_vc_paralysis_first_date`, `comp_vc_paralysis_first_note_id`, `comp_vc_paralysis_first_evidence_text`, `comp_vc_paralysis_resolution_date` (if fo_resolution='resolved'), `comp_vc_paralysis_n_notes_documenting`.

**Invariants:** `cpm.comp_vc_paralysis_evidence_tier` nonnull ≥ 88; `cpm.comp_vc_paresis_evidence_tier` nonnull ≥ 71.

**Script:** `scripts/330_vc_complication_tiering.py`.

---

## Script 331 — Calcium denominator recovery from LLM labs

**Problem (verified):** `cpm.postop_calcium_min_value` nonnull = 544 out of 10,871. `longitudinal_lab_canonical_v1` has only 188 calcium rows across 166 RIDs (dominated by anti-Tg and Tg). Expected hypocalcemia rate is 20–30% of total thyroidectomy patients → should see ~500–1,300 confirmed cases, currently 98.

**New source identified:** `note_entities_llm_labs.result_json` — a 2,000-row sample showed 31 calcium-entity mentions (~1.5%); full-scale calcium/PTH evidence reaches **300 distinct RIDs** — enough to roughly double the calcium denominator.

**Fix:**

1. Parse `note_entities_llm_labs.result_json` using the standard UNNEST pattern:
   ```sql
   WITH ent AS (
     SELECT research_id, note_id, note_date, note_type, extracted_at,
            UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS e
       FROM main.note_entities_llm_labs
      WHERE result_json IS NOT NULL
   )
   SELECT research_id, note_id, note_date,
          json_extract_string(e, '$.entity_type') AS entity_type,
          json_extract_string(e, '$.entity_value') AS entity_value,
          json_extract_string(e, '$.entity_date') AS entity_date,
          json_extract_string(e, '$.evidence_text') AS evidence_text,
          CAST(json_extract_string(e, '$.confidence') AS DOUBLE) AS confidence
     FROM ent
    WHERE json_extract_string(e, '$.entity_type') IN ('calcium','pth','parathyroid_hormone','corrected_calcium','ionized_calcium');
   ```
2. Normalize: parse `entity_value` into numeric value + unit. Map to mg/dL using unit crosswalk (mmol/L × 4.008 = mg/dL; pmol/L × 0.00943 = ng/dL for PTH).
3. Apply plausibility filter: calcium 4.0–14.8 mg/dL (same bounds as existing cleaned data). Rows outside → `lab_orphan_cohort_review_v1` with reason='calcium_out_of_range_llm'.
4. Insert surviving rows into `longitudinal_lab_canonical_v1` with:
   - `source_table = 'note_entities_llm_labs'`
   - `source_script = '331_calcium_from_llm_labs'`
   - `ingestion_wave = 'v1_0_llm_recovery'`
   - `data_completeness_tier = 'medium'` (LLM-derived, not flowsheet-verified)
   - `provenance_note` = evidence_text (first 200 chars) + note_id
5. **Re-run the complication classifier** that populates `comp_hypocalcemia_confirmed`, `comp_hypoparathyroidism_confirmed`, `postop_calcium_min_value`, `has_low_calcium_flag`. Use existing logic (don't rewrite it) — rely on the widened `longitudinal_lab_canonical_v1` input.

**Invariants:**
- `postop_calcium_min_value` nonnull ≥ 1,500 (target; report claimed 2,000+ achievable — accept whatever actually lands, but fail if <1,000).
- `comp_hypocalcemia_confirmed = TRUE` count ≥ 300 (up from 98).
- `longitudinal_lab_canonical_v1` calcium rows ≥ 600 (up from 188).
- No existing calcium value in `longitudinal_lab_canonical_v1` overwritten — LLM rows are ADDITIVE, tagged with their `source_table`.

**Script:** `scripts/331_calcium_denominator_recovery.py`.

---

## Script 332 — path_stage_raw extension beyond current 4,070

**Problem (verified):** `cpm.path_stage_raw` nonnull = 4,070. `cpm.gm_path_stage_raw` nonnull = 4,070. The eligible staged cohort is larger — CPM has ~8,000 patients with at least one of `T_stage`, `N_stage`, or `M_stage` populated.

**Fix:**

1. Derive `path_stage_raw` as `CONCAT('T', T_stage, 'N', N_stage, 'M', M_stage)` wherever all three are present but `path_stage_raw IS NULL`.
2. For partial cases (T-only, TN-only), use a conservative form: `'T' || T_stage || COALESCE('N' || N_stage, 'Nx') || COALESCE('M' || M_stage, 'Mx')`.
3. Same derivation for `gm_path_stage_raw` (use Genomic Marker / LLM-derived stage columns if those are the inputs — check `path_synoptics` and `synoptic_tumor_long_v1` for the right source).
4. Handle `AJCC_version` — if path_synoptics has separate 7th-edition and 8th-edition T/N/M, prefer 8th.

**Invariants:**
- `path_stage_raw` nonnull ≥ 6,500 (target; fail if <5,000).
- No existing `path_stage_raw` value changed — backfill NULL-only.

**Script:** `scripts/332_path_stage_raw_extension.py`.

---

## Script 333 — rai_scan_findings_v9 backfill

**Problem (verified):** `cpm.rai_scan_findings_v9` nonnull = 527 (not zero as the prior report claimed, but still sparse). Source is `note_entities_llm_rai_detailed.result_json` — entity_type should include `wb_scan_finding`, `focal_uptake_site`, `thyroid_remnant_uptake`.

**Fix:**

1. Parse `note_entities_llm_rai_detailed.result_json` with the standard UNNEST pattern (filter entity_type ∈ {wb_scan_finding, focal_uptake_site, thyroid_remnant_uptake, wb_scan_negative}).
2. Per (research_id, rai_episode_id) — join to `rai_treatment_episode_v2` on (research_id, rai_date ± 30d window).
3. Aggregate findings into a compact string: `'pos_sites: [neck, lung]; remnant_uptake: minimal; focal: mediastinal'`. Or write as JSON LIST and add `rai_scan_findings_v9_json` column (safer — keep the raw column as legacy text, add structured JSON alongside).
4. Backfill `cpm.rai_scan_findings_v9` only where currently NULL.
5. Add companion per Constraint 7: `rai_scan_findings_v9_source_note_id` + `_source_note_date` + `_source_evidence_text` to CPM (new cols).

**Invariants:** `cpm.rai_scan_findings_v9` nonnull ≥ 1,500 (target; report said the column was broken — it wasn't, but coverage is low relative to the ~2,700 RIDs in `rai_treatment_episode_v2`).

**Script:** `scripts/333_rai_scan_findings_backfill.py`.

---

## Script 334 — op_esophageal_inv_any derivation

**Problem (verified):** `cpm.op_esophageal_inv_any` nonnull = 0. New finding: `note_entities_llm_airway_invasion.result_json` contains 'esophag' mentions across **381 distinct RIDs**. These are not being harvested.

**Fix:**

1. Parse `note_entities_llm_airway_invasion.result_json` filtering where `entity_value ILIKE '%esophag%'` OR `entity_type = 'esophageal_invasion'` OR `evidence_text ILIKE '%esophag%'`.
2. Classify `present_or_negated` into:
   - `op_esophageal_inv_any = TRUE` when present_or_negated='present' or 'invading' at confidence ≥ 0.5
   - `op_esophageal_inv_any = FALSE` when present_or_negated='negated' or 'absent'
   - NULL otherwise
3. Companion columns per Constraint 7:
   - `op_esophageal_inv_first_date` (earliest confirming note_date)
   - `op_esophageal_inv_first_note_id`
   - `op_esophageal_inv_first_evidence_text`
   - `op_esophageal_inv_extent` ∈ {abutting, invading_partial, full_thickness, NULL}
   - `op_esophageal_inv_n_notes_documenting`

**Note on completeness:** the 381 RIDs are only the ones where airway-invasion extraction happened to touch esophageal mentions. Full operative-note coverage requires the dedicated esophageal-invasion LLM run queued in the RunPod prompt. This script gets us from 0 to ~381 as a cheap interim fix.

**Invariants:** `cpm.op_esophageal_inv_any` nonnull ≥ 300 (conservative — some of the 381 will fail the confidence/present filter).

**Script:** `scripts/334_op_esophageal_inv_any_from_airway.py`.

---

## Script 335 — Archive round 3 (redundant tables after Prompts 2 + 3 complete)

**Candidates** (re-check each with `duckdb_views()` / `duckdb_tables()` reference scan before archiving):

| Object | Reason | Archive name |
|---|---|---|
| `main.tirads_llm_extracted_v2` | Haiku-era methodological comparator, superseded by `tirads_v2_*` | `tirads_llm_extracted_v2_pre335_<UTCZ>` |
| `main.tirads_llm_validation_v2` | If `verify_us_nodule_v1` (Prompt 2 Script 315) covers this, archive | conditional |
| `main.extracted_tirads_validated_v1` | If `tirads_v2_nodule_patient_rollup_v1` + `canonical_us_nodule_master_v1` (Prompt 1 Script 299) have superseded it | conditional — check references first |
| `main.tumor_pathology` | 253-col legacy, superseded by `path_synoptics` + `synoptic_tumor_long_v1` | conditional |
| `main.path_size_adjudication_v241` | Versioned adjudication, should be closed if path_size_adjudication decisions are final | Logan sign-off |
| `main.ret_note_entity_adjudication_v226` | Versioned, check if superseded | conditional |
| `main.ret_patient_adjudicated_v226` | Versioned, check if superseded | conditional |
| `main.ete_adjudication_v1` | If 26 low-conf rows retained in CPM as `present_ungraded`, archive | conditional |
| `main.data_dictionary_v279` | If Script 326 generates a new dictionary, archive old | conditional |
| `main.clinical_note_ln_extracted_v1` | If `ln_master_rollup_v1` + Prompt 2 Script 323's `verify_ln_v1` supersede | conditional |
| `main.extracted_rln_injury_refined_v2` | If `complication_phenotype_v1` has absorbed this, archive | conditional |
| `main.extracted_ete_subgraded_v1` | If `ete_adjudication_v1` or CPM ETE cols supersede | conditional |
| `main.extracted_braf_recovery_v1` | If `canonical_molecular_tested_v1` or `genetics_per_test_master_v1` cover | conditional |
| `main.extracted_ras_patient_summary_v1` | Same | conditional |
| `main.extracted_fna_bethesda_v1` | If `fna_episode_master_v2` covers | conditional |
| `main.extracted_postop_labs_expanded_v1` | If `longitudinal_lab_canonical_v1` now fully covers (post-331), archive | conditional — check after 331 |
| `main.nsqip_enrichment`, `main.nsqip_patient_summary` | If enriched data merged into CPM, archive. Otherwise retain. | conditional |
| `main.patient_completion_oed_path_linkage_v1` | Scratch linkage table — if used only to build CPM columns, archive | conditional |
| `main.episode_analysis_resolved_v1_dedup` | Scratch | conditional |
| `main.lesion_analysis_resolved_v1` | Scratch | conditional |
| `main.patient_analysis_resolved_v1` | Scratch | conditional |
| `main.specimen_source_xref_v1`, `main.specimen_master_v1`, `main.specimen_tumor_focus_v1`, `main.specimen_genomic_assay_v1` | If consolidated under `genetics_per_test_master_v1` or `molecular_test_episode_v2`, archive | conditional |
| `main.serial_imaging_us`, `main.thyroid_sizes`, `main.thyroid_weights` | If `canonical_us_*` masters (Scripts 299–301) cover, archive | conditional |
| `main.survival_cohort_enriched` | If `canonical_survival_followup_v1` covers, archive | conditional |
| `main.tumor_stage_heterogeneity_v1` | If CPM now exposes max-stage / heterogeneity flags, archive | conditional |

**Procedure per object (mandatory):**

1. `SELECT COUNT(*) FROM duckdb_views() WHERE view_definition ILIKE '%<name>%' AND schema_name != 'archive_pub_v1_0'` — if non-zero, log blocker and skip.
2. `SELECT COUNT(*) FROM duckdb_tables() t JOIN duckdb_views() v ON ...` — same for view dependencies.
3. CTAS to archive schema.
4. Verify `src rowcount == dest rowcount`.
5. Log to `manuscript_workspace.archive_move_log_v1`.
6. `DROP TABLE main.<name>`.

**DO NOT ARCHIVE:**
- `clinical_notes_long` — free-text source of truth
- `path_synoptics`, `ultrasound_reports`, `ct_imaging`, `mri_imaging`, `nuclear_med`, `fna_cytology`, `molecular_results`, `molecular_testing`, `molecular_variant_long`, `fna_history`, `fna_episode_master_v2`, `tumor_episode_master_v2` — Excel source tables with provenance metadata
- `__readme` — required meta
- `data_dictionary_v279` — unless Script 326 produced a replacement
- Anything referenced by a non-archive view

**Target end state:** main schema drops from 121 objects toward ~60–70 canonical objects. Fewer than 10 `extracted_*_v1` tables should remain (and each one that does should justify itself in `__readme`).

**Script:** `scripts/335_archive_round3.py`. Every archive move must log to the move log; no silent drops.

---

## Script 336 — Final main-schema audit + __readme refresh

1. Re-run `main` object count. Print one-line categorical summary (CPM / canonical_* / note_entities_llm_* / note_entities_* / verify_* / path_* / tirads_v2_* / us_* / molecular_* / operative_* / lab_* / complication_* / adjudication_queues / meta).
2. For every table still in `main`, verify it falls into one of the above categories. Print orphans.
3. Refresh `main.__readme` with the final categorical map + last-updated timestamp + git sha of HEAD.
4. Re-run the four CPM invariants: rows=10,871; distinct_rid=10,871; fna_path_outcome null count stable; column count (accept whatever's final — just log it to the audit artifact).
5. Print Tier 2 completeness invariant (from Prompt 2 Script 313b).
6. Print `verify_*` concordance summary (from Prompt 2 Script 326).
7. Write `scripts/output/336_postcleanup_audit_round3.md` with archive diff + final table count + low-concordance flags.

**Script:** `scripts/336_final_main_audit.py`.

---

## Git discipline (same as prior prompts)

Per script:
```bash
cd "/Users/ros/THyroid 2026"
git add scripts/<N>_*.py
python -m pyflakes scripts/<N>_*.py
git commit -m "Script <N>: <summary>"
git push origin main
```

## Definition of done

1. `operative_episode_detail_v2` has ≥ 10,800 distinct RIDs and per-RID row distribution matches `n_surgeries_v2` within 2%.
2. `cpm.tirads_v2_worst_category` nonnull ≥ 3,021. `cpm.tirads_v2_any_suspicious_ln_on_us` nonnull ≥ 4,073.
3. `cpm.comp_vc_paralysis_evidence_tier` nonnull ≥ 88. `cpm.comp_vc_paresis_evidence_tier` nonnull ≥ 71.
4. `cpm.postop_calcium_min_value` nonnull ≥ 1,500. `cpm.comp_hypocalcemia_confirmed = TRUE` count ≥ 300.
5. `cpm.path_stage_raw` nonnull ≥ 6,500.
6. `cpm.rai_scan_findings_v9` nonnull ≥ 1,500.
7. `cpm.op_esophageal_inv_any` nonnull ≥ 300 (interim; full coverage blocked on RunPod prompt).
8. `main` object count reduced toward ≤ 70. `__readme` refreshed.
9. All scripts committed individually and pushed.
10. `336_postcleanup_audit_round3.md` committed.
