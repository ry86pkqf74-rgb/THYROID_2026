# VIEW Labeling Pass — Phase 0 Discovery (STOP GATE)

**Date:** 2026-04-21  
**Databases scanned:** `thyroid_canonical_publication_v1_0` (primary), `Thyroid 2026 UPdated` (archive — **identical** `main.*` VIEW list)  
**Method:** `information_schema.tables` where `table_schema = 'main'` and `table_type = 'VIEW'` (note: in DuckDB use **single quotes** for `'main'`; double quotes refer to a case-folded identifier, not the string `main`).

---

## 1. Full inventory: `main.*` VIEWs (14)

Same 14 names in both databases (order: alphabetical).

| # | `table_name` | Classification | Rationale |
|---|----------------|----------------|-----------|
| 1 | `canonical_us_exam_master_VIEW_v2` | **KEEP-AS-VIEW → rename** | US exam grain; TIRADS Part B; should become `canonical_us_exam_master_VIEW_v2`. |
| 2 | `canonical_us_patient_master_VIEW_v2` | **KEEP-AS-VIEW → rename** | US patient grain; view over `cupm_v2_canonical_backfill_v1` per Part B; target `canonical_us_patient_master_VIEW_v2`. |
| 3 | `longitudinal_lab_VIEW_v1` | **KEEP-AS-VIEW; name already includes `_VIEW`** | Matches QA pattern `%_VIEW_%`; optional later rename to stricter `canonical_*_*_VIEW_vN` is **separate** product decision. |
| 4 | `molecular_fusions_unnested_VIEW_v2` | **KEEP-AS-VIEW → rename** | Derived unnested view; no SQL hits; 1 MD hit; target `molecular_fusions_unnested_VIEW_v2`. |
| 5 | `molecular_variants_unnested_VIEW_v2` | **KEEP-AS-VIEW → rename** | Same; **zero** repo matches — rename for catalog consistency. |
| 6 | `thyroglobulin_lab_VIEW_v1` | **KEEP-AS-VIEW; name already includes `_VIEW`** | Same as row 3. |
| 7–14 | `database_snapshots`, `databases`, `owned_shares`, `query_history`, `recent_queries`, `shared_with_me`, `storage_info`, `storage_info_history` | **PLATFORM (MotherDuck catalog / UI)** | Not canonical study tables; **recommend out of scope** for this rename pass. Renames risk breaking MotherDuck tooling/audits; treat as **Logan-approved exceptions** to the strict `main.*` VIEW ≤→ `_VIEW_` QA query if the team adopts it verbatim. |

**SHOULD-BE-TABLE:** None flagged from catalog + reader scan. `canonical_us_patient_master_VIEW_v2` is intentionally a view over a persisted backfill table (per Part B) — not an accident.  

**ARCHIVE-ONLY:** None of the 14; platform views are “leave alone,” not “archive.”

---

## 2. `views_readable` (reference) — 45 VIEWs

Alphabetical list (for naming sanity; **do not rename** per handoff — already follow `..._v1` / readable naming):

`Analysis_Patient_Resolved`, `Cervical_LN_from_Notes_LLM`, `Cohort_Descriptive_Full`, `Complications`, `Data_Dictionary`, `Diagnosis_Unified`, `FNA_Cytology`, `FNA_Episode_Master`, `Genetics_Testing`, `Genetics_Variants`, `Genetics_from_Notes_LLM`, `Labs_Calcium`, `Labs_Longitudinal`, `Labs_PTH`, `Labs_TSH`, `Labs_Tg_Postop_Surveillance`, `Labs_Thyroglobulin`, `Labs_VitaminD`, `NSQIP_Linkage`, `PMHx_from_Notes_LLM`, `PSHx_from_Notes_LLM`, `Pathology_Outcome_Classification`, `Pathology_Specimen_Master`, `Pathology_Synoptics`, `Pathology_Tumor_Characteristics`, `Pathology_Tumor_Focus`, `Patient_Cross_Domain_Timeline`, `Patient_Master_Canonical`, `RAI_Treatment_Episode`, `RAI_from_Notes_LLM`, `Recurrence_Event_Clean`, `Recurrence_Status`, `Registry_Detail_Tables`, `Registry_Domain_Map`, `Surgery_Episode_Detail`, `Surgery_from_Notes_LLM`, `Survival_Cohort_Enriched`, `Survival_Followup`, `Symptoms_from_Notes_LLM`, `Tumor_Episode_Master`, `US_Lymph_Nodes_Wide_v2`, `US_Nodules_TIRADS`, `US_Nodules_Wide_v2`, `US_Reports_Raw`, `US_Thyroid_Gland_Wide_v2`

---

## 3. Reader footprint (repo-wide)

Patterns used (boundary-anchored, per handoff):

- **SQL:** `[^A-Za-z0-9_]<V>[^A-Za-z0-9_]|^<V>[^A-Za-z0-9_]|[^A-Za-z0-9_]<V>$`  
- **Python:** `['\".\s]<V>['\"\s]|<V>\b` (with `<V>` escaped)  
- **Markdown:** `\b<V>\b`  

*Caveat:* Short or generic names (e.g. `databases`) match **English prose** heavily in `.md` — MD counts for platform views are **inflated** and not all “object references.”

### 3.1 Per-object counts

| `main` VIEW | SQL hits | Py hits | MD hits | Files (any) | Top 5 files (combined hits) |
|-------------|----------|---------|---------|-------------|------------------------------|
| `canonical_us_exam_master_VIEW_v2` | 4 | 66 | 35 | 57 | `US_rollups_to_views_raw_schema_move_cursor_prompt_20260421.md` (14); `scripts/frozen/369_us_v2_views_and_registry.py` (5); `scripts/output/_us_rollups_inspect2.py` (5); `CPM_tirads_preB_canonical_backfill_cursor_prompt_20260421.md` (5); `scripts/output/_cpm_tirads_partB_phase1_coverage.py` (4) |
| `canonical_us_patient_master_VIEW_v2` | 10 | 144 | 54 | 67 | `scripts/preB_cupm_v2_canonical_backfill.py` (52); `scripts/output/_cpm_tirads_partB_phase1_coverage.py` (28); `CPM_tirads_partB_execution_cursor_prompt_20260421.md` (16); `US_rollups_to_views_raw_schema_move_cursor_prompt_20260421.md` (11); `CPM_tirads_preB_canonical_backfill_cursor_prompt_20260421.md` (8) |
| `database_snapshots` | 0 | 7 | 16 | 19 | `scripts/124_md_live_release_audit.py` (3); `studies/20260407_repo_live_validation/live_db_audit.md` (2); `studies/20260407_publication_signoff_live/md_introspection_snapshot.md` (2); (more with 1 hit each) |
| `databases` | 1 | 30 | 135 | 138 | `scripts/124_md_live_release_audit.py` (7); `docs/motherduck_release_runbook_v2.md` (5); `scripts/144_md_repo_current_state_summary.py` (3); `scripts/archive/221_final_database_consolidation.py` (3); `studies/20260407_release_candidate_audit/blockers.md` (3) |
| `longitudinal_lab_VIEW_v1` | 0 | 22 | 23 | 14 | `CURSOR_PROMPT_LAB_CONSOLIDATION_347_20260421.md` (13); `scripts/347_lab_master_canonical_v1_build.py` (11); `CURSOR_PROMPT_RERUN_255_AND_VERIFY_20260421.md` (4); `scripts/286_cpm_missing_data_backfill.py` (3); `scripts/348_lab_ingestion_refactor_verify.py` (3) |
| `molecular_fusions_unnested_VIEW_v2` | 0 | 0 | 1 | 1 | `US_rollups_to_views_raw_schema_move_cursor_prompt_20260421.md` (1) |
| `molecular_variants_unnested_VIEW_v2` | 0 | 0 | 0 | 0 | *no matches* |
| `owned_shares` | 0 | 0 | 3 | 3 | 1 hit each in audit/provenance MD |
| `query_history` | 0 | 16 | 28 | 29 | `tests/test_specimen_fhir_scripts_offline.py` (5); `docs/repo_update_audit_20260407.md` (4); `docs/motherduck_database_contract_v1.md` (3); … |
| `recent_queries` | 0 | 5 | 21 | 20 | (top) `studies/20260408T035955Z_cursor_repo_audit/summary.md` (3); `scripts/144_md_repo_current_state_summary.py` (2); … |
| `shared_with_me` | 0 | 0 | 3 | 3 | 1 hit each |
| `storage_info` | 0 | 0 | 3 | 3 | 1 hit each |
| `storage_info_history` | 0 | 0 | 4 | 4 | 1 hit each |
| `thyroglobulin_lab_VIEW_v1` | 0 | 38 | 22 | 22 | `CURSOR_PROMPT_LAB_CONSOLIDATION_347_20260421.md` (11); `scripts/347_lab_master_canonical_v1_build.py` (8); `scripts/255_rebuild_rai_tg_rollups.py` (4); `scripts/prompt6_352_wiring_gap_sweep.py` (4); `scripts/273_canonical_cleanup_phase2_3.py` (4) |

---

## 4. Proposed rename map (KEEP-AS-VIEW, study-owned objects only)

| old_name | new_name |
|----------|----------|
| `main.canonical_us_exam_master_VIEW_v2` | `main.canonical_us_exam_master_VIEW_v2` |
| `main.canonical_us_patient_master_VIEW_v2` | `main.canonical_us_patient_master_VIEW_v2` |
| `main.molecular_fusions_unnested_VIEW_v2` | `main.molecular_fusions_unnested_VIEW_v2` |
| `main.molecular_variants_unnested_VIEW_v2` | `main.molecular_variants_unnested_VIEW_v2` |

**No rename in this pass (already `_VIEW` in name, or out of scope):**  
`longitudinal_lab_VIEW_v1`, `thyroglobulin_lab_VIEW_v1` — keep unless Logan wants a **separate** normalization to `canonical_<domain>_<grain>_VIEW_vN`.  

**Platform `main` VIEWs (7–14 in §1):** **no rename**; recommend **explicit allowlist** in Phase 6 QA so the strict “no VIEW without `_VIEW_` in name” does not require renaming MotherDuck system objects.

---

## 5. Estimated work after approval

- **Union of all files** matching any of the **four** renames (SQL+Py+MD, combined): **71** unique paths.  
- **Excluding `scripts/frozen/**` (per handoff: do not migrate frozen; flag only):** **37** non-frozen files would still be updated in a **live** migration.  
- **`scripts/frozen/**` only:** **34** files reference the US v2 view names (historical); leave unchanged; final QA should grep with `--exclude-dir=scripts/frozen` as in §6.2 of the prompt.

**Compatibility views (Phase 2):** optional only if you split migration across commits; with ~37 non-frozen files, an **atomic** rename + single migration commit may be enough without shims.

---

## 6. Decisions for Logan (approval required)

1. **Approve** the **four-row** rename map above?  
2. **Confirm** platform `main` VIEWs stay **unrenamed** and **exempt** from the Phase 6 “all VIEWs must contain `_VIEW_`” check.  
3. **Optional follow-up (not this pass):** rename `longitudinal_lab_VIEW_v1` / `thyroglobulin_lab_VIEW_v1` to strict `canonical_*_*_VIEW_vN` for consistency?  
4. **Frozen tree:** keep `scripts/frozen` **read-only** for old object names; OK?

---

## 7. STOP GATE

**Do not execute Phases 1–7** until Logan approves the rename map and platform-view exception policy.

---

*Generated in Phase 0 (read-only). MotherDuck token sourced per repo TOML / `motherduck_client.get_token()` — no secrets logged.*
