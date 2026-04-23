# Cursor Agent Prompts — THYROID_2026 Issue Fixes

Each block below is a self-contained prompt to paste into a Cursor agent chat, one at a time, in order. After the agent runs a prompt, check the verification output before moving to the next.

## Conventions for every prompt
- **Target DB**: `/Users/loganglosser/THYROID_2026/backups/thyroid_2026_full_backup_20260422_174849.duckdb` (the local canonical copy; MotherDuck trial has expired). All SQL assumes this file is opened via `duckdb.connect(path)` and tables are referenced as `main.<name>`.
- **All new objects** go in `manuscript_workspace.*`. Never mutate `main.*` in place.
- **All migrations** are idempotent: `CREATE OR REPLACE VIEW` / `CREATE OR REPLACE TABLE`.
- **Each prompt ends in a verification query** whose output the agent must paste back.
- **Store the SQL** each prompt runs in `qc_framework_v1/migrations/NN_<issue>.sql` so the whole chain is replayable.

Issue IDs reference `ISSUE_REGISTRY.md`.

---

## 00 — Foundation: manual review queue + workspace schema

> **Context**: THYROID_2026 research DB. I need a workspace schema and a manual-review queue table that downstream fixes emit into.
>
> **Do**:
> 1. Open `/Users/loganglosser/THYROID_2026/backups/thyroid_2026_full_backup_20260422_174849.duckdb` with duckdb (Python).
> 2. Create schema `manuscript_workspace` if missing.
> 3. Create table `manuscript_workspace.qc_manual_review_queue_v1` with columns:
>    `queue_id BIGINT PRIMARY KEY` (auto-seq via sequence `manuscript_workspace.qc_queue_seq`),
>    `issue_id VARCHAR NOT NULL`,
>    `research_id INTEGER`,
>    `source_table VARCHAR`,
>    `source_pk VARCHAR`,
>    `context_json JSON`,
>    `reason VARCHAR`,
>    `status VARCHAR DEFAULT 'open'`,
>    `reviewer_notes VARCHAR`,
>    `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`,
>    `resolved_at TIMESTAMP`.
> 4. Save SQL to `qc_framework_v1/migrations/00_foundation.sql` and run it.
>
> **Verify**:
> ```sql
> SELECT table_schema, table_name FROM information_schema.tables
> WHERE table_schema='manuscript_workspace' ORDER BY table_name;
> SELECT COUNT(*) AS queue_rows FROM manuscript_workspace.qc_manual_review_queue_v1;
> ```

---

## 01 — [PATH01 / OP05] Re-key path malignant to global operative namespace

> **Context**: `main.canonical_operative_events_v1` is now clean: 11,773 globally-unique `surgery_episode_id` values, 1:1 with `(research_id, resolved_surgery_date)`. `main.canonical_path_malignant_events_v1.surgery_episode_id` still carries patient-local ordinals (3 distinct values, 1,434 NULL). 5,254 path rows / 3,220 pts (2,624 PTC) have non-null ordinals that do not match any operative episode. The fix is a join to the operative table, not an MD5 hash.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_path_malignant_events_v1_keyed` = path LEFT JOIN `main.canonical_operative_events_v1` op ON `path.research_id = op.research_id AND path.surgery_date = CAST(op.resolved_surgery_date AS TIMESTAMP)`.
> 2. Add columns:
>    - `surgery_episode_uid_global BIGINT`: `op.surgery_episode_id` when joined.
>    - `surgery_episode_uid_fallback VARCHAR`: `md5(CAST(research_id AS VARCHAR)||'|'||CAST(surgery_date AS VARCHAR))` when no op match but `surgery_date IS NOT NULL`.
>    - `surgery_episode_uid VARCHAR`: `COALESCE(CAST(surgery_episode_uid_global AS VARCHAR), surgery_episode_uid_fallback)`.
>    - `surgery_episode_uid_source VARCHAR`: one of `operative_match`, `md5_fallback`, `unknown_no_date`.
> 3. Emit every row with `surgery_episode_uid_source='md5_fallback'` to `qc_manual_review_queue_v1` with `issue_id='OP05'` and `context_json={surgery_date, tumor_ordinal, primary_histology, specimen_id}` — these are pathology records for surgeries the operative table doesn't know about.
> 4. Every downstream path prompt reads from this view going forward.
>
> **Verify** (expect op-match coverage ≈ 80–90% of non-null-date rows; unmatched goes to queue):
> ```sql
> SELECT surgery_episode_uid_source, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed GROUP BY 1;
> SELECT COUNT(DISTINCT surgery_episode_uid) AS n_uids,
>        COUNT(DISTINCT research_id) AS n_pts,
>        COUNT(*) AS n_rows
> FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='OP05';
> ```

---

## 02 — [PATH15] Re-link pathology rows to `specimen_tumor_focus_v1`

> **Context**: 3,026 path_malignant rows (~45%) have NULL `specimen_focus_id` / `linkage_confidence` / `linkage_score`. This blocks per-focus analysis and proper multifocality.
>
> **Do**:
> 1. Build `manuscript_workspace.path_focus_link_v1` that joins `manuscript_workspace.canonical_path_malignant_events_v1_keyed` to `main.specimen_tumor_focus_v1` on the best available keys: (`research_id`, `surgery_date`, `laterality`, `site`, `size_greatest_dimension_cm`) with fallback to (`research_id`, `surgery_date`, `laterality`, `size_greatest_dimension_cm`) then (`research_id`, `surgery_date`, `laterality`).
> 2. Emit one row per path_malignant row, with columns `(research_id, surgery_episode_uid, tumor_ordinal, specimen_focus_id, linkage_tier, linkage_confidence)` — `linkage_tier` in {`exact`, `size_laterality`, `laterality_only`, `none`}.
> 3. For rows that remain `linkage_tier='none'`, insert one row per event into `manuscript_workspace.qc_manual_review_queue_v1` with `issue_id='PATH15'`.
>
> **Verify** (expect `linkage_tier='none'` count ≤ the original 3,026):
> ```sql
> SELECT linkage_tier, COUNT(*) FROM manuscript_workspace.path_focus_link_v1
> GROUP BY 1 ORDER BY 2 DESC;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH15';
> ```

---

## 03 — [PATH14] Rebuild multifocality (number_of_tumors, multifocality_flag)

> **Context**: `number_of_tumors` and `multifocality_flag` on `main.canonical_path_malignant_events_v1` do not fire for any patient even though 1,666 patients have >1 row. Derivation is broken and must be rebuilt from the row count per surgery episode.
>
> **Do**:
> 1. Create view `manuscript_workspace.path_episode_multifocality_v1` keyed by `(research_id, surgery_episode_uid)` with:
>    - `number_of_tumors = COUNT(*) OVER (PARTITION BY research_id, surgery_episode_uid)`
>    - `multifocality_flag = (number_of_tumors > 1)`
>    - `bilateral_flag = (COUNT(DISTINCT laterality) FILTER (WHERE laterality IN ('left','right')) = 2)`
>    Source from `manuscript_workspace.canonical_path_malignant_events_v1_keyed`.
> 2. The existing `number_of_tumors`/`multifocality_flag` columns in main remain untouched (contract: never mutate `main`), but cohort_v2 will read from this view.
>
> **Verify** (should mirror the 1,666 multifocal pts):
> ```sql
> SELECT multifocality_flag, COUNT(DISTINCT research_id) AS n_pts
> FROM manuscript_workspace.path_episode_multifocality_v1
> GROUP BY 1;
> ```

---

## 04 — [PATH20-21] Rebuild discordance flags

> **Context**: `discordance_t_stage_flag` fires on 47% of rows (clearly broken logic). `discordance_laterality_flag` never fires despite 216 real laterality↔site contradictions (PATH17).
>
> **Do**:
> 1. In `manuscript_workspace`, create view `path_event_discordance_v1` with one row per path_malignant row plus recomputed flags:
>    - `discordance_laterality_flag`: TRUE when `laterality` ∈ {`left`,`right`} AND `site` contains the opposite side token (e.g., laterality=`left` and `site` LIKE `%right%`) OR vice versa.
>    - `discordance_t_stage_flag`: TRUE when `t_stage_ajcc8` disagrees with size band from `size_greatest_dimension_cm` (T1a ≤1cm, T1b 1–2, T2 2–4, T3 >4 confined, T4 any size w/ gross_ete). Apply AJCC8 thyroid rule set exactly.
> 2. Both derivations are explicit and documented inline with comments tying each branch to an AJCC8 rule.
>
> **Verify** (expect laterality flag ≈ 216, t-stage flag dramatically < 3,152):
> ```sql
> SELECT
>   SUM(CASE WHEN discordance_laterality_flag THEN 1 END) AS lat_flag,
>   SUM(CASE WHEN discordance_t_stage_flag   THEN 1 END) AS t_flag,
>   COUNT(*) AS n_rows
> FROM manuscript_workspace.path_event_discordance_v1;
> ```

---

## 05 — [GEN01] Derive stable `molecular_episode_uid`

> **Context**: `main.canonical_molecular_genetics_v2.molecular_episode_id` is collapsed to 3 ordinals across 1,384 rows. Build the canonical replacement paralleling PATH01.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_molecular_genetics_v2_keyed` that re-emits all columns of `main.canonical_molecular_genetics_v2` plus:
>    ```sql
>    md5(
>      CAST(research_id AS VARCHAR) || '|' ||
>      COALESCE(CAST(resolved_test_date AS VARCHAR), '') || '|' ||
>      COALESCE(platform, 'UNKNOWN') || '|' ||
>      COALESCE(CAST(platform_version AS VARCHAR), '')
>    ) AS molecular_episode_uid
>    ```
> 2. Every downstream GEN prompt reads from this view.
>
> **Verify**:
> ```sql
> SELECT COUNT(DISTINCT molecular_episode_uid) AS n_uids,
>        COUNT(*) AS n_rows,
>        COUNT(DISTINCT research_id) AS n_pts
> FROM manuscript_workspace.canonical_molecular_genetics_v2_keyed;
> ```

---

## 06 — [GEN09] Re-link `specimen_genomic_assay_v1`

> **Context**: `main.specimen_genomic_assay_v1` is ~98% unlinked to both `specimen_master_v1` and the molecular genetics table.
>
> **Do**:
> 1. Create `manuscript_workspace.specimen_genomic_assay_v1_relinked` joining `main.specimen_genomic_assay_v1` to (a) `main.specimen_master_v1` on best `(research_id, collection_date, specimen_site)` match, and (b) `manuscript_workspace.canonical_molecular_genetics_v2_keyed` on `(research_id, resolved_test_date, platform)`.
> 2. Emit `link_specimen_tier`, `link_molecular_tier` each in {`exact`,`date_only`,`patient_only`,`none`}.
> 3. Emit rows still `none` on both sides into `qc_manual_review_queue_v1` with `issue_id='GEN09'`.
>
> **Verify**:
> ```sql
> SELECT link_specimen_tier, link_molecular_tier, COUNT(*)
> FROM manuscript_workspace.specimen_genomic_assay_v1_relinked
> GROUP BY 1,2 ORDER BY 3 DESC;
> ```

---

## 07 — [GEN10] Rename notes-derived molecular table to mentions layer

> **Context**: `main.canonical_molecular_genetics_from_notes_v2` is a notes-extraction layer, not a structured genomic result, but its name implies parity with the structured table.
>
> **Do**:
> 1. Create view `manuscript_workspace.molecular_mentions_from_notes_v2` that exposes every column of `main.canonical_molecular_genetics_from_notes_v2` unchanged.
> 2. Add a persistent note in `qc_framework_v1/README.md` stating: "`main.canonical_molecular_genetics_from_notes_v2` is deprecated in favor of `manuscript_workspace.molecular_mentions_from_notes_v2`; it is a mentions layer and must never be joined as a peer of `canonical_molecular_genetics_v2`."
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.molecular_mentions_from_notes_v2;
> ```

---

## 08 — [HIST01-03] Normalize `histology_final` in cohort

> **Context**: `histology_final` has 77 whitespace issues, ~172 PTC variant strings, and 179 rows with `metastatic ` prefix collapsing site info into histology.
>
> **Do**:
> 1. Create view `manuscript_workspace.manuscript_cohort_v1_histology_clean` that emits all cohort columns plus:
>    - `histology_final_clean`: `LOWER(TRIM(REGEXP_REPLACE(histology_final,'\s+',' ','g')))` then map: everything containing `papillary` + `carcinoma` → `papillary thyroid carcinoma`, ditto `follicular` → `follicular thyroid carcinoma`, `medullary` → `medullary thyroid carcinoma`, `anaplastic` → `anaplastic thyroid carcinoma`, `hurthle`/`oncocytic` → `oncocytic thyroid carcinoma`.
>    - `histology_metastatic_prefix_flag`: TRUE when original starts with `metastatic `.
>    - `histology_variant_extracted`: any subtype tokens (`tall cell`, `columnar`, `diffuse sclerosing`, `follicular variant`, `solid variant`, etc.) extracted from the original string.
> 2. Keep the original `histology_final` column for audit.
>
> **Verify**:
> ```sql
> SELECT histology_final_clean, COUNT(*) AS n
> FROM manuscript_workspace.manuscript_cohort_v1_histology_clean
> GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
> SELECT histology_metastatic_prefix_flag, COUNT(*) FROM manuscript_workspace.manuscript_cohort_v1_histology_clean GROUP BY 1;
> ```

---

## 09 — [ETE01-02] Normalize ETE into controlled vocabulary

> **Context**: `extrathyroidal_extension` column has 35+ distinct raw strings; mapping target is `{none, minimal, microscopic, gross, extensive, NULL}` with `minimal` ≡ `microscopic` as a grouped equivalence class. Also 211 patients have `gross_ete=1` paired with ETE text = `minimal`/`microscopic`/`focal` (ETE02 contradiction).
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_path_malignant_events_v1_ete_clean` with columns:
>    - `ete_grade`: controlled vocab via mapping:
>        - NULL, `x`, `X`, `\`x`, `c/a`, `n/a`, `indeterminate`, `* (see margin comment)` → NULL
>        - `false`, `no` (case-insensitive) → `none`
>        - `minimal` and variants (`yes, minimal`, `yes (minimal)`, `minimal into fat`, `Yes;minimal;`) → `minimal`
>        - `microscopic` and misspellings (`microscopiic`, `microscopic extension`, `x\n(single microscopic focus of extension)`) → `microscopic`
>        - `focal`, `focal right side`, `focal early extension into perithyroidal fat`, `yes (focal)` → `minimal` (focal == minimal per path convention)
>        - `present`, `present (perithyroidal...)`, `true`, `yes`, `yes;`, bare `Yes` → `present_unspecified`
>        - `extensive`, `extesive`, `yes, extensive`, `Extensive` → `extensive`
>        - `gross` → `gross`
>    - `ete_grade_grouped`: collapses `minimal`+`microscopic` → `minimal_microscopic`, everything else passes through.
>    - `ete_discordance_flag`: TRUE when `gross_ete=1` AND `ete_grade` IN (`minimal`,`microscopic`,`present_unspecified`).
> 2. Rows with `ete_discordance_flag=TRUE` go to `qc_manual_review_queue_v1` with `issue_id='ETE02'`.
>
> **Verify**:
> ```sql
> SELECT ete_grade, ete_grade_grouped, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_ete_clean GROUP BY 1,2 ORDER BY 3 DESC;
> SELECT SUM(CASE WHEN ete_discordance_flag THEN 1 END) FROM manuscript_workspace.canonical_path_malignant_events_v1_ete_clean;
> ```

---

## 10 — [LN01-04] Multi-source LN architecture

> **Context**: Collapse of pathology and imaging LN counts into a single `ln_positive_final` is causing 10 rows where positives > examined (LN01), 28 rows with positives but 0 denominator (LN02), 51 raw↔final disagreements (LN03). The decided architecture is: separate sources, never merged.
>
> **Do**:
> 1. Create view `manuscript_workspace.ln_per_patient_multisource_v1` with columns:
>    `research_id`, `ln_path_positive`, `ln_path_examined`, `ln_us_suspicious_count`, `ln_ct_suspicious_count`, `ln_mri_suspicious_count`, `ln_clinical_positive_flag`, plus availability booleans `ln_data_available_{path,us,ct,mri,clinical}`.
>    - Path: rollup from `manuscript_workspace.canonical_path_malignant_events_v1_keyed` summing `ln_involved` / `ln_examined` per patient.
>    - US: count `ln_suspicious` rows in `main.canonical_us_lymph_node_v2` per patient (once USLN01 is rebuilt — may be 0 until then).
>    - CT: count rows in `main.ct_imaging` where LN flag TRUE.
>    - MRI: same against `main.mri_imaging`.
>    - Clinical: any TRUE in `main.canonical_cervical_ln_clinical_patient_rollup_v1`.
> 2. Within the path stream, any row where `ln_involved > ln_examined` goes to `qc_manual_review_queue_v1` with `issue_id='LN01'`.
> 3. Rows where `ln_path_positive > 0 AND COALESCE(ln_path_examined,0)=0` go to the queue with `issue_id='LN02'`.
> 4. The collapsed `ln_positive_final` / `path_ln_examined_raw` / `path_ln_positive_raw` columns are left on `main.manuscript_cohort_v1` (no mutation), but cohort_v2 will read from this new view.
>
> **Verify**:
> ```sql
> SELECT
>   COUNT(DISTINCT research_id) FILTER (WHERE ln_data_available_path)    AS path_pts,
>   COUNT(DISTINCT research_id) FILTER (WHERE ln_data_available_us)      AS us_pts,
>   COUNT(DISTINCT research_id) FILTER (WHERE ln_data_available_ct)      AS ct_pts,
>   COUNT(DISTINCT research_id) FILTER (WHERE ln_data_available_mri)     AS mri_pts,
>   COUNT(DISTINCT research_id) FILTER (WHERE ln_data_available_clinical) AS clin_pts
> FROM manuscript_workspace.ln_per_patient_multisource_v1;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id IN ('LN01','LN02');
> ```

---

## 11 — [PATH02, PATH19] Normalize `primary_histology` on path events

> **Context**: `primary_histology` has case/whitespace/typo variants plus 300 rows with `metastatic `/`recurrent ` prefix (PATH19) that belong on a separate flag.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_path_malignant_events_v1_histology_clean` emitting all columns plus:
>    - `primary_histology_clean`: normalized to the same controlled set used in HIST (prompt 08).
>    - `histology_metastatic_flag`: TRUE if original starts with `metastatic `.
>    - `histology_recurrent_flag`: TRUE if starts with `recurrent `.
>    - `primary_histology_raw`: alias of original for audit.
> 2. Rows where the clean value is not in the controlled vocab (e.g. benign labels in the malignant table) go to the queue with `issue_id='PATH03'`.
>
> **Verify**:
> ```sql
> SELECT primary_histology_clean, COUNT(*)
> FROM manuscript_workspace.canonical_path_malignant_events_v1_histology_clean
> GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
> SELECT histology_metastatic_flag, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_histology_clean GROUP BY 1;
> ```

---

## 12 — [PATH04] Normalize `histology_variant`

> **Context**: `histology_variant` is raw prose — needs controlled subtype values.
>
> **Do**:
> 1. Build a lookup dim `manuscript_workspace.dim_histology_variant_v1` with (`variant_raw`, `variant_clean`) — derived from `SELECT DISTINCT histology_variant`. Target clean values: `classical`, `tall_cell`, `columnar_cell`, `hobnail`, `diffuse_sclerosing`, `follicular_variant_encapsulated`, `follicular_variant_infiltrative`, `solid_variant`, `cribriform_morular`, `oncocytic`, `warthin_like`, `other`, NULL.
> 2. Create view `manuscript_workspace.canonical_path_malignant_events_v1_variant_clean` with a `histology_variant_clean` column joined on the dim.
>
> **Verify**:
> ```sql
> SELECT histology_variant_clean, COUNT(*)
> FROM manuscript_workspace.canonical_path_malignant_events_v1_variant_clean
> GROUP BY 1 ORDER BY 2 DESC;
> ```

---

## 13 — [PATH05-10] Normalize invasion / margin / ENE columns

> **Context**: `margin_status`, `lymphatic_invasion`, `vascular_invasion`, `perineural_invasion`, `capsular_invasion`, `extranodal_extension` all hold free-text prose mixing state (`yes/no/indeterminate`) with extent/location.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean` that for each of the 6 columns emits three derived columns:
>    - `<col>_state` ∈ {`negative`, `positive`, `indeterminate`, `not_evaluated`, NULL}
>    - `<col>_extent` ∈ {`focal`, `extensive`, `minimal`, NULL} where relevant
>    - `<col>_location_text` (verbatim suffix after state)
> 2. Use a central mapping dim `manuscript_workspace.dim_invasion_vocab_v1` so the same rules apply across all 6 columns.
> 3. Rows where state cannot be determined go to the queue with `issue_id='PATH05'..'PATH10'` accordingly.
>
> **Verify**:
> ```sql
> SELECT margin_status_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> SELECT lymphatic_invasion_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> SELECT vascular_invasion_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> SELECT perineural_invasion_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> SELECT capsular_invasion_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> SELECT extranodal_extension_state, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_invasion_clean GROUP BY 1;
> ```

---

## 14 — [PATH11] Flag nodal positive>0 with denominator 0/NULL

> **Context**: 47 path rows have `nodal_disease_positive_count > 0` but denominator 0 or NULL — a row-level integrity violation.
>
> **Do**: Insert these rows into `qc_manual_review_queue_v1` with `issue_id='PATH11'`, `source_pk = research_id||'|'||surgery_episode_uid||'|'||tumor_ordinal`, `context_json = {nodal_disease_positive_count, nodal_disease_total_count, ln_involved, ln_examined}`. Block affected surgery_episodes from cohort_v2 nodal staging downstream.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH11';
> ```
> Expect ≈ 47.

---

## 15 — [PATH12] Size disagreement between greatest-dim and per-surgery

> **Context**: 106 rows where `size_greatest_dimension_cm > tumor_size_cm_per_surgery`.
>
> **Do**: Emit these rows to `qc_manual_review_queue_v1` with `issue_id='PATH12'`. Also: `size_greatest_dimension_cm` represents the tumor focus and `tumor_size_cm_per_surgery` represents the whole-surgery largest — if the former exceeds the latter the per-surgery rollup is broken. Add derived column `tumor_size_cm_trusted = GREATEST(size_greatest_dimension_cm, tumor_size_cm_per_surgery)` in `manuscript_workspace.canonical_path_malignant_events_v1_size_clean` and use that downstream.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH12';
> ```

---

## 16 — [PATH13] Dedup pathology event rows

> **Context**: 30 rows across 15 groups are exact duplicates on `(research_id, surgery_date, laterality, site, size_greatest_dimension_cm, primary_histology)`.
>
> **Do**: Create view `manuscript_workspace.canonical_path_malignant_events_v1_dedup` keyed by the canonical episode + a deterministic tie-break: among duplicates, keep the row with (a) non-NULL `specimen_focus_id`, then (b) highest `data_completeness_pct`, then (c) lowest `tumor_ordinal`. Emit dropped rows to the queue with `issue_id='PATH13'`.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) AS in_v1 FROM main.canonical_path_malignant_events_v1;
> SELECT COUNT(*) AS dedup FROM manuscript_workspace.canonical_path_malignant_events_v1_dedup;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='PATH13';
> ```
> Expect `dedup = in_v1 - 15`.

---

## 17 — [PATH16] Flag weak-linkage pathology pathway

> **Context**: 1,434 rows have `resolution_rule='STL_only'` — the weak-linkage pathway without TEM confirmation. Not a deletion issue, but cohort stratification needs this flag surfaced.
>
> **Do**: Add column `path_linkage_strength` to `manuscript_workspace.canonical_path_malignant_events_v1_keyed` as a view extension: `CASE WHEN resolution_rule = 'STL_only' THEN 'weak' WHEN resolution_rule LIKE '%STL%TEM%' THEN 'strong' ELSE 'other' END`. Ensure cohort sensitivity analysis can filter.
>
> **Verify**:
> ```sql
> SELECT path_linkage_strength, COUNT(*) FROM manuscript_workspace.canonical_path_malignant_events_v1_keyed GROUP BY 1;
> ```

---

## 18 — [PATH17-18] Fix laterality↔site contradictions

> **Context**: 216 rows have `laterality` directly contradicting `site` (e.g., `laterality='left'` AND `site LIKE '%right%'`). Separately, 3,153 rows have `laterality='bilateral'` paired with a single-side site label (grain mismatch — `bilateral` is a patient attribute, not a single-focus attribute).
>
> **Do**:
> 1. Add to the dedup view columns: `laterality_site_conflict_flag` (PATH17) and `bilateral_grain_mismatch_flag` (PATH18).
> 2. For PATH17 rows (conflict), queue with `issue_id='PATH17'` and in the downstream cohort view **prefer `site`** over `laterality` (site is more specific; laterality is a derived attribute).
> 3. For PATH18 rows (bilateral paired with single-side site), promote `laterality` to NULL at the event grain and set a patient-level `patient_has_bilateral_disease` flag in the multifocality view (prompt 03).
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN laterality_site_conflict_flag THEN 1 END) AS p17,
>        SUM(CASE WHEN bilateral_grain_mismatch_flag THEN 1 END) AS p18
> FROM manuscript_workspace.canonical_path_malignant_events_v1_dedup;
> ```
> Expect p17 ≈ 216, p18 ≈ 3,153.

---

## 19 — [AJCC01-03] Staging calc-flag without components

> **Context**: `ajcc8_calc_flag=TRUE` but N-stage NULL on 53 event rows (AJCC01) / 269 cohort rows (AJCC02); AJCC7 same pattern 220 rows (AJCC03). If the calc flag is TRUE, all three of T/N/M must be populated.
>
> **Do**:
> 1. Add column `ajcc_calc_valid_flag` in `manuscript_workspace.canonical_path_malignant_events_v1_staging_clean`: TRUE iff `ajcc8_calc_flag` AND T/N/M all non-NULL. Similarly `ajcc7_calc_valid_flag`.
> 2. Rows where `ajcc*_calc_flag=TRUE AND ajcc*_calc_valid_flag=FALSE` → queue with `issue_id` matching AJCC01 (events) or AJCC03 (AJCC7).
> 3. Cohort downstream uses `ajcc_calc_valid_flag` instead of the raw flag.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN ajcc8_calc_flag AND NOT ajcc_calc_valid_flag THEN 1 END) AS invalid_ajcc8,
>  SUM(CASE WHEN ajcc7_calc_flag AND NOT ajcc7_calc_valid_flag THEN 1 END) AS invalid_ajcc7
> FROM manuscript_workspace.canonical_path_malignant_events_v1_staging_clean;
> ```

---

## 20 — [REC01] Recurrence before first surgery

> **Context**: 31 patients have `recurrence_date < first_surgery_date`. These are temporally impossible and must be chart-reviewed.
>
> **Do**: For each of the 31 patients, insert a row into `qc_manual_review_queue_v1` with `issue_id='REC01'`, `context_json={recurrence_date, recurrence_type, first_surgery_date, surgery_date, surg_first_date}`. Block these patients from any time-to-recurrence analysis in cohort_v2 until `status='resolved'`. Create view `manuscript_workspace.recurrence_event_clean_v1_temporal` that joins to cohort and adds `temporal_valid_flag = recurrence_date >= first_surgery_date`.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='REC01';
> SELECT SUM(CASE WHEN NOT temporal_valid_flag THEN 1 END) FROM manuscript_workspace.recurrence_event_clean_v1_temporal;
> ```

---

## 21 — [REC02-03] Recurrence flag/date mismatch

> **Context**: REC02 = flag TRUE but date NULL (1,764 pts). REC03 = date present but flag not TRUE (TBD).
>
> **Do**:
> 1. Add to cohort overlay view `manuscript_workspace.manuscript_cohort_v1_recurrence_clean`:
>    - `any_recurrence_final`: TRUE iff BOTH a date and a flag are present, FALSE iff neither, NULL otherwise.
>    - `recurrence_unknown_date_flag`: TRUE iff flag TRUE but date NULL (REC02).
>    - `recurrence_orphan_date_flag`: TRUE iff date present but no flag (REC03).
> 2. Emit REC02 rows to queue; emit REC03 rows to queue; time-to-event analyses drop `recurrence_unknown_date_flag=TRUE`.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN recurrence_unknown_date_flag THEN 1 END) AS rec02,
>  SUM(CASE WHEN recurrence_orphan_date_flag  THEN 1 END) AS rec03
> FROM manuscript_workspace.manuscript_cohort_v1_recurrence_clean;
> ```

---

## 22 — [SURG01-02] Reconcile three surgery-date columns

> **Context**: Cohort has `surgery_date`, `first_surgery_date`, `surg_first_date`. 171 patients have all three disagreeing (SURG01); 8,559 have all three identical (SURG02).
>
> **Do**:
> 1. In `manuscript_workspace.manuscript_cohort_v1_surgery_reconciled`, derive `surgery_date_canonical` with this priority: (a) if any two of the three agree, use that value; (b) if all three disagree, use `first_surgery_date` (SoT per operative episodes); (c) if only one is populated, use that.
> 2. Add `surgery_date_source_rank` ∈ {`consensus_2of3`,`first_surgery_fallback`,`single_only`,`all_null`}.
> 3. SURG01 rows (all disagree) → queue with `issue_id='SURG01'`.
> 4. Document the SoT decision in `qc_framework_v1/NOTES/surgery_date_SoT.md`: `first_surgery_date` from `canonical_operative_events_v1` wins ties.
>
> **Verify**:
> ```sql
> SELECT surgery_date_source_rank, COUNT(*) FROM manuscript_workspace.manuscript_cohort_v1_surgery_reconciled GROUP BY 1;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='SURG01';
> ```

---

## 23 — [FNA01] FNA after first surgery (uses operative SoT)

> **Context**: 349 FNA events / 286 pts (154 PTC) have `fna_date_resolved > first_surgery_date` using `canonical_operative_events_v1` as the SoT. Either mis-entered dates or actual post-op cytology (different clinical event — should not be counted as index FNA).
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_fna_events_v1_temporal` joining FNA events to `manuscript_workspace.first_surgery_date_v1` (derived from `canonical_operative_events_v1`) and adding:
>    - `post_surgery_fna_flag = (fna_date_resolved > first_surgery_date)`
>    - `fna_pre_surgery_flag = (fna_date_resolved <= first_surgery_date)`
>    - `fna_temporal_status` ∈ {`pre_op`, `post_op`, `no_surgery_date`, `no_fna_date`}.
> 2. Index FNA selection (`is_index_fna`) requires `fna_pre_surgery_flag=TRUE`.
> 3. Emit every post-surgery FNA event to queue with `issue_id='FNA01'`.
>
> **Verify**:
> ```sql
> SELECT fna_temporal_status, COUNT(*) AS events, COUNT(DISTINCT research_id) AS pts
> FROM manuscript_workspace.canonical_fna_events_v1_temporal GROUP BY 1 ORDER BY 2 DESC;
> ```
> Expect `post_op` ≈ 349 events / 286 pts.

---

## 24 — [TIR01-02] ACR 2017 band + concordance consistency guard

> **Context**: TR1=0pts, TR2=2pts, TR3=3pts, TR4=4-6pts, TR5=7+pts. Registry says currently 0 rows fail — so this is a *guard* to prevent regressions as new data lands.
>
> **Do**: Create view `manuscript_workspace.canonical_us_nodule_v2_tirads_guard` with columns:
> - `tirads_band_expected` from `acr2017_tirads_points` using the ACR cutpoints.
> - `tirads_band_mismatch_flag = (tirads_band_expected != acr2017_tirads_category)`.
> - `tirads_concordance_mismatch_flag = (acr2017_vs_updated_concordant IS NOT NULL AND acr2017_tirads_category = updated_tirads_category AND NOT acr2017_vs_updated_concordant)` OR the reverse.
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN tirads_band_mismatch_flag THEN 1 END) AS band_mm,
>        SUM(CASE WHEN tirads_concordance_mismatch_flag THEN 1 END) AS conc_mm
> FROM manuscript_workspace.canonical_us_nodule_v2_tirads_guard;
> ```

---

## 25 — [TIR03] Multi-nodule under-explosion (60 exams)

> **Context**: 60 US exams report multiple nodules in the narrative but the structured `canonical_us_nodule_v2` table has only 1 row per exam (56 patients / 60 exams). Registry calls for LLM re-parse of these flagged exams, then spot-check 200 `inm_v1_only` rows.
>
> **Do** (this is a multi-step agent task):
> 1. Build query that identifies the 60 candidate exams: rows in `canonical_us_nodule_v2` where the linked report text (join `main.imaging_exam_master_v1` → report text if available) contains "nodule 2"/"second nodule"/"two nodules"/"multiple nodules"/numbered patterns but the exam has only 1 nodule row.
> 2. Emit candidate list to `manuscript_workspace.qc_tir03_llm_candidates_v1` with `(research_id, us_exam_id, report_text, n_current_nodules)`.
> 3. Create a Python script `qc_framework_v1/tir03_llm_reparse.py` that reads this candidate list, calls Claude/GPT to extract structured nodule records per exam, writes results to `manuscript_workspace.canonical_us_nodule_v2_tir03_patch_v1`.
> 4. **Do not run the LLM call** in this prompt — just produce the candidate list and the script skeleton. Logan will review the 60 candidates before authorizing the LLM run.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_tir03_llm_candidates_v1;
> ```
> Expect ≈ 60.

---

## 26 — [US01-03] Size/location/aggregate handling in nodule table

> **Context**: US01 = 3,657 rows with all size fields NULL; US02 = 5,039 rows with laterality AND location both NULL; US03 = 141 rows are gland-aggregate rows that should not be in a per-nodule table.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_us_nodule_v2_filtered` with added columns:
>    - `us_row_type`: `nodule_with_measures` | `nodule_sizeless` | `nodule_locationless` | `aggregate_row` | `shell`.
> 2. Aggregate rows (US03) are identified by criteria provided by Logan (TBD — leave placeholder logic that flags rows where `nodule_index_within_exam IS NULL` AND no per-nodule measures).
> 3. Downstream cohort nodule counts filter to `us_row_type='nodule_with_measures'` unless explicitly overridden.
>
> **Verify**:
> ```sql
> SELECT us_row_type, COUNT(*) FROM manuscript_workspace.canonical_us_nodule_v2_filtered GROUP BY 1;
> ```

---

## 27 — [US04] `inm_v1_only` weak-resolution nodule rows

> **Context**: ~8,000 rows with `resolution_rule='inm_v1_only'` (or NULL) and no size/location. This is the imaging analog of PATH16.
>
> **Do**: Add column `us_resolution_strength` to `canonical_us_nodule_v2_filtered` = `CASE WHEN resolution_rule='inm_v1_only' OR resolution_rule IS NULL THEN 'weak' WHEN resolution_rule LIKE '%llm%' THEN 'strong' ELSE 'other' END`. Cohort downstream filters on this.
>
> **Verify**:
> ```sql
> SELECT us_resolution_strength, COUNT(*) FROM manuscript_workspace.canonical_us_nodule_v2_filtered GROUP BY 1;
> ```

---

## 28 — [USGLAND01-02] Gland-exam shell rows and missing parenchymal fields

> **Context**: `main.canonical_us_thyroid_gland_v2` has 13,578 rows; 6,785 are complete shells with no measurements (USGLAND01), and 100% of rows have all parenchymal-phenotype fields NULL (USGLAND02 — the parse layer never populated them).
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_us_thyroid_gland_v2_shape` that:
>    - Tags each row with `gland_row_type` in {`measured`, `shell`}.
>    - Documents in a comment that parenchymal-phenotype extraction needs a rebuild pass (out of scope for this prompt).
> 2. Emit one row per patient with only shell exams to `qc_manual_review_queue_v1` with `issue_id='USGLAND01'`.
> 3. Open a TODO entry in `qc_framework_v1/NOTES/usgland_parenchymal_rebuild.md` for USGLAND02 LLM pass.
>
> **Verify**:
> ```sql
> SELECT gland_row_type, COUNT(*) FROM manuscript_workspace.canonical_us_thyroid_gland_v2_shape GROUP BY 1;
> ```

---

## 29 — [USLN01] US lymph node table is entirely shell

> **Context**: `main.canonical_us_lymph_node_v2` has 6,801 rows, 100% shell (no LN measurements populated). Blocks multi-source LN (prompt 10) from having any US component.
>
> **Do**:
> 1. Produce candidate list `manuscript_workspace.qc_usln01_llm_candidates_v1`: every (research_id, us_exam_id) where the parent exam's report text mentions `lymph node`, `LAD`, `lymphadenopathy`, `abnormal node`, `cervical node`.
> 2. Create Python skeleton `qc_framework_v1/usln01_llm_extract.py` that (when authorized) calls LLM over the report text to extract structured LN rows: `(research_id, us_exam_id, ln_index, level, size_mm, suspicious_flag, features_json)`.
> 3. Do not execute the LLM pass — produce list + skeleton only.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_usln01_llm_candidates_v1;
> ```

---

## 30 — [CT01-04] CT normalization

> **Context**:
> - CT01: 975 rows where `lymph_node_mentioned=TRUE` but no location/level (ct_ln_level NULL).
> - CT02: 170 rows where `thyroid_not_visualized=TRUE` but other thyroid abnormality flags also TRUE (logically impossible).
> - CT03: 23 rows where `thyroid_normal=TRUE` but abnormality flags also TRUE.
> - CT04: 5,233 rows with `tracheal_deviation` flag but direction NULL.
>
> **Do**: Create view `manuscript_workspace.ct_imaging_clean` with added flags `ct_internal_contradiction_flag` (CT02/CT03 rules) and `ct_ln_underspecified_flag` (CT01) and `ct_tracheal_direction_missing_flag` (CT04). Rows with `ct_internal_contradiction_flag=TRUE` → queue with matching issue_id.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN ct_internal_contradiction_flag THEN 1 END) AS contra,
>  SUM(CASE WHEN ct_ln_underspecified_flag THEN 1 END) AS ln_under,
>  SUM(CASE WHEN ct_tracheal_direction_missing_flag THEN 1 END) AS trach_miss
> FROM manuscript_workspace.ct_imaging_clean;
> ```

---

## 31 — [MRI01-03] MRI normalization

> **Context**: MRI01 = 45 rows with explicit API/parse errors; MRI02 = 71 rows with LN mentioned but no location; MRI03 = 5 rows with `thyroid_normal=1` AND abnormality flags.
>
> **Do**: Create view `manuscript_workspace.mri_imaging_clean` with `mri_parse_error_flag`, `mri_ln_underspecified_flag`, `mri_internal_contradiction_flag`. MRI01 rows → queue (re-parse); MRI03 rows → queue.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN mri_parse_error_flag THEN 1 END) AS parse,
>  SUM(CASE WHEN mri_ln_underspecified_flag THEN 1 END) AS ln,
>  SUM(CASE WHEN mri_internal_contradiction_flag THEN 1 END) AS contra
> FROM manuscript_workspace.mri_imaging_clean;
> ```

---

## 32 — [IEM01] Imaging exam master: `exam_date` NULL

> **Context**: 2,050 rows in `imaging_exam_master_v1` have NULL `exam_date`.
>
> **Do**: Emit each to queue with `issue_id='IEM01'` and `context_json={exam_id, modality, source_table}`. Upstream modalities hold the date — linkage back to `ct_imaging.date`/`mri_imaging.date`/US exam date should populate most. Build recovery view `manuscript_workspace.imaging_exam_master_v1_dated` that coalesces `exam_date` with the modality-native date column.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN exam_date IS NULL THEN 1 END)                     AS null_in_main,
>  SUM(CASE WHEN exam_date_coalesced IS NULL THEN 1 END)          AS null_after_recovery
> FROM manuscript_workspace.imaging_exam_master_v1_dated;
> ```
> Expect `null_after_recovery` ≪ 2,050.

---

## 33 — [IEM02-05] Imaging exam master ↔ canonical disagreement

> **Context**: IEM02 = 7,319 rows with `largest_nodule_cm` NULL despite canonical US having sizes; IEM03 = 19 exams with `n_nodules` disagreement; IEM04 = 7 exams with size disagreement >0.1cm; IEM05 = 2,506 source rows from `raw_us_tirads_scored_v1` overcount nodules.
>
> **Do**: Rebuild `largest_nodule_cm` and `n_nodules` on `manuscript_workspace.imaging_exam_master_v1_dated` by aggregating from `manuscript_workspace.canonical_us_nodule_v2_filtered` where `us_row_type='nodule_with_measures'`. Add `iem_nodule_count_disagreement_flag` and `iem_largest_size_disagreement_flag` for audit. Exclude `raw_us_tirads_scored_v1` from rebuild source.
>
> **Verify**:
> ```sql
> SELECT
>  SUM(CASE WHEN iem_nodule_count_disagreement_flag THEN 1 END) AS count_mm,
>  SUM(CASE WHEN iem_largest_size_disagreement_flag THEN 1 END) AS size_mm
> FROM manuscript_workspace.imaging_exam_master_v1_dated;
> ```

---

## 34 — [NM01] Nuclear med `scandate` unparseable

> **Context**: 1,364 rows in `main.nuclear_med` have `scandate` that cannot be parsed to a date.
>
> **Do**: Create view `manuscript_workspace.nuclear_med_dated` with `scandate_resolved DATE` via `TRY_STRPTIME(scandate, '%Y-%m-%d')` and `TRY_CAST(scandate AS DATE)` fallbacks, plus `scandate_parse_status` in {`parsed_iso`,`parsed_cast`,`unparseable`}. Unparseable rows → queue.
>
> **Verify**:
> ```sql
> SELECT scandate_parse_status, COUNT(*) FROM manuscript_workspace.nuclear_med_dated GROUP BY 1;
> ```

---

## 35 — [NM02] `scan_present` 100% non-standard — inspect first

> **Context**: All 2,220 rows have a non-standard `scan_present` value. Before fixing, dump the distinct values so Logan can authorize the mapping.
>
> **Do**:
> 1. Produce `manuscript_workspace.qc_nm02_distinct_values_v1` = `SELECT DISTINCT scan_present, COUNT(*) FROM main.nuclear_med GROUP BY 1 ORDER BY 2 DESC`.
> 2. **Stop**. Report the distinct-value table and wait for Logan's mapping before continuing.
>
> **Verify**:
> ```sql
> SELECT * FROM manuscript_workspace.qc_nm02_distinct_values_v1 ORDER BY count DESC LIMIT 50;
> ```

---

## 36 — [NM03] Nuclear med no findings or impression text

> **Context**: 522 rows have neither `findings_text` nor `impression_text` — empty shells.
>
> **Do**: Add column `nm_empty_shell_flag = (findings_text IS NULL AND impression_text IS NULL)` to `manuscript_workspace.nuclear_med_dated`. Shell rows → queue with `issue_id='NM03'`. Downstream RAI analyses filter `nm_empty_shell_flag=FALSE`.
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN nm_empty_shell_flag THEN 1 END) FROM manuscript_workspace.nuclear_med_dated;
> ```

---

## 37 — [NM04] Nuclear med scantype / radiotracer NULL

> **Context**: 64 rows missing `scantype`, 110 rows missing `radiotracer`.
>
> **Do**: Add `nm_typing_incomplete_flag` to `nuclear_med_dated`. Create lookup `manuscript_workspace.dim_radiotracer_v1` canonicalizing the existing values. When `scantype` can be inferred from `radiotracer` (e.g., I-131 → whole-body uptake / RAI; Tc-99m → uptake scan), backfill into `scantype_inferred`.
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN nm_typing_incomplete_flag THEN 1 END) FROM manuscript_workspace.nuclear_med_dated;
> ```

---

## 38 — [GEN02] Backfill `platform_version` from `platform_raw`

> **Context**: 422 rows (267 ThyroSeq + 119 Afirma + 36 NGS) have NULL structured `platform_version` despite a version signal in `platform_raw` (`v2`, `v3`, `GSC`, `GEC`, etc.).
>
> **Do**: Create view `manuscript_workspace.canonical_molecular_genetics_v2_version_backfill` extending the keyed view (prompt 05) with:
> - `platform_version_inferred`: parse from `platform_raw` via regex (`thyroseq ?v?([23])` → int; `gsc`/`GSC` → 2 for Afirma; `gec`/`GEC` → 1 for Afirma).
> - `platform_version_final = COALESCE(platform_version, platform_version_inferred)`.
> - `platform_version_source` in {`structured`,`inferred_from_raw`,`unknown`}.
>
> **Verify**:
> ```sql
> SELECT platform, platform_version_source, COUNT(*)
> FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill
> GROUP BY 1,2 ORDER BY 1,2;
> ```

---

## 39 — [GEN03] `parse_status` sparse/inconsistent

> **Context**: `parse_status` is sparsely populated — some rows have detailed strings, many NULL.
>
> **Do**:
> 1. Dump distinct values: `SELECT DISTINCT parse_status, COUNT(*) FROM canonical_molecular_genetics_v2 GROUP BY 1`.
> 2. Add `parse_status_norm` to the version-backfill view mapping distinct values to `{ok, partial, failed, unknown}`. NULL → `unknown`.
>
> **Verify**:
> ```sql
> SELECT parse_status_norm, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill GROUP BY 1;
> ```

---

## 40 — [GEN04, GEN12] Normalize result-class and status columns

> **Context**: `overall_result_class` is dominated by NULL/other; the four status columns (`gene_mutations_status`, `gene_fusions_status`, `cna_status`, `gep_status`) hold non-normalized strings.
>
> **Do**: Add to `canonical_molecular_genetics_v2_version_backfill`:
> - `overall_result_class_clean` ∈ {`positive`, `negative`, `suspicious`, `inconclusive`, `insufficient_specimen`, `unknown`}.
> - For each of the 4 status columns, a `<col>_clean` with the same vocab.
>
> **Verify**:
> ```sql
> SELECT overall_result_class_clean, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill GROUP BY 1;
> SELECT gene_mutations_status_clean, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill GROUP BY 1;
> ```

---

## 41 — [GEN05] `molecular_confidence` 100% NULL

> **Context**: 1,384/1,384 rows NULL. The column either needs backfill from a parse heuristic or documented deprecation.
>
> **Do**: Decision-deferred — write a TODO in `qc_framework_v1/NOTES/gen05_confidence.md` with the two options and the evidence needed to pick one (a) backfill from `n_fields_parsed`/`parse_status` OR (b) drop the column from cohort_v2. **Stop**; do not implement.
>
> **Verify**: `ls /Users/loganglosser/THYROID_2026/qc_framework_v1/NOTES/gen05_confidence.md`.

---

## 42 — [GEN06] `resolved_test_date` 65% NULL

> **Context**: Two-thirds of molecular rows lack a resolved test date.
>
> **Do**: Attempt backfill: (a) from `test_date_native` cast; (b) from linked FNA episode's `fna_date_resolved`; (c) from linked surgery episode's `surgery_date`. Emit `test_date_source` ∈ {`resolved`, `native_cast`, `via_fna`, `via_surgery`, `unresolved`}. Column `resolved_test_date_final` on view `canonical_molecular_genetics_v2_version_backfill`.
>
> **Verify**:
> ```sql
> SELECT test_date_source, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill GROUP BY 1;
> ```

---

## 43 — [GEN07] `risk_of_malignancy_pct` out-of-range

> **Context**: ROM point/low/high values outside [0,100].
>
> **Do**: Add `rom_pct_out_of_range_flag = (rom_percent_point < 0 OR rom_percent_point > 100 OR rom_percent_low < 0 OR rom_percent_high > 100 OR rom_percent_low > rom_percent_high)`. Rows with flag=TRUE → queue. In the view, NULL the offending columns and set `rom_pct_cleaned=TRUE`.
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN rom_pct_out_of_range_flag THEN 1 END) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill;
> ```

---

## 44 — [GEN08] Fusion flag without structured fusions

> **Context**: 486 patients have `fusion_detected=TRUE` but zero rows in `main.molecular_fusions_unnested_VIEW_v2`. Either the header flag is wrong or the unnest parser is failing.
>
> **Do**:
> 1. Cross-join to `main.molecular_fusions_unnested_VIEW_v2` per `(research_id, molecular_episode_uid)` and compute `unnested_fusion_rowcount`.
> 2. Add flag `fusion_flag_vs_unnest_mismatch = (fusion_detected AND unnested_fusion_rowcount=0)` in the version-backfill view.
> 3. Mismatch rows → queue with `issue_id='GEN08'`. Check `gene_fusions_raw` on those rows — if non-empty, the unnest parser is broken; if empty, the flag is false-positive.
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN fusion_flag_vs_unnest_mismatch THEN 1 END) AS mm,
>        COUNT(DISTINCT research_id) FILTER (WHERE fusion_flag_vs_unnest_mismatch) AS pts
> FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill;
> ```
> Expect pts ≈ 486.

---

## 45 — [GEN11] `specimen_adequacy` mostly NULL

> **Context**: Populated from `specimen_adequacy_raw`/`specimen_adequacy_norm` inconsistently; most rows NULL.
>
> **Do**: Add `specimen_adequacy_final` = `COALESCE(specimen_adequacy_norm, <mapping of specimen_adequacy_raw>)` with target vocab `{adequate, limited, inadequate, unknown}`.
>
> **Verify**:
> ```sql
> SELECT specimen_adequacy_final, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_version_backfill GROUP BY 1;
> ```

---

## 46 — Final: cohort_v2 overlay view

> **Context**: Once prompts 01–45 have run, build the cohort view that stitches every clean/derived view together.
>
> **Do**: Create `manuscript_workspace.manuscript_cohort_v2` that left-joins:
> - base: `manuscript_workspace.manuscript_cohort_v1_histology_clean` (one row per patient)
> - + `ln_per_patient_multisource_v1`
> - + patient-level rollup of `canonical_path_malignant_events_v1_ete_clean` (ete_grade_grouped, ete_discordance_flag)
> - + `path_episode_multifocality_v1`
> - + `manuscript_cohort_v1_surgery_reconciled.surgery_date_canonical`
> - + `manuscript_cohort_v1_recurrence_clean.any_recurrence_final`
> - Exclude patients with any open `qc_manual_review_queue_v1` row where `status='open'` AND `issue_id` is in the critical set: `{LN01, LN02, REC01, PATH11, PATH15, FNA01, SURG01, TIR03, GEN09}`.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) AS v1, (SELECT COUNT(*) FROM manuscript_workspace.manuscript_cohort_v2) AS v2,
>        (SELECT COUNT(*) FROM main.manuscript_cohort_v1) AS v1_orig;
> ```

---

## Deferred / out of scope for this round

- **IPS01** — already clean; no prompt needed. Just confirm guard with `SELECT * FROM main.imaging_patient_summary_v1 LIMIT 0` once per release.
- **GEN05** — decision pending (see prompt 41).
- **TIR03 LLM pass** — candidate list only; execution gated on Logan.
- **USLN01 / USGLAND02 LLM pass** — candidate lists only; execution gated on Logan.

---

## Run order (short version)

```
00 → 01 → 02 → 03 → 04 → 05 → 06 → 07
08 → 09 → 10
11 → 12 → 13
14 → 15 → 16 → 17 → 18
19
20 → 21 → 22
23 → 47 → 48 → 49 → 50       # FNA02–05 after 23
51                            # IFNA rebuild (depends on 23 + 50)
52 → 53                       # OP01–02, OP03–04
54                            # GEN13 (depends on 05)
55                            # GEN14 (depends on 01)
56                            # GEN15
57                            # GEN16
58                            # REC04
59                            # REC05
24 → 25
26 → 27 → 28 → 29
30 → 31
32 → 33
34 → 35 (pause) → 36 → 37
38 → 39 → 40 → (41 pause) → 42 → 43 → 44 → 45
46
```

---

# Batch 6 additions (from 2026-04-22 PM investigation)

## 47 — [FNA02] Re-resolve `fna_date_resolved` from `fna_date_raw`

> **Context**: 1,516 FNA rows / 1,141 pts (464 PTC) have `fna_date_raw` populated but `fna_date_resolved` NULL. Upstream parser missed these.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_fna_events_v1_date_resolved` extending FNA events with:
>    - `fna_date_resolved_v2 DATE`: `COALESCE(fna_date_resolved, TRY_STRPTIME(fna_date_raw,'%Y-%m-%d'), TRY_STRPTIME(fna_date_raw,'%m/%d/%Y'), TRY_STRPTIME(fna_date_raw,'%b %d %Y'), TRY_STRPTIME(fna_date_raw,'%d %b %Y'))`.
>    - `fna_date_resolved_source` ∈ {`existing`, `reparsed_iso`, `reparsed_us`, `reparsed_natural`, `unresolved`}.
> 2. Rows that remain `unresolved` → queue with `issue_id='FNA02'`.
> 3. Downstream prompts (temporal, linkage) switch to `fna_date_resolved_v2`.
>
> **Verify**:
> ```sql
> SELECT fna_date_resolved_source, COUNT(*) FROM manuscript_workspace.canonical_fna_events_v1_date_resolved GROUP BY 1;
> ```

---

## 48 — [FNA03] Recompute `days_to_surgery`

> **Context**: 280 rows have `days_to_surgery < 0` on the existing column — a mix of the FNA01 post-op contamination and genuine parse drift.
>
> **Do**: In `manuscript_workspace.canonical_fna_events_v1_temporal` (prompt 23 revised), add `days_to_surgery_v2 = DATE_DIFF('day', fna_date_resolved_v2, first_surgery_date_canonical)`. Negative values after recomputation that aren't already flagged by FNA01 → queue with `issue_id='FNA03'`. (Expect most of the 280 to collapse into FNA01's 349.)
>
> **Verify**:
> ```sql
> SELECT SUM(CASE WHEN days_to_surgery_v2 < 0 THEN 1 END) AS neg,
>        SUM(CASE WHEN days_to_surgery_v2 < 0 AND NOT post_surgery_fna_flag THEN 1 END) AS neg_not_fna01
> FROM manuscript_workspace.canonical_fna_events_v1_temporal;
> ```

---

## 49 — [FNA04] Dedup strict-duplicate FNA rows

> **Context**: 4 duplicate-signature FNA rows / 2 excess / 2 pts.
>
> **Do**: Create view `manuscript_workspace.canonical_fna_events_v1_dedup` keyed on `(research_id, fna_date_resolved_v2, laterality, specimen_location, bethesda_final_num, pathology_diagnosis)`. Within each duplicate group keep the row with (a) non-NULL `fna_date_resolved_v2`, (b) highest `bethesda_final_num`, (c) lowest `fna_event_id`. Dropped rows → queue with `issue_id='FNA04'`.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) AS v1 FROM main.canonical_fna_events_v1;
> SELECT COUNT(*) AS dedup FROM manuscript_workspace.canonical_fna_events_v1_dedup;
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='FNA04';
> ```

---

## 50 — [FNA05] Rebuild `bethesda_final` in patient rollup

> **Context**: 6 pts (2 PTC) have NULL `bethesda_final` in `canonical_fna_patient_rollup_v1` despite non-null preop event Bethesda values.
>
> **Do**: Create view `manuscript_workspace.canonical_fna_patient_rollup_v1_bethesda_rebuilt` that overrides `bethesda_final` with `MAX(bethesda_final_num) FILTER (WHERE fna_pre_surgery_flag=TRUE)` from the temporal view (prompt 23). Audit column `bethesda_final_source_rebuilt` ∈ {`existing`, `rebuilt`} for each patient.
>
> **Verify**:
> ```sql
> SELECT bethesda_final_source_rebuilt, COUNT(*) FROM manuscript_workspace.canonical_fna_patient_rollup_v1_bethesda_rebuilt GROUP BY 1;
> SELECT COUNT(*) AS rebuilt_nulls_recovered
> FROM manuscript_workspace.canonical_fna_patient_rollup_v1_bethesda_rebuilt
> WHERE bethesda_final_source_rebuilt='rebuilt';
> ```
> Expect `rebuilt_nulls_recovered ≈ 6`.

---

## 51 — [IFNA01–06] Rebuild `imaging_fna_linkage_v3` end-to-end

> **Context**: This is a linker-design defect, not a data defect. All 3,339 eligible links share `size_score=0.5` (size neutralized); 445 admit explicit laterality conflict; 510 allow future imaging (img_date > fna_date); 873 FNAs link to multiple nodules; 814 FNAs tie at rank 1 (e.g. pt 9096 FNA 1 → 13 rank-1 nodules including future imaging). Fix is a full rebuild as `v4`, not an overlay.
>
> **Do** — build `manuscript_workspace.imaging_fna_linkage_v4`:
> 1. **Eligibility**: `img_date <= fna_date_resolved_v2` AND `fna_date_resolved_v2 - img_date <= INTERVAL 365 DAY` AND NOT (both lateralities populated AND differ).
> 2. **Scoring**:
>    - `temporal_score = GREATEST(0, 1 - (fna_date - img_date) / 365)`.
>    - `laterality_score`: 1.0 match; 0.5 one-side NULL; 0 (ineligible).
>    - `size_score`: `1 - LEAST(1, ABS(img_size_cm - fna_size_cm) / GREATEST(img_size_cm, fna_size_cm))` when both populated; 0.5 otherwise.
>    - `linkage_score = 0.4*temporal_score + 0.3*laterality_score + 0.3*size_score`.
> 3. **Rank**: `ROW_NUMBER() OVER (PARTITION BY research_id, fna_episode_id ORDER BY linkage_score DESC, ABS(fna_date-img_date), imaging_exam_id)` — deterministic, no ties.
> 4. **Unique winner index**: create `manuscript_workspace.imaging_fna_linkage_v4_index` = rows where `rank=1` — exactly one row per FNA episode.
> 5. **Diagnostics columns** on v4: `n_eligible_nodules_per_fna`, `n_eligible_fnas_per_nodule`.
> 6. Write a side-by-side comparison table `manuscript_workspace.imaging_fna_linkage_v3_vs_v4` with counts of defects fixed per IFNA01-06.
>
> **Verify** (expect zero violations on v4):
> ```sql
> SELECT
>   SUM(CASE WHEN size_score=0.5 AND img_size_cm IS NOT NULL AND fna_size_cm IS NOT NULL THEN 1 END) AS ifna01_residual,
>   SUM(CASE WHEN img_laterality<>fna_laterality AND img_laterality IS NOT NULL AND fna_laterality IS NOT NULL THEN 1 END) AS ifna02_residual,
>   SUM(CASE WHEN img_date > fna_date_resolved_v2 THEN 1 END) AS ifna03_residual
> FROM manuscript_workspace.imaging_fna_linkage_v4 WHERE analysis_eligible_link_flag;
> SELECT COUNT(DISTINCT (research_id, fna_episode_id)) AS fnas,
>        COUNT(*) AS rank1_rows
> FROM manuscript_workspace.imaging_fna_linkage_v4_index;
> ```
> Expect `fnas = rank1_rows` and all three residuals = 0.

---

## 52 — [OP01-02] Operative laterality-procedure contradictions

> **Context**: 33 `total_thyroidectomy` rows have unilateral `laterality` (OP01); 3 `hemithyroidectomy` rows have `laterality='bilateral'` (OP02). Combined 36 rows across 36 pts (24 PTC).
>
> **Do**: Add to `manuscript_workspace.canonical_operative_events_v1_guard`:
> - `procedure_laterality_conflict_flag` with enum `conflict_type` ∈ {`total_unilateral`, `hemi_bilateral`, `none`}.
> Emit flagged rows to queue with matching `issue_id` and set `procedure_normalized_trusted = NULL` for these rows. Downstream joins read `procedure_normalized_trusted`.
>
> **Verify**:
> ```sql
> SELECT conflict_type, COUNT(*) FROM manuscript_workspace.canonical_operative_events_v1_guard GROUP BY 1;
> ```

---

## 53 — [OP03-04] Relink procedure codes to operative episodes

> **Context**: 904 procedure-code rows are ambiguous across multiple episodes (OP03); 11,134 have NULL `linked_surgery_episode_id` (OP04).
>
> **Do**: Build `manuscript_workspace.canonical_operative_procedure_codes_v1_relinked` joining on `(research_id, code_date ≈ resolved_surgery_date within 7 days, procedure_normalized match)`. Emit:
> - `linked_surgery_episode_id_v2` (global op ID).
> - `linkage_source` ∈ {`existing`, `reattributed_exact_date_and_procedure`, `reattributed_date_only`, `orphan`}.
> - `ambiguity_flag` if two candidates tie on the scoring.
> OP03 ambiguous-after-relink rows → queue with `issue_id='OP03'`. OP04 orphan rows → queue with `issue_id='OP04'`.
>
> **Verify**:
> ```sql
> SELECT linkage_source, COUNT(*) FROM manuscript_workspace.canonical_operative_procedure_codes_v1_relinked GROUP BY 1;
> ```
> Expect `existing` + `reattributed_*` ≫ `orphan`.

---

## 54 — [GEN13] Relink `specimen_genomic_assay_v1` → `canonical_molecular_genetics_v2`

> **Context**: 9,267 assay rows with non-null `molecular_episode_id` fail to match any canonical_molecular_genetics_v2 row on the same `(research_id, molecular_episode_id)`. Both tables hold patient-local ordinals that aren't synchronized across them. After GEN01 assigns a real `molecular_episode_uid`, the same MD5 formula must be applied to the assay table so they can join.
>
> **Do**:
> 1. Create view `manuscript_workspace.specimen_genomic_assay_v1_keyed` with the same MD5 on `(research_id, resolved_test_date, platform, platform_version)` as `manuscript_workspace.canonical_molecular_genetics_v2_keyed` (prompt 05).
> 2. LEFT JOIN on `molecular_episode_uid`. Add `assay_to_molecular_link_tier` ∈ {`exact_uid`, `patient_plus_date`, `patient_only`, `none`}.
> 3. Rows at `none` → queue with `issue_id='GEN13'`.
>
> **Verify**:
> ```sql
> SELECT assay_to_molecular_link_tier, COUNT(*) FROM manuscript_workspace.specimen_genomic_assay_v1_keyed GROUP BY 1;
> ```

---

## 55 — [GEN14] Relink assay `surgery_episode_id` to global operative namespace

> **Context**: 311 assay rows (223 pts / 105 PTC) have non-null `surgery_episode_id` that doesn't match any operative episode (same local-ordinal problem as OP05).
>
> **Do**: Extend `specimen_genomic_assay_v1_keyed` with a LEFT JOIN to `canonical_operative_events_v1` on `(research_id, resolved_test_date within 90 days of resolved_surgery_date)` to adopt the global op ID. Add `surgery_episode_uid_global_v2` and `surgery_episode_link_source` ∈ {`existing_matched`, `remapped_date`, `none`}. Gap → queue with `issue_id='GEN14'`.
>
> **Verify**:
> ```sql
> SELECT surgery_episode_link_source, COUNT(*) FROM manuscript_workspace.specimen_genomic_assay_v1_keyed GROUP BY 1;
> ```

---

## 56 — [GEN15] Relink molecular → FNA episode

> **Context**: 360 `canonical_molecular_genetics_v2` rows (347 pts / 122 PTC) have non-null `linked_fna_episode_id` but no matching FNA on `(research_id, fna_index)`.
>
> **Do**: In the molecular keyed view (prompt 05), LEFT JOIN to `manuscript_workspace.canonical_fna_events_v1_dedup` (prompt 49) on `(research_id, fna_index = linked_fna_episode_id)`. Add `linked_fna_event_id_v2` and `fna_link_source` ∈ {`existing_matched`, `remapped_by_index`, `remapped_by_date_proximity`, `none`}. Date-proximity fallback: nearest FNA within ±180 days of `resolved_test_date`. Gap → queue with `issue_id='GEN15'`.
>
> **Verify**:
> ```sql
> SELECT fna_link_source, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_keyed GROUP BY 1;
> ```

---

## 57 — [GEN16] Re-extract BRAF variant when flag set

> **Context**: 180 rows / 175 pts (115 PTC) have `braf_flag=TRUE` but `braf_variant` NULL. Clinically important because BRAF is the dominant PTC driver — models reading BRAF status will misclassify these patients.
>
> **Do**:
> 1. Create view `manuscript_workspace.canonical_molecular_genetics_v2_braf_clean` extending the keyed view with:
>    - `braf_variant_reparsed`: regex over `platform_raw || test_result_summary || report_text_ref` for `V600E`, `K601E`, `p\.V600E`, `c\.1799T>A`, `T1799A`. First match wins; normalize output to `p.V600E`, `p.K601E`, etc.
>    - `braf_variant_final = COALESCE(braf_variant, braf_variant_reparsed)`.
>    - `braf_variant_source` ∈ {`existing`, `reparsed`, `flag_without_variant`}.
> 2. `flag_without_variant` rows → queue with `issue_id='GEN16'`. In cohort_v2, analyses stratified on BRAF status require `braf_variant_final IS NOT NULL`.
>
> **Verify**:
> ```sql
> SELECT braf_variant_source, COUNT(*) FROM manuscript_workspace.canonical_molecular_genetics_v2_braf_clean GROUP BY 1;
> ```
> Expect `flag_without_variant` ≪ 180 (reparsed should recover most).

---

## 58 — [REC04] Recurrence after last-known-alive

> **Context**: 2 rows / 2 pts (both PTC) where `recurrence_date > last_known_alive_date`. Low volume but date integrity — either stale `last_known_alive_date` or mis-entered recurrence.
>
> **Do**: Join `recurrence_event_clean_v1` to `canonical_survival_followup_v1` and emit these 2 rows to queue with `issue_id='REC04'` and `context_json={recurrence_date, last_known_alive_date, death_date, vital_status_current}`. Chart review.
>
> **Verify**:
> ```sql
> SELECT COUNT(*) FROM manuscript_workspace.qc_manual_review_queue_v1 WHERE issue_id='REC04';
> ```

---

## 59 — [REC05] Structural recurrence flag TRUE but site NULL

> **Context**: 1,818 rows (1,420 PTC) with `structural_recurrence_flag=TRUE AND recurrence_site IS NULL`. Composition: 1,764 from `extracted_recurrence_refined_v1 + structural_date_unknown`; 54 from `extracted_recurrence_refined_v1 + structural_confirmed`. The 54 are confirmed events with failed site extraction — highest value. The 1,764 suggest the upstream rule is firing on weak evidence.
>
> **Do** (two-part fix):
> 1. **Tighten the flag**: create view `manuscript_workspace.recurrence_event_clean_v1_tightened` with:
>    - `structural_recurrence_flag_strict = (structural_recurrence_flag AND (recurrence_site IS NOT NULL OR recurrence_date IS NOT NULL))`
>    - `structural_recurrence_possible_flag = (structural_recurrence_flag AND recurrence_site IS NULL AND recurrence_date IS NULL)`
>    Cohort_v2 analyses use `_flag_strict` for primary endpoints and `_flag_possible` for sensitivity analysis.
> 2. **Targeted site re-extraction for the 54 `structural_confirmed`**: build `manuscript_workspace.qc_rec05_llm_candidates_v1` = those 54 rows plus source note text. Skeleton `qc_framework_v1/rec05_site_reextract.py`. LLM call gated on Logan.
> 3. Emit all 1,818 rows to queue with `issue_id='REC05'` and `context_json={source_table, recurrence_definition, structural_flag_strict, structural_flag_possible}`.
>
> **Verify**:
> ```sql
> SELECT
>   SUM(CASE WHEN structural_recurrence_flag_strict THEN 1 END) AS strict,
>   SUM(CASE WHEN structural_recurrence_possible_flag THEN 1 END) AS possible
> FROM manuscript_workspace.recurrence_event_clean_v1_tightened;
> SELECT COUNT(*) FROM manuscript_workspace.qc_rec05_llm_candidates_v1;
> ```
> Expect `possible ≈ 1,764`, LLM candidate list ≈ 54.

---

## Cohort_v2 overlay — revised dependency chain

> Prompt 46 now also depends on: 47 (FNA02 dates), 48 (FNA03 recompute), 50 (FNA05 rollup), 51 (IFNA rebuild), 53 (OP03/04 procedure codes), 54 (GEN13 assay→molecular), 55 (GEN14 assay→operative), 56 (GEN15 molecular→FNA), 57 (GEN16 BRAF), 59 (REC05 tightening). Add the following to the "exclude patients with any open queue row" critical set in prompt 46: `OP05`, `GEN13`, `GEN16`, `REC05`.

