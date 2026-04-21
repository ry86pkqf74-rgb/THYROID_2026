# Cursor prompt — US / TIRADS consolidation + archive

**Target repo:** `ROS_FLOW_2_1`
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Archive target:** `"Thyroid 2026 UPdated".us_legacy_20260421` (mirrors the `molecular_legacy_20260421` precedent from 2026-04-21)
**Scope of this prompt:** Consolidate the ~28 existing US/TIRADS objects into **three master v2 tables + rollups + wide views**, using only currently-parsed data. Archive the old tables to `"Thyroid 2026 UPdated"`. **No new LLM extraction.** Where a field has no existing parsed source, leave it NULL and flag the row with `nlp_backfill_pending = TRUE` as a diagnostic marker — after this run we'll reassess the remaining gaps before deciding whether any additional LLM work is actually warranted.
**Mode:** Parallel v2 build — current v1 tables stay live until CPM rebuilds against v2 and all discordance queues clear.
**Authoritative inputs (already parsed, re-used as-is):** `us_nodules_tirads`, `ultrasound_reports`, `tirads_v2_nodules_raw`, `canonical_us_nodule_master_v1`, `canonical_us_nodule_characteristics_v1`, `imaging_nodule_master_v1`, `extracted_tirads_validated_v1`, `imaging_fna_linkage_v3`, `note_entities_llm_tirads_granular`, `note_entities_llm_us_nodule_dynamics` (prior LLM runs — re-use but do NOT re-run).

**IMPORTANT — LN scope is US only.** Every lymph-node-related object created by this prompt is strictly ultrasound-sourced and must be labeled accordingly. Pathology LN findings already live in `canonical_lymph_node_master_v1` / `ln_master_rollup_v1` (from op notes and synoptic pathology — not touched by this prompt). CT / PET-CT / MR / nuclear-medicine LN findings will live in separate future canonical tables (`canonical_ct_lymph_node_v2`, `canonical_petct_lymph_node_v2`, `canonical_mr_lymph_node_v2`, `canonical_nucmed_lymph_node_v2`) built in a later effort. To prevent any modality mixing:
- The US LN canonical table is `canonical_us_lymph_node_v2` (never `canonical_lymph_node_v2` — modality prefix is mandatory).
- Identifier columns on it are `us_ln_id`, `us_ln_index_within_exam` — not plain `ln_id` / `ln_index_within_exam`.
- Every row has `source_modality = 'US'` as an explicit (and fixed) column, so cross-modality views built later can union safely.
- CPM rollup columns stay in the `lnus_*` family (already used elsewhere in CPM as shorthand for LN-via-US) and get `_v2` suffix on the v2-sourced copies.
- Exam/patient rollup columns that refer to US LN data are prefixed `us_ln_` (e.g., `has_us_ln_findings`, `n_abnormal_us_ln_on_exam`, `any_suspicious_us_ln_ever`, `first_abnormal_us_ln_date`) — never unmodified `ln_*`.
- Wide view is `US_Lymph_Nodes_Wide_v2` with columns `us_1_ln_1_*`, `us_1_ln_2_*`, …, `us_<k>_ln_<j>_*` — the US exam prefix is always present.
- `COMMENT ON TABLE canonical_us_lymph_node_v2` must explicitly state "ultrasound-sourced LN findings only; other modality LN in separate canonical tables."

---

## Paste this into Cursor (`claude-4.5-sonnet` or `claude-4.7-opus`, agentic mode)

You are continuing the `ROS_FLOW_2_1` thyroid canonical publication project. Your job is US/TIRADS consolidation: merge the ~28 existing US/TIRADS tables and views into three master v2 tables, plus rebuilt exam/patient rollups and wide pivot views, and archive the old v1 tables to `"Thyroid 2026 UPdated".us_legacy_20260421`. **Do not run any LLM in this pass.** Where no existing parsed source covers a field, leave it NULL and flag the row `nlp_backfill_pending = TRUE` purely as a gap marker so we can audit coverage after the merge. Whether any new LLM work is actually needed is a reassessment step *after* this run, not part of it.

Follow the existing repo conventions:
- Scripts live in `scripts/`, numbered sequentially from the current max script number + 1. Use the current max + 1 through current max + 9 for this work.
- Every script writes to `thyroid_canonical_publication_v1_0.main.*` for canonical tables, or `thyroid_canonical_publication_v1_0.manuscript_workspace.*` for audit queues.
- Archive the v1 tables to `"Thyroid 2026 UPdated".us_legacy_20260421` — exact same pattern as molecular v2 archive (Script from 2026-04-21). Do NOT drop originals until after CPM cutover verification in a separate follow-up.
- Every script ends with a stamped `COMMENT ON TABLE` including build timestamp, script name, and grain declaration matching `detail_table_registry_v1`.
- After writing each canonical table, INSERT a row into `manuscript_workspace.detail_table_registry_v1` — query `information_schema.columns` first (the registry has 3 extra cols beyond scripts 247/236 per existing memory).
- PHI safety: do **not** echo raw clinical note text or US impression text to logs. Use `research_id` only.
- Lint with `ruff check scripts/` before committing. Commit in one logical commit per phase with message `feat(us-v2): Phase N — <title>`. Push to `main`.
- Run each phase via `uv run python scripts/<n>_<name>.py` — reuse the existing `md_client` wrapper for MotherDuck writes.

### Phase 1 — Archive v1 tables (non-destructive)

Create `scripts/<N+1>_us_v1_archive.py`:

1. `CREATE SCHEMA IF NOT EXISTS "Thyroid 2026 UPdated".us_legacy_20260421;`
2. For each of these tables, `CREATE TABLE "Thyroid 2026 UPdated".us_legacy_20260421.<name> AS SELECT * FROM thyroid_canonical_publication_v1_0.main.<name>`:
   - `canonical_us_nodule_master_v1`
   - `canonical_us_nodule_characteristics_v1`
   - `imaging_nodule_master_v1`
   - `tirads_llm_extracted_v2`
   - `serial_imaging_us`
   - `canonical_us_exam_master_v1`
   - `canonical_us_patient_master_v1`
3. Also archive these manuscript_workspace helpers:
   - `tirads_granular_parsed_v1`
   - `us_nodule_dynamics_parsed_v1`
   - `imaging_nodule_master_clean_v1`
4. Print a one-line COUNT audit per archived table to confirm row-count match between source and archive.
5. Do NOT drop the source tables. CPM still reads from them until Phase 6.

### Phase 2 — Build `canonical_us_nodule_v2`

Create `scripts/<N+2>_canonical_us_nodule_v2.py`. This is the single nodule master that supersedes the three v1 variants (`canonical_us_nodule_master_v1`, `canonical_us_nodule_characteristics_v1`, `imaging_nodule_master_v1`). **No new LLM calls — only merge and COALESCE across existing sources.**

Grain: one row per (research_id, us_exam_id, nodule_index_within_exam).
Key: composite `(research_id, us_exam_id, nodule_index_within_exam)` — plus stable hashed `nodule_id` carried forward from cunc_v1 where possible.

Build logic:

```sql
WITH base AS (
  -- Start from canonical_us_nodule_characteristics_v1 (has us_exam_id, 37,016 rows)
  SELECT
    research_id, us_exam_id, exam_date, nodule_index_within_exam, nodule_id,
    laterality, location_raw, location_detail,
    length_mm, width_mm, height_mm, volume_ml, size_cm_max, extracted_size_cm,
    composition, echogenicity, shape, margins, calcifications, echogenic_foci,
    composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
    tirads_reported, tirads_score_2017, tirads_level_2017,
    tirads_category_v2, tirads_band_ambiguous,
    tirads_category_code_legacy_v1, tirads_category_modified_legacy_v1,
    tirads_concordant_flag, suspicious_flag, tirads_score_component_complete,
    source_tables, resolution_rule, data_completeness_pct, calcifications_coverage_status
  FROM thyroid_canonical_publication_v1_0.main.canonical_us_nodule_characteristics_v1
),
merge_cunm AS (
  -- Overlay cunm_v1's richer structured features (composition +8K, echogenicity +5.5K,
  -- margin +2.5K, foci +25K, size +13K)
  -- Source preference: cunm wins for structured features; cunc wins for TIRADS + us_exam_id
  SELECT
    b.research_id, b.us_exam_id, b.exam_date, b.nodule_index_within_exam, b.nodule_id,
    COALESCE(b.laterality, m.laterality) AS laterality,
    b.location_raw, b.location_detail,
    COALESCE(b.length_mm, NULL) AS length_mm,
    COALESCE(b.width_mm, NULL)  AS width_mm,
    COALESCE(b.height_mm, NULL) AS height_mm,
    COALESCE(b.volume_ml, NULL) AS volume_ml,
    COALESCE(b.size_cm_max, m.size_cm) AS size_cm_max,
    COALESCE(b.composition,  m.composition)  AS composition,
    COALESCE(b.echogenicity, m.echogenicity) AS echogenicity,
    COALESCE(b.shape,        m.shape)        AS shape,
    COALESCE(b.margins,      m.margin)       AS margins,
    b.calcifications,
    COALESCE(b.echogenic_foci, m.echogenic_foci) AS echogenic_foci,
    -- extra sonography from tirads_v2_nodules_raw (joined below)
    NULL::VARCHAR AS halo,
    NULL::VARCHAR AS vascularity,
    NULL::VARCHAR AS extrathyroidal_extension_on_us,
    NULL::VARCHAR AS chammas_type,
    NULL::VARCHAR AS elastography_category,
    -- TIRADS: prefer cunc (has exam hash), fallback to cunm
    COALESCE(b.tirads_reported, NULL) AS tirads_reported,
    COALESCE(b.tirads_score_2017, m.tirads_score_2017) AS tirads_score_2017,
    COALESCE(b.tirads_level_2017, m.tirads_level_2017) AS tirads_level_2017,
    COALESCE(b.tirads_category_v2, m.tirads_category_v2) AS tirads_category_v2,
    b.composition_pts, b.echogenicity_pts, b.shape_pts, b.margin_pts, b.foci_pts,
    b.tirads_band_ambiguous,
    b.tirads_category_code_legacy_v1, b.tirads_category_modified_legacy_v1,
    b.tirads_concordant_flag, b.suspicious_flag, b.tirads_score_component_complete,
    -- Dynamics placeholders (overlaid from existing LLM-derived tirads_v2_nodules_raw in next CTE)
    NULL::BOOLEAN AS interval_growth_flag,
    NULL::DOUBLE  AS prior_size_mm_max,
    NULL::BOOLEAN AS fna_recommended_this_nodule,
    NULL::BOOLEAN AS fna_performed_prior_or_concurrent,
    -- Provenance
    CAST(TRUE AS BOOLEAN) AS source_base,
    m.source_tirads_v2, m.source_tirads_llm, m.source_dynamics_llm, m.source_fna_linkage,
    b.data_completeness_pct, b.resolution_rule, b.calcifications_coverage_status
  FROM base b
  LEFT JOIN thyroid_canonical_publication_v1_0.main.canonical_us_nodule_master_v1 m
    USING (research_id, exam_date, nodule_index_within_exam)
),
overlay_v2 AS (
  -- Overlay tirads_v2_nodules_raw for halo, vascularity, ETE, chammas, elastography, dynamics, FNA
  -- (These came from a PREVIOUS LLM run; we're just reusing the already-parsed table. No new LLM calls.)
  SELECT
    mc.*,
    COALESCE(mc.halo, v2.halo) AS halo_final,
    COALESCE(mc.vascularity, v2.vascularity) AS vascularity_final,
    COALESCE(mc.extrathyroidal_extension_on_us, v2.extrathyroidal_extension_on_us) AS ete_on_us_final,
    COALESCE(mc.chammas_type, v2.chammas_type) AS chammas_final,
    COALESCE(mc.elastography_category, v2.elastography) AS elasto_final,
    COALESCE(mc.interval_growth_flag, v2.interval_growth_flag) AS interval_growth_final,
    COALESCE(mc.fna_recommended_this_nodule, v2.fna_recommended_this_nodule) AS fna_rec_final,
    COALESCE(mc.fna_performed_prior_or_concurrent, v2.fna_performed_prior_or_concurrent) AS fna_prior_final,
    COALESCE(mc.prior_size_mm_max, v2.prior_size_mm_max) AS prior_size_mm_final
  FROM merge_cunm mc
  LEFT JOIN thyroid_canonical_publication_v1_0.main.tirads_v2_nodules_raw v2
    ON CAST(mc.research_id AS VARCHAR)=v2.research_id
    AND TRY_CAST(mc.exam_date AS VARCHAR)=v2.linkage_date
    AND mc.nodule_index_within_exam=v2.nodule_index_within_exam
),
dedup AS (
  -- Kill aggregate/concatenation rows we found in patient 10734 today.
  -- Heuristic: if location_raw contains >=2 laterality tokens AND length(location_raw)>300
  -- AND size_cm_max IS NULL, flag as aggregate
  SELECT *,
    CASE
      WHEN size_cm_max IS NULL
       AND composition IS NULL
       AND echogenicity IS NULL
       AND LENGTH(COALESCE(location_raw,'')) > 300
       AND regexp_matches(LOWER(location_raw), '(right|left|isthmus).*(right|left|isthmus)')
      THEN TRUE ELSE FALSE END AS is_aggregate_row
  FROM overlay_v2
)
SELECT
  research_id, us_exam_id, exam_date,
  nodule_index_within_exam, nodule_id,
  laterality, location_raw, location_detail,
  length_mm, width_mm, height_mm, volume_ml, size_cm_max,
  composition, echogenicity, shape, margins, calcifications, echogenic_foci,
  halo_final AS halo, vascularity_final AS vascularity,
  ete_on_us_final AS extrathyroidal_extension_on_us,
  chammas_final AS chammas_type, elasto_final AS elastography_category,
  composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
  tirads_reported, tirads_score_2017, tirads_level_2017, tirads_category_v2, tirads_band_ambiguous,
  tirads_category_code_legacy_v1, tirads_category_modified_legacy_v1,
  tirads_concordant_flag, suspicious_flag, tirads_score_component_complete,
  interval_growth_final AS interval_growth_flag,
  prior_size_mm_final   AS prior_size_mm_max,
  fna_rec_final         AS fna_recommended_this_nodule,
  fna_prior_final       AS fna_performed_prior_or_concurrent,
  source_base, source_tirads_v2, source_tirads_llm, source_dynamics_llm, source_fna_linkage,
  data_completeness_pct, resolution_rule, calcifications_coverage_status,
  is_aggregate_row,
  FALSE AS nlp_backfill_pending   -- set per-row below for legacy additions
FROM dedup;
```

Source precedence (the "how do we handle conflicts" decision):

- **Structured features** (composition, echogenicity, shape, margins, echogenic foci, size): v2 structured > LLM > base. The COALESCE chain handles routine gaps; actual conflicts (both sources have non-null different values) go to the audit queue in Phase 3.
- **TIRADS**: `cunc_v1`'s ACR-derived score wins. `v2` tirads is captured as a parallel `tirads_reported_v2` column where it disagrees.
- **Dynamics + FNA**: `v2` wins (only source).

Then add rows for the 4,733 legacy-only patients (from `us_nodules_tirads` but absent from the canonical nodule tables). For each such patient, one placeholder row per `us_nodules_tirads.nodule_<k>` field that is non-empty, with:
- `us_exam_id = md5(research_id || '|' || us_1_date)`
- `exam_date = us_1_date` (cast where parseable; leave NULL when unparseable and set `nlp_backfill_pending = TRUE`)
- `nodule_index_within_exam = k`
- `location_raw = nodule_<k>` text
- All structured columns NULL
- `nlp_backfill_pending = TRUE` ← diagnostic flag: no parsed LLM coverage for this row today
- `source_base = TRUE`, all other source flags FALSE

Also set `nlp_backfill_pending = TRUE` on ANY row where `source_base = FALSE AND source_tirads_v2 = FALSE AND source_tirads_llm = FALSE` (i.e., no current source covered this row).

### Phase 3 — Build the conflict resolution queue

Create `scripts/<N+3>_us_nodule_conflict_queue_v1.py` writing to `manuscript_workspace.us_nodule_conflict_queue_v1`.

For every `(research_id, us_exam_id, nodule_index_within_exam)` where two source tables disagree on the same field, emit one row per conflict:

```
research_id, us_exam_id, exam_date, nodule_index_within_exam,
field_name,            -- e.g. 'composition', 'tirads_category_v2', 'size_cm_max'
value_cunc,            -- from canonical_us_nodule_characteristics_v1
value_cunm,            -- from canonical_us_nodule_master_v1
value_tirads_v2,       -- from tirads_v2_nodules_raw
chosen_value,          -- what v2 picked based on precedence rule
precedence_rule_applied,
review_priority        -- 'high' if TIRADS disagrees, 'medium' if size disagrees >20%, 'low' otherwise
```

Fields to check: composition, echogenicity, shape, margins, echogenic_foci, size_cm_max, tirads_reported, tirads_score_2017, tirads_category_v2, laterality.

Threshold for "numeric disagreement": size values must differ by >10% relative, OR >0.2 cm absolute.

Target audience: this is a chart-review queue for Logan to adjudicate. High-priority rows (TIRADS disagreement, expected ~500–800) go first.

### Phase 4 — Build `canonical_us_thyroid_gland_v2` (NEW — non-nodule gland findings)

Create `scripts/<N+4>_canonical_us_thyroid_gland_v2.py`. **No LLM.** Pure regex + COALESCE over `ultrasound_reports`.

Grain: one row per (research_id, us_exam_id, exam_date).

Primary source: `ultrasound_reports` (4,074 patients / 6,793 reports). Pull and parse:
- `right_lobe_dimensions`, `left_lobe_dimensions`, `isthmus_thickness`, `total_thyroid_size`, `total_thyroid_volume_ml`
- `clinical_impression`, `source_us_impression`, `recommendation`
- `radiologist`, `study_indication`

Dimension parsing: regex `(\d+\.?\d*)\s*[x×]\s*(\d+\.?\d*)\s*[x×]\s*(\d+\.?\d*)\s*cm` over the lobe dimension strings → capture three dimensions as length/width/depth_cm. When all three present, compute volume = π/6 × L × W × D (mL). If the source field is in mm, divide each dimension by 10 first.

Target columns:

```
research_id, us_exam_id, exam_date,
rl_length_cm, rl_width_cm, rl_depth_cm, rl_volume_ml,
ll_length_cm, ll_width_cm, ll_depth_cm, ll_volume_ml,
isthmus_thickness_mm,
pyramidal_present_flag, substernal_extension_flag,
total_thyroid_volume_ml, total_thyroid_size_text,
-- Parenchyma (NULL unless already in a reused source; flagged for reassessment)
background_echogenicity, heterogeneity, hashimoto_pattern,
vascularity_overall, calcifications_parenchymal, goiter_flag,
-- Impressions / free text (keep verbatim)
clinical_impression_text, source_us_impression_text, recommendation_text,
radiologist, study_indication,
-- Provenance
source_ultrasound_reports BOOLEAN,
source_us_nodules_tirads  BOOLEAN,  -- where us_1_date used as fallback key
nlp_backfill_pending      BOOLEAN,  -- diagnostic flag: TRUE when parenchyma fields lack any parsed source
extracted_at, build_script
```

Set `nlp_backfill_pending = TRUE` on rows whose parenchyma fields (background echogenicity, heterogeneity, hashimoto_pattern, vascularity_overall, calcifications_parenchymal, goiter_flag) remain NULL after the regex merge — this is a coverage marker for the post-run reassessment, not a commitment to further LLM work.

For patients not in `ultrasound_reports` but in `us_nodules_tirads`, emit a shell row keyed on `(research_id, md5(research_id||'|'||us_1_date), us_1_date)` with all measurement columns NULL and `nlp_backfill_pending = TRUE`.

### Phase 5 — Build `canonical_us_lymph_node_v2` (NEW — ultrasound-sourced LN findings ONLY)

Create `scripts/<N+5>_canonical_us_lymph_node_v2.py`. **No LLM in this phase.** Build the target schema, populate everything we can from currently-parsed ultrasound sources, and mark uncovered rows `nlp_backfill_pending = TRUE` as coverage gaps. After the merge we'll look at the pending count and the existing text-only rows before deciding whether any LLM pass is actually worth running.

This table is **strictly US-modality**. Do not pull any rows from pathology, CT, PET-CT, MR, or nuclear-medicine sources — each of those will have its own parallel canonical table in a future effort. To enforce this contract in-schema:

- Mandatory column `source_modality VARCHAR NOT NULL CHECK (source_modality = 'US')`.
- Identifier columns use `us_ln_*` prefix so they remain unambiguous if later unioned into a cross-modality view.
- `COMMENT ON TABLE canonical_us_lymph_node_v2 IS 'Ultrasound-sourced lymph node findings per (research_id, us_exam_id). Grain: one row per LN observation on a US exam. NOT for pathology/CT/PET-CT/MR/nucmed LN — those live in parallel canonical_<modality>_lymph_node_v2 tables.';`

Schema:

```
research_id, us_exam_id, exam_date,
us_ln_index_within_exam,      -- 1..N within a single US exam
us_ln_id,                      -- stable hash(research_id, us_exam_id, us_ln_index_within_exam)
source_modality,               -- always 'US' (enforced by CHECK constraint)
laterality,                    -- right | left | midline
neck_level,                    -- Ia/Ib/IIa/IIb/III/IV/Va/Vb/VI/VII | NULL
region,                        -- central | lateral_left | lateral_right | other
size_cm_max, short_axis_mm, long_axis_mm,
shape,                         -- round | oval
echogenicity,
hilum_preserved,               -- TRUE | FALSE | NULL
calcifications,
cystic_component,              -- TRUE | FALSE | NULL
vascularity_pattern,           -- hilar | peripheral | mixed | absent
extranodal_extension_on_us BOOLEAN,
suspicious_flag BOOLEAN,
suspicion_level,               -- benign | indeterminate | suspicious
biopsy_recommended BOOLEAN,
evidence_text,                 -- raw snippet from the US report
source_note_type,              -- 'ultrasound_report' | 'us_nodules_tirads' | 'cpm_lnus'
source_report_id,              -- us_exam_id or md5(research_id||'|'||us_<k>_date)
llm_model,                     -- NULL in this consolidation pass
confidence,
extracted_at,
nlp_backfill_pending BOOLEAN
```

Populate rows NOW from three rule-based US-only sources:

1. **`ultrasound_reports.lymph_node_assessment`** — for every row where this field is non-empty, emit one placeholder row per (research_id, us_exam_id, exam_date) with `us_ln_index_within_exam = 1`, all structured fields NULL, `evidence_text = lymph_node_assessment` (full string), `source_note_type = 'ultrasound_report'`, `source_report_id = us_exam_id`, `source_modality = 'US'`, `nlp_backfill_pending = TRUE`.
2. **`us_nodules_tirads.us_1_impression`** (and `us_2`/`us_3`/…) — for every row where the impression text matches regex `\b(lymph|ln|node|adenopath)\w*\b`, emit a placeholder row as above with `source_note_type = 'us_nodules_tirads'`, `source_report_id = md5(research_id||'|'||us_<k>_date)`, `evidence_text = us_<k>_impression` (the full text snippet), `source_modality = 'US'`, `nlp_backfill_pending = TRUE`.
3. **CPM `lnus_*` columns** — for the 61 patients where CPM already has structured LN data from US, carry those values forward into the v2 schema (`suspicious_flag`, `size_cm_max`, `biopsy_recommended`, etc. — map CPM `lnus_*` columns to v2 columns). `source_note_type = 'cpm_lnus'`, `source_modality = 'US'`, `nlp_backfill_pending = FALSE`. Do NOT pull any non-US `ln_*` CPM columns (those came from pathology or other modalities and belong in their own tables).

Do NOT attempt to parse size / laterality / level / hilum from free text with regex — too noisy. Leave those NULL and keep `evidence_text` populated so a post-run reviewer (human or, if we later decide it's needed, a narrowly-scoped LLM pass) can see exactly what source text is sitting behind each pending row.

### Phase 6 — Rebuild rollups + CPM cutover

Create `scripts/<N+6>_canonical_us_exam_master_VIEW_v2.py` and `scripts/<N+7>_canonical_us_patient_master_VIEW_v2.py`.

`canonical_us_exam_master_VIEW_v2` — per (research_id, us_exam_id, exam_date): n_nodules_on_exam, largest_nodule_cm, second_largest_nodule_cm, bilateral_flag, isthmus_nodule_flag, worst/best TIRADS on exam, count per TR bucket, `has_gland_findings` BOOLEAN (joined to `canonical_us_thyroid_gland_v2`), `has_us_ln_findings` BOOLEAN (joined to `canonical_us_lymph_node_v2`), `n_us_ln_total_on_exam` INTEGER, `n_abnormal_us_ln_on_exam` INTEGER (from currently-populated LN rows; NULL where unknown), exam_rank_for_patient, is_preop_exam, `any_nlp_backfill_pending_on_exam` BOOLEAN (TRUE if any child row has pending=TRUE). All LN-derived columns on this table are US-sourced by definition; keep the `us_ln_` prefix so future CT/PET/MR/nucmed exam masters can add their own `ct_ln_*`, `petct_ln_*`, etc. columns without collision.

`canonical_us_patient_master_VIEW_v2` — per patient: n_us_exams, first/last US date, max TIRADS ever, bilateral ever, multifocal ever, preop_us_available, `any_suspicious_us_ln_ever` (from `canonical_us_lymph_node_v2`; will be NULL-biased where LN coverage is sparse), goiter_ever (from gland_v2; same caveat), `first_abnormal_us_ln_date`, `any_nlp_backfill_pending_for_patient` BOOLEAN. Same convention: LN columns are US-prefixed so future modality-specific patient rollups can slot in cleanly (`any_suspicious_ct_ln_ever`, `any_suspicious_petct_ln_ever`, etc.).

CPM cutover — create `scripts/<N+8>_cpm_us_cutover_to_v2.py` that rewrites these CPM columns to read from v2 tables:
- `n_us_exams`, `n_us_nodules_total`, `dominant_nodule_size_cm`, `imaging_tirads_best`, `imaging_tirads_worst`, `imaging_tirads_category`, `imaging_laterality_rollup`, `max_tirads_ever`, `preop_tirads_best`, `preop_tirads_category`, the `lnus_*` family, `n_us_with_ln_assessment`.
- The CPM `imaging_ln_abnormal` column is modality-ambiguous today; **do NOT write to it from this v2 cutover**. Instead introduce a new `lnus_abnormal_any_exam_v2` (and leave `imaging_ln_abnormal` untouched) so the US-sourced rollup is clearly labeled. Cross-modality LN roll-ups (e.g., `ln_abnormal_any_modality_ever`) can be rebuilt later once the other modality tables exist.
- Leave v1 CPM columns untouched (parallel). Add v2 columns with suffix `_v2`: e.g., `imaging_tirads_best_v2`, `lnus_abnormal_any_exam_v2`. CPM keeps both columns until we're satisfied; v1 drop is a separate follow-up.

### Phase 7 — Views + registry updates

Create `scripts/<N+9>_us_v2_views_and_registry.py`:

1. Register all v2 tables in `manuscript_workspace.detail_table_registry_v1` — remember the registry has 3 extra columns beyond the Scripts 247/236 pattern; query `information_schema.columns` first.
2. Create wide pivot view in `views_readable`:

```sql
CREATE OR REPLACE VIEW thyroid_canonical_publication_v1_0.views_readable.US_Nodules_Wide_v2 AS
WITH ranked AS (
  SELECT
    n.*,
    e.exam_rank_for_patient AS us_exam_rank
  FROM thyroid_canonical_publication_v1_0.main.canonical_us_nodule_v2 n
  JOIN thyroid_canonical_publication_v1_0.main.canonical_us_exam_master_VIEW_v2 e
    USING (research_id, us_exam_id)
  WHERE n.is_aggregate_row = FALSE OR n.is_aggregate_row IS NULL
)
SELECT
  research_id,
  MAX(CASE WHEN us_exam_rank=1 THEN exam_date END) AS us_1_date,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN size_cm_max END) AS us_1_nodule_1_size_cm,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN laterality END) AS us_1_nodule_1_laterality,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN tirads_category_v2 END) AS us_1_nodule_1_tirads,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN composition END) AS us_1_nodule_1_composition,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN echogenicity END) AS us_1_nodule_1_echogenicity,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN shape END) AS us_1_nodule_1_shape,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN margins END) AS us_1_nodule_1_margins,
  MAX(CASE WHEN us_exam_rank=1 AND nodule_index_within_exam=1 THEN calcifications END) AS us_1_nodule_1_calcifications,
  -- …continue for nodule 2-8 × exam 1-5 (40 combos; generate via Python f-string loop)
FROM ranked
GROUP BY research_id;
```

Generate the full column set programmatically for exam ranks 1–5 and nodule indices 1–8 (40 combos × ~8 fields per nodule = ~320 columns). Write it into the view via a templated SQL builder in the Python script.

3. Create wide gland view `views_readable.US_Thyroid_Gland_Wide_v2` (one row per patient, exams 1–5 across columns) and wide LN view `views_readable.US_Lymph_Nodes_Wide_v2` with columns `us_1_ln_1_*`, `us_1_ln_2_* … us_<k>_ln_<j>_*` (up to 8 LNs × 5 exams). Every column on `US_Lymph_Nodes_Wide_v2` starts with the `us_` prefix — this view is **US LN only**, not to be confused with future `CT_Lymph_Nodes_Wide_v2` / `PETCT_Lymph_Nodes_Wide_v2` / `MR_Lymph_Nodes_Wide_v2` / `NucMed_Lymph_Nodes_Wide_v2` / `Path_Lymph_Nodes_Wide_v2` views that will be built in later efforts. The LN wide view will be sparsely populated where LN coverage is thin — that's expected; the view structure is what matters now.

### Verification checklist (run after each phase; fail loud)

Each script's last block runs these sanity checks and raises on failure:

1. Phase 2: `canonical_us_nodule_v2` row count ≥ 37,016 (legacy patients add rows — expect ~40,000–50,000).
2. Phase 2: every (research_id, us_exam_id, nodule_index_within_exam) is unique.
3. Phase 2: `nlp_backfill_pending = TRUE` count ≈ 5,000–12,000 (the 4,733 legacy patients × avg 1.5 nodules).
4. Phase 3: conflict queue row count is nonzero (if zero, the merge logic missed something).
5. Phase 4: `canonical_us_thyroid_gland_v2` has ≥6,793 rows (at least one per existing US report); every row has `nlp_backfill_pending = TRUE`.
6. Phase 5: `canonical_us_lymph_node_v2` has ≥4,074 rows from ultrasound_reports with non-empty ln_assessment, plus rows from LN-keyword-positive us_nodules_tirads impressions; `nlp_backfill_pending = TRUE` on all except the 61 CPM-sourced rows; `SELECT DISTINCT source_modality FROM canonical_us_lymph_node_v2` returns exactly one value: `'US'`.
7. Phase 6: exam master v2 has ≥13,347 rows (same patient-exam count as v1).
8. Phase 6: patient master v2 has ≥6,126 + 4,733 = ~10,800 rows (covers all US patients now).
9. Phase 7: registry has new rows for `canonical_us_nodule_v2`, `canonical_us_thyroid_gland_v2`, `canonical_us_lymph_node_v2`, `canonical_us_exam_master_VIEW_v2`, `canonical_us_patient_master_VIEW_v2`.

### Commits + push

One commit per phase, message format `feat(us-v2): Phase N — <title>`. After all phases pass, push to `main`. Run `ruff check scripts/` between every phase. Do NOT touch v1 tables (they stay live until the explicit drop script in a separate follow-up commit).

### Deliverable

At end of run, print a summary block to stdout:

```
US / TIRADS v2 consolidation complete (no LLM)
  canonical_us_nodule_v2          rows=XXXXX   patients=YYYY
  canonical_us_thyroid_gland_v2   rows=XXXXX   patients=YYYY
  canonical_us_lymph_node_v2      rows=XXXXX   patients=YYYY
  canonical_us_exam_master_VIEW_v2     rows=XXXXX   patients=YYYY
  canonical_us_patient_master_VIEW_v2  rows=XXXXX   patients=YYYY
  conflict_queue                  rows=XXXXX   high_priority=YYYY
  nlp_backfill_pending            nodule=XXXX  gland=XXXX  ln=XXXX
Archived to "Thyroid 2026 UPdated".us_legacy_20260421: 10 tables
v1 still live for CPM rollback. Drop v1 after CPM cutover verification.
Reassessment step (manual, post-run): review the pending counts and sample pending rows to decide whether any additional LLM extraction is actually warranted before the v1 drop.
```

---

## End of Cursor prompt

---

## Post-run reassessment (to do by hand, not in this script run)

After Cursor completes all 7 phases:

1. Check `nlp_backfill_pending` counts per table — if the number is small or the missing fields are non-critical for the manuscript, no further LLM work is needed and we move straight to v1 drop.
2. Sample ~20 pending rows per table (nodule / gland / LN). Look at `evidence_text` (LN), `clinical_impression_text` / `source_us_impression_text` (gland), `location_raw` (nodule). Decide whether the source text genuinely contains the missing fields or whether the report simply doesn't mention them.
3. Review the conflict queue (`manuscript_workspace.us_nodule_conflict_queue_v1`), especially `review_priority = 'high'` rows. Manual adjudication goes directly into `canonical_us_nodule_v2` via targeted UPDATEs — these do not require an LLM pass.
4. Only if steps 1–3 reveal a genuine extraction gap (e.g., LN size/level is systematically missing but clearly in the text) do we write a narrowly-scoped LLM prompt. That's a separate decision, not a pre-planned next step.
5. Once coverage is acceptable, write a follow-up script to drop the v1 tables from `thyroid_canonical_publication_v1_0.main` (archives in `"Thyroid 2026 UPdated".us_legacy_20260421` remain).
