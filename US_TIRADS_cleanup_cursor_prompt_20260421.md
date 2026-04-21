# Cursor prompt — US / TIRADS consolidation to v2 (parallel build)

**Target repo:** `ROS_FLOW_2_1`
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Archive target:** `"Thyroid 2026 UPdated".us_legacy_20260421` (mirrors the `molecular_legacy_20260421` precedent from 2026-04-21)
**Mode:** Parallel v2 build — current v1 tables stay live until CPM rebuilds against v2 and all discordance queues clear.
**Authoritative inputs:** `us_nodules_tirads`, `ultrasound_reports`, `tirads_v2_nodules_raw`, `note_entities_llm_tirads_granular`, `note_entities_llm_us_nodule_dynamics`, `extracted_tirads_validated_v1`, `imaging_fna_linkage_v3`.

---

## Paste this into Cursor (`claude-4.5-sonnet` or `claude-4.7-opus`, agentic mode)

You are continuing the `ROS_FLOW_2_1` thyroid canonical publication project. We are cleaning up the US / TIRADS section of the master canonical DB (`thyroid_canonical_publication_v1_0`) by building three new parallel tables at v2 — one for nodules, one for thyroid gland (non-nodule) findings, one for US lymph nodes — plus rebuilt exam/patient rollups. Current v1 tables stay live; we cut CPM over to v2 in the final phase.

Follow the existing repo conventions:
- Scripts live in `scripts/`, numbered sequentially from the current max script number + 1. Use the current max + 1 through current max + 9 for this work.
- Every script writes to `thyroid_canonical_publication_v1_0.main.*` for canonical tables, or `thyroid_canonical_publication_v1_0.manuscript_workspace.*` for audit queues.
- Archive the v1 tables to `"Thyroid 2026 UPdated".us_legacy_20260421` — exact same pattern as molecular v2 archive (Script 2026-04-21). Do not drop originals until after CPM cutover verification.
- Every script ends with a stamped COMMENT ON TABLE with build timestamp, script name, and grain declaration matching `detail_table_registry_v1`.
- After writing each canonical table, INSERT a row into `manuscript_workspace.detail_table_registry_v1` — query `information_schema.columns` first (the registry has 3 extra cols beyond scripts 247/236 per existing memory).
- PHI safety: do **not** echo raw clinical note text or US impression text to logs. Use `research_id` only. Use `entity_value` / `evidence_text` in the LLM parses but never dump whole notes.
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

Create `scripts/<N+2>_canonical_us_nodule_v2.py`. This is the single nodule master that supersedes the three v1 variants.

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
  -- Overlay cunm_v1's richer structured features (composition +8K, echogenicity +5.5K, margin +2.5K, foci +25K, size +13K)
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
    -- extra sonography from tirads_v2_nodules_raw (below join)
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
    -- Dynamics placeholders (overlay in next CTE)
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
  -- Overlay tirads_v2_nodules_raw for halo, vascularity, extrathyroidal extension, chammas, elastography
  -- Join on (research_id, exam_date, nodule_index_within_exam) — cast research_id to VARCHAR for v2
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
  -- Heuristic: if location_raw contains >=2 laterality tokens AND length(location_raw)>300 AND size_cm_max IS NULL,
  -- AND an earlier nodule in the same exam already has the same raw text prefix, flag as aggregate
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
  is_aggregate_row
FROM dedup;
```

Key points on source precedence (answers the "how to handle conflicts" decision):

- **Structured features** (composition, echogenicity, shape, margins, echogenic foci, size): v2 structured > LLM > base. The COALESCE chain handles routine gaps; actual conflicts (both sources have non-null different values) go to the audit queue in Phase 3.
- **TIRADS**: `cunc_v1`'s ACR-derived score wins. `v2` tirads is captured as a parallel `tirads_reported_v2` column where it disagrees.
- **Dynamics + FNA**: `v2` wins (only source).

Also insert an `nlp_backfill_pending` boolean flag: TRUE for rows where `source_base = FALSE AND source_tirads_v2 = FALSE AND source_tirads_llm = FALSE` — i.e., rows that came from the legacy wide table without any LLM pass yet. This covers the 4,733-patient gap that the follow-up LLM backfill will resolve.

Also add rows for the 4,733 legacy-only patients (answers question 1 — incorporate now). For each such patient, one placeholder row per `us_nodules_tirads.nodule_<k>` field that is non-empty, with:
- `us_exam_id = md5(research_id || '|' || us_1_date)`
- `exam_date = us_1_date` (cast where parseable)
- `nodule_index_within_exam = k`
- `location_raw = nodule_<k>` text
- All structured columns NULL
- `nlp_backfill_pending = TRUE`
- `source_base = TRUE`, all other source flags FALSE

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

Target audience: this is a chart-review queue for you to adjudicate. High-priority rows (TIRADS disagreement, 1-row spotcheck expected ~500-800) go first.

### Phase 4 — Build `canonical_us_thyroid_gland_v2` (NEW)

Create `scripts/<N+4>_canonical_us_thyroid_gland_v2.py`.

Grain: one row per (research_id, us_exam_id, exam_date).
Sources:
- Primary: `ultrasound_reports` for 4,074 patients / 6,793 reports — parse `right_lobe_dimensions`, `left_lobe_dimensions`, `isthmus_thickness`, `total_thyroid_size`, `total_thyroid_volume_ml`, `clinical_impression`, `source_us_impression`, `recommendation`.
- Parse the dimensions text ("4.3 x 1.6 x 1.6 cm") with a regex: capture three dimensions as length/width/depth_cm. Compute volume when all three present (π/6 × L × W × D).

Target columns:

```
research_id, us_exam_id, exam_date,
rl_length_cm, rl_width_cm, rl_depth_cm, rl_volume_ml,
ll_length_cm, ll_width_cm, ll_depth_cm, ll_volume_ml,
isthmus_thickness_mm,
pyramidal_present_flag, substernal_extension_flag,
total_thyroid_volume_ml, total_thyroid_size_text,
-- Parenchyma (Phase 5 LLM pass will fill these; NULL initially)
background_echogenicity, heterogeneity, hashimoto_pattern,
vascularity_overall, calcifications_parenchymal, goiter_flag,
-- Impressions / free text (keep)
clinical_impression_text, source_us_impression_text, recommendation_text,
radiologist, study_indication,
-- Provenance
source_ultrasound_reports BOOLEAN,
source_us_nodules_tirads  BOOLEAN,  -- where us_1_date used as fallback key
nlp_backfill_pending      BOOLEAN,
extracted_at, build_script
```

For patients not in `ultrasound_reports` but in `us_nodules_tirads`, emit a shell row keyed on `(research_id, md5(research_id||'|'||us_1_date), us_1_date)` with all measurement columns NULL and `nlp_backfill_pending = TRUE`.

### Phase 5 — Build `canonical_us_lymph_node_v2` (NEW) + LLM pass

Create `scripts/<N+5>_canonical_us_lymph_node_v2.py`. **Separate LLM pass** per decision 3 — do not reuse `note_entities_llm_cervical_ln_detail`.

Two-step pass:

**Step 5a:** Empty shell build. Create the table schema and insert one placeholder row per (research_id, us_exam_id) that has a non-empty `ultrasound_reports.lymph_node_assessment`, with `nlp_backfill_pending = TRUE`. Also emit placeholder rows for us_nodules_tirads-only patients where the raw `us_1_impression` contains any LN keyword (`lymph|ln|node` as regex) — flag as `source_text_only = TRUE`.

Schema:

```
research_id, us_exam_id, exam_date,
ln_index_within_exam, ln_id,
laterality,            -- right | left | midline
neck_level,            -- Ia/Ib/IIa/IIb/III/IV/Va/Vb/VI/VII | NULL
region,                -- central | lateral_left | lateral_right | other
size_cm_max, short_axis_mm, long_axis_mm,
shape,                 -- round | oval
echogenicity,
hilum_preserved,       -- TRUE | FALSE | NULL
calcifications,
cystic_component,      -- TRUE | FALSE | NULL
vascularity_pattern,   -- hilar | peripheral | mixed | absent
extranodal_extension_on_us BOOLEAN,
suspicious_flag BOOLEAN,
suspicion_level,       -- benign | indeterminate | suspicious
biopsy_recommended BOOLEAN,
evidence_text,
source_note_type,
source_report_id,
llm_model,
confidence,
extracted_at,
nlp_backfill_pending BOOLEAN
```

**Step 5b:** LLM extraction. Use Qwen2.5-32B-Instruct-AWQ on Vast.ai H200 (same model/infra as Script 221 / `tirads_v2_integration`). System prompt must enumerate the entity schema (laterality, level, region, size, short/long axis, shape, echogenicity, hilum, calcifications, cystic, vascularity pattern, extranodal extension, suspicion, biopsy recommended, evidence_text). Input corpus: `ultrasound_reports.lymph_node_assessment` + `ultrasound_reports.source_us_impression` + `us_nodules_tirads.us_1_impression` for LN-keyword-positive reports only. Output JSON in the same `{entities: [...]}` shape as existing LLM tables. Parse and populate `canonical_us_lymph_node_v2`, flip `nlp_backfill_pending = FALSE` on covered rows.

Do Step 5a in this phase. **Step 5b (LLM run) is a follow-up after the v2 tables are in place** — write the parse script as a stub that reads from `note_entities_llm_us_lymph_node` (not yet created; the table name is fixed now so the parse is ready to run when the LLM output lands).

Also parallel: run the same LLM in a second pass over `ultrasound_reports.source_us_impression` for **thyroid gland parenchyma** entities (background echogenicity, heterogeneity, hashimoto_pattern, vascularity_overall, calcifications_parenchymal). Same stub pattern — write `note_entities_llm_us_thyroid_gland` parse script, don't run the extraction in this phase.

### Phase 6 — Rebuild rollups + CPM cutover

Create `scripts/<N+6>_canonical_us_exam_master_VIEW_v2.py` and `scripts/<N+7>_canonical_us_patient_master_VIEW_v2.py`.

`canonical_us_exam_master_VIEW_v2` — per (research_id, us_exam_id, exam_date): n_nodules_on_exam, largest_nodule_cm, second_largest_nodule_cm, bilateral_flag, isthmus_nodule_flag, worst/best TIRADS on exam, count per TR bucket, `has_gland_findings` BOOLEAN (joined to gland_v2), `has_ln_findings` BOOLEAN (joined to ln_v2), `n_abnormal_ln_this_exam` INTEGER, exam_rank_for_patient, is_preop_exam.

`canonical_us_patient_master_VIEW_v2` — per patient: n_us_exams, first/last US date, max TIRADS ever, bilateral ever, multifocal ever, preop_us_available, any suspicious LN ever (from ln_v2), goiter_ever (from gland_v2), first abnormal LN date.

CPM cutover — create `scripts/<N+8>_cpm_us_cutover_to_v2.py` that rewrites these CPM columns to read from v2 tables:
- `n_us_exams`, `n_us_nodules_total`, `dominant_nodule_size_cm`, `imaging_tirads_best`, `imaging_tirads_worst`, `imaging_tirads_category`, `imaging_laterality_rollup`, `imaging_ln_abnormal`, `max_tirads_ever`, `preop_tirads_best`, `preop_tirads_category`, `lnus_*` family, `n_us_with_ln_assessment`.
- Leave v1 CPM columns untouched (parallel). Add v2 columns with suffix `_v2`: e.g., `imaging_tirads_best_v2`. CPM keeps both columns until you're satisfied, then we drop v1 in a follow-up script.

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

3. Create wide gland view `views_readable.US_Thyroid_Gland_Wide_v2` and wide LN view `views_readable.US_Lymph_Nodes_Wide_v2` with analogous pivot logic (LN: us_1_ln_1_*, us_1_ln_2_* … up to 8 LNs × 5 exams).

### Verification checklist (run after each phase; fail loud)

Each script's last block runs these sanity checks and raises on failure:

1. Phase 2: `canonical_us_nodule_v2` row count ≥ 37,016 (legacy patients add rows — expect ~40,000–50,000).
2. Phase 2: every (research_id, us_exam_id, nodule_index_within_exam) is unique.
3. Phase 2: `nlp_backfill_pending = TRUE` count ≈ 5,000–12,000 (the 4,733 legacy patients × avg 1.5 nodules).
4. Phase 3: conflict queue row count is nonzero (if zero, the merge logic missed something).
5. Phase 4: `canonical_us_thyroid_gland_v2` has ≥6,793 rows (at least one per existing US report).
6. Phase 5: `canonical_us_lymph_node_v2` has ≥4,074 placeholder rows (one per ultrasound_reports row with ln_assessment).
7. Phase 6: exam master v2 has ≥13,347 rows (same patient-exam count as v1).
8. Phase 6: patient master v2 has ≥6,126 + 4,733 = ~10,800 rows (covers all US patients now).
9. Phase 7: registry has new rows for `canonical_us_nodule_v2`, `canonical_us_thyroid_gland_v2`, `canonical_us_lymph_node_v2`, `canonical_us_exam_master_VIEW_v2`, `canonical_us_patient_master_VIEW_v2`.

### Commits + push

One commit per phase, message format `feat(us-v2): Phase N — <title>`. After all phases pass, push to `main`. Run `ruff check scripts/` between every phase. Do NOT touch v1 tables (they stay live until the explicit drop script in a separate follow-up commit).

### Deliverable

At end of run, print a summary block to stdout:

```
US / TIRADS v2 build complete (parallel)
  canonical_us_nodule_v2          rows=XXXXX   patients=YYYY
  canonical_us_thyroid_gland_v2   rows=XXXXX   patients=YYYY
  canonical_us_lymph_node_v2      rows=XXXXX   patients=YYYY
  canonical_us_exam_master_VIEW_v2     rows=XXXXX   patients=YYYY
  canonical_us_patient_master_VIEW_v2  rows=XXXXX   patients=YYYY
  conflict_queue                  rows=XXXXX   high_priority=YYYY
  nlp_backfill_pending            nodule=XXXX  gland=XXXX  ln=XXXX
Archived to "Thyroid 2026 UPdated".us_legacy_20260421: 10 tables
v1 still live for CPM rollback. Drop v1 after CPM cutover verification.
Next step: LLM backfill run for pending rows (Phase 5b separate script).
```

---

## End of Cursor prompt
