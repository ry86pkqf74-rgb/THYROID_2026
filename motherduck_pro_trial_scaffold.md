# MotherDuck Pro Trial — 4-Item Scaffold

**Queued for execution AFTER 363 close-out** (CPM feeder repoint + cascade strip both committed; close-out package signed).

**Trial start:** _________________ (fill in when you click "Start Trial")
**Trial end:**   _________________ (start + 24hr)
**Decision-by:** _________________ (start + 22hr — gives buffer to cancel)

**Execution order:** Item 1 → Item 2 → Item 3 in parallel with Item 4 (which runs throughout). All four should complete in ~3 hrs of focused work.

---

## Item 1 — Build "363 Invasion Canonical — Sign-Off Review" Dive

**Goal:** publication-ready interactive QA artifact for the 363 build. Becomes the template for every future Tier-2 close-out Dive (364, 365, 366, 367).

**Why this is the headliner:** Dives is THE Pro feature. If you build one good Dive today, you have a tangible Pro deliverable to evaluate against the subscription cost. Same workflow that a clinical reviewer or co-author could use to sign off on the canonical without you running queries for them.

**Estimated time:** 90-120 min

**Prereqs:**
- 363 closed at SHA: ___________
- All 16 QA gates green
- `canonical_invasion_events_v1` and `canonical_invasion_patient_rollup_v1` live in `main`

**Dive cells (in order — paste each into a fresh cell in the MotherDuck Dives UI):**

### Cell 1 — Markdown header

```markdown
# Script 363 — Invasion Canonical Sign-Off Review

**Build SHA:** [fill in]
**Build timestamp:** [fill in]
**Reviewer:** Logan Glosser (LGLOSSE@emory.edu)

This Dive surfaces the verification queries used at CHECKPOINT 1 sign-off.
Re-run any cell to confirm the canonical state. All queries are read-only.

## Tables under review
- `main.canonical_invasion_events_v1` — 51,773 rows / 10,871 patients
- `main.canonical_invasion_patient_rollup_v1` — 10,871 rows × 30 cross-modal BOOL flags

## What's in this Dive
1. Build metadata + Pattern 9 compliance check
2. Per-field rate breakdown (the headline numbers)
3. V/L split verification (vascular_microscopic vs lymphatic_microscopic vs both)
4. Finding_status distribution (confirms the corrected LLM ladder)
5. Cross-modal flag rollup spot-check
6. Soft_tissue source breakdown (Watch #1)
7. The 14 residual unmapped LLM entity_types (Watch #2 — deferred to 367)
8. All 16 QA gate quick-rerun
```

### Cell 2 — Build metadata + Pattern 9 compliance

```sql
-- Build metadata + Pattern 9 (build_ts must be TIMESTAMP not TIMESTAMPTZ)
SELECT 
  table_name,
  column_name,
  data_type
FROM information_schema.columns
WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
  AND table_schema = 'main'
  AND table_name IN ('canonical_invasion_events_v1', 'canonical_invasion_patient_rollup_v1')
  AND column_name IN ('build_ts', 'build_script', 'extraction_run_id')
ORDER BY table_name, column_name;
-- Expected: build_ts data_type = 'TIMESTAMP' (NOT 'TIMESTAMP WITH TIME ZONE')
```

### Cell 3 — Per-field rate breakdown (the headline numbers)

```sql
SELECT 
  invasion_type,
  COUNT(DISTINCT research_id) AS n_patients_present_anywhere,
  ROUND(100.0 * COUNT(DISTINCT research_id) / 10871, 2) AS pct_cohort
FROM main.canonical_invasion_events_v1
WHERE finding_status = 'present'
GROUP BY invasion_type
ORDER BY n_patients_present_anywhere DESC;
-- Expected (post-fix v3-iter-2):
--   vascular_microscopic    1109   10.20%
--   gross_ete               1146   10.54%
--   capsular                 941    8.66%
--   lymphatic_microscopic    780    7.18%
--   soft_tissue              493    4.54%
--   microscopic_ete          279    2.57%
--   perineural               122    1.12%
--   esophageal                69    0.63%
--   tracheal                  14    0.13%
--   airway                     1    0.01%
--   local                      0   EXCISED
```

### Cell 4 — V/L split verification

```sql
WITH per_pt AS (
  SELECT research_id,
    BOOL_OR(invasion_type = 'vascular_microscopic'  AND finding_status = 'present') AS has_vasc,
    BOOL_OR(invasion_type = 'lymphatic_microscopic' AND finding_status = 'present') AS has_lymph
  FROM main.canonical_invasion_events_v1
  GROUP BY research_id
)
SELECT
  COUNT(*)                                                AS total_pts_with_v_or_l,
  SUM(CASE WHEN has_vasc AND NOT has_lymph THEN 1 END)    AS vasc_only,
  SUM(CASE WHEN has_lymph AND NOT has_vasc THEN 1 END)    AS lymph_only,
  SUM(CASE WHEN has_vasc AND has_lymph THEN 1 END)        AS both_v_and_l,
  ROUND(100.0 * SUM(CASE WHEN has_vasc THEN 1 END) / 10871, 2) AS pct_vasc,
  ROUND(100.0 * SUM(CASE WHEN has_lymph THEN 1 END) / 10871, 2) AS pct_lymph
FROM per_pt
WHERE has_vasc OR has_lymph;
-- Expected: pct_vasc ≈ 10.20, pct_lymph ≈ 7.18, both ≥ 655
```

### Cell 5 — Finding_status distribution (confirms LLM ladder fix)

```sql
SELECT 
  invasion_type,
  finding_status,
  COUNT(*) AS n_mentions,
  COUNT(DISTINCT research_id) AS n_patients
FROM main.canonical_invasion_events_v1
GROUP BY invasion_type, finding_status
ORDER BY invasion_type, 
  CASE finding_status 
    WHEN 'present' THEN 1 
    WHEN 'suspected' THEN 2 
    WHEN 'indeterminate' THEN 3 
    WHEN 'absent' THEN 4 
  END;
-- Sanity check: SUSPECTED bucket should be NON-ZERO across types
-- (zero suspected = ladder mis-ordered, "cannot be ruled out" got bucketed wrong)
-- Expected total suspected mentions ≈ 101 across all types
```

### Cell 6 — Cross-modal flag rollup spot-check

```sql
SELECT 
  COUNT(*) AS total_patients,
  SUM(any_vascular_microscopic_anywhere::INT)   AS vasc_anywhere,
  SUM(any_lymphatic_microscopic_anywhere::INT)  AS lymph_anywhere,
  SUM(any_capsular_anywhere::INT)               AS capsular_anywhere,
  SUM(any_perineural_anywhere::INT)             AS perineural_anywhere,
  SUM(any_soft_tissue_anywhere::INT)            AS soft_tissue_anywhere,
  SUM(any_gross_ete_anywhere::INT)              AS gross_ete_anywhere,
  SUM(any_microscopic_ete_anywhere::INT)        AS microscopic_ete_anywhere
FROM main.canonical_invasion_patient_rollup_v1;
-- Expected: vasc=1109, lymph=780, capsular=941, perineural=122, soft_tissue=493
-- (matches Cell 3 — rollup parity gate verifies this at build time)
```

### Cell 7 — Soft_tissue source breakdown (Watch #1)

```sql
-- Confirm soft_tissue isn't dominated by op_note alone
SELECT 
  source_modality || '/' || source_kind AS source,
  COUNT(*) AS n_mentions,
  COUNT(DISTINCT research_id) AS n_patients
FROM main.canonical_invasion_events_v1
WHERE invasion_type = 'soft_tissue' AND finding_status = 'present'
GROUP BY 1
ORDER BY n_mentions DESC;
-- Expected: synoptic_path/llm dominant (~95.9% of patients), op_note small minority
```

### Cell 8 — The 14 residual unmapped LLM entity_types (Watch #2)

```sql
-- These are deferred to Script 367 (LLM table cleanup)
-- All 14 are in the v3 EXCISE list — they're staging/mass-effect/general-histology, not invasion
WITH all_llm_entity_types AS (
  SELECT 
    'note_entities_llm_airway_invasion' AS source_table,
    json_extract_string(value, '$.entity_type') AS entity_type,
    research_id
  FROM main.note_entities_llm_airway_invasion,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
  
  UNION ALL
  
  SELECT 
    'note_entities_llm_vascular_invasion',
    json_extract_string(value, '$.entity_type'),
    research_id
  FROM main.note_entities_llm_vascular_invasion,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
)
SELECT 
  source_table,
  entity_type,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT research_id) AS n_patients
FROM all_llm_entity_types
WHERE entity_type IN (
  'tracheal_deviation','tracheal_narrowing','substernal_extension','esophageal_compression',
  'vascular_encasement','mass_effect','airway_compromise_grade','vocal_cord_imaging',
  'vascular_invasion_type','vessel_count','necrosis','mitotic_rate','ptnm_stage','dedifferentiation'
)
GROUP BY source_table, entity_type
ORDER BY source_table, n_rows DESC;
-- Expected: 14 rows, total ~17,193 mention rows
```

### Cell 9 — All 16 QA gates quick-rerun

```sql
WITH gate_results AS (
  SELECT 'events_rowcount_nonzero' AS gate, 
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1) > 0 AS pass,
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1)::TEXT AS detail
  UNION ALL SELECT 'rollup_parity_with_events',
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_invasion_events_v1) =
    (SELECT COUNT(*) FROM main.canonical_invasion_patient_rollup_v1),
    'events_distinct_pts == rollup_rows'
  UNION ALL SELECT 'local_invasion_type_extinct',
    NOT EXISTS (SELECT 1 FROM main.canonical_invasion_events_v1 WHERE invasion_type = 'local'),
    '"local" must NOT appear'
  UNION ALL SELECT 'no_cross_db_archive_sourcing',
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1 
     WHERE source_table LIKE 'archive_pub_v1_0.%') = 0,
    'archive sources forbidden'
  UNION ALL SELECT 'vl_split_vascular_min',
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_invasion_events_v1 
     WHERE invasion_type = 'vascular_microscopic' AND finding_status = 'present') >= 682,
    'vasc pts >= 682'
  UNION ALL SELECT 'vl_split_lymphatic_min',
    (SELECT COUNT(DISTINCT research_id) FROM main.canonical_invasion_events_v1 
     WHERE invasion_type = 'lymphatic_microscopic' AND finding_status = 'present') >= 780,
    'lymph pts >= 780 (ratcheted)'
  UNION ALL SELECT 'finding_status_distribution_sanity',
    (SELECT COUNT(*) FROM main.canonical_invasion_events_v1 WHERE finding_status = 'suspected') > 0,
    'suspected bucket must be populated'
)
SELECT 
  gate, 
  CASE WHEN pass THEN 'PASS' ELSE 'FAIL' END AS status,
  detail
FROM gate_results
ORDER BY status DESC, gate;
-- Add the 9 remaining gates following the same pattern (preservation_op_note_*, view_resolves_*, etc.)
```

**Save the Dive:**
- Use MCP tool `save_dive` with title: "363 Invasion Canonical — Sign-Off Review v1"
- Capture the returned `dive_id` and URL → log it in the eval doc (Item 4)

---

## Item 2 — 367 LLM Cleanup Scoping Probe (Pro compute stress test)

**Goal:** run the full-table UNNEST queries that have been deferred because they MAY OOM on free tier. If they run cleanly on Pro, that's a concrete data point for the trial verdict.

**Estimated time:** 30-45 min

**Pre-flight (cheap queries — confirm scope before the heavy probe):**

```sql
-- (1) Inventory all LLM tables
SELECT table_name, estimated_size 
FROM duckdb_tables()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'main' 
  AND table_name LIKE 'note_entities_llm_%'
ORDER BY estimated_size DESC;
```

```sql
-- (2) Row count per LLM table (should be cheap — metadata)
SELECT 
  table_name,
  estimated_size AS approx_row_count
FROM duckdb_tables()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'main' 
  AND table_name LIKE 'note_entities_llm_%'
ORDER BY estimated_size DESC;
```

```sql
-- (3) JSON shape probe — confirm result_json starts with '{"entities":' across tables
-- (some LLM tables use different JSON schemas — find them before UNNEST'ing)
SELECT 
  table_name,
  COUNT(*) AS rows_with_entities_shape
FROM (
  SELECT 'note_entities_llm_pathology' AS table_name 
  WHERE EXISTS (SELECT 1 FROM main.note_entities_llm_pathology 
                WHERE result_json LIKE '{"entities":%' LIMIT 1)
  -- repeat per table or generate dynamically
)
GROUP BY table_name;
```

**THE HEAVY QUERY — the actual Pro stress test:**

```sql
-- Full UNNEST across every note_entities_llm_* table
-- Surfaces every (table, entity_type) combination with row + patient counts
-- This is the query that's been deferred — it's been a worry on free tier
WITH all_llm AS (
  SELECT 'note_entities_llm_pathology' AS source_table,
         json_extract_string(value, '$.entity_type') AS entity_type,
         research_id
  FROM main.note_entities_llm_pathology,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
  
  UNION ALL
  
  SELECT 'note_entities_llm_synoptic_pathology_enrichment',
         json_extract_string(value, '$.entity_type'),
         research_id
  FROM main.note_entities_llm_synoptic_pathology_enrichment,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
  
  UNION ALL
  
  SELECT 'note_entities_llm_vascular_invasion',
         json_extract_string(value, '$.entity_type'),
         research_id
  FROM main.note_entities_llm_vascular_invasion,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
  
  UNION ALL
  
  SELECT 'note_entities_llm_airway_invasion',
         json_extract_string(value, '$.entity_type'),
         research_id
  FROM main.note_entities_llm_airway_invasion,
    UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS t(value)
  WHERE result_json LIKE '{"entities":%'
  
  -- ADD ALL OTHER note_entities_llm_* TABLES from Cell (1) inventory
)
SELECT 
  source_table,
  entity_type,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT research_id) AS n_patients
FROM all_llm
GROUP BY source_table, entity_type
ORDER BY source_table, n_rows DESC;
```

**Mapped vs unmapped triage:**

```sql
-- Cross-reference: which (source_table, entity_type) combos are already 
-- consumed by ANY canonical_*_v1 table vs orphaned
WITH llm_universe AS ([above CTE]),
     consumed AS (
       SELECT DISTINCT source_table, source_kind 
       FROM main.canonical_invasion_events_v1
       WHERE source_kind = 'llm'
       -- UNION with all OTHER canonical tables that have source_table cols
     )
SELECT 
  l.source_table,
  l.entity_type,
  l.n_patients,
  CASE WHEN c.source_table IS NOT NULL THEN 'MAPPED' ELSE 'UNMAPPED' END AS status
FROM llm_universe l
LEFT JOIN consumed c ON l.source_table = c.source_table
ORDER BY status DESC, l.n_patients DESC;
```

**Deliverable:** save output to `qa/qa_script_367_scoping_<TIMESTAMP>.md`. Sections:
- LLM table inventory (count, est size)
- All (table × entity_type) combos with row + patient counts
- MAPPED vs UNMAPPED breakdown
- Estimated cleanup scope: tables to drop, tables to migrate, entity_types to consolidate
- **Pro compute observation:** did the heavy UNNEST query complete? How long? Memory pressure? Error?

---

## Item 3 — Test Dive sharing end-to-end

**Goal:** validate the share workflow before you'd actually need it for a real co-investigator sign-off.

**Estimated time:** 15-20 min

**Steps:**
1. Use the Dive from Item 1 (363 Sign-Off Review)
2. Call `share_dive_data` MCP tool with your dive_id and a test recipient email
3. Test with: logan.glosser@gmail.com (your personal Gmail) as the secondary
4. Open the share URL from the secondary account / incognito window — verify access
5. Try editing as recipient (does Pro grant view-only or interactive?)
6. List all current shares: `list_shares` MCP tool — confirm the share is there
7. Revoke the share — confirm secondary loses access

**Questions to answer (write into Item 4 eval doc):**
- Does the recipient need their own Pro account, or does view-access work for free-tier accounts?
- Permission model: read-only vs. interactive vs. fork-and-modify?
- Can shared Dives be embedded (iframe) on a public page, or only via authenticated URL?
- Does the share survive Pro trial expiry, or does the share disappear when you downgrade?
- Audit trail: can you see who opened the Dive and when?

---

## Item 4 — Pro trial evaluation document (CONVERT / CANCEL / DEFER)

**Goal:** decision document filled in throughout the trial, finalized 1-2 hrs before trial expires.

**Estimated time:** 20-30 min total (mostly accumulated during Items 1-3)

**Save to:** `/Users/ros/THyroid 2026/motherduck_pro_trial_verdict.md`

**Template:**

```markdown
# MotherDuck Pro Trial — Verdict

**Trial period:** [start datetime] to [end datetime]
**Decision:** [ CONVERT / CANCEL / DEFER ]
**Decision rationale:** [one paragraph]

---

## What I used Pro for

### Item 1 — Dive ("363 Invasion Canonical — Sign-Off Review")
- **Built:** [yes/no]
- **Cells:** [N total]
- **Dive URL:** [paste]
- **Verdict:** [does this replace something I'd otherwise build manually? Y/N. ROI:]

### Item 2 — 367 scoping probe (compute stress test)
- **Heavy UNNEST query:** [completed / OOM / timed out]
- **Wall time:** [seconds]
- **Memory pressure observed:** [Y/N — any spill warnings]
- **Verdict:** [is Pro compute meaningfully different from free? Y/N]

### Item 3 — Sharing
- **Share created:** [yes/no]
- **Recipient access worked:** [yes/no]
- **Recipient needed own Pro account:** [yes/no]
- **Permission model:** [read-only / interactive / both]
- **Verdict:** [does this enable real-world collaborator workflows? Y/N]

---

## Pro headroom observed
- **Compute ceiling:** [estimate vs free tier]
- **Storage:** [used GB / quota]
- **Concurrency:** [observation]
- **Sharing seats:** [N available]

## Pro features I'd actually use ongoing
- [list — be specific, name features by feature, not just "more compute"]

## Pro features I would NOT use
- [list — features that don't apply to my workflow]

## Cost vs alternatives
- **MotherDuck Pro:** $[X]/month (capture exact pricing from billing page)
- **Alternative 1: stay on free tier + dbt Core + GX** — $0/mo, but no Dives, no sharing
- **Alternative 2: self-host DuckDB + custom Streamlit dashboard** — $0/mo + dev time
- **Alternative 3: Hex (notebook-as-a-service)** — $[Y]/month, less DB-native

## Verdict rationale
[paragraph explaining the call. Anchor on: was there a specific Pro feature
I used that I would CONTINUE to use? If "I built one Dive and never opened
it again," that's a CANCEL signal. If "I'm already planning Dives for 364-367
close-outs and want to share one with [collaborator]," that's a CONVERT signal.]

## Action items post-decision
- [ ] If CONVERT: set up budget tracking, add MotherDuck billing alert
- [ ] If CANCEL: export any Dives to local SQL files (Dives are Pro-only — they disappear on downgrade)
- [ ] If DEFER: note the specific Pro feature you'd want first, set a re-trial trigger
```

---

## Execution sequence (when 363 closes)

1. Confirm Cursor's 363 close-out package is signed → record final SHA
2. Click "Start Pro Trial" in MotherDuck → record exact start timestamp here
3. **Item 1** (Dive) → 1.5-2 hrs
4. **Item 2** (compute probe) → 30-45 min — RUN IN PARALLEL with Item 1 if possible
5. **Item 3** (share test) → 15-20 min
6. **Item 4** (eval doc) → fill in incrementally during 1-3
7. Decision call by trial_start + 22 hr (gives 2 hr buffer to actually click cancel/convert)

**Total active time:** ~3 hrs spread across the trial day. Trial is 24 hr so you have plenty of headroom — don't spend the whole day, just hit each item once.

---

## Rules of engagement

- **Don't build anything you wouldn't keep.** If a Dive cell is "demo only," skip it. The Item 1 Dive should be something you'd actually re-open in 6 months.
- **Don't migrate the whole repo to dbt today.** That's a multi-week project that doesn't need Pro. Stay focused on Pro-specific features.
- **Document gotchas as they happen.** If sharing requires a workaround, write it in Item 4 immediately. Don't trust memory.
- **Cancel by default if uncertain.** Easier to retry the trial later than to forget and get charged.
