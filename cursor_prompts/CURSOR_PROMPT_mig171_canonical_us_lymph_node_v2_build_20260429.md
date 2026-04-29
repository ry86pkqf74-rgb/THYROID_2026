# Cursor Prompt — mig_171 canonical_us_lymph_node_v2 BUILD (design + skeleton)

**Lane:** 60 / mig_171
**Batch_id:** `mig_171_canonical_us_lymph_node_v2_build_20260429`
**Generated:** 2026-04-29
**Type:** Tier-2 BUILD design + skeleton SQL + verification plan. **Logan ratifies before any apply.** No data writes from agent.

---

## §0 Why this lane exists

`CF-mig150-TP-UPSTREAM-NOT-IN-MAIN` is currently tagged on **9 PM cols** (Cowork live verified 2026-04-29). The CF reads roughly: "the PM column derives from a third-party / upstream extraction that is not currently materialized in `main` as a Tier-2 canonical, so the PM value is unsourced from a downstream-analyst perspective." The closure path is to BUILD `canonical_us_lymph_node_v2` as a real Tier-2 canonical (events grain + patient_rollup grain) and re-derive the 9 PM cols from it.

Cowork live probe (2026-04-29) confirmed:
- `main.canonical_us_lymph_node_v1` does **NOT exist** in the publication DB. There is no v1 to refactor — this is a fresh BUILD.
- 9 cols carry the CF tag in `canonical_column_verification_registry_v1`.

This lane is a **design + skeleton** lane, not an apply lane. The agent's deliverable is:
1. A design Markdown that decides events vs patient_rollup grain, source inventory, exam_id recipe, and column list.
2. A skeleton SQL that creates the table shells (no INSERT yet).
3. A verification plan that Logan can ratify before mig_171b builds and populates.

## §1 Governance posture

- Read-only against MotherDuck for source profiling (`query`, never `query_rw`).
- No schema mutations, no INSERTs, no registry writes.
- Output: design + skeleton SQL + verification plan + commented probe SQL. Cowork applies a future `mig_171b` after Logan ratifies the design.
- AGENTS-governance binding: this is a BUILD lane, the highest-stakes type. Two prior governance violations (mig_155, mig_165) — do **NOT** apply on MD.

## §2 Required source inventory (probe section)

Profile every candidate source. Live MD probes the agent must run + paste in the design doc:

```sql
-- §2a Existing US-related tables in main
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND table_name LIKE 'canonical_us%'
ORDER BY table_name;
-- Expect: canonical_us_exam_master_VIEW_v2, canonical_us_patient_master_VIEW_v2,
--   canonical_ultrasound_nodule_v1, etc. — confirm v1 table absent.

-- §2b PM cols carrying the CF
SELECT column_name, COALESCE(verification_status,'unknown') AS status, notes
FROM main.canonical_column_verification_registry_v1
WHERE schema_name='main' AND table_name='canonical_patient_master'
  AND notes ILIKE '%CF-mig150-TP-UPSTREAM-NOT-IN-MAIN%'
ORDER BY column_name;
-- Expect: 9 rows. Capture the 9 col names verbatim into the design doc.

-- §2c LN extraction sources
SELECT table_name FROM information_schema.tables
WHERE table_schema='main'
  AND (table_name ILIKE '%lymph_node%' OR table_name ILIKE '%_ln_%' OR table_name ILIKE 'note_entities_%ln%')
ORDER BY table_name;

-- §2d Path-malignant LN events shape (if any)
SELECT column_name FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_path_malignant_events_v1'
  AND (column_name ILIKE '%ln%' OR column_name ILIKE '%lymph%' OR column_name ILIKE '%nodal%')
ORDER BY column_name;

-- §2e clinical_notes_long LN-related extractions
SELECT column_name FROM information_schema.columns
WHERE table_schema='main' AND table_name='clinical_notes_long'
ORDER BY column_name LIMIT 200;
```

Agent must paste **observed** results into design doc, not just queries.

## §3 Design decisions the doc must answer

1. **Grain of events table** — per-LN per-exam? per-LN-per-side per-exam? per-exam summary? Recommend per-LN per-exam if source data supports it; fall back to per-side per-exam if not. Justify with row-count probe.
2. **exam_id recipe** — per `project_exam_id_portability.md`: hash recipe must be locked per modality. US v2 had a hash mismatch; do NOT reuse v1's recipe blindly. Propose a recipe and verify it joins to `canonical_us_exam_master_VIEW_v2` 1:1 before locking.
3. **Source priority** — ranked list of sources (clinical_notes_long extractions, note_entities_llm_*, path_malignant LN events). Justify ranking by coverage + recency.
4. **Patient_rollup definition** — for each per-LN col, what patient-level rollup does the manuscript want? max_size_mm, any_suspicious_present, etc.
5. **Column list** — every per-LN attribute the v2 should expose; cross-reference to the 9 PM CF cols.
6. **Cohort coverage probe** — how many of the 10,871 patients does this v2 reach? Probe live, document.
7. **Date type policy** — every clinical date col must be `DATE` per `feedback_clinical_dates_calendar_only.md`. Audit/provenance timestamps stay TIMESTAMP.

## §4 Skeleton SQL structure (do not INSERT)

```sql
-- §4a Events grain
CREATE TABLE IF NOT EXISTS main.canonical_us_lymph_node_events_v2 (
  research_id        VARCHAR     NOT NULL,
  us_exam_id         VARCHAR     NOT NULL,            -- hash recipe locked per §3.2
  ln_event_id        VARCHAR     PRIMARY KEY,         -- (rid, us_exam_id, ln_index, side) hash
  ln_index           INTEGER,
  side               VARCHAR,                          -- left/right/central/bilateral/unspecified
  level              VARCHAR,                          -- I-VII or 'unspecified'
  size_short_mm      DOUBLE,
  size_long_mm       DOUBLE,
  -- ... agent fills in based on source profile
  exam_date          DATE        NOT NULL,             -- DATE per clinical-dates rule
  source_table       VARCHAR     NOT NULL,
  source_row_id      VARCHAR,
  build_ts           TIMESTAMP   DEFAULT CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  build_migration    VARCHAR     DEFAULT 'mig_171b'
);

-- §4b Patient rollup grain (cohort-wide)
CREATE TABLE IF NOT EXISTS main.canonical_us_lymph_node_patient_rollup_v2 (
  research_id              VARCHAR PRIMARY KEY,
  has_us_ln_findings       BOOLEAN,
  n_us_ln_events           INTEGER,
  -- ... agent fills in patient-level rollup cols matching the 9 CF cols
  build_ts                 TIMESTAMP DEFAULT CAST(CURRENT_TIMESTAMP AS TIMESTAMP),
  build_migration          VARCHAR DEFAULT 'mig_171b'
);
```

Skeleton only — no INSERT. mig_171b will populate.

## §5 Verification plan (the doc must spec it)

For each col on the new tables, the verification plan must spec:
- Source-of-truth pre-compute (e.g., re-derive from upstream and mass-equivalence)
- Cohort coverage check (10,871 patients; this v2 covers X)
- Cross-validation against any verified overlapping canonical (e.g., path_malignant events LN attribs)
- Date type checks (DATE not TIMESTAMP for clinical dates)
- Exam-id portability check (every us_exam_id in v2 events must resolve in `canonical_us_exam_master_VIEW_v2`)
- Cohort-uniformity sweep on every BOOLEAN col (Type-A and Type-B both directions)

Plan should follow the **extraction-faithfulness pattern** (`feedback_extraction_faithfulness_llm_canonical.md`) where applicable.

## §6 Required CFs to open

- `CF-mig171-DESIGN-RATIFICATION-PENDING` (informational; closes when Logan ratifies the design)
- `CF-mig171-EXAM-ID-RECIPE-LOCK` (informational; documents the chosen recipe + provenance)
- One `CF-mig171-SOURCE-COVERAGE-<source>` per source ranked outside top-2 (documents the gap)

## §7 SQL files + Markdown structure

- `qc_framework_v1/migrations/171_canonical_us_lymph_node_v2_skeleton_20260429.sql` — skeleton CREATE TABLE only, with header notes; no INSERTs.
- `qc_framework_v1/reports/mig_171_canonical_us_lymph_node_v2_design_20260429.md` — design doc with all §3 decisions, §2 probe outputs pasted, §4 skeleton inlined, §5 verification plan.
- `qc_framework_v1/migrations/171_design_probes_20260429.sql` — commented probe SQL Logan can replay.

## §8 Git workflow

- Commit message: `qc: mig_171 canonical_us_lymph_node_v2 design + skeleton (read-only)`
- Files staged surgically:
  - `qc_framework_v1/migrations/171_canonical_us_lymph_node_v2_skeleton_20260429.sql`
  - `qc_framework_v1/migrations/171_design_probes_20260429.sql`
  - `qc_framework_v1/reports/mig_171_canonical_us_lymph_node_v2_design_20260429.md`
- Push to origin/main.
- Per `feedback_surgical_git_add.md`: never `git add -A`.

## §9 Out of scope

- Do NOT INSERT data. Skeleton CREATE TABLE only — no `CREATE TABLE AS SELECT`, no `INSERT INTO`.
- Do NOT register the new tables in `canonical_table_signoff_registry_v1` or `canonical_column_verification_registry_v1`. mig_171b registers when it builds.
- Do NOT modify the 9 PM cols. mig_171c (later) re-derives PM from v2 once it's verified.
- Do NOT touch `canonical_us_exam_master_VIEW_v2`, `canonical_ultrasound_nodule_v1`, or any other US objects.
- Do NOT use cross-DB sourcing (`feedback_no_cross_db_canonical_sourcing.md`) — every source must be live in `main`.
- Do NOT propose Tier-2 builds for other LN-adjacent domains (lateral_levels_v10, ene_levels_v9, etc.) in this lane — mig_174 covers their parser.

## §10 Apply governance

This lane authors design + skeleton **only**. Logan ratifies the design before mig_171b is authored. mig_171b will:
1. Pre-snapshot any tables we touch
2. INSERT data into events + rollup
3. Register in signoff + column registries
4. Verify per the §5 plan
5. Open `mig_171c` to re-derive the 9 PM cols and close `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN`

Per AGENTS governance: agent ships SQL + design only. Cowork applies after Logan ratifies. **No `query_rw` calls anywhere in this lane.**
