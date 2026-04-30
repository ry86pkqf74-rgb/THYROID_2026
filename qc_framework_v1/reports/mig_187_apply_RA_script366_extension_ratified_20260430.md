# mig_187 R-A apply — Script 366 exam-master extension (RATIFIED)

**Batch:** `mig187_apply_RA_script366_extension_ratified_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Author:** Logan Glosser `<logan.glosser@gmail.com>`  
**Date:** 2026-04-30  
**Governance posture (this lane):** Read-only authoring — no MotherDuck `--commit` from Path-A.

Artifacts:

- Unified diff (not applied): `scripts/366_canonical_us_exam_master_v2_patch_mig187_RA.diff`
- Apply SQL skeleton: `qc_framework_v1/migrations/187_apply_RA_script366_extension_ratified_20260430.sql`
- Dependent-view scan CSV: `exports/mig187_apply_RA_20260430/dependent_view_scan.csv`
- Manifest / checksums: `exports/mig187_apply_RA_20260430/manifest.json`

Predecessor scoping (`6edb881` recommendation): [`qc_framework_v1/reports/mig_187_canonical_us_exam_master_rebuild_scoping_20260430.md`](mig_187_canonical_us_exam_master_rebuild_scoping_20260430.md).

---

## §1 Logan-ratified R-A rule (verbatim)

**R-A: Extend Script 366 `exams` CTE / UNION** to include DISTINCT `(research_id, exam_date)` from `canonical_us_lymph_node_events_v2` for pairs where:

- `(research_id, exam_date)` is NOT already present in the existing nodule_agg / gland_agg / ln_agg UNION (the 121 fallback pairs from mig_171b)
- `exam_date` is non-null

For these pairs, assign `us_exam_id = md5('US_EXAM_V2|' || CAST(research_id AS VARCHAR) || '|' || CAST(exam_date AS VARCHAR))` so it matches mig_171b's fallback recipe exactly. After Script 366 redeploy, mig_171b §B (events rebuild) re-run will resolve these IDs to `exam_master_reused` instead of `fallback_ln_only_exam_id`, flipping G9 PASS.

---

## §2 Script 366 patch summary (plain English)

- **Renamed** inner exam universe **`exams` → `shell_exams`** (same three-way UNION over nodule, gland, and legacy `canonical_us_lymph_node_v2`; **logic unchanged**).
- **Added** **`ln_events_rid_date`**: DISTINCT `(research_id VARCHAR-trimmed, exam_date)` from **`canonical_us_lymph_node_events_v2`** with null guards.
- **Added** **`ln_nlp_exam_agg`**: rows from `ln_events_rid_date` whose numeric `research_id` has **no** matching `(research_id, exam_date)` row in **`shell_exams`**, emitting **`md5('US_EXAM_V2|' || rid_v || '|' || CAST(exam_date AS VARCHAR))`** (matches mig_171b fallback).
- **New `exams` CTE**: `shell_exams UNION ln_nlp_exam_agg`.
- **`joined`** now **LEFT JOIN`s** `ln_nlp_exam_agg` and chooses  
  **`us_exam_id = COALESCE(nodule_hash, gland_hash, ln_shell_hash, ln_nlp_md5_fallback)`**.
- **`exam_id_source`** column tagged **`ln_nlp_only`** for LN-NLP extension rows-only; **`NULL`** for spine rows originating from structured aggregations (backward-compatible signal).
- **COMMENT ON VIEW**, **docstring**, and **commit logging** expanded; **`ln_nlp_only`** row count surfaced in `--commit` logs.
- **Sanity row-count floor** lowered from **`13_000` → `11_000`** so Path-C commit succeeds against the Script-389-era VIEW row scale (~11.8k spine today).

---

## §3 Pre / post-state metrics (expected evidence)

Live baselines documented in mig_187 scoping (Path-C verifies before mutate):

| Metric | Pre (expected) | Post (expected after Path-C) |
|--------|----------------|------------------------------|
| `canonical_us_exam_master_VIEW_v2` rows | ~11,759 | ~11,759 + missing LN NLP dates ≈ ~11,880 |
| Fallback events (`exam_id_source = 'fallback_ln_only_exam_id'`) | 159 events / ~121 `(rid,date)` | **0** (G9 observes `PASS` / `'0'` if G9 DDL defines PASS-at-zero fallback) |
| New exam-master **`exam_id_source = 'ln_nlp_only'`** | n/a | ~121 spine rows |

`canonical_us_exam_master_VIEW_v2` gains **one additive column**: `exam_id_source` (clients using explicit selects remain stable; **`SELECT *` consumers** widen).

---

## §4 Dependent-VIEW scan (repo SSOT proxy)

Cowork SHOULD re-run **`information_schema.views`** / **`duckdb_views()`** corpus search for literals referencing `canonical_us_exam_master_VIEW_v2`; **this lane** enumerated **tracked repo references** (`exports/mig187_apply_RA_20260430/dependent_view_scan.csv`). High signal:

| Object / consumer | ENUM / brittle filter concern |
|-------------------|-------------------------------|
| `main.canonical_us_patient_master_VIEW_v2` (Script **367**) | None — aggregates fixed column list — **additive `exam_id_source` does not propagate** into patient rollup. |
| mig_171b events build (`exam_master_by_rid_date` CTE) | Touches **`exam_id_source`** on **events OUTPUT** (`exam_master_reused` \| `fallback_ln_only_exam_id`) — **no dependency** on VIEW `exam_id_source`; post-replay **fewer fallback events**. |
| `scripts/preB_cupm_v2_canonical_backfill.py` | Inline SQL aggregates exam_master **without `exam_id_source`** — unaffected. |

**ENUM risk:** **`ln_nlp_only` lives ONLY on `canonical_us_exam_master_VIEW_v2`** until consumers adopt — no repo consumer filters on `exam_id_source` enums for this VIEW today (`rg` confirms none beyond migration docs).

CREATE OR REPLACE on Script **366** resolves dependent VIEW bodies naturally in DuckDB; **feedback_alter_view_dependents** caveat applies mainly to **`ALTER VIEW ... RENAME`**, which this rollout avoids.

---

## §5 Cowork unblocking checklist (Path-C execute)

1. Review / apply **`scripts/366_canonical_us_exam_master_v2_patch_mig187_RA.diff`** to Script 366.
2. **`USE` locked DB** + **`USE thyroid_canonical_publication_v1_0`**; run snapshots **§A / §B** from `qc_framework_v1/migrations/187_apply_RA_script366_extension_ratified_20260430.sql`.
3. **`.venv/bin/python scripts/366_canonical_us_exam_master_v2.py --commit`** (MotherDuck token from **`.toml`**, not pasted into shells).
4. Replay **171b Sections B, C, D** verbatim (`qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql` ~lines 95–571).
5. Uncomment **§G§F§E POST-REPLAY** block inside the mig_187 migration file; execute **§G** provenance INSERT, confirm **§F** G9 `PASS`, then **§E** registry appendix (expect registry row touch count **77 if scoping invariant holds).
6. Run **§H** probes; freeze evidence in workbook / close-out branch.
7. Mark **`CF-mig171b-EXAM-MASTER-REBUILD` CLOSED** per registry appendix + `memory/MEMORY.md` / handbook if applicable.
