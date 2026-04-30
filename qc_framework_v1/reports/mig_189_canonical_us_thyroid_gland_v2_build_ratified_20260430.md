# mig_189 — `canonical_us_thyroid_gland_v2` events + rollup build (RATIFIED skeleton)

**Batch:** `mig189_canonical_us_thyroid_gland_v2_build_ratified_20260430`  
**Database:** `thyroid_canonical_publication_v1_0`  
**Author:** Logan Glosser `<logan.glosser@gmail.com>`  
**Date:** 2026-04-30  
**Ratification posture:** authoring-only in repo; Cowork executes Path-C APPLY on MotherDuck with RW token governance.

Primary SQL artifact: [`../migrations/189_canonical_us_thyroid_gland_v2_build_20260430.sql`](../migrations/189_canonical_us_thyroid_gland_v2_build_20260430.sql)  
Carrier-forward lane: **`CF-117-US-GLAND-PARENCHYMA`** (closes on registry UPDATE §E after green validation).

---

## §1 NLP-supplemental inventory (executor-owned)

Baseline shell table (literature / mig_117 lineage): **`canonical_us_thyroid_gland_v2`** ≈ **13,578 rows / 10,859 patients** — Cowork confirms against live DuckDB counts before apply.

Upstream NLP spine **must exist before §B**:

- Table: **`main.clinical_note_thyroid_us_extracted_v1`**
- Row shape patterned on **`clinical_note_ln_extracted_v1`** minus LN identifiers; modality gate identical to mig_171b (`evidence_source_modality = 'imaging'` plus `(ultrasound|sonogram|sonographic)` cue).
- **Gate §0d** (`COUNT DISTINCT` NLP `(research_id VARCHAR, parsed exam_date DATE)` absent from gland_v2 dated keys):

```sql
WITH gland_keys AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id, exam_date
  FROM main.canonical_us_thyroid_gland_v2
  WHERE exam_date IS NOT NULL
),
nlp_keys AS (
  SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id,
                  COALESCE(
                    CAST(TRY_STRPTIME(NULLIF(TRIM(entity_date), ''), '%m/%d/%Y') AS DATE),
                    TRY_CAST(NULLIF(TRIM(entity_date), '') AS DATE),
                    CAST(TRY_STRPTIME(NULLIF(TRIM(note_date), ''), '%m/%d/%Y') AS DATE),
                    TRY_CAST(NULLIF(TRIM(note_date), '') AS DATE)
                  ) AS exam_date
  FROM main.clinical_note_thyroid_us_extracted_v1
  WHERE extraction_status = 'ok'
    AND evidence_source_modality = 'imaging'
    AND regexp_matches(
          LOWER(COALESCE(evidence_text,'')||' '||COALESCE(entity_value,'')||' '||COALESCE(source_note_type,'')),
          '(ultrasound|sonogram|sonographic)'
        )
)
SELECT COUNT(*) AS n_pairs_nlp_only
FROM nlp_keys n
WHERE n.exam_date IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM gland_keys g
    WHERE g.research_id = n.research_id AND g.exam_date = n.exam_date
  );
```

- **Decision rule (ratified):** if `n_pairs_nlp_only` **\< 100**, **stop** lane — escalate to Logan; do **not** apply §B–§F DDL in production.
- Placeholder CSV: [`exports/mig189_us_gland_build_20260430/nlp_supplemental_inventory.csv`](../../exports/mig189_us_gland_build_20260430/nlp_supplemental_inventory.csv) ships `PENDING` until Path-C executes the probe and commits the CSV (optional git refresh).

Supporting column/registry probes documented in §0 of migration (information_schema inventory left to executor workbook).

---

## §2 Build SQL design rationale

- **Mirrors mig_171b**: `exam_master_by_rid_date` reuse of singleton `canonical_us_exam_master_VIEW_v2` rows → shared `MIN(us_exam_id)`; otherwise deterministic `md5('US_EXAM_V2|'||research_id||'|'||coalesce(cast exam_date,text,'NULL_EXAM'))` aligning LN/gland portability policy.
- **Structured spine:** every row `canonical_us_thyroid_gland_v2` projects to `exam_id_source_nominal ∈ {structured,fallback}`; null `exam_date` rows carry `exam_date_unavailable_fallback_flag=TRUE` (satisfies G3).
- **NLP supplemental:** gated US imaging rows map free-text cues into phenotype slots (`background_echogenicity_*`, heterogeneity/hashimoto/vascular/calc/goiter/pyramidal/substernal) with PHI-limited snippets.
- **Deduplication:** `ROW_NUMBER` over `(research_id, us_exam_id, coalesce(cast exam_date,text))`, priority NLP (1) over shell (2) — collapsing duplicate exam manifestations.
- **Does not mutate** canonical shell table (`canonical_us_thyroid_gland_v2` untouched).
- Snapshot: `canonical_us_thyroid_gland_v2_shell_pre_mig189_20260430`.

---

## §3 Ten-gate validation (expected post-state)

| Gate | ID | PASS expectation |
|---|---|---|
| G1 | `G1_rid_with_events_has_true_rollup` | All distinct gland-event patients show `has_us_gland_events=TRUE` on rollup. |
| G2 | `G2_events_row_count_ge_shell` | `COUNT(events)` ≥ `COUNT(shell)` additive baseline; WARN if dedup collapses materially. |
| G3 | `G3_exam_date_or_fallback_flag` | Zero rows with both `exam_date` NULL **and** `exam_date_unavailable_fallback_flag=FALSE`. |
| G4 | `G4_us_exam_id_deterministic_recipe` | Matches singleton EM reuse else md5 hash. |
| G5 | `G5_no_duplicate_natural_tuple_rid_exam` | No duplicate tuples on `(research_id, us_exam_id, exam_date sentinel)`. |
| G6 | `G6_rollup_schema_and_rid_parity` | Rollup rows=10,871 and `COUNT(has_us_gland_events)` = `DISTINCT event research_id`. |
| G7 | `G7_exam_source_distribution` | Structured >0; null-date ⇒ fallback; NLP zero ⇒ **WARN**. |
| G8 | `G8_nlp_supplemental_joins_exam_master` | Warn until mig_187 aligns EM rows for NLP dates; eventual PASS ideal. |
| G9 | `G9_fallback_exam_alignment_probe` | Dated non-fallback exams exist (`structured` ∪ `nlp_supplemental`). |
| G10 | `G10_measurement_plausibility` | Lobe dims ≤25 cm; isthmus ≤40 mm; total volume ≤600 mL sentinel guard. |

Output table: `main.val_mig189_canonical_us_thyroid_gland_build_v1`.

---

## §4 CF closure trace (registry)

`UPDATE … WHERE COALESCE(notes,'') ILIKE '%CF-117-US-GLAND-PARENCHYMA%'` on `main.canonical_column_verification_registry_v1` for `table_name='canonical_us_thyroid_gland_v2'` appends mig_189 closure string (distinct from prior mig_117 textual trace — events/rollup lineage now enumerated).

Cowork verifies **28 targeted rows** pre/post COUNT match probe in §E comment block.

---

## §5 Cowork Path-C unblock checklist

1. Confirm MotherDuck **`clinical_note_thyroid_us_extracted_v1`** exists + ingestion QC sign-off equivalent to LN extractor.
2. Execute **§0d** NLP-only pair gate; CSV export refresh under `exports/mig189_us_gland_build_20260430/`.
3. Apply migration top-to-bottom; watch first-time bind errors (table rename drift).
4. Run validation table SELECT; reconcile G7 WARN / G8 WARN against mig_187 exam-master state.
5. Optional: replay Script 366 if EM universe must absorb NLP supplemental dates (`mig171b`/mig187 precedence).
6. Git commit reproducible manifests + regenerated CSV after live probe replaces placeholders.
