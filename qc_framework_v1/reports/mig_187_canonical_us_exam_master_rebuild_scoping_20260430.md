# mig_187 — canonical_us_exam_master rebuild scoping  
**Batch (proposed):** `mig_187_canonical_us_exam_master_rebuild_20260430`  
**Database:** `thyroid_canonical_publication_v1_0`  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Carry-forward:** CF-mig171b-EXAM-MASTER-REBUILD  
**Date:** 2026-04-30  

Related prompt: [`cursor_prompts/CURSOR_PROMPT_mig187_exam_master_rebuild_20260430.md`](../../cursor_prompts/CURSOR_PROMPT_mig187_exam_master_rebuild_20260430.md).

---

## §0 Executive summary

- Live MotherDuck read-only probes (**2026-04-30**) confirm **159** lymph-node events with `exam_id_source = 'fallback_ln_only_exam_id'`, across **121** distinct `(research_id, exam_date)` pairs (**not** 159 distinct exam keys).
- Every fallback pair joins **nothing** on the exact `(research_id, exam_date)` against `canonical_us_exam_master_VIEW_v2`; there are **0** duplicate-exam rows for those dates causing ambiguity.
- **All 159** fallback events source from `clinical_note_ln_extracted_v1` (US-modality NLP gate); **none** originate from legacy `canonical_us_lymph_node_v2`.
- **`canonical_us_exam_master_VIEW_v2`** row count: **11,759** — matches prior probe (Cowork prompt).
- **`canonical_us_exam_master_VIEW_v2`** is **`CREATE VIEW` …** over nodule/gland/`canonical_us_lymph_node_v2` UNION (see DuckDB catalog). Code SSOT for deployment: **`scripts/366_canonical_us_exam_master_v2.py`** (`build_sql()`, `COMMENT_SQL`, `--commit`).
- Recommendation for resolution: prefer **R-A** (extend Script 366 exam universe using the same deterministic `md5('US_EXAM_V2|' || …)` identity so mig_171b §B rerun yields `exam_master_reused` + G9 PASS) unless governance chooses materialized supplemental rows (**R-B**) or documenting fallbacks (**R-C**).

**Ratification:** Do not apply DDL or `--commit` 366 until Logan selects R-A / R-B / R-C. Skeleton apply SQL: [`qc_framework_v1/migrations/187_canonical_us_exam_master_rebuild_TBD_20260430.sql`](../migrations/187_canonical_us_exam_master_rebuild_TBD_20260430.sql).

---

## §1 Fallback inventory (`exam_id_source = fallback_ln_only_exam_id`)

### §1.1 Counts

| Metric | Value |
|--------|------|
| Fallback **events** | 159 |
| Distinct `(research_id, exam_date)` | 121 |
| Events per pair (min / max / mean) | 1 / 4 / 1.31 |

### §1.2 Source table distribution

| `source_table` | Events |
|----------------|--------|
| `clinical_note_ln_extracted_v1` | 159 |
| `canonical_us_lymph_node_v2` | 0 |

### §1.3 Exam-master overlap at exact `(rid, exam_date)`

| Category | Pair count (of 121) |
|----------|---------------------|
| **No row** in `canonical_us_exam_master_VIEW_v2` | 121 |
| Exactly one EM row (`n_em_rows = 1`) | 0 |
| Multiple EM rows (ambiguous hash) | 0 |

### §1.4 Patient-level coverage among fallback pairs

| Category | Pair count |
|----------|------------|
| Patient has **zero** exams in EM (non-null dates) | 26 |
| Patient has ≥1 EM exam but **not** on LN fallback date | 95 |

### §1.5 SQL used (reproducible)

```sql
SELECT COUNT(*) AS fallback_events,
       COUNT(DISTINCT concat(research_id, '|', CAST(exam_date AS VARCHAR))) AS distinct_rid_date_keys
FROM main.canonical_us_lymph_node_events_v2
WHERE exam_id_source = 'fallback_ln_only_exam_id';
```

(Group-by profile matches prompt §1.)

---

## §2 Root cause — missing from exam_master

### §2.1 Mechanism from mig_171b

[`qc_framework_v1/migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql`](../migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql) derives `exam_master_by_rid_date` by grouping `canonical_us_exam_master_VIEW_v2` by `(research_id, exam_date)` and using `CASE WHEN em.n_exam_rows = 1 THEN em.us_exam_id …` else deterministic fallback `md5('US_EXAM_V2|' || research_id || '|' || CAST(exam_date AS VARCHAR))`.

If there is **no** EM row at that `(rid, date)`, `join` misses → fallback.

### §2.2 Why LN NLP dates disappear from VIEW

[`scripts/366_canonical_us_exam_master_v2.py`](../../scripts/366_canonical_us_exam_master_v2.py) constructs the exam VIEW from:

1. **`canonical_us_nodule_v2`**
2. **`canonical_us_thyroid_gland_v2`**
3. **`canonical_us_lymph_node_v2`** (legacy shell; `ln_agg`)

The universe is **`UNION` of distinct `(research_id, exam_date)`** from those three sources only.

mig_171b **explicitly mixes** NLP extractions (`clinical_note_ln_extracted_v1`, US modality filter) plus legacy shell rows — but **never backfills Shell v2 at those NLP dates.** Therefore LN-only cervical findings with a usable **parsed `exam_date`** can exist in **`canonical_us_lymph_node_events_v2`** while **still absent** from the Script 366 UNION → **exam_master exposes no spine row** → fallback.

Additional observed split:

- **26 / 121** pairs have patients with **no** EM rows at all — LN mention may reference US without structured nodule/gland rows.
- **95 / 121** pairs occur on calendar dates absent from structured US rollup for that patient (date drift vs clinical dictation vs report-only LN block).

Duplicate `us_exam_id` ambiguity (`n_em_rows > 1`) was **eliminated as a contributor** — **0 / 121** pairs show multiple EM rows on the LN date.

### §2.3 Not a date-format bug in mig_171b join

Evidence: **100% missing** EM row at paired `exam_date`; not “same day different CAST.” If casts split a single physical exam across two incompatible dates, we'd expect some pairs with duplicate EM ambiguity — **none observed.**

---

## §3 R-A / R-B / R-C comparison

| Rule | Approach | Pros | Cons / risk |
|------|-----------|------|-------------|
| **R-A** | Extend Script **366** exam universe (`exams` CTE UNION) — or materially seed LN rows — so each `(research_id, exam_date)` appearing in LN events yields **exactly one** EM row whose `us_exam_id` **matches mig_171b’s locked MD5**. Redeploy VIEW; rerun mig_171b §B. | **Clean joinability** downstream; clears G9; IDs align with deterministic recipe already in codebase. | Touches foundational US VIEW; regression tests on dependents (CPM, TIRADS) required. |
| **R-B** | Add **`canonical_us_exam_ln_only_supplement_v1`** (patch), UNION into VIEW build in 366. | Clear audit trail (“LN-only NLP supplement”); separates structured vs NLP-only dates. | New table lifecycle + ingestion rules; VIEW patch still needs maintenance. |
| **R-C** | Keep deterministic fallback forever; downgrade G9 to PASS-with-doc OR accept WARN; manuscript documents non-resolving IDs. | **Zero schema churn**; deterministic IDs still consistent for internal keys. | **Downstream linkage** to `canonical_us_exam_master_VIEW_v2` by `us_exam_id` stays **broken** for 159 Events; violates “exam ID portability” storyline unless explicitly scoped. |

**Suggested default:** **R-A**, implemented by extending **Script 366** with a UNION of distinct `(research_id, exam_date)` from **`canonical_us_lymph_node_events_v2`** (or narrower: rows that today would fallback), assigning `us_exam_id` equivalent to mig_171b’s MD5 fallback for those pairs until nodule/hash governance supplies a richer ID.

---

## §4 Manuscript implications

- **Linkage:** While `exam_id_source = fallback_ln_only_exam_id`, an inner join `events → canonical_us_exam_master_VIEW_v2 ON (research_id, exam_date)` **returns no row** despite `exam_date` being populated — the fallback `us_exam_id` exists only semantically inside events.
- **Within-US-LN aggregates:** unaffected if analysis stays inside `canonical_us_lymph_node_events_v2`; **cross-domain** joins (exam-level TIRADS, nodules, glands) silently drop LN-only NLP exams unless R-A/R-B resolves.
- **R-C** accepts that limitation with explicit caveat; R-A/R-B restores join surface at exam grain.

---

## §5 Sample ten fallback deterministic IDs — Logan spot-check

| `research_id` | `exam_date` | Deterministic fallback `md5('US_EXAM_V2|' \| rid \| '|' \| date)` |
|---------------|-------------|--------------------------------------|
| 10001 | 2023-09-19 | `b24f41056b75f2406c97b964c2fdb772` |
| 10077 | 2024-10-17 | `38d9a56c279dc41e5282ab61ba13ad14` |
| 10082 | 2024-01-02 | `b4d8a4080516e0e40c4742238c465560` |
| 10088 | 2024-10-08 | `2d779e73d06ad0663eac106fe66e73d9` |
| 10116 | 2023-04-03 | `1b0b943d952d43dfc7e0fbce2221d587` |
| 10147 | 2024-01-26 | `4ef32a758539d5ac7ff6b6e3ddf2f1aa` |
| 10251 | 2024-04-09 | `1557d00887349700ef4de31f185f230d` |
| 10285 | 2023-05-25 | `a7fa0100086653295cd0ff1da08d101a` |
| 10324 | 2023-10-24 | `9e38e31979bf61c4a141388ae0a14320` |
| 10342 | 2024-02-27 | `32acadefc2dda37acc97c0af608f1c96` |

(Re-verified vs live MD `md5(...)` aggregation query.)

---

## §6 Verification reference — mig_171b G9

[`171b_canonical_us_lymph_node_v2_build_20260429.sql`](../migrations/171b_canonical_us_lymph_node_v2_build_20260429.sql) check `G9_fallback_exam_ids_pending_rebuild` — **`PASS`** when fallback count **`0`**; **`WARN`** while **159**.

---

_End of mig_187 scoping report._
