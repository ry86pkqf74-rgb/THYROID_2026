# Prompt 4 — Imaging canonical `nlp_backfill_pending` status (BigQuery)

**Project / dataset:** `thyroid-canonical-pub-2026.pub_canonical`  
**Evidence run:** validation queries executed via `bq` CLI (UTC 2026-05-14 session).

## Executive summary

**`nlp_backfill_pending = TRUE` is not an “unfinished batch job” bit in the sense of a single backfill script left mid-run.** For most of these tables it marks **rows whose structured columns were never populated by a parsing/LLM pass** (only `evidence_text` / heuristics / legacy hints). Achieving **pending = 0 everywhere** would require one of:

1. **Semantic policy change:** treat regex/heuristic/CPM-ingest rows as “complete” and set `nlp_backfill_pending = FALSE` without new NLP (loses the explicit “needs richer parse” signal), or  
2. **New work:** a BQ-native multimodal LN + US-nodule NLP merge (analogous to MotherDuck `377_absorb_llm_entities_into_v2.py` for nodules, plus an LN counterpart), run to completion against `pub_canonical`.

Until (2) exists and runs, **an explicit caveat is appropriate** rather than treating `pub_canonical` as NLP-complete for these grains.

## Live counts (acceptance SQL)

```sql
SELECT 'us_nodule_v2' AS t, COUNTIF(nlp_backfill_pending) AS pending
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
UNION ALL SELECT 'ct_ln_v1', COUNTIF(nlp_backfill_pending)
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_ct_lymph_node_v1`
UNION ALL SELECT 'mri_ln_v1', COUNTIF(nlp_backfill_pending)
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_mri_lymph_node_v1`
UNION ALL SELECT 'nucmed_ln_v1', COUNTIF(nlp_backfill_pending)
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_nucmed_lymph_node_v1`
UNION ALL SELECT 'us_ln_v2', COUNTIF(nlp_backfill_pending)
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_lymph_node_v2`;
```

| Table (`t`)   | `pending` | `total_rows` | Notes |
|---------------|-----------|--------------|--------|
| us_nodule_v2  | **2,061** | 37,579       | Matches prior expectation; see §US nodule |
| ct_ln_v1      | **6,665** | 6,665        | **100%** of rows pending (not 1,296 — likely stale prior snapshot) |
| mri_ln_v1     | **306**   | 306          | **100%** pending |
| nucmed_ln_v1  | **188**   | 188          | **100%** pending |
| us_ln_v2      | **7,694** | 7,702        | Only **8** rows not pending |

## Per-table interpretation (repo + data)

### `canonical_us_nodule_v2` (2,061)

- **Driver:** all 2,061 pending rows have `source_base = source_tirads_v2 = source_tirads_llm = source_us_nodules_tirads = FALSE` (no v2 source flags set).
- **Intended resolution path (MotherDuck):** Script **362** defines `nlp_backfill_pending` when structured sources do not cover the row; Script **376/377** absorb LLM entities and would clear pending when `source_tirads_llm` (or other sources) becomes true.
- **Gap:** BigQuery `pub_canonical` has **not** been through that absorption to closure for this orphan cohort — or rows were materialized without flag propagation expected by script 362.

### `canonical_ct_lymph_node_v1` / `canonical_mri_lymph_node_v1` / `canonical_nucmed_lymph_node_v1`

- **CT `llm_model` distribution:** `regex_heuristic_v3` (4,701), `regex_heuristic_v4_enriched` (1,910), `legacy_structured_mig329` (54). Regex/heuristic ingestion **by design** leaves structured fields sparse; **`nlp_backfill_pending` stays TRUE** until a richer parser populates them.
- **mig_329** (`scripts/mig_329_ct_mri_ln_legacy_positive_backfill_bq.py`) explicitly inserts minimal legacy-positive rows with **`nlp_backfill_pending = TRUE`** (“hints only”).
- **NucMed:** Prompt 3 triage noted structured gap positives can land with `nlp_struct_status` / pending semantics; same class of issue.

### `canonical_us_lymph_node_v2` (7,694 / 7,702)

- MotherDuck builder **364b** documents **“NO LLM in this pass”** and sets `nlp_backfill_pending = TRUE` for ultrasound-report and impression-derived rows; only the **CPM lnus*** slice uses `FALSE` (observed **8** rows with `source_note_type = cpm_lnus` in BQ).
- BQ currently shows **627 `cpm_lnus_v3`**, **274 `cpm_lnus_v2`**, **6,793 `ultrasound_report`** — **all pending = TRUE** except the 8 `cpm_lnus` rows. This matches “evidence_text ingested, structured LN features not NLP-filled.”

## Recommended caveat language (Methods / Limitations)

> Multimodal cervical lymph-node canonical tables (`canonical_*_lymph_node_v1` / `canonical_us_lymph_node_v2`) and a subset of ultrasound nodule rows retain `nlp_backfill_pending = TRUE` in `pub_canonical`. The flag indicates that **per-node structured features were not fully resolved** from imaging narratives (many rows were created from regex/heuristic extraction, CPM-derived LNUS summaries, or legacy gap-closure inserts with retained `evidence_text`). Analyses that require complete laterality, level, size, or suspicion fields should filter or join accordingly or use rows with `nlp_backfill_pending IS NOT TRUE`. Full NLP structuring was not run to completion in BigQuery as of the imaging-freeze checkpoint.

## Acceptance vs user target

- **User target:** `pending = 0` everywhere **or** accepted caveat.  
- **This document:** satisfies **caveat path** with **live per-table counts** and **root cause**.  
- **Note on CT count drift:** If an earlier report cited **1,296** CT pending rows, current `pub_canonical` has **6,665** rows **all** flagged pending — reconcile against snapshot date or partial-table query before relying on historical numbers.
