# Lymph Node Extraction — Remediation Summary

**Date:** 2026-03-27  
**Related audit:** `LYMPH_NODE_EXTRACTION_COMPLETENESS_AUDIT.md`  
**Verdict from audit:** **NOT COMPLETE** — structured synoptic/pathology fields dominate; full pathology narrative is not systematically scanned in production.

---

## What was added (this change set)

1. **`utils/pathology_ln_narrative_extract.py`**  
   Deterministic, non-LLM extraction from free text with explicit fields:
   - `ln_positive_path`, `ln_examined_path`, `ln_no_nodes_flag`
   - `ln_parse_status`, `ln_source_type`, evidence snippet, `pattern_id`  
   Covers additional patterns vs `HistologyDetailExtractor` alone (negation / “no nodal metastasis”, `0 of Y negative`, metastatic phrasing variants).

2. **`tests/test_pathology_ln_narrative_extract.py`**  
   Unit tests for the above (6 cases).

3. **`studies/proposal2_ete_staging/run_pathology_ln_audit.py`**  
   Offline audit against `manuscript_cohort_v1.csv`; writes  
   `studies/proposal2_ete_staging/outputs/pathology_ln_audit_summary.json`.  
   Optional `--md` samples pathology-like notes from MotherDuck when `MOTHERDUCK_TOKEN` is set (**does not** replace full-corpus materialization).

4. **Documentation**  
   Completeness audit markdown (this folder).

---

## What was *not* done (requires your approval / MotherDuck)

- **No** new MotherDuck tables were created in this session (token unavailable in the agent environment).
- **No** overwrite of `manuscript_cohort_v1` or publication bundles.
- **No** automatic rewire of `enrich_from_v2_extractors()` to run histology narrative at scale (would be long-running and needs resource planning).

---

## Recommended production integration (next steps)

1. **ETL table** (suggested name): `extracted_pathology_ln_narrative_v1`  
   - Grain: `(research_id, note_row_id)` or `(research_id, pathology_episode_id)` if available.  
   - Columns: those returned by `PathologyLNParseResult.to_row()` plus `note_type`, `char_length`.

2. **Join priority** (defense-in-depth):  
   - Primary: structured `path_synoptics` / `tumor_pathology` counts.  
   - Secondary: narrative extract **only when** structured NULL and `ln_parse_status IN ('parsed_pair', ...)`.  
   - Never silently overwrite non-null structured values; log conflicts.

3. **Wire extractors** (choose one):  
   - **A.** Batch job over `clinical_notes_long` where `note_type` indicates pathology/CAP report.  
   - **B.** Integrate `extract_pathology_ln_from_text` into `HistologyDetailExtractor` and include `HistologyDetailExtractor` in `enrich_from_v2_extractors` (performance concern: full note scan).

4. **Validation**  
   - Extend `scripts/29_validation_engine.py` (or manuscript gate) with: positive ≤ examined; N1 vs ln_positive reconciliation report; era-stratified missingness.

5. **Manuscript**  
   - After materialization, refresh exports via existing freeze scripts (`scripts/90_manuscript_freeze_rebuild.py` / `105` per your standard workflow) and update metric registry.

---

## Files touched

| Path | Role |
|------|------|
| `utils/pathology_ln_narrative_extract.py` | Narrative parse API |
| `tests/test_pathology_ln_narrative_extract.py` | Tests |
| `studies/proposal2_ete_staging/run_pathology_ln_audit.py` | Audit runner |
| `studies/proposal2_ete_staging/outputs/pathology_ln_audit_summary.json` | Generated metrics |
| `LYMPH_NODE_EXTRACTION_COMPLETENESS_AUDIT.md` | Full audit |
| `LYMPH_NODE_EXTRACTION_REMEDIATION_SUMMARY.md` | This file |

---

## Re-run

```bash
.venv/bin/python -m pytest tests/test_pathology_ln_narrative_extract.py -q
.venv/bin/python studies/proposal2_ete_staging/run_pathology_ln_audit.py
```
