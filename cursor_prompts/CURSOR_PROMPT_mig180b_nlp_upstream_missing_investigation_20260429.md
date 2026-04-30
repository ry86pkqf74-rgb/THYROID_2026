# Cursor Prompt — mig_180b NLP UPSTREAM-MISSING family investigation (12 families / 38 cols)

**Date:** 2026-04-29 (very late evening)
**Lane:** mig_180b / nlp_upstream_missing_investigation
**Batch (proposed):** `mig_180b_nlp_upstream_missing_investigation_20260429`
**Predecessor:** mig_180 (CLOSED at `9736a14` — flipped 116 nlp_* cols; 12 families flagged with `CF-mig180-NLP-UPSTREAM-MISSING-<family>`)
**Posture:** Read-only investigation + SQL-only authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** `main.canonical_column_verification_registry_v1` (registry-only updates per family)

---

## Mission

mig_180 closed 116 nlp_* cols by stamping verified status, but 12 families couldn't have their upstream Tier 1 sources located during audit. mig_180b investigates each family's lineage, locates the actual upstream tables (likely under `note_entities_*` / `canonical_*` / `manuscript_workspace.*`), and either:
- (a) Replays the derivation and flips status → properly `verified` with `derivation_vs_<source>` methodology, OR
- (b) Confirms upstream is genuinely missing/placeholder → reclassifies status `verified` → `na` (Type-B placeholder) with appropriate methodology.

**38 cols across 12 families:**

| Family | Cols | Sample |
|---|---:|---|
| `nlp_funcoutcome_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_imaging_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_labs_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_ne_complications_*` | 2 | has_data, n_rows |
| `nlp_ne_genetics_*` | 2 | has_data, n_rows |
| `nlp_ne_medications_*` | 2 | has_data, n_rows |
| `nlp_ne_problemlist_*` | 2 | has_data, n_rows |
| `nlp_ne_staging_*` | 2 | has_data, n_rows |
| `nlp_physexam_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_ptdecision_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_radtx_*` | 4 | has_data, key_finding, n_entities, n_notes |
| `nlp_usnodule_*` | 4 | has_data, key_finding, n_entities, n_notes |

---

## Required scope

### §1 Per-family upstream lineage discovery

For each of the 12 families:
1. Search `information_schema.tables` for likely upstream candidates: `note_entities_<family>`, `note_entities_llm_<family>`, `canonical_<family>_*`, `nlp_<family>_*` in any schema.
2. Search PM build script (`scripts/132_*` or similar) for the actual derivation expression that originally populated these cols.
3. Search git log + grep `manuscript_workspace.*` for any `mig_<N>_*` build that materialized these cols.

For each family, output to `qc_framework_v1/reports/mig_180b_upstream_lineage_audit_20260429.md`:
- Family name
- Discovered upstream table(s) (or "GENUINELY MISSING")
- Derivation rule (verbatim or reconstructed)
- Cohort coverage in upstream (n_distinct_rids)
- Recommended action: (a) derivation_vs_<table> verify, (b) na reclass with `helper_<family>_pending_real_extraction`, (c) status quo with informational note

### §2 Re-derivation probe per family (read-only)

For families where upstream IS discovered:
```sql
SELECT
  COUNT(*) AS pm_total,
  COUNT(*) FILTER (WHERE pm.<col> IS DISTINCT FROM rederived.<col>) AS n_mismatches
FROM main.canonical_patient_master pm
LEFT JOIN (<rederivation CTE>) rederived USING (research_id);
```

Pass criterion: `n_mismatches = 0` → flip to `derivation_vs_<source>` methodology with proper batch_id.
Fail criterion: `n_mismatches > 0` → flag with `CF-mig180b-DERIVATION-DRIFT-<family>` and surface for Logan review.

### §3 Author apply SQL

`qc_framework_v1/migrations/180b_nlp_upstream_missing_investigation_apply_20260429.sql` with:
- §A pre-snapshot of affected registry rows
- §B per-family status flips (verified-with-proper-methodology OR verified→na reclass) — use CASE-based UPDATEs grouped by recommendation
- §C registry note appendix per family (closure note for `CF-mig180-NLP-UPSTREAM-MISSING-<family>`)
- §D resync `canonical_table_signoff_registry_v1` for canonical_patient_master
- §E post-state probes

### §4 Audit/report

`qc_framework_v1/reports/mig_180b_upstream_lineage_audit_20260429.md` with:
- 12-family lineage table (per §1)
- Per-family re-derivation probe results (per §2)
- Recommendations table: how many cols stay verified / how many reclass to na
- New CF tags opened (if any)

---

## Governance reminders

- Read-only audit + SQL authoring only. Cowork applies via Path C.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.
- No `BEGIN TRANSACTION;`/`COMMIT;`.

---

## Deliverables

1. `qc_framework_v1/migrations/180b_nlp_upstream_missing_investigation_apply_20260429.sql`
2. `qc_framework_v1/reports/mig_180b_upstream_lineage_audit_20260429.md`

Commit message: `qc: mig_180b NLP UPSTREAM-MISSING 12-family lineage investigation + apply authoring`

---

End of prompt.
