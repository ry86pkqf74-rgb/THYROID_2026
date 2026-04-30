# mig_222 — Lane F multi-nodule under-explosion + deferred LLM absorption triage

**Run label:** `mig_222_multi_nodule_under_explosion_triage_20260430`  
**Prompt section:** `Lane F: Multi-nodule under-explosion + deferred LLM absorption triage`  
**Target:** `thyroid_canonical_publication_v1_0`

## Scope

Lane F covered two live manuscript-workspace queues:

| Queue | Rows | Distinct patients | Grain |
|---|---:|---:|---|
| `manuscript_workspace.qc_tir03_llm_candidates_v1` | 448 | 319 | US exam |
| `manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1` | 825 | 825 | Patient |

All queued items were triaged. The queue schemas contain exam/patient-level ambiguity signals only, not deterministic per-nodule LLM feature mappings. A live `tirads_llm_extracted_v2` table was not present in `information_schema.tables`, so bulk feature absorption would risk cross-nodule contamination.

## Decision policy

Conservative publication-safe policy:

- **Absorb:** 0 rows — no deterministic per-nodule LLM feature mapping was available.
- **Document as limitation:** all 448 candidate exams + 825 deferred patients.
- **Extractor-bug escalation:** 0 rows — this is a source/attribution limitation, not a proven extractor arithmetic bug.

## Database changes authored

Migration file: `qc_framework_v1/migrations/222_multi_nodule_under_explosion_triage_20260430.sql`

Planned/apply behavior:

1. Pre-snapshot affected `main.canonical_us_nodule_v2` rows and both queue tables into `"Thyroid 2026 UPdated".archive_pub_v1_0`.
2. Add `main.canonical_us_nodule_v2.multi_nodule_attribution_unresolved BOOLEAN DEFAULT FALSE`.
3. Create durable triage ledger `manuscript_workspace.us_multi_nodule_attribution_triage_v1` with one row per queued exam/patient item and priority features (`is_malignant`, FNA, path, molecular).
4. Flag affected canonical nodule rows (`10,570` distinct nodule rows preflight estimate).
5. Empty both QC queues after archival + triage-ledger creation.
6. Register the new canonical column and recompute `canonical_us_nodule_v2` signoff counts.
7. Insert provenance row in `manuscript_workspace.cpm_reconciliation_provenance_v1`.

## Preflight evidence

Artifacts: `exports/mig222_multi_nodule_triage_preflight/`

Key preflight outputs:

- Queue counts: 448 candidate exams / 825 deferred patients.
- Candidate rows with canonical nodule rows: all 448 candidate exams had matching `canonical_us_nodule_v2` rows.
- Deferred patients with canonical nodule rows: all 825 deferred patients had matching `canonical_us_nodule_v2` rows.
- Affected canonical rows to flag: 10,570 distinct nodule rows.
- `canonical_us_nodule_v2` pre-mig_222 signoff: 54 verified + 4 na = 58 cols.
- `mig_222` governance precheck: 0 existing col-registry rows and 0 provenance rows.

## Priority distribution from preflight

Candidate exam queue:

| is_malignant | has_fna | has_path | has_molecular | exams | patients |
|---|---|---|---|---:|---:|
| true | true | true | true | 70 | 47 |
| true | true | true | false | 109 | 78 |
| true | false | true | false | 22 | 17 |
| true | false | false | false | 1 | 1 |
| false | true | false | true | 32 | 24 |
| false | true | false | false | 182 | 124 |
| false | false | false | true | 2 | 2 |
| false | false | false | false | 30 | 26 |

Deferred patient queue:

| is_malignant | has_fna | has_path | has_molecular | patients |
|---|---|---|---|---:|
| true | true | true | true | 152 |
| true | true | true | false | 202 |
| true | true | false | true | 6 |
| true | true | false | false | 4 |
| true | false | true | true | 14 |
| true | false | true | false | 87 |
| true | false | false | true | 1 |
| true | false | false | false | 2 |
| false | true | false | true | 53 |
| false | true | false | false | 192 |
| false | false | false | true | 8 |
| false | false | false | false | 104 |

## Manuscript implication

For nodule-level TIRADS phenotype analyses, rows with `multi_nodule_attribution_unresolved=TRUE` should be excluded or handled in sensitivity analyses when exact per-nodule attribution matters. Patient-level/exam-level sensitivity analyses may retain these rows with explicit limitation language.

## Post-apply verification (live MotherDuck)

Post-apply artifacts: `exports/mig222_multi_nodule_triage_postapply/`

| Check | Result | Status |
|---|---:|---|
| Triage ledger total | 1,273 | PASS |
| Candidate exam rows categorized | 448 | PASS |
| Deferred patient rows categorized | 825 | PASS |
| `qc_tir03_llm_candidates_v1` remaining rows | 0 | PASS |
| `us_llm_absorption_deferred_multi_nodule_v1` remaining rows | 0 | PASS |
| `canonical_us_nodule_v2.multi_nodule_attribution_unresolved=TRUE` | 10,570 | PASS |
| `canonical_us_nodule_v2` signoff | 55 verified + 4 na = 59 cols | PASS |
| New col registry row | 1 (`batch_id=mig_222_multi_nodule_under_explosion_triage_20260430`) | PASS |
| Provenance row | 1 | PASS |
| 5-gate audit | 186 / 0 / 0 / 0 / 0 | PASS |
| Governance gap (§12) | 0 rows | PASS |
| Clinical date violations (§14) | 0 rows | PASS |

Priority-tier distribution is preserved in `exports/mig222_multi_nodule_triage_postapply/priority_tier.csv`.

