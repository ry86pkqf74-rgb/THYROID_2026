# Prompt 3 — Nuclear medicine LN gap triage (legacy worklist)

## Worklist definition
Matches `prompt3` legacy filter on `pub_legacy_source_20260416.nuclear_med` (node/aden substring), excluding any `research_id` already present in `pub_canonical.canonical_nucmed_lymph_node_v1`.

## Counts
- Rows triaged (distinct patients): **173**
- **Positive (genuine LN / nodal uptake language): 2**
- **Negative / non-LN interpretations: 171**
- **Ambiguous (no strong pos/neg rule): 0**

## Label distribution

| label | n |
|---|---:|
| `NEGATIVE_ADENOMA_NOT_LYMPH` | 151 |
| `NEGATIVE_PARATHYROID_THYROID_ADENOMA_LANGUAGE` | 12 |
| `NEGATIVE_SINGLE_NEGATION` | 2 |
| `POSITIVE_LN_FINDING` | 2 |
| `NEGATIVE_EXPLICIT` | 2 |
| `NEGATIVE_EXPLICIT_PET_MEDIASTINAL` | 1 |
| `NEGATIVE_INTRATHORACIC_ADENOMA_LANGUAGE` | 1 |
| `NEGATIVE_BENIGN_THYROID_ADENOMA_CONTEXT` | 1 |
| `NEGATIVE_MULTI_NEGATION` | 1 |

## Post-insert canonical checks (BQ)
Executed `bq query` using `studies/prompt3_nucmed_ln_gap_triage_20260514/insert_gap_positives.sql` (staging table + `MERGE` into canonical).
- `canonical_nucmed_lymph_node_v1` row total: **188** (prior **186** + **2** gap positives).
- Patients with legacy node/aden worklist **not** represented in canonical: **171** (all ruled **negative**/non-target by triage logic — no insert required).

## Acceptance note
The **173-patient gap is closed for coverage**: **2 POS** received canonical rows (`nlp_structuring_status='nlp_backfill_pending'` where fields could not be fully structured); **171 NEG** remain absent from canonical by design after documentation.

## Item B — LN master rollup mapping
78-column stewardship matrix: **`PROMPT3_ln_master_rollup_field_mapping.csv`** (generator: `gen_ln_master_mapping_csv.py`). Surfaces tumour-grain substitutes on `canonical_tumor_characteristics_v1` wherever CPM LN rollups are the patient-level rollup but not a literal column rename.

## Backfill artefacts
- Positive payload (JSON Lines): **`canonical_nucmed_ln_gap_positive.jsonl`**
- Per-patient labels: **`triage_per_patient.csv`**
- Idempotent **`MERGE` SQL**: **`insert_gap_positives.sql`**
