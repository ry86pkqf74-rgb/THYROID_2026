# Reviewer Defense: How Were RAI Receipt and Dose Established?

## RAI Data Hierarchy

| Layer | Table | Scope | Role |
|-------|-------|-------|------|
| 1 | `rai_treatment_episode_v2` | 1,857 episodes, 862 patients | Canonical episode table (script 22) |
| 2 | `extracted_rai_validated_v1` | 862 episodes | Source-reliability-scored validation |
| 3 | `extracted_rai_dose_refined_v1` | 307 unique dose records | NLP + structured dose extraction |
| 4 | `val_rai_structural_coverage_v1` | 27 metrics | Formal coverage audit |

## Assertion Status Classification

Each RAI mention is classified by certainty:

| Status | Definition | Use in Analysis |
|--------|-----------|-----------------|
| `definite_received` | 0 patients | Would require nuclear medicine scan confirmation |
| `likely_received` | 35 patients | Documented dose + treatment context; used as "confirmed" |
| `planned` | Variable | Mentioned as planned, no completion evidence |
| `historical` | Variable | Referenced in history, not current treatment |
| `negated` | Variable | Explicitly not received |
| `ambiguous` | Variable | Insufficient context to classify |

Note: 0 patients meet the `definite_received` threshold because nuclear medicine scan reports are absent from the corpus. The 35 `likely_received` patients have dose documentation in clinical notes and serve as the confirmed RAI arm.

## RAI Dose Coverage

| Metric | Value |
|--------|-------|
| Episodes with dose | 761 of 1,857 (41.0%) |
| Pre-backfill coverage | 371 (20.0%) |
| Average dose (where available) | 141.8 mCi |

### Dose Sources

| Source | N Doses | Method |
|--------|---------|--------|
| Structured fields | 49 | Direct from institutional data |
| NLP-linked to episode | 145 | Extracted from notes, matched to episode |
| NLP-standalone | 113 | Extracted dose without episode linkage |

Script 76 Phase B backfill increased coverage from 20% to 41% by relaxing temporal matching windows and adding dose provenance tracking (`dose_source`, `dose_confidence` columns).

## Nuclear Medicine Limitation

**ZERO nuclear medicine reports exist in `clinical_notes_long`**. This is formally classified as a first-class structural limitation:

| Source Domain | Availability | Tier |
|---------------|-------------|------|
| Nuclear medicine reports | ABSENT | `first_class_structural_limitation` |
| Structured RAI orders | ABSENT | `first_class_structural_limitation` |
| Endocrine notes | PARTIAL | Mentions of RAI treatment |
| Discharge summaries | PARTIAL | Post-treatment documentation |
| Op notes | PARTIAL | Occasional RAI planning references |

All RAI data originates from secondary clinical documentation (endocrine notes, discharge summaries, medication records), NOT from nuclear medicine scan reports or structured treatment orders. This caps dose coverage at ~41% and prevents `definite_received` classification.

## ATA Response-to-Therapy

Among 862 patients with any RAI episode:

| Response | N | % |
|----------|---|---|
| Excellent | 82 | 9.5% |
| Indeterminate | 55 | 6.4% |
| Biochemical incomplete | 5 | 0.6% |
| Structural incomplete | 380 | 44.1% |
| Insufficient data | 340 | 39.4% |

Criteria: Tg < 0.2 ng/mL = excellent; 0.2–1.0 = indeterminate; > 1.0 without structural disease = biochemical incomplete; structural disease present = structural incomplete.

## Key References

- **Canonical episodes**: `rai_treatment_episode_v2` (MotherDuck, 1,857 rows)
- **Validated episodes**: `extracted_rai_validated_v1` (MotherDuck, 862 rows)
- **Dose table**: `extracted_rai_dose_refined_v1` (MotherDuck, 307 rows)
- **Structural coverage audit**: `val_rai_structural_coverage_v1` (MotherDuck, 27 rows)
- **Source limitation audit**: `val_rai_source_limitation_v1` (MotherDuck, 5 rows)
- **Response summary**: `vw_rai_response_summary` (MotherDuck, 5 rows)
- **RAI gap doc**: `docs/rai_structural_gap_maximization_20260313.md`
- **Script 76 Phase B**: `scripts/76_canonical_gap_closure.py` (RAI dose backfill)
