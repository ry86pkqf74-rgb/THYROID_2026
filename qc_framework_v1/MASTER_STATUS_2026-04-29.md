# Master Status Snapshot — Thyroid Canonical Publication v1.0 Cleanup

**As of:** 2026-04-29 (UTC), tip of `origin/main` = `8305607`

## Headline numbers

| Metric | Value | Δ from session start (50 verified) |
|---|---|---|
| Verified canonicals | 87 / 175 | +37 |
| In-progress | 1 (`canonical_patient_master`) | +1 |
| Not-started | 87 / 175 | -38 |
| Total cols verified | ~3,200 / 5,502 | +~2,000 |
| 5-gate audit | 1=87, 2=0, 3=0, 4=0, 5=20 (all CFs) | clean |

## Lane closures this session (mig_116 → mig_130)

| Mig | Owner | Closure | Cols | Pattern |
|---|---|---|---|---|
| mig_116 | Cursor 9 | `canonical_molecular_genetics_v2` | 69+5 | source-family archive replay |
| mig_117 (us_v2) | Cursor 10 | 3 US v2 imaging tables | 53+4, 28+4, 23+6 | multi-source domain sanity |
| mig_117 (audit) | Cowork | provenance allowlist + 4 CF notes | (notes only) | audit refinement |
| mig_118 | Cowork | `canonical_operative_procedure_codes_v1` | 7+9 | hybrid pattern #9 |
| mig_119 | Cursor 11 | `canonical_frozen_section_patient_rollup_v1` | 187+1 | rollup rebuild + date CF |
| mig_120 | Cursor 12 | path_malignant + path_benign rollups | 14+3, 13+3 | 5-method pattern mix |
| mig_121 | Cursor 13 | ete_event_resolved + ete_inline_adjudication | 57+5, 9+3 | 6-method cross-table |
| mig_122 | Cursor 14 | `canonical_recurrence_v1` SHELL | 11+1 | cohort-wide degenerate (placeholder) |
| mig_123 | Cursor 15 | `canonical_survival_followup_v1` | 9+4 | derivation re-derivation |
| mig_124 | Cursor 16 | `canonical_molecular_genetics_from_notes_v2` | 17+11 | extraction-faithfulness |
| mig_125 | Cursor 17 | `canonical_recurrence_resolved_v1` | 16+3 | hybrid + cross-check fixes |
| mig_126 | Cursor 18 | meta-registry pair + 2 pre-reconcile fixes | 12+1, 12+2 | self-referential meta |
| mig_127 | Cowork | audit refinement (na filter) | (template only) | audit evolution |
| mig_128 | Cursor 20 | 5 tier3_extraction `error` cols | 5+65 | raw-mirror-exempt extension |
| mig_129 | Cursor 21 | 16 manuscript_workspace tier3_helpers | 43+~ | category-driven batch |
| mig_130 | Cursor 22 | `canonical_patient_master` OPERATIVE cluster | 233 cols (partial) | derivation + cluster pattern |

## Active in-flight

- **Lane 19 RESUME**: Script 203b RW rebuild + mig_123 (Logan approved Option 1 fixes; awaiting Cursor execute)
- **Lane 23**: `canonical_patient_master` PATHOLOGY cluster (~82 cols)
- **Lane 24**: `canonical_patient_master` LYMPH_NODE cluster (~80 cols)
- **Lane 25**: `canonical_patient_master` LABS cluster (~65 cols)

## Patient_master verification map (1,598 cols)

| Cluster | Cols | Status |
|---|---|---|
| Operative (op_*, surg*, ops_*, nsqip_*, etc.) | 233 | ✓ verified mig_130 |
| Pathology | ~82 | Lane 23 in flight |
| Lymph node | ~80 | Lane 24 in flight |
| Labs | ~65 | Lane 25 in flight |
| PMH+PSH | ~64 | Future Cursor lane |
| US imaging | ~44 | Future Cursor lane |
| RAI | ~36 | Future Cursor lane |
| Recurrence | ~30 | DEFERRED until Lane 19 RW lands |
| FNA | ~25 | Future Cursor lane |
| ETE | ~20 | Future Cursor lane |
| Survival | ~18 | Future Cursor lane |
| Medications | ~17 | Future Cursor lane |
| Molecular | ~7 | Future Cursor lane |
| Complications | ~5 | Future Cursor lane |
| Frozen section | ~3 | Future Cursor lane |
| Demographics | ~2 | Future Cursor lane |
| Other / residual | ~975 | Multi-lane effort (largest scope) |
| **Total** | **1,598** | **233 verified, 4 na, 1,361 not_started** |

## Open carry-forwards

### Date-retype batch (single future migration consolidates all)
20 gate-5-flagged cols across these CFs:
- CF-100-DATE-RETYPE (1: frozen_section_events.frozen_section_date)
- CF-117-DATE-RETYPE (4: molecular_v2.test_dates, ete_event_resolved.last_known_alive_date, etc.)
- CF-119-FROZEN-ROLLUP-DATE-RETYPE (14: frozen_section_patient_rollup_v1.frozen_*_date)
- CF-120/path-DATE-RETYPE (2: path_malignant_patient_rollup_v1.earliest/latest_malignant_path_date)
- CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE (1: closes when Lane 19 RW lands)
- CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE (2: canonical_patient_master.first_surgery_date + surg_first_date; SSOT is first_surgery_date_v2 DATE)

### Methodology / data-quality CFs (Lane 19 RESUME opens 4)
- CF-mig123-UPSTREAM-DATE-202-TYPO (2 patients: 12057, 10622)
- CF-mig123-NEGATIVE-TTR-9-PATIENTS
- CF-mig123-LEGACY-COMPLETION-CHECK-6674
- CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE (Logan-approved phase-2 union; within 1-2 sessions)

### Other long-tail
- CF-mig126-DATA-TYPE-DRIFT (9 rows)
- CF-mig126-ORDINAL-POSITION-DRIFT (119 rows)
- CF-118-UPSTREAM-DATE-FORMAT-DRIFT (note_entities_procedures.note_date now VARCHAR MM/DD/YYYY)
- CF-58-1/2/3 (parathyroid LLM extraction edge cases)

## What's left after current lanes finish

After Lanes 19/23/24/25 land (~+255 cols across patient_master + +CFs closed on recurrence_v1):

1. **Patient_master remaining clusters** (~1,100 cols across 13+ thematic clusters)
2. **75 manuscript_workspace tier3_helper tables** (Lane 21 closed 16/91)
3. **Raw mirror sources** (12 tables: path_synoptics 311 cols, manuscript_cohort_v1 151 cols, etc.) — sample-based verification
4. **Final batch date-retype migration** (consolidates CF-100/117/119/120/mig122/mig123/mig130)
5. **CF-mig124 path-canonical lineage** (phase-2 recurrence union)
6. **Archive cleanup** (419 snapshots in archive_pub_v1_0)
