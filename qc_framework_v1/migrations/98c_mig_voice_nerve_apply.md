# mig_98c — Apply Logan decisions: voice/nerve (rln_injury + vocal_cord_paralysis)

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck `.eras` account) **Engagement:** mig_98c v2 (post unified workbook `voice_nerve_unified__mig_98c_v2.xlsx`) **Executor:** `qc_framework_v1/scripts/apply_mig_98c_voice_nerve_decisions.py`**Applied:** 2026-04-28

## Architectural change

Per Logan's review feedback, rln_injury and vocal_cord_paralysis are two views of the same clinical category — mechanism (RLN nerve damage) vs functional outcome (VCP). Reviewed jointly per patient. Pre-existing vs operative attribution derived from phenotype `timing_days_post_surgery` rather than re-reviewed manually.

## Cohort & decisions

- 95 candidates from structured-signal selection (phenotype `confirmed_flag=TRUE` ∪ `extracted_rln_injury_refined_v2` ∪ mig_98a vocal_cord_paralysis keepers).
- Logan reviewed 89 candidates manually; Claude resolved the remaining 6 + 34 NEEDS_CONTEXT cases by date-based attribution (timing_days_post_surgery).

Final decisionnActionOPERATIVE19KEEP present in canonical; evidence=probableCANCER_RELATED3KEEP; evidence=definitiveINTENTIONAL_SACRIFICE2KEEP; evidence=definitivePOSTOP_LATE4KEEP; evidence=probablePREEXISTING8DELETE from complications + INSERT PMHPRIOR_OP3DELETE + INSERT PMHNOT_OPERATIVE26DELETE + INSERT PMH (timing_days &gt;5y typically)POSSIBLY_PRIOR_OP3DELETE + INSERT PMHNO (template FP)27DELETE only95

Plus 635 no_signal pts (mig_98c structured-signal exclusion): all rln_injury present rows DELETED (template FP by exclusion).

## Date-based attribution rules

- `timing_days < 0` or `timing_window='pre_surgery'` → PREEXISTING
- `timing_days 0–30` → OPERATIVE
- `timing_days 31–180` → POSTOP_LATE (still operative-attributable)
- `timing_days 181–365` → POSTOP_VERY_LATE
- `timing_days 1y–5y` → POSSIBLY_PRIOR_OP
- `timing_days >5y` → NOT_OPERATIVE (almost certainly not from index surgery)

## Phases (apply script)

1. **Snapshot** rln_injury + VCP rows pre-apply → `archive_pub_v1_0.canonical_complications_events_v1_pre98c_voice_nerve_<ts>` (2,280 rows)
2. **PMH INSERT** — 55 rows in `canonical_pmh_events_v1` (one per pmh_rid × ctype that existed):
   - 40 rln_injury PMH rows
   - 15 vocal_cord_paralysis PMH rows
   - source_table = `mig_98c_pmh_synthetic`, is_preexisting=TRUE, anchor_source = `mig_98c_classifier_logan_curated`
3. **DELETE** non-keep rln_injury rows: 1,086 present + 46 suspected
4. **DELETE** VCP rows for 67 non-keep candidates (97 rows)
5. **Standardize** evidence_strength on KEEP rows:
   - INTENTIONAL_SACRIFICE → definitive (7 rows)
   - CANCER_RELATED → definitive (12 rows)
   - OPERATIVE → probable (88 rows)
   - POSTOP_LATE → probable (20 rows)
6. **Rebuild** `canonical_complications_patient_rollup_v1` via Script 364 step_5
7. **Provenance** row `mig98c_voice_nerve_<UTCdate>`

## Post-state assertions (all green)

- rln_present_rows: 1,150 → **64** (21 keep pts; some KEEP candidates VCP-only)
- rln_suspected_rows: 66 → **20**
- vcp_present_rows: 236 → **139** (69 keep pts; 46 untouched mig_98a-only keepers
  - 23 mig_98c KEEP rln+vcp overlaps; 39 reclassified)
- canonical_pmh_events_v1 rln_injury rows: 0 → **40**
- canonical_pmh_events_v1 vocal_cord_paralysis rows: 0 → **15**
- chyle_leak rollup unchanged from mig_98b (1 / 3 / 5)

## Cross-migration carry-forwards

- **CF-COMPLICATIONS-REGISTRY-FLIP** — registry stays not_started until mig_99 (after all 8 sub-migs: 98a vcp ✓, 98b chyle ✓, 98c voice/nerve ✓, 98d seroma, 98e hematoma, 98f hypoparathyroidism, 98g hypocalcemia_clinical, 98h mortality)
- **CF-VCP-MIG98A-RECLASSIFIED** — 39 of mig_98a's 108 VCP keepers were reclassified in mig_98c (many moved to PMH for prior-surgery/pre-existing attribution). mig_98a's decisions remain valid for VCP existence; mig_98c added attribution layer.
- **CF-RLN-VCP-PHENOTYPE-MISS** — phenotype tier under-counted some clinical cases (similar to chyle_leak); note-text classifier + date-based attribution closed the gap. Worth a future phenotype-pipeline fix.

## Pattern reuse (for mig_98d-h)

- Date-based attribution via `timing_days_post_surgery` is generalizable
- Joint-review pattern for related complications (e.g., hypocalcemia + hypoparathyroidism are similarly two views of one entity)
- `canonical_pmh_events_v1` insert template with `is_preexisting=TRUE` + `source_table='mig_98c_pmh_synthetic'` + `anchor_source` for traceability
