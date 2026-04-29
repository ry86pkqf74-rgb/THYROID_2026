# mig_176 / mig_177 / mig_174 review package — 2026-04-29

Generated for Logan after the apply queue drain (gate1 165→169, PM 1441→1461 verified). All numbers verified live against MotherDuck.

---

## mig_176 — dominant_nodule_size_cm v1/v2 reconcile

### Findings (live re-verification, all 1,065 mismatches + 166 v2-only)

**Headline:** R2 (`COALESCE(v1, v2)`) is even stronger than the original report claimed. Nothing in the "unable to confirm" bucket — every patient's value source-replays exactly.

| Subset | n | Source replay |
|---|---:|---|
| Mismatch (v1 ≠ v2, both non-null) | 1,065 | v1 = `imaging_patient_summary_v1.dominant_nodule_size_cm` exactly for **1,065/1,065**; v2 = `canonical_us_nodule_v2.size_cm_max` exactly for **1,065/1,065** |
| v2-only (v1 NULL, v2 non-null) | 166 | **All 166** have v2 = `canonical_us_nodule_v2.size_cm_max` — `V2_MATCHES_US_SOURCE` |
| Extreme outliers (v2 > 10 cm) | 19 | All 19 replay from upstream — but v2's *upstream extraction* is implausible |

### Files

- **`mig176_v2_only_166pts.csv`** — full 166-pt list with v2 size, US source check, location, TIRADS, n_nodules, exam dates. **All show `V2_MATCHES_US_SOURCE`.** Under R2 these all use v2 (no v1 to compete); the source data is consistent.

- **`mig176_extreme_v2_implausible_19pts.csv`** — 19 patients where v2 > 10 cm. All replay from `canonical_us_nodule_v2.size_cm_max` cleanly, but the value is clinically implausible (e.g., rid 8931 v2=48 cm; rid 12152 v2=19 cm; rid 12141 v2=18 cm). The US note text from the report shows the actual largest nodule for these patients is in the 3-9 cm range — meaning **`canonical_us_nodule_v2`'s extraction logic is reading the thyroid lobe size or some aggregate as a "nodule size"** for these 19 patients. v1 (from `imaging_exam_master_v1.largest_nodule_cm`) reads the same source patients correctly (1.05–3.05 cm, matches the noted nodules). **R2's `COALESCE(v1, v2)` correctly picks v1 for all 19 — sidesteps the upstream v2 extraction bug.**

### Interpretation

R2 is the right call. The 166 v2-only set is fine — v2 was correctly extracted from US data when v1 was unavailable. The mismatches are all cases where v2's upstream extraction got confused (probably reading a non-nodule structure as a nodule); v1 is the cleaner source there.

**Action:** Author mig_176b apply lane (Cowork-direct, ~6 query_rw calls): add `dominant_nodule_size_cm_resolved DOUBLE` + `dominant_nodule_size_cm_resolution_rule VARCHAR` to canonical_patient_master, populate via `COALESCE(v1, v2)` + rule labels, register in column-registry, append CF closure note. Open `CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS` informational on the 19 outliers for future canonical_us_nodule_v2 build review.

---

## mig_177 — invasion family (LVI) ambiguity review

You ratified vasc=yes / capsular=yes / preserve grade-versioned=yes. LVI was the one where you wanted to see ambiguity.

### Headline finding for LVI

Two clear buckets explain almost all the drift:

**(1) PM=T but no event-present row — 2,614 patients** — the bulk of these are FALSE-POSITIVES. Stratified by source values:

| pm_lvi_grade | path_event lymphatic_invasion | path_event vascular_invasion | n_pts | Interpretation |
|---|---|---|---:|---|
| `x` | `x` | `x` | **2,369** (90.6%) | All source values are `x` (not assessed) — PM lit up despite no actual finding. **PM bug — flip to FALSE.** |
| `x` | `x` | `focal` / `extensive` / `present` | 138 | Source has VASCULAR invasion but NO lymphatic. PM aliased vasc → lvi somehow. **Flip to FALSE for LVI specifically.** |
| `null` | `x` | various | 46 | NULL pm grade with `x` lvi. Same PM mis-fire as above. |
| `indeterminate` / typos | `indeterminate|x` / `c/a|x` / `foacl` | various | ~25 | Genuine indeterminate / typo'd vocabulary. **Strict event semantics correctly says no `present` — these become FALSE in the rederive.** |
| `x` | `null` | `null` | 2 | Path_event has no rows; PM should be NULL not TRUE. |

**(2) Rollup-only positives — 120 patients** (rollup says T, events say not-present):

| Sub-disposition | n | What it means |
|---|---:|---|
| `NO_EVENT_ROWS` | **91** | No canonical_invasion_events_v1 row exists for lymphatic_microscopic, but rollup says TRUE. **All 91 have `path_vasc_raw` populated with `present`/`focal`/`extensive`** — rollup is computing `any_lymphatic_microscopic_anywhere` using VASCULAR data. Build error in `canonical_invasion_patient_rollup_v1`. |
| `ABSENT_ONLY` | 27 | Events explicitly say `absent`, but rollup says TRUE. Synoptic-path absent-only override path. **Strict event-present says FALSE — flip rollup.** |
| `INDETERMINATE_ONLY` | 2 | Events say `indeterminate`, rollup says TRUE. **Strict event-present says FALSE — flip rollup.** |

### Files

- **`mig177_lvi_2614_strata_summary.csv`** — 33 strata covering all 2,614 patients, with `(pm_lvi_grade, path_event_lvi_raw, path_event_vasc_raw, n_patients)`. The first row alone (`x / x / x → 2,369 patients`) makes the call obvious.

- **`mig177_lvi_120_rollup_only.csv`** — full 120 patients with sub_disposition + path_lvi_raw + path_vasc_raw. The `NO_EVENT_ROWS` 91 are particularly damning for the rollup — they have only vascular evidence yet rollup marked `any_lymphatic_microscopic_anywhere = TRUE`.

### Interpretation

**Strict event rederive is the right call (your "yes" stands).** The mig_177 report recommended `EXISTS canonical_invasion_events_v1 WHERE invasion_type='lymphatic_microscopic' AND finding_status='present'` — this gets it right because:
- The 2,369 source-`x` patients become FALSE (events correctly excluded them — no `present` row)
- The 91 rollup-only NO_EVENT_ROWS patients become FALSE (no event row of any kind to satisfy EXISTS)
- The 27 absent-only patients become FALSE
- Only the 780 patients with actual `present` rows in events stay TRUE

**Vocabulary CFs to open in mig_177b:**
- `CF-mig177-LVI-VOCAB-X-NOT-ASSESSED` — the `x` value is the dominant cause of false-positives; document semantics
- `CF-mig177-LVI-TYPO-foacl` — "foacl" → "focal", "extrensive" → "extensive", "indeterminent" → "indeterminate", "X" / "x" / `null`
- `CF-mig177-LVI-c_a` — `c/a` ("can't assess") values; treat same as `x`
- `CF-mig177-ROLLUP-VASC-ALIAS-LVI` — the 91 NO_EVENT_ROWS patients show `canonical_invasion_patient_rollup_v1.any_lymphatic_microscopic_anywhere` is computed using vascular signal; rollup needs rebuild after mig_177b

**You preserve legacy values per your direction:** `lvi_grade`, `lvi_ordinal_worst`, and `lvi_any_present_path` get renamed/audited rather than overwritten. New `lvi_event_present` BOOLEAN added.

---

## mig_174 — Option A ratified

Per-side BOOLEAN columns. Drafting Cursor prompt at `cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md`. Apply scope:

- `cnln_img_laterality VARCHAR` → preserved as legacy
- New cols (BOOLEAN, default FALSE): `cnln_img_left_present`, `cnln_img_right_present`, `cnml_img_central_present`, `cnln_img_bilateral_present`, `cnln_img_lateral_neck_present`
- Token-level parser: split on `;` → trim whitespace → lowercase → drop literal `'null'` → map to canonical token
- Same pattern recommended for `lateral_levels_v10`, `ene_levels_v9` if structurally similar (Cursor agent verifies first)

---

## Files in this package

```
exports/mig176_177_174_review_20260429/
├── README.md                                          (this file)
├── mig176_v2_only_166pts.csv                          (166 rows; all source-replayable)
├── mig176_extreme_v2_implausible_19pts.csv            (19 rows; v2>10cm; v1 from imaging is correct)
├── mig177_lvi_2614_strata_summary.csv                 (33 strata covering 2,614 PM=T/Event=F pts)
└── mig177_lvi_120_rollup_only.csv                     (120 rollup-only positives by sub-disposition)
```
