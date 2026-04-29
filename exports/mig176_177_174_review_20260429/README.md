# mig_176 / mig_177 / mig_174 review package — 2026-04-29 (UPDATED with raw-source findings)

Generated for Logan after the apply queue drain (gate1 165→169, PM 1441→1461 verified). All numbers verified live against MotherDuck. **Updated 2026-04-29 with critical findings from path_synoptics + raw US Reports review.**

---

## CRITICAL UPDATE (2026-04-29) — events extractor bug discovered

While pulling raw `path_synoptics` data for the 316 ambiguous LVI patients, a major upstream bug was found:

**`canonical_invasion_events_v1` is missing legitimate `lymphatic_microscopic` event rows for two CAP synoptic patterns:**

1. **Combined CAP field "Lymph-Vascular Invasion: Present"** — the CAP template's combined field is being parsed as `vascular_microscopic` ONLY, not also as `lymphatic_microscopic`. Captures both lymph + vasc but only emits vasc.
2. **Newer separate-field "Lymphatic Invasion: Present"** — when the synoptic explicitly has a separate `Lymphatic Invasion: Present` line, it's being missed entirely.

This means **the events table was the WRONG source of truth for LVI in 91+ rollup-only NO_EVENT_ROWS patients.** Logan ratified pausing mig_177b until events are rebuilt.

---

## mig_177 LVI — final disposition by bucket (Logan-ratified pause + per-bucket call)

| Bucket | n | Source pattern | Action |
|---|---:|---|---|
| **2,369** x/x/x | 2,369 | All sources are `'x'` (not assessed). No signal anywhere. | **FLIP TO FALSE** |
| **196** PM_T_EVF_signal | 196 | Synoptic explicitly says: `Angioinvasion: Present` + **`Lymphatic Invasion: Not identified`** (separate fields, newer CAP). PM aliased vasc → LVI. | **FLIP TO FALSE** (PM was wrong; events correctly didn't emit lymphatic) |
| **91** rollup_only_no_event_rows | 91 | Synoptic shows combined CAP `"Lymph-Vascular Invasion: Present"` OR newer separate `"Lymphatic Invasion: Present"`. Events extractor MISSED these. Rollup is right. | **KEEP TRUE** (events rebuild needed; rollup was right) |
| **27** rollup_only_absent | 27 | Mix: ~20 patients have combined-CAP "Lymph-Vascular: Present" or separate "Lymphatic: Present" (events incorrectly emitted absent); ~7 truly have lymphatic Not identified | **MOSTLY KEEP TRUE; 5-7 individual flips needed** — see [PART1 CSV column "cowork_recommendation"](mig177_lvi_316_full_evidence_PART1_rollup_only.csv) |
| **2** rollup_only_indeterminate | 2 | Synoptic says "indeterminate" / "cannot be determined" | **FLIP TO FALSE** (indeterminate is not "present") |

**Net effect on PM `lvi_any_present_path`:** ~3,392 TRUE → ~898 TRUE (780 events_present + 91 rollup-no-event + ~22-25 rollup_only_absent that have actual lymphatic+ in source + 2 multi-tumor edge cases). Drop of ~2,494 false-positives.

---

## Files in this package (UPDATED)

```
exports/mig176_177_174_review_20260429/
├── README.md                                                       (this file)
├── mig176_v2_only_166pts.csv                                       (166 rows; all V2_MATCHES_US_SOURCE)
├── mig176_extreme_v2_implausible_19pts.csv                         (19 rows; v2>10cm; v1 from imaging is correct)
├── mig176_us_reports_raw_19_extreme_outliers.csv                   (RAW US REPORTS for the 19; you can verify v2 inflation directly — e.g. rid 8931 max nodule is 19.1×14.6×15.0 mm = 1.91 cm, which IS v1; v2=48 is fabricated)
├── mig177_lvi_2614_strata_summary.csv                              (33 strata covering 2,614 PM=T/Event=F pts)
├── mig177_lvi_120_rollup_only.csv                                  (120 rollup-only positives — original)
├── mig177_lvi_316_full_evidence_PART1_rollup_only.csv              (NEW — all 120 rollup-only with LVI evidence + Cowork per-pt recommendation)
└── build_review_csvs.py                                            (reference script; not used due to .eras SSO gap)
```

The 196 PM_T_EVF_signal patients are NOT in a per-patient CSV (they all follow the same pattern: separate-field `Lymphatic Invasion: Not identified` → flip to FALSE). Sample evidence quoted in this README; available on request.

---

## mig_176 — verdict unchanged: R2 ratified (now with raw US data)

The [raw US Reports](mig176_us_reports_raw_19_extreme_outliers.csv) for the 19 extreme outliers confirm the v2 upstream extraction bug definitively:

- **rid 8931** (v1=1.91cm, v2=48cm): largest actual nodule across ALL 6 US exams is 19.1×14.6×15.0 mm = **1.91 cm** (TR4 isthmus nodule on 2016-03-25). v2's "48 cm" has zero source support — no measurement anywhere in the raw US data exceeds 22.7 mm.
- **rid 12152** (v1=2.30, v2=19): largest actual nodule across all exams is 20.2×14.6×16.3 mm = **2.02 cm**. v2's "19" is fabricated.
- **rid 6886** (v1=2.27, v2=12.5): one US note describes "12.5 x 4.6 x 9.4 cm in place of right thyroid lobe" — v2 read the lobe-replacement-mass dimensions as a "nodule size".

R2 (`COALESCE(v1, v2)`) correctly picks v1 for all 19 extreme cases. **Action:** author mig_176b apply (Cowork-direct, ~6 query_rw calls); open `CF-mig176b-V2-UPSTREAM-EXTRACTION-INFLATION-19PTS` informational for future canonical_us_nodule_v2 rebuild.

---

## mig_174 — Option A ratified

[Cursor prompt drafted](../../cursor_prompts/CURSOR_PROMPT_mig174b_apply_per_side_boolean_20260429.md) — per-side BOOLEAN columns for `cnln_img_laterality`. Sister-lane probe for `lateral_levels_v10` / `ene_levels_v9` in the same prompt.

---

## NEW required Cursor lane — mig_177-events-rebuild

The `canonical_invasion_events_v1` build needs to handle:

1. **Combined CAP field** "Lymph-Vascular Invasion: Present" → emit BOTH `vascular_microscopic` AND `lymphatic_microscopic` events with `finding_status='present'`.
2. **Older format** "Angiolymphatic invasion: Yes/Present" → emit BOTH events.
3. **"Lymphangitic invasion present"** (rid 1535 example) → emit `lymphatic_microscopic`.
4. **Newer separate-field "Lymphatic Invasion: Present"** → emit `lymphatic_microscopic` (currently missed).
5. **"< N per 2mm2"** quantitative-invasion phrasing (rid 11599 example) → treat as `present` (it's a mitotic-style measurement, not "Not identified").
6. Typo handling: `foacl` → focal, `extrensive` → extensive, `indeterminent` → indeterminate, `c/a` → cannot_assess.

Also rebuild `canonical_invasion_patient_rollup_v1.any_lymphatic_microscopic_anywhere` after events fix — current rollup is partly built on vasc signal (the 91 NO_EVENT_ROWS rollup-only finding) and needs to be re-derived from corrected events.

Open carry-forwards:
- `CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS` — 91 patients minimum; events extractor parses combined CAP field as vasc-only
- `CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS` — patients where separate-field newer-CAP `Lymphatic Invasion: Present` was missed
- `CF-mig177-PM-VASC-ALIAS-LVI` — 196 patients; PM `lvi_any_present_path=TRUE` despite explicit `Lymphatic Invasion: Not identified`
- `CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X` — vocab cleanup needed in events extractor

---

## Next steps (post-Logan-review)

1. **Author mig_177-events-rebuild Cursor prompt** — events table rebuild for canonical_invasion_events_v1 with the 6 patterns above
2. **Hold mig_177b** — pending events rebuild + verification
3. **Apply mig_176b** — Cowork-direct R2 apply (no waiting needed; mig_176 is independent)
4. **mig_174b Cursor lane** — already drafted; agent can pick up

---

End of README.
