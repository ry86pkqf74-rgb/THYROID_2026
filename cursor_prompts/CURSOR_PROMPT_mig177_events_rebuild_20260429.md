# Cursor Prompt — canonical_invasion_events_v1 rebuild for LVI extraction bug

**Date:** 2026-04-29
**Lane:** 68 / mig_177-events-rebuild
**Batch (proposed):** `mig_177_events_rebuild_lvi_extraction_20260429`
**Predecessor:** mig_177 read-only adjudication (`ab01292`); Cowork follow-up review at `exports/mig176_177_174_review_20260429/README.md`
**Logan ratification:** 2026-04-29 — pause mig_177b; rebuild events first
**Posture:** SQL-only authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C with full pre-snapshot.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Primary table:** `main.canonical_invasion_events_v1` (and downstream `canonical_invasion_patient_rollup_v1`)

---

## Mission

The current `canonical_invasion_events_v1` build is missing legitimate `lymphatic_microscopic` event rows for at least **91 patients** (Cowork verified) and likely many more across the 2,614 PM=T/Event=F + 27 ABSENT_ONLY rollup-only sets. Rebuild the LVI extraction logic to handle all 6 documented source patterns.

---

## Source bug evidence (live MotherDuck verified)

The Cowork review file `exports/mig176_177_174_review_20260429/mig177_lvi_316_full_evidence_PART1_rollup_only.csv` documents 120 rollup-only patients where:
- The synoptic clearly states lymphatic invasion is **present**
- But `canonical_invasion_events_v1` has no `invasion_type='lymphatic_microscopic'` row with `finding_status='present'` for that patient
- And in 91 of those, no LVI event row exists at all

Six concrete patterns drive the bug:

### Pattern 1: Combined CAP field "Lymph-Vascular Invasion: Present"

```
Lymph-Vascular Invasion:    Present, focal extent (less than 4 vessels)
Lymph-Vascular Invasion:    Present, extensive extent (4 or more vessels)
Lymph-Vascular Invasion:    Present
```

The CAP synoptic template literally says "Lymph-Vascular" — a combined field for both lymphatic AND vascular. Current build emits only `vascular_microscopic`. **Required:** emit BOTH `vascular_microscopic` AND `lymphatic_microscopic` events with `finding_status='present'` (and matching extent if specified).

Examples: rid 1532, 1535, 1543, 1548, 1570, 1580, 1598, and ~80 more.

### Pattern 2: Older format "Angiolymphatic invasion: Yes/Present"

```
Angiolymphatic invasion:    Yes
ANGIOLYMPHATIC INVASION IS PRESENT
ANGIOLYMPHATIC INVASION IS IDENTIFIED
```

Same as pattern 1 — a combined field. **Required:** emit BOTH events.

Examples: rid 256, 456, 462 (all three from path_dx_summary text).

### Pattern 3: "Lymphangitic invasion present"

```
MULTIFOCAL LYMPHANGITIC INVASION PRESENT
```

Lymphangitic = lymphatic. **Required:** emit `lymphatic_microscopic` event with `finding_status='present'`.

Example: rid 1535.

### Pattern 4: Newer separate-field "Lymphatic Invasion: Present"

```
Angioinvasion (vascular invasion):  Not identified
Lymphatic Invasion:                  Present
```

Newer CAP template separates the fields. Current build appears to handle "Angioinvasion: Present" but misses "Lymphatic Invasion: Present" when angio is "Not identified".

Examples: rid 2144, 2147, 2151, 2158, 2161, 2183, 2184, 2192, 2194, 2205, 2233, 8636, 11651.

### Pattern 5: Quantitative "< N per 2mm2"

```
Lymphatic Invasion:    < 3 per 2mm2
```

This is a quantitative present-state, not "Not identified". **Required:** emit `lymphatic_microscopic` with `finding_status='present'` (and capture the count in `evidence_qualifier` or similar).

Example: rid 11599.

### Pattern 6: Vocabulary typos / variants

| Raw | Normalize to |
|---|---|
| `foacl` | `focal` |
| `extrensive` | `extensive` |
| `indeterminent` | `indeterminate` |
| `Focal` (mixed case) | `focal` |
| `c/a` | `cannot_assess` |
| `n/a`, `null`, `nan`, empty | NULL |
| `X` (uppercase) | `x` |

Apply at extraction time so downstream filters match.

---

## Required scope

1. **Read the existing canonical_invasion_events_v1 build script** — locate it in `scripts/` (likely script 363 per memory `project_invasion_canonical_mig_91_progress`). Do NOT rebuild from scratch unless the existing script is fundamentally incompatible.

2. **Audit patterns 1-6 against the existing build** — show which patterns the current code handles and which it misses, with line-number references.

3. **Author updated extraction SQL/Python** that handles all 6 patterns. Critical correctness requirement: when the synoptic says "Lymph-Vascular Invasion: Present" the build emits BOTH a `vascular_microscopic` event AND a `lymphatic_microscopic` event for that tumor, both with `finding_status='present'`.

4. **Rebuild plan:**
   - §A pre-snapshot of `canonical_invasion_events_v1` to `archive_pub_v1_0.canonical_invasion_events_v1_pre_mig177events_20260429`
   - §B re-extract from `path_synoptics` with new logic
   - §C verify post-state: count rows by `invasion_type` × `finding_status`; expect `lymphatic_microscopic` PRESENT count to grow substantially (current 1,233 PRESENT rows / 780 patients; expect to grow by ~300-500 patients minimum)
   - §D registry resync for `canonical_invasion_events_v1`
   - §E rebuild `canonical_invasion_patient_rollup_v1.any_lymphatic_microscopic_anywhere` from refreshed events

5. **Verify cohort uniformity does not regress** — `vascular_microscopic` PRESENT count should stay at 2,883 rows / 1,109 patients minimum (mig_177 baseline). `capsular`, `perineural` axes should be unchanged.

6. **Open carry-forwards:**
   - `CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS` — closed by this rebuild
   - `CF-mig177-EVENTS-LYMPHATIC_PRESENT_SEPARATE_MISS` — closed by this rebuild
   - `CF-mig177-EVENTS-VOCAB-FOACL-EXTRENSIVE-INDETERMINENT-CA-X` — closed by this rebuild
   - `CF-mig177-ROLLUP-VASC-ALIAS-LVI` — closed by §E rollup rebuild

---

## Governance reminders

- This is a **STRUCTURAL DATA WRITE** — pre-snapshot the entire `canonical_invasion_events_v1` table (not just registry rows) before any modification
- Cowork executes the apply via Path C with explicit pre-snapshot verification
- Read-only audit + SQL authoring only in this Cursor session
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits
- DuckDB MCP wrapper applies one statement per call — do NOT use `BEGIN TRANSACTION;` / `COMMIT;`
- Pre-flight cohort parity: 10,871 patients invariant

---

## Deliverables

1. `qc_framework_v1/migrations/179_canonical_invasion_events_v1_rebuild_lvi_20260429.sql` — apply SQL with embedded pre-flight + post-state verification probes
2. `qc_framework_v1/reports/mig_179_invasion_events_rebuild_audit_20260429.md` — read-only audit of the 6 patterns + line-numbered diff vs existing build script + expected vs actual counts
3. Updated build script (likely scripts/363_*) with new logic — do NOT rename; in-place fix preserving existing infrastructure
4. **DO NOT** modify `canonical_patient_master.lvi_*` columns in this lane — that's mig_177b's job, downstream

Commit message: `qc: mig_179 canonical_invasion_events_v1 rebuild for LVI extraction bug (CF-mig177-EVENTS-LYMPH_VASCULAR_COMBINED-MISS)`

---

End of prompt.
