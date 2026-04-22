# Cursor Prompt — Script 363: Cross-Modal Invasion Findings Canonical — **v3**

> **SUPERSEDES `cursor_prompt_script_363_invasion_v2.md`.**
>
> v2 was committed in `0fd2411` and rejected by Logan after direct
> MotherDuck verification surfaced 4 issues:
>
> 1. **Cross-DB violation.** v2 sourced from
>    `archive_pub_v1_0.synoptic_tumor_long_v1_pre361_*` and
>    `tumor_episode_master_v2_pre361_*`. Logan's directive: master
>    canonicals are standalone live objects in `main`; no
>    `FROM archive_pub_v1_0.*` in any build script.
>    **Pattern 8 (archive as permanent source dependency) is REJECTED**
>    and removed from the reusable patterns inventory. See
>    `feedback_no_cross_db_canonical_sourcing.md`.
> 2. **Classification bug.** `invasion_type='local'` was a catch-all
>    bucket. Direct probe of v2's `canonical_invasion_events_v1` showed:
>    - ~1,200 patient-events from `note_entities_llm_vascular_invasion`
>      ("present", "minimal", "focal", ETE) landing in `local` instead
>      of `vascular_microscopic` / proper ETE classification.
>    - ~700+ tracheal-deviation LLM entries (CT) dumped into `local`
>      (these are mass effect, not invasion).
>    - ~85 substernal-extension entries also in `local` (mass effect).
> 3. **V/L aggregation.** v2 conflated `vascular_invasion` and
>    `lymphatic_invasion` into a single `vascular_microscopic` type.
>    Per Logan's per-patient probe with full vocabulary normalisation:
>    vascular alone = 6.27% cohort; lymphatic alone = 7.20%; both = 293
>    pts; union = 10.78%. These are clinically distinct AJCC
>    descriptors and require separate invasion_type slots.
> 4. **Pattern 9 violation.** v2's `build_ts` column is `TIMESTAMP WITH
>    TIME ZONE`. Per `reference_duckdb_timestamp_tz.md`, must be
>    `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`.
>
> v3 reset: `scripts/363_reset_v3.py` snapshotted the v2 build to
> `archive_pub_v1_0.canonical_invasion_*_v1_pre363v3_<BUILD_TS>` and
> dropped the live tables + views + 2 registry rows.

---

## Changes vs v2

### A. Sourcing — NO cross-DB

- **DROP narrative_path entirely.** Coverage loss: 48 patients in
  `gross_ete` only (0.44% cohort). All 48 have alternate `gross_ete`
  coverage from other modalities (the other modalities cover 1,209
  patients; narrative-unique is 48). Acceptable per Logan.
- **DO NOT reference** `archive_pub_v1_0.synoptic_tumor_long_v1_pre361_*`,
  `tumor_episode_master_v2_pre361_*`, or
  `canonical_tumor_characteristics_v1_pre361_*` anywhere in build SQL.
- All sourcing from `main` only.

### B. Invasion_type taxonomy — NEW vocabulary (10 types, was 7)

**KEEP / FIX:**
- `gross_ete` (existing — fix sourcing per A)
- `microscopic_ete` (existing — disambiguated via entity_value modifier
  for LLM ETE entities)
- `vascular_microscopic` — `vascular_invasion` ONLY (no lymphatic)
- `airway` — direct airway invasion (NOT deviation/compression)
- `tracheal` — direct tracheal invasion (NOT deviation/compression)
- `esophageal` — direct esophageal invasion (NOT compression)

**NEW v3:**
- `lymphatic_microscopic` — `lymphatic_invasion` ONLY (split from
  `vascular_microscopic`)
- `capsular` — `capsular_invasion` (split from `local`)
- `perineural` — `perineural_invasion` (split from `local`)
- `soft_tissue` — `soft_tissue_invasion` (split from `local`); also
  receives op_note `local_invasion_flag` (intra-op surgeon-noted direct
  extension into adjacent tissue)

**REMOVED:**
- `local` — the catch-all bucket. NOT in vocabulary anymore. The
  `local_invasion_type_extinct` QA gate enforces this.

### C. Entity_type → invasion_type mapping (corrected)

**FROM `note_entities_llm_vascular_invasion`:**
- `vascular_invasion` / `vascular_invasion_focal` /
  `vascular_invasion_extensive` / `angioinvasion` /
  `lymphovascular_invasion` → `vascular_microscopic`
- `lymphatic_invasion` → `lymphatic_microscopic`
- `capsular_invasion` → `capsular`
- `perineural_invasion` / `perineural_invasion_detailed` → `perineural`
- `soft_tissue_invasion` → `soft_tissue`

**FROM `note_entities_llm_airway_invasion` (DIRECT-INVASION only):**
- `airway_invasion` / `laryngeal_invasion` /
  `hypopharyngeal_invasion` → `airway`
- `tracheal_invasion` / `tracheal_involvement` → `tracheal`
- `esophageal_invasion` / `esophageal_involvement` → `esophageal`
- `extrathyroidal_extension` / `ete_on_imaging` /
  `extrathyroidal_extension_present` / `extranodal_extension` /
  `strap_muscle_invasion` → `gross_ete` OR `microscopic_ete`
  (disambiguated by `entity_value` modifier: contains
  "minimal"/"microscopic"/"focal" → microscopic_ete; else gross_ete)

**EXCISE (NULL invasion_type → CTE filter drops):**
- `tracheal_deviation`, `tracheal_displacement`, `tracheal_compression`,
  `tracheal_narrowing` (compression, not invasion)
- `substernal_extension` (anatomic extension, not invasion)
- `esophageal_compression` (compression, not invasion)
- `vascular_encasement` (tumor-around-vessel, not vessel-wall invasion
  — different pathophysiology)
- `mass_effect` (compression/displacement)
- `airway_compromise_grade` (severity descriptor)
- `vocal_cord_imaging` (finding category, not invasion)
- `vascular_invasion_type`, `vessel_count`, `mitotic_rate`, `necrosis`,
  `ptnm_stage`, `dedifferentiation` (not invasion findings at all)
- `rln_involvement` (deferred — clinically meaningful but not in
  Logan's keep list; route to a future canonical if needed)

**FROM `main.canonical_path_malignant_events_v1` (LIVE structured cols):**
- `vascular_invasion` → `vascular_microscopic`
- `lymphatic_invasion` → `lymphatic_microscopic`
- `capsular_invasion` → `capsular`
- `perineural_invasion` → `perineural`
- `extrathyroidal_extension` → `gross_ete` (gross-flavored values) or
  `microscopic_ete` (minimal/microscopic/focal modifiers)
- `gross_ete` BIGINT → `gross_ete` (1 → present; 0 → absent)

**FROM `main.canonical_operative_events_v1` (LIVE structured BOOL flags):**
- `gross_ete_flag` → `gross_ete`
- `tracheal_involvement_flag` → `tracheal`
- `esophageal_involvement_flag` → `esophageal`
- `local_invasion_flag` → `soft_tissue` (NEW v3: surgeon-noted gross
  extension into adjacent tissue. Was `'local'` in v2; routed to
  `soft_tissue` in v3 since the surgeon doesn't differentiate
  capsular vs perineural at gross inspection)
- `strap_muscle_involvement_flag` — NOT included (stays only on
  `canonical_operative_events_v1` per Q3, permanent)

### D. `source_kind` column — orthogonal modality × kind dimensions
Unchanged from v2. Pattern 12.

### E. Pattern 9 fix
Every `build_ts` column populated as `CAST(CURRENT_TIMESTAMP AS
TIMESTAMP)`. No TIMESTAMPTZ.

### F. New QA gates (hard, not informational)
- `local_invasion_type_extinct`: `'local'` is NOT in
  `SELECT DISTINCT invasion_type` of events table.
- `no_cross_db_archive_sourcing`: `COUNT(*) WHERE source_table LIKE
  'archive_pub_v1_0.%'` must be 0.
- `vl_split_vascular_min`: `vascular_microscopic` present-finding
  patient count ≥ 682 (Logan's 6.27% forecast).
- `vl_split_lymphatic_min`: `lymphatic_microscopic` present-finding
  patient count ≥ 783 (Logan's 7.20% forecast).
- `vl_split_intersection_min`: patients present in BOTH
  `vascular_microscopic` AND `lymphatic_microscopic` ≥ 293.

### G. Step 0.f.2 — Excised entity_type row counts
New Step 0 substep that probes each LLM table for the EXCISED
entity_types (full json_extract, not just sample) and writes counts to
the JSON keys probe report. Logan's CHECKPOINT 1.G requirement so the
audit trail captures exactly what's being thrown away.

---

## Forecast numbers (Logan's MotherDuck probes; v3 should approximately match)

| invasion_type | n_patients | % cohort | Notes |
|---|---:|---:|---|
| `vascular_microscopic` | ~682 | ~6.27% | Per-FIELD vascular alone (LLM gain may add) |
| `lymphatic_microscopic` | ~783 | ~7.20% | Per-FIELD lymphatic alone (LLM gain may add) |
| `capsular` | ~827 | ~7.60% | Per-FIELD capsular |
| `perineural` | ~102 | ~0.94% | Per-FIELD perineural |
| `gross_ete` | ~1,209 | ~11.12% | v2 11.56% minus 48 narrative-unique |
| `microscopic_ete` | TBD | TBD | Subset of v2 ETE-via-modifier |
| `soft_tissue` | TBD | TBD | Surgeon-noted local + LLM soft_tissue_invasion |
| `airway` | ~72 | ~0.66% | Roughly v2 |
| `tracheal` | ~14 | ~0.13% | Roughly v2 |
| `esophageal` | ~185 | ~1.70% | Roughly v2 |
| `local` | **REMOVED** | — | Vocabulary excised |

---

## Build commit sequence

1. **Reset (commit 1)** — `python scripts/363_reset_v3.py --commit`.
   Snapshots v2 to archive, drops live tables + views + registry rows.
   Surgical commit + push. Done already in this session.
2. **v3 spec + script (commit 2)** — this doc + updated
   `scripts/363_invasion_canonical.py`. Surgical commit + push.
3. **CHECKPOINT 1 (interactive)** — Logan reviews v3 dry-run output.
   Specifically:
   - Coverage census matrix (4 modalities, no archive sources)
   - Excised entity_type counts (Step 0.f.2 output)
   - Sidecar QA breakdown forecast vs Logan's clinical realism table
   - Confirmations: 0 archive references, no `'local'` invasion_type
4. **Build (commit 3)** — `python scripts/363_invasion_canonical.py
   --commit --skip-strip`. All 14 hard QA gates must PASS.
5. **CPM repoint (commit 4)** — `scripts/363_cpm_feeder_repoint.py`
   updates 16 CPM cols to read from
   `canonical_invasion_patient_rollup_v1.any_<type>_anywhere`.
   New rollup column names for v3:
   `any_vascular_microscopic_anywhere` (was `_local_anywhere` for some);
   `any_lymphatic_microscopic_anywhere` (NEW);
   `any_capsular_anywhere` (NEW);
   `any_perineural_anywhere` (NEW);
   `any_soft_tissue_anywhere` (NEW).
6. **CHECKPOINT 2 (interactive)** — Logan signs off on CPM repoint.
7. **Cascade strip (commit 5)** — `python scripts/363_invasion_canonical.py
   --commit --phase 7`. ALTER DROP 4 invasion BOOL flags from
   `canonical_operative_events_v1`. `strap_muscle_involvement_flag`
   stays per Q3.

---

## Reusable patterns inventory (post-v3)

| # | Name | Status |
|---|---|---|
| 1 | Mention-key partition | ✓ |
| 2 | Window OVER vs GROUP BY rn=1 | ✓ |
| 3 | TRY_CAST date discipline | ✓ |
| 4 | NULLIF(CONCAT_WS, '') for narratives | ✓ |
| 5 | Window-FILTER → SUM(CASE) over (PARTITION) | ✓ |
| 6 | Autonomous archive lookup (for archiving, not sourcing) | ✓ |
| 7 | Pre-CTE column existence check | ✓ |
| ~~8~~ | ~~Archive as permanent source dependency~~ | **REJECTED v3** |
| 9 | VARCHAR vocab → finding_status normalisation (with TIMESTAMP not TIMESTAMPTZ) | ✓ |
| 10 | `result_json` UNNEST + json_extract_string design | ✓ |
| 11 | Modality coverage census → placeholder | ✓ |
| 12 | Orthogonal source_modality × source_kind | ✓ |
| 13 | Idempotent registry DELETE-WHERE-canonical_version | ✓ |
| 14 | LLM `result_json` UNNEST template (with error-row filter) | ✓ |
| **15** | **EXCISE non-invasion entity_types (mass-effect / compression / staging) — row-counted in Step 0.f.2** | **NEW v3** |
