# Cursor Prompt — Script 363: Cross-Modal Invasion Findings Canonical (NEW DOMAIN) — **v2**

> **SUPERSEDES `cursor_prompt_script_363_invasion.md` (v1).**
>
> The v1 spec was written before handoff verification against the live
> MotherDuck state and made four assumptions that don't hold against the
> deployed `thyroid_canonical_publication_v1_0` database. v2 captures the
> ground-truth schema, sources, and decisions that came out of the
> handoff probe (2026-04-22). v1 is preserved as a paper trail; **all new
> work follows v2**.
>
> **Changes vs v1:**
> 1. `canonical_path_malignant_events_v1` has **no** invasion columns —
>    the synoptic/narrative-path invasion data lives in pre361 archive
>    snapshots (`archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_*`
>    + `synoptic_tumor_long_v1_pre361_*` + `tumor_episode_master_v2_pre361_*`)
>    as VARCHAR categorical fields and a BIGINT `gross_ete`. v2 reads
>    directly from those archive tables (this becomes Pattern 8 — archive
>    as permanent source dependency) and adds a Step 0.5 normalisation
>    pass that maps the categorical vocabulary to `finding_status ∈
>    {present, absent, suspected, indeterminate}` deterministically.
> 2. `note_entities_llm_airway_invasion` and
>    `note_entities_llm_vascular_invasion` do **not** have
>    `entity_type` / `present_or_negated` / `confidence` / `evidence_text`
>    as columns. They have `result_json` VARCHAR with a
>    `{"entities": [{...}, ...]}` structure — each entity dict carries the
>    `entity_type`, `entity_value`, `present_or_negated`, `confidence`,
>    and `evidence_text` keys the v1 spec assumed. v2 uses
>    `json_extract*` plus an `UNNEST` pass to flatten entities into rows
>    (this becomes Pattern 10 — `result_json` probing in Step 0).
> 3. The 9-modality cross-modal plan census-fails to ~4-6 live modalities
>    (PET-CT, ultrasound, nucmed have **zero** LLM coverage in either
>    invasion table; vascular has only path_synoptics + OPNOTE; airway
>    has path_synoptics + ct_imaging + OPNOTE + mri_imaging). Empty
>    modalities are dropped to placeholder columns per spec — that is
>    Pattern 11 (eligible-denominator gates) doing its job, not a build
>    failure.
> 4. Step 7 cascade strip targets **only** `canonical_operative_events_v1`
>    (gross_ete_flag, tracheal_involvement_flag,
>    esophageal_involvement_flag, local_invasion_flag — all small-N but
>    real). `canonical_path_malignant_events_v1` has nothing to drop.
>    `strap_muscle_involvement_flag` stays on the operative canonical
>    permanently (per Q3 in the v1 spec, unchanged).
> 5. Step 4 registry sync uses a DELETE-WHERE-canonical_version-first
>    idempotent pattern (Pattern 12 — baked in to prevent the
>    361/362-style row duplication; see
>    `scripts/registry_dedupe_36x_canonicals.py` for the post-hoc fix).

---

## Context for Cursor

You are working on the Emory thyroid cancer lakehouse in MotherDuck
(`thyroid_canonical_publication_v1_0`). This is the third of four planned
consolidations — and the only one building a genuinely **new** Tier-2 domain
(not a rename-plus-enrichment).

**Pre-flight (already verified 2026-04-22):**
- Script 361 deployed: 6 canonicals live (path_malignant 6,689/4,137;
  path_benign 11,688/10,871; path_gland 28,724/10,731; rollups
  parity-matched).
- Script 362 deployed: 3 canonicals live (operative_events 11,773/10,871;
  operative_patient_rollup 10,871/10,871; operative_procedure_codes
  21,691/4,712). `operative_episode_detail_v2` correctly dropped.
- 363 prereqs all present: `canonical_frozen_section_events_v1`
  (7,081/4,116), `note_entities_llm_airway_invasion` (48,169/10,856),
  `note_entities_llm_vascular_invasion` (39,210/10,868).
- Pre361 archive snapshots present in
  `"Thyroid 2026 UPdated".archive_pub_v1_0`:
    - `canonical_tumor_characteristics_v1_pre361_20260422_002245` (11,106 rows)
    - `synoptic_tumor_long_v1_pre361_20260422_002245` (11,103 rows)
    - `tumor_episode_master_v2_pre361_20260422_002245` (11,691 rows)
    - + 4 others (benign / unified / outcome / malignant_diagnosis) with
      no invasion-relevant columns (verified).
- `detail_table_registry_v1` deduped by
  `scripts/registry_dedupe_36x_canonicals.py` at SHA `10d7371`
  (24 → 9 rows for v1_0_script361 + v1_0_script362).

Follow the canonical naming convention (`canonical_<domain>_events_v1` /
`canonical_<domain>_patient_rollup_v1`, view suffix `_VIEW_v1`). Match
the close-out pattern used by Scripts 347 / 360 / 361 / 362.

---

## Why this script exists (read before designing)

Logan's requirement (Q3, verbatim): *"I think some of those flags have
duplicate pulls; those flags should trace back independently across
different reports. For example. I want to know if gross ete flag was
seen in op note versus pathology report versus US versus CT Vs. pet CT,
vs MRI etc. Tracheal/esophageal involvements similar parsing to ete.
Strap muscle is likely only operative note/synoptic."*

And Q4: *"Should be per patient with date and source noted form parsed."*

And Q9: *"Just get all info into one clean table, no redundancy."*

Translation:
- **Single source of truth** for invasion findings. After 363 is green,
  the 4 invasion flags on `canonical_operative_events_v1` will be
  ALTER-DROPPED (Step 7 cascade strip).
- **Event grain, not patient-collapsed.** Every mention of invasion gets
  its own row with `source_modality` and `finding_date`. Consumers
  needing a single boolean do a query-time MAX (or use the rollup).
- **Every modality that could plausibly document the finding is included
  as a source**, even if coverage is sparse. Sparse / zero coverage is
  logged as a carry-forward; it doesn't block the build.

**Strap muscle is explicitly excluded** from this canonical (per Q3).
`strap_muscle_involvement_flag` stays permanently on
`canonical_operative_events_v1`. Do NOT strip it in Step 7.

---

## Scope & decisions (locked)

Build 2 new canonicals:
1. `main.canonical_invasion_events_v1` — event grain (one row per
   invasion-finding mention per modality).
2. `main.canonical_invasion_patient_rollup_v1` — patient grain (one row
   per `research_id` with at least one invasion finding).

Plus a **cascade strip** (Step 7) that ALTER-DROPs invasion columns
**only** from `canonical_operative_events_v1`. Runs only under
`--phase 7`.

**Invasion types (7 — unchanged from v1):**
- `gross_ete` — gross extrathyroidal extension (AJCC pT3b trigger)
- `microscopic_ete` — microscopic ETE (AJCC pT3a; histopathology-only typically)
- `tracheal` — tracheal invasion / involvement
- `esophageal` — esophageal invasion / involvement
- `vascular_microscopic` — microscopic vascular invasion (pathology-only typically)
- `airway` — airway invasion (laryngeal, hypopharyngeal — broader than tracheal)
- `local` — generic local invasion / "locally advanced disease" without specific structure

**Source modalities (live data — census-confirmed 2026-04-22):**

| modality        | source                                                    | n_mentions notes                                                  |
|-----------------|-----------------------------------------------------------|-------------------------------------------------------------------|
| `op_note`       | live `canonical_operative_events_v1` invasion BOOL flags  | gross 28 / tracheal 14 / esophageal 69 / local 29 (all sparse)    |
| `op_note_llm`   | `note_entities_llm_airway_invasion` WHERE note_type='OPNOTE' (475 substantive) + `note_entities_llm_vascular_invasion` WHERE note_type='OPNOTE' (30 substantive) | LLM enrichment of op-note free text — separate modality from structured `op_note` |
| `synoptic_path` | `archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_20260422_002245` | gross_ete BIGINT (1,571) + extrathyroidal_extension VARCHAR (6,250) + capsular_invasion VARCHAR (1,912) + vascular_invasion VARCHAR (5,810) + lymphatic_invasion VARCHAR (5,355) + perineural_invasion VARCHAR (2,220) |
| `narrative_path`| `archive_pub_v1_0.synoptic_tumor_long_v1_pre361_20260422_002245` + `archive_pub_v1_0.tumor_episode_master_v2_pre361_20260422_002245` | overlapping VARCHAR vocabularies; some uniques |
| `path_synoptics_llm` | `note_entities_llm_airway_invasion` WHERE note_type='path_synoptics' (119 substantive) + `note_entities_llm_vascular_invasion` WHERE note_type='path_synoptics' (7,516 substantive) | LLM enrichment of pathology synoptic free text |
| `frozen_section`| `canonical_frozen_section_events_v1`                       | scope cols TBD by Step 0 column probe (likely no native invasion cols — frozen_section_any_malignant_flag exists; check for ETE/invasion flags) |
| `ct`            | `note_entities_llm_airway_invasion` WHERE note_type='ct_imaging' (4,847 substantive) | airway/ETE only; no vascular signal |
| `mri`           | `note_entities_llm_airway_invasion` WHERE note_type='mri_imaging' (165 substantive) | sparse but present |
| `ultrasound`    | (none — 0 LLM coverage)                                    | DROP to placeholder (Pattern 11) |
| `pet_ct`        | (none — 0 LLM coverage)                                    | DROP to placeholder (Pattern 11) |
| `nucmed`        | (none — 0 LLM coverage)                                    | DROP to placeholder (Pattern 11) |

Modalities expected to populate the live build: **`op_note`,
`op_note_llm`, `synoptic_path`, `narrative_path`, `path_synoptics_llm`,
`frozen_section`, `ct`, `mri`** = **8 modalities** (vs the original
9-modality plan; us / pet_ct / nucmed dropped as carry-forward gaps).

**Note on op_note vs op_note_llm split:** The v1 spec collapsed both
into `op_note`. v2 keeps them as **separate source_modality values**
because Logan's Q3 requirement is "trace back independently across
different reports" — and the structured op-note flags (handful of TRUE
values per type) and the LLM op-note extractions (475 + 30 substantive
JSON rows) are independent evidence streams that cross-validate one
another. If Logan prefers a single `op_note` modality with sub-source
provenance instead, surface in CHECKPOINT 1. Same applies to
`synoptic_path` (structured) vs `path_synoptics_llm` (LLM) — kept
separate by default; collapsing is a mechanical change at Step 1.

---

## Build Scope — Script File: `scripts/363_invasion_canonical.py`

> **Filename note:** an unrelated `scripts/363_us_nodule_conflict_queue_v1.py`
> already exists from the parallel ultrasound numbering scheme. They
> share the `363_` prefix but have distinct domain names; no collision.

Define `SCRIPT_ID = "363"` and `BUILD_TS = datetime.now(timezone.utc)
.strftime("%Y%m%d_%H%M%S")` at the top. Idempotent steps. Use
`from motherduck_client import get_token, token_mode`.

### CLI flag contract (matches Scripts 361 / 362)

```
python scripts/363_invasion_canonical.py --dry-run
python scripts/363_invasion_canonical.py --commit --skip-strip
python scripts/363_invasion_canonical.py --commit --phase 7
python scripts/363_invasion_canonical.py --commit --phase 7 --force-strip
```

| Flag           | Behavior                                                                                                |
|----------------|---------------------------------------------------------------------------------------------------------|
| `--dry-run`    | Plan + print SQL, no writes. Default.                                                                    |
| `--commit`     | Execute writes.                                                                                          |
| `--phase N`    | Run only step N. `--phase 0,1,2,3,4,5,6,8` is valid (comma-list).                                       |
| `--skip-strip` | Run all phases except 7. Used for the build-and-verify commit.                                          |
| `--force-strip`| Override CPM-feeder-still-points-at-doomed-cols safety gate (7.4). Requires explicit user acknowledgement. |

Typical flow:
1. `python scripts/363_invasion_canonical.py --dry-run` → inspect Step 0
   coverage census + categorical mapping + JSON key catalog.
2. **CHECKPOINT 1 (interactive)** — surface census matrix + modality
   gating decisions to user.
3. `python scripts/363_invasion_canonical.py --commit --skip-strip`
   → build invasion canonicals, run QA, leave 362 invasion cols intact.
4. Run separate CPM feeder repoint (interim commit between build and strip).
5. **CHECKPOINT 2 (interactive)** — show CPM repoint outcome + Step 7
   pre-strip safety gates.
6. `python scripts/363_invasion_canonical.py --commit --phase 7` →
   ALTER DROP invasion cols from `canonical_operative_events_v1` (only).

---

### Step 0 — Pre-flight, coverage census, vocab + JSON key catalogs

#### 0.a Dependency check
Assert all of these exist with `COUNT(*) > 0`:
- `main.canonical_path_malignant_events_v1` (Script 361)
- `main.canonical_path_benign_events_v1` (Script 361)
- `main.canonical_operative_events_v1` (Script 362)
- `main.canonical_frozen_section_events_v1` (Script 360)
- `main.note_entities_llm_airway_invasion`
- `main.note_entities_llm_vascular_invasion`
- `archive_pub_v1_0.canonical_tumor_characteristics_v1_pre361_*`
- `archive_pub_v1_0.synoptic_tumor_long_v1_pre361_*`
- `archive_pub_v1_0.tumor_episode_master_v2_pre361_*`

For the three archive tables, use `LIKE 'canonical_tumor_characteristics_v1_pre361_%'`
+ `ORDER BY table_name DESC LIMIT 1` to pick the most recent snapshot
deterministically (so the script doesn't break if a future maintenance
operation creates additional pre361 snapshots). Hold the resolved name
in a constant log line: `f"  resolved synoptic_path source archive: {name}"`.

#### 0.b Modality coverage census (per Q10 — verify before build)

For each candidate modality, count substantive rows per source. Output
matrix to stdout AND to
`/Users/ros/THyroid 2026/invasion_coverage_census_<BUILD_TS>.md`.
Replace the v1 spec's column list with these actual columns:

| modality           | source_table                                   | source_field/filter                | n_mentions | n_patients |
|--------------------|------------------------------------------------|------------------------------------|-----------:|-----------:|
| op_note            | canonical_operative_events_v1                  | gross_ete_flag/tracheal_/esophageal_/local_ | computed | computed |
| op_note_llm        | note_entities_llm_airway_invasion (OPNOTE)     | LENGTH(result_json)>200             | computed | computed |
| op_note_llm        | note_entities_llm_vascular_invasion (OPNOTE)   | LENGTH(result_json)>200             | computed | computed |
| synoptic_path      | …canonical_tumor_characteristics_v1_pre361_*   | extrathyroidal_extension/capsular_invasion/vascular_invasion/lymphatic_invasion/perineural_invasion/gross_ete | computed | computed |
| narrative_path     | …synoptic_tumor_long_v1_pre361_*               | (same set, where present)           | computed | computed |
| narrative_path     | …tumor_episode_master_v2_pre361_*              | (same set, where present)           | computed | computed |
| path_synoptics_llm | note_entities_llm_*_invasion (path_synoptics)  | LENGTH(result_json)>200             | computed | computed |
| frozen_section     | canonical_frozen_section_events_v1             | TBD by Step 0.c column probe        | computed | computed |
| ct                 | note_entities_llm_airway_invasion (ct_imaging) | LENGTH(result_json)>200             | computed | computed |
| mri                | note_entities_llm_airway_invasion (mri_imaging)| LENGTH(result_json)>200             | computed | computed |
| ultrasound         | (none — confirm 0)                             | n/a                                 | 0        | 0        |
| pet_ct             | (none — confirm 0)                             | n/a                                 | 0        | 0        |
| nucmed             | (none — confirm 0)                             | n/a                                 | 0        | 0        |

**Coverage gates (informational — DROP-to-placeholder, not abort):**
- `n_patients = 0` → DROP modality from build, log to
  `placeholder_modalities VARCHAR[]` on the events table comment, do
  not abort.
- `n_patients > 0 but < 1% of cohort (~108 patients)` → INCLUDE with
  warning; carry-forward item.
- `op_note + op_note_llm + synoptic_path + narrative_path` ALL zero →
  ABORT (those are the invasion backbone).

#### 0.c Column existence + frozen_section invasion-col probe
For each source table, query `information_schema.columns` and confirm
the expected columns. On `canonical_operative_events_v1` confirm the
4 strip-target columns exist (gross_ete_flag, tracheal_involvement_flag,
esophageal_involvement_flag, local_invasion_flag). On
`canonical_frozen_section_events_v1` probe for any `*ete*`, `*invasion*`,
`*involv*` columns; if found, list and add to the modality plan.

#### 0.d Date column type probes
For each source table, query `information_schema.columns` for the date
column used in Step 1. Apply `TRY_CAST(... AS DATE)` discipline in the
Step 1 UNION (Pattern 3). All pre361 archives have `surgery_date ::
TIMESTAMP` or `surg_date :: TIMESTAMP`; LLM tables have `note_date ::
VARCHAR` (must `TRY_CAST`). Operative canonical has
`surgery_date_native`; frozen has `fs_day` or similar — probe.

#### 0.e Categorical vocab probe (NEW vs v1 — feeds Pattern 9)
For each VARCHAR invasion column on the pre361 archive tables, query
`SELECT col, COUNT(*) GROUP BY 1 ORDER BY 2 DESC LIMIT 50` and write
the result to
`/Users/ros/THyroid 2026/invasion_categorical_vocab_<BUILD_TS>.md`.
Cross-check against the constants `VARCHAR_TO_FINDING_STATUS` /
`VARCHAR_TO_ETE_SUBTYPE` (defined at top of script — see Step 0.5
below). Any VARCHAR value not covered by the dict → log to
`unmapped_categorical_values VARCHAR[]` and treat as `indeterminate`.
Carry-forward item.

#### 0.f result_json key probe (NEW vs v1 — feeds Pattern 10)
Sample 20 rows per LLM table per `note_type` where
`LENGTH(result_json) > 100`. Parse JSON, enumerate distinct
`entity_type` values, write to
`/Users/ros/THyroid 2026/invasion_llm_json_keys_<BUILD_TS>.md`. Cross-check against the constant
`ENTITY_TYPE_TO_INVASION_TYPE` mapping dict (defined at top of script).
Unmapped entity types → log to `unmapped_entity_types VARCHAR[]`.
Carry-forward item; do not skip the rows.

#### 0.g Pre-flight archive (pre363 snapshot of strip target)
Snapshot `canonical_operative_events_v1` to
`archive_pub_v1_0.canonical_operative_events_v1_pre363strip_<BUILD_TS>`
**only when Step 7 is in scope for the current invocation**. Use the
autonomous archive lookup pattern from Script 361 Pattern 6
(idempotent — skip if a same-name + same-row-count snapshot already
exists). Do NOT snapshot during Step 0 of `--skip-strip` runs (avoids
unused archive proliferation; Step 7 will create its own snapshot when
it runs).

---

### Step 0.5 — Categorical vocab normalisation constants (NEW vs v1)

Define at top of script (before Step 1 CTE construction). Pattern 9 — deterministic VARCHAR → finding_status mapping:

```python
# Maps lowercase + stripped VARCHAR values → finding_status.
# Build by lowercasing + stripping trailing punctuation (';', '.') first.
# Anything not in this dict → 'indeterminate' + log to
# unmapped_categorical_values for carry-forward.
VARCHAR_TO_FINDING_STATUS: dict[str, str] = {
    # Present
    "present": "present", "yes": "present", "true": "present",
    "minimal": "present", "microscopic": "present",
    "extensive": "present", "focal": "present",
    "multifocal": "present", "infiltrative": "present",
    "minimally invasive": "present", "widely invasive": "present",
    "yes (minimal)": "present", "yes, minimal": "present",
    "yes (focal)": "present", "yes, extensive": "present",
    "1 focus": "present", "identified": "present", "prominent": "present",
    "limited": "present", "invasive": "present",
    # Absent
    "no": "absent", "false": "absent", "none": "absent",
    "not identified": "absent",
    # Indeterminate / suspected
    "indeterminate": "indeterminate", "indetermiante": "indeterminate",
    "indeterminent": "indeterminate", "indeeterminate": "indeterminate",
    "suspicious": "suspected",
    "c/a": "indeterminate",  # "cannot assess"
    "n/s": "indeterminate",  # "not specified"
    # Synoptic placeholder values for "field unused"
    "x": "indeterminate",  # 3,000+ rows — do NOT treat as missing
    "*": "indeterminate", "* (see margin comment)": "indeterminate",
    "`x": "indeterminate",
    # Common typos seen in archive vocab probe
    "preesent": "present", "presnt": "present",
    "extensivre": "present", "extensiver": "present",
    "extrensive": "present", "estensive": "present",
    "extesive": "present",
    "foacl": "present",
    "widely invasivre": "present",
}

# When the column is `extrathyroidal_extension`, the VARCHAR value
# additionally encodes ETE *subtype* (gross vs microscopic). Map to
# both invasion_type and finding_status.
EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE: dict[str, str] = {
    "minimal": "microscopic_ete",
    "microscopic": "microscopic_ete",
    "yes (minimal)": "microscopic_ete",
    "yes, minimal": "microscopic_ete",
    "minimally invasive": "microscopic_ete",
    "extensive": "gross_ete",
    "widely invasive": "gross_ete",
    "yes, extensive": "gross_ete",
    "yes (extensive)": "gross_ete",
    "extensiver": "gross_ete",
    "extensivre": "gross_ete",
    "yes": "gross_ete",  # default to gross when unspecified
    "true": "gross_ete",
    "present": "gross_ete",  # default to gross when unspecified
    "focal": "microscopic_ete",
    "multifocal": "microscopic_ete",
    # Absent / indeterminate values produce the parent gross_ete
    # invasion_type with the corresponding finding_status (handled in
    # SQL — these dict entries aren't consulted for those values).
}

# `gross_ete BIGINT` on canonical_tumor_characteristics_v1_pre361_*:
# only value seen is 1 (1,571 rows). Map 1 → present, NULL → unknown.
# Treat any other future numeric value as "indeterminate" + log.
```

**Implementation pattern:** at Step 0.5 the script loads the
authoritative pre-flight categorical vocabularies (queried from the
archive tables) and computes the set difference against
`VARCHAR_TO_FINDING_STATUS.keys()`. Unmapped values → log + populate
`unmapped_categorical_values`. The Step 1 SQL CTEs then reference a
DuckDB-side `CASE WHEN` ladder built programmatically from these dicts,
not Python-side post-processing — keeps the work in MotherDuck for
performance.

---

### Step 0.6 — LLM `entity_type` mapping constants (NEW vs v1 — Pattern 10)

```python
# Maps the JSON-extracted `entity_type` value → the canonical
# `invasion_type` value used on canonical_invasion_events_v1.
# Probed against ~200 LLM result_json samples 2026-04-22.
ENTITY_TYPE_TO_INVASION_TYPE: dict[str, str] = {
    # airway_invasion table
    "ete_on_imaging": "gross_ete",
    "extrathyroidal_extension": "gross_ete",
    "extrathyroidal_extension_present": "gross_ete",
    "tracheal_invasion": "tracheal",
    "tracheal_involvement": "tracheal",
    "tracheal_compression": "tracheal",
    "tracheal_deviation": "local",  # deviation alone isn't invasion
    "esophageal_invasion": "esophageal",
    "esophageal_involvement": "esophageal",
    "esophageal_compression": "esophageal",
    "airway_invasion": "airway",
    "laryngeal_invasion": "airway",
    "hypopharyngeal_invasion": "airway",
    "soft_tissue_invasion": "local",
    "substernal_extension": "local",
    "rln_involvement": "local",  # recurrent laryngeal nerve
    # vascular_invasion table
    "vascular_invasion": "vascular_microscopic",
    "vascular_invasion_extensive": "vascular_microscopic",
    "vascular_invasion_focal": "vascular_microscopic",
    "angioinvasion": "vascular_microscopic",
    "lymphatic_invasion": "vascular_microscopic",  # often co-classified
    "perineural_invasion": "local",
    "perineural_invasion_detailed": "local",
}

# entity_value → finding_status (overrides the parent dict above when
# the JSON path returns a categorical string in entity_value rather
# than relying on present_or_negated)
ENTITY_VALUE_TO_FINDING_STATUS: dict[str, str] = {
    "absent": "absent",
    "negated": "absent",
    "no": "absent",
    "not identified": "absent",
    "present": "present",
    "yes": "present",
    "indeterminate": "indeterminate",
    "suspicious": "suspected",
    # Common compound values seen in entity_value probes — pattern: any
    # value containing "yes" → present; "no"/"absent" → absent. Use a
    # SQL CASE WHEN ILIKE ladder for robustness.
}
```

**Status precedence (when entity has both `present_or_negated` and
`entity_value`):**
1. If `entity_value` is in the dict above → use that.
2. Else if `entity_value ILIKE '%yes%'` or starts with "present" → present.
3. Else if `entity_value ILIKE '%no%'` or starts with "absent" or "not " → absent.
4. Else fall back to `present_or_negated`: 'present' → present, 'negated' → absent.
5. Else `indeterminate`.

---

### Step 1 — Build `main.canonical_invasion_events_v1`

Grain: one row per (`research_id × invasion_type × source_modality ×
source_row_id`) invasion-finding mention.

**Construction pattern:** one CTE per (`source_modality`, `invasion_type`)
combo, then `UNION ALL`. Per Pattern 7, validate each CTE's source
columns exist BEFORE building the CTE — skip CTEs whose source columns
are missing and log to `placeholder_cte_combos VARCHAR[]`.

**Columns on the events table (final):**

| column                              | type      | notes                                                                    |
|-------------------------------------|-----------|--------------------------------------------------------------------------|
| `invasion_event_id`                 | VARCHAR   | `md5(research_id ‖ source_modality ‖ source_table ‖ source_row_id ‖ invasion_type)` |
| `research_id`                       | BIGINT    |                                                                          |
| `invasion_type`                     | VARCHAR   | one of 7 enumerated above                                                |
| `finding_status`                    | VARCHAR   | present \| absent \| suspected \| indeterminate                          |
| `source_modality`                   | VARCHAR   | one of the 8 live modalities                                             |
| `source_table`                      | VARCHAR   | fully qualified                                                          |
| `source_row_id`                     | VARCHAR   | best-available row identifier                                            |
| `finding_date`                      | DATE      | `TRY_CAST` from source's date column                                     |
| `linked_surgery_episode_id`         | BIGINT    | nullable; per linkage rules below                                        |
| `linked_path_malignant_event_id`    | BIGINT    | nullable; same-day join to `canonical_path_malignant_events_v1`           |
| `linkage_method`                    | VARCHAR   | `exact_episode` \| `temporal_90d` \| `temporal_90d_ambiguous` \| `unlinked` \| `na_source_is_surgical` |
| `n_candidate_episodes`              | INT       | `COUNT(*) OVER (PARTITION BY research_id, finding_date)` against `canonical_operative_events_v1` BEFORE rn=1 (Pattern 2) |
| `linkage_ambiguous_multi_episode`   | BOOLEAN   | TRUE when n_candidate_episodes > 1 AND not exact_episode/na_source_is_surgical. Deterministic pick = lowest surgery_episode_id |
| `confidence`                        | DOUBLE    | from `entity.confidence` for LLM sources; NULL for structured sources    |
| `evidence_span_hash`                | VARCHAR   | `md5(entity.evidence_text)` for LLM sources; NULL for structured. **Never store raw text (PHI).** |
| `extraction_run_id`                 | VARCHAR   | LLM tables only (extracted_at + llm_model)                                |
| `build_script`                      | VARCHAR DEFAULT '363' |                                                              |
| `build_ts`                          | TIMESTAMP |                                                                          |

**CTE pattern — structured op-note source (example for gross_ete from canonical_operative_events_v1):**

```sql
WITH cte_gross_ete_op_note AS (
    SELECT
        'gross_ete' AS invasion_type,
        CASE
            WHEN gross_ete_flag = TRUE  THEN 'present'
            WHEN gross_ete_flag = FALSE THEN 'absent'
            ELSE 'indeterminate'
        END AS finding_status,
        'op_note' AS source_modality,
        'main.canonical_operative_events_v1' AS source_table,
        CAST(surgery_episode_id AS VARCHAR) AS source_row_id,
        TRY_CAST(research_id AS BIGINT) AS research_id,
        TRY_CAST(surgery_date_native AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        NULL::VARCHAR AS extraction_run_id,
        surgery_episode_id AS exact_linked_episode_id  -- na_source_is_surgical
    FROM main.canonical_operative_events_v1
    WHERE gross_ete_flag IS NOT NULL
       OR surgery_date_native IS NOT NULL
)
```

**CTE pattern — VARCHAR archive source (example for synoptic_path
extrathyroidal_extension, which produces BOTH gross_ete and
microscopic_ete rows):**

```sql
WITH cte_ete_synoptic_path AS (
    SELECT
        -- Subtype branch from EXTRATHYROIDAL_VALUE_TO_ETE_SUBTYPE
        CASE
            WHEN LOWER(TRIM(TRAILING ';' FROM extrathyroidal_extension)) IN
                ('minimal','microscopic','focal','multifocal',
                 'yes (minimal)','yes, minimal','minimally invasive')
            THEN 'microscopic_ete'
            WHEN LOWER(TRIM(TRAILING ';' FROM extrathyroidal_extension)) IN
                ('extensive','widely invasive','yes, extensive',
                 'extensiver','extensivre','yes (extensive)')
            THEN 'gross_ete'
            ELSE 'gross_ete'   -- default unspecified to gross
        END AS invasion_type,
        -- Status branch from VARCHAR_TO_FINDING_STATUS
        CASE
            WHEN LOWER(TRIM(TRAILING ';' FROM extrathyroidal_extension)) IN
                ('no','false','none','not identified') THEN 'absent'
            WHEN LOWER(TRIM(TRAILING ';' FROM extrathyroidal_extension)) IN
                ('x','*','`x','c/a','n/s','indeterminate','indetermiante')
            THEN 'indeterminate'
            WHEN LOWER(TRIM(TRAILING ';' FROM extrathyroidal_extension)) =
                'suspicious' THEN 'suspected'
            ELSE 'present'
        END AS finding_status,
        'synoptic_path' AS source_modality,
        '<resolved_archive_fq>' AS source_table,
        CAST(path_surgery_id AS VARCHAR) AS source_row_id,
        TRY_CAST(research_id AS BIGINT) AS research_id,
        TRY_CAST(surgery_date AS DATE) AS finding_date,
        NULL::DOUBLE AS confidence,
        NULL::VARCHAR AS evidence_span_hash,
        NULL::VARCHAR AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM "Thyroid 2026 UPdated".archive_pub_v1_0
        ."<resolved_archive_table_name>"
    WHERE extrathyroidal_extension IS NOT NULL
)
```

**CTE pattern — LLM JSON source (example for path_synoptics_llm
vascular):**

```sql
WITH path_llm_vascular_unnested AS (
    SELECT
        TRY_CAST(research_id AS BIGINT) AS research_id,
        note_row_id,
        note_type,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        extracted_at,
        llm_model,
        UNNEST(json_extract(result_json, '$.entities')::JSON[]) AS entity_json
    FROM main.note_entities_llm_vascular_invasion
    WHERE note_type = 'path_synoptics'
      AND LENGTH(result_json) > 100
),
cte_vascular_path_synoptics_llm AS (
    SELECT
        -- entity_type → invasion_type via ENTITY_TYPE_TO_INVASION_TYPE
        CASE json_extract_string(entity_json, '$.entity_type')
            WHEN 'vascular_invasion'         THEN 'vascular_microscopic'
            WHEN 'vascular_invasion_focal'   THEN 'vascular_microscopic'
            WHEN 'vascular_invasion_extensive' THEN 'vascular_microscopic'
            WHEN 'angioinvasion'             THEN 'vascular_microscopic'
            WHEN 'lymphatic_invasion'        THEN 'vascular_microscopic'
            WHEN 'perineural_invasion'       THEN 'local'
            ELSE NULL  -- skipped via WHERE filter below
        END AS invasion_type,
        -- finding_status: precedence ladder (entity_value first, then
        -- present_or_negated, then ILIKE %yes%/%no%, then indeterminate)
        CASE
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                IN ('absent','negated','no','not identified') THEN 'absent'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                IN ('present','yes','identified') THEN 'present'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                = 'indeterminate' THEN 'indeterminate'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                = 'suspicious' THEN 'suspected'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                ILIKE 'yes%' THEN 'present'
            WHEN LOWER(json_extract_string(entity_json, '$.entity_value'))
                ILIKE 'no%' THEN 'absent'
            WHEN json_extract_string(entity_json, '$.present_or_negated')
                = 'present' THEN 'present'
            WHEN json_extract_string(entity_json, '$.present_or_negated')
                = 'negated' THEN 'absent'
            ELSE 'indeterminate'
        END AS finding_status,
        'path_synoptics_llm' AS source_modality,
        'main.note_entities_llm_vascular_invasion' AS source_table,
        note_row_id AS source_row_id,
        research_id,
        note_date_parsed AS finding_date,
        TRY_CAST(json_extract_string(entity_json, '$.confidence') AS DOUBLE)
            AS confidence,
        md5(json_extract_string(entity_json, '$.evidence_text'))
            AS evidence_span_hash,
        extracted_at || '|' || llm_model AS extraction_run_id,
        NULL::BIGINT AS exact_linked_episode_id
    FROM path_llm_vascular_unnested
    WHERE json_extract_string(entity_json, '$.entity_type') IN
        ('vascular_invasion','vascular_invasion_focal',
         'vascular_invasion_extensive','angioinvasion',
         'lymphatic_invasion','perineural_invasion')
)
```

The same 3 patterns (structured BOOL, archive VARCHAR, LLM JSON) cover
every (modality, invasion_type) combo. Build the full UNION ALL by
iterating a Python config list of `(modality, invasion_type, source_table,
source_kind, ...)` tuples. **Each CTE is gated by a column-existence
pre-check** (Pattern 7).

**Linkage layer (after the UNION):**

```sql
SELECT
    md5(CAST(research_id AS VARCHAR) || '|' || source_modality
        || '|' || source_table || '|' || source_row_id
        || '|' || invasion_type) AS invasion_event_id,
    research_id, invasion_type, finding_status, source_modality,
    source_table, source_row_id, finding_date, confidence,
    evidence_span_hash, extraction_run_id,
    -- linked_surgery_episode_id: prefer exact, else temporal ±90d nearest
    COALESCE(
        exact_linked_episode_id,
        (SELECT MIN(oe.surgery_episode_id)
           FROM main.canonical_operative_events_v1 oe
          WHERE oe.research_id = u.research_id
            AND ABS(DATE_DIFF('day',
                    TRY_CAST(oe.surgery_date_native AS DATE),
                    u.finding_date)) <= 90)
    ) AS linked_surgery_episode_id,
    -- linked_path_malignant_event_id: same-day join
    (SELECT MIN(pm.path_event_id)
        FROM main.canonical_path_malignant_events_v1 pm
       WHERE pm.research_id = u.research_id
         AND TRY_CAST(pm.surg_date AS DATE) = u.finding_date
    ) AS linked_path_malignant_event_id,
    CASE
        WHEN exact_linked_episode_id IS NOT NULL THEN 'na_source_is_surgical'
        WHEN /* exact match found */ THEN 'exact_episode'
        WHEN /* multi candidate */ THEN 'temporal_90d_ambiguous'
        WHEN /* single temporal */ THEN 'temporal_90d'
        ELSE 'unlinked'
    END AS linkage_method,
    COUNT(*) OVER (PARTITION BY research_id, finding_date) AS n_candidate_episodes,
    /* linkage_ambiguous_multi_episode = ... */
    '363' AS build_script,
    CURRENT_TIMESTAMP AS build_ts
FROM (<UNION ALL of CTEs>) u
```

(Pseudocode for the linkage CASE branches — implement as a windowed
lookup table to avoid scalar subquery cost. Pattern 2: never use
`GROUP BY` after `rn=1` for the ambiguity count; use the OVER clause.)

**Expected row count band (informational):** Sum of:
- op_note: ≈140 rows (28+14+69+29 from current invasion flag counts)
- op_note_llm: ≈505 rows (475 airway + 30 vascular substantive entities)
- synoptic_path: ≈22,000 rows (1,571 gross_ete + ~20,000 across
  VARCHAR cols; lots of `'x'` indeterminate)
- narrative_path: ≈10,000 rows (overlap with synoptic_path; some
  uniques)
- path_synoptics_llm: ≈7,635 rows (119+7,516)
- frozen_section: TBD by Step 0.c probe (likely 0 if no native
  invasion cols)
- ct: ≈4,847 rows
- mri: ≈165 rows

**Total expected band: 40,000 ± 15,000 rows / 8,000 ± 2,000 patients.**
Record in QA JSON as informational. Do NOT gate on the band.

---

### Step 2 — Build `main.canonical_invasion_patient_rollup_v1`

Grain: one row per `research_id` (only patients with at least one
invasion finding — expected ~8,000 ± 2,000).

For each (`invasion_type × source_modality`) combo that survived the
census, emit:

| column                                    | type      |
|-------------------------------------------|-----------|
| `any_<type>_in_<modality>`                | BOOLEAN   | (e.g. `any_gross_ete_in_synoptic_path`) |

Cross-modal aggregates per `invasion_type`:

| column                                    | type      |
|-------------------------------------------|-----------|
| `any_<type>_anywhere`                     | BOOLEAN   |
| `any_<type>_in_op_or_path`                | BOOLEAN   | TRUE if present in op_note OR op_note_llm OR synoptic_path OR narrative_path OR path_synoptics_llm OR frozen_section |
| `any_<type>_in_imaging`                   | BOOLEAN   | TRUE if present in ct OR mri (ultrasound/pet_ct/nucmed dropped per census) |
| `earliest_<type>_date`                    | DATE      |
| `latest_<type>_date`                      | DATE      |
| `n_modalities_with_<type>`                | INT       |

Discordance flags per `invasion_type` (informational, not gated):

| column                                    | type      |
|-------------------------------------------|-----------|
| `<type>_path_imaging_concordant`          | BOOLEAN   | TRUE if (any in op/path) == (any in imaging); NULL if neither has data |

Build via:

```sql
CREATE TABLE main.canonical_invasion_patient_rollup_v1 AS
SELECT
    research_id,
    BOOL_OR(invasion_type='gross_ete' AND source_modality='synoptic_path'
            AND finding_status='present')
        AS any_gross_ete_in_synoptic_path,
    -- ... one BOOL_OR per (type, modality) combo ...
    -- ... cross-modal aggregates ...
    '363' AS build_script,
    CURRENT_TIMESTAMP AS build_ts
FROM main.canonical_invasion_events_v1
GROUP BY research_id
```

---

### Step 3 — Views (2) in `views_readable`

```sql
CREATE OR REPLACE VIEW views_readable.invasion_events_VIEW_v1 AS
  SELECT * FROM main.canonical_invasion_events_v1;
CREATE OR REPLACE VIEW views_readable.invasion_patient_rollup_VIEW_v1 AS
  SELECT * FROM main.canonical_invasion_patient_rollup_v1;
```

---

### Step 4 — `detail_table_registry_v1` sync (idempotent — Pattern 12)

```sql
-- Pattern 12: DELETE-WHERE-canonical_version FIRST, then INSERT.
-- Prevents the 361/362-style row duplication that
-- scripts/registry_dedupe_36x_canonicals.py just cleaned up.
DELETE FROM manuscript_workspace.detail_table_registry_v1
WHERE canonical_version = 'v1_0_script363';

INSERT INTO manuscript_workspace.detail_table_registry_v1 (...)
VALUES
    (... canonical_invasion_events_v1 ...),
    (... canonical_invasion_patient_rollup_v1 ...);
```

Use `information_schema.columns` introspection (per
`reference_detail_table_registry_schema.md` — confirmed columns:
`detail_table_name, schema_name, join_key, grain, total_rows,
total_patients, domain, feeds_master_columns, description,
canonical_version, feeds_master_columns_secondary,
feeds_master_columns_array, needs_manual_review`). Set
`domain = 'invasion_findings'`. Fill all 13 columns explicitly per the
authoritative pattern from `scripts/registry_dedupe_36x_canonicals.py`.

---

### Step 5 — CPM feeder audit (report only)

Print a report of every `nlp_*` / `op_*` column on
`main.canonical_patient_master` that may be sourced from any of the
about-to-be-stripped invasion columns on `canonical_operative_events_v1`.
Use `git grep -E 'gross_ete_flag|tracheal_involvement_flag|esophageal_involvement_flag|local_invasion_flag' scripts/`.

Output the repointing recommendations to
`/Users/ros/THyroid 2026/invasion_cpm_feeder_repoint_plan.md` with the
target rollup column name (e.g.,
`cpm.nlp_gross_ete → canonical_invasion_patient_rollup_v1.any_gross_ete_anywhere`).

This is a **HARD blocker** for Step 7 (gate 7.4). The repointing
script must run AFTER Step 5 and BEFORE Step 7, OR the CPM feeders
must fall back gracefully.

---

### Step 6 — Zero-drift QA → `qa/qa_script_363_invasion.json`

**Hard gates (script returns exit 2 if any fail):**

1. `events_rowcount_nonzero` — `COUNT(*)` on events > 0.
2. `rollup_parity_with_events` — `COUNT(*)` on rollup ==
   `COUNT(DISTINCT research_id)` on events.
3. `backbone_modalities_present` — at least one of `op_note` /
   `synoptic_path` / `narrative_path` / `op_note_llm` /
   `path_synoptics_llm` has rows.
4. `invasion_type_coverage` — all 7 invasion types appear in at least
   one row, EXCEPT those listed in `placeholder_invasion_types`.
5. `preservation_op_note` — `SUM(gross_ete_flag IS TRUE)` on
   `canonical_operative_events_v1` ==
   `COUNT(DISTINCT research_id || '|' || surgery_episode_id)` on
   events WHERE `invasion_type='gross_ete' AND source_modality='op_note'
   AND finding_status='present'`. Repeat for tracheal / esophageal /
   local. Zero drift.
6. `preservation_synoptic_path` — for each VARCHAR archive source col,
   compare count of normalised-as-present rows in events to count of
   normalised-as-present in source. Allow ±0 — but if the categorical
   mapping dict has a typo bucket that's partially absent in events,
   surface as carry-forward (still a hard fail to force investigation).
7. `preservation_llm_unnested` — for each LLM source, compare
   `COUNT(*)` in events WHERE `source_modality LIKE '%_llm'` to
   `SUM(json_array_length(json_extract(result_json, '$.entities')))`
   from the source for matching `entity_type`s. Off-by-one within ±5
   acceptable (some entities have null `entity_type`).
8. `view_resolves_invasion_events_VIEW_v1` — `SELECT 1 FROM ... LIMIT 1`.
9. `view_resolves_invasion_patient_rollup_VIEW_v1`.

**Informational (logged, not gated):**
10. `coverage_census_matrix` (Step 0.b output)
11. `linkage_method_distribution`
12. `linkage_ambiguity_rate` (expect <30%; wider than 362 due to ±90d window)
13. `discordance_rate_per_type` (rates >25% flag upstream NLP issues)
14. `placeholder_modalities`, `placeholder_invasion_types`, `placeholder_cte_combos`
15. `unmapped_categorical_values`, `unmapped_entity_types`
16. `varchar_date_parse_failures` per source table

---

### Step 7 — Cascade strip (runs ONLY under `--phase 7` or full `--commit` without `--skip-strip`)

#### 7.1 Pre-strip archive snapshot (Pattern 6 — autonomous lookup, idempotent)

```python
table = 'canonical_operative_events_v1'
archive_name = f"{table}_pre363strip_{BUILD_TS}"
# Look for existing same-row-count snapshot first
existing = con.execute("""
    SELECT table_name FROM "Thyroid 2026 UPdated".information_schema.tables
    WHERE table_schema = 'archive_pub_v1_0'
      AND table_name LIKE ?
    ORDER BY table_name DESC
""", [f"{table}_pre363strip_%"]).fetchall()
# parity match (live row count == archive row count) → reuse
# else → CREATE TABLE ... AS SELECT * FROM live
```

Note: `canonical_path_malignant_events_v1` is **not** snapshotted —
nothing to strip there.

#### 7.2 Pre-strip safety gates (ALL must pass — abort if any fail unless `--force-strip`)

1. `main.canonical_invasion_events_v1` exists + COUNT(*) > 0.
2. `main.canonical_invasion_patient_rollup_v1` exists + COUNT(*) > 0.
3. The Step 6 `preservation_op_note` gate passed on the last QA run
   (read from `qa/qa_script_363_invasion.json`).
4. **HARD blocker:** CPM feeder repoint plan exists at
   `/Users/ros/THyroid 2026/invasion_cpm_feeder_repoint_plan.md` AND
   the repointing script has been applied (signaled by a
   `cpm_repoint_applied: true` row in some agreed-upon ledger — TBD
   between Step 5 and Step 7. **Default behavior: abort with clear
   error pointing to `--force-strip` if `--force-strip` not passed.**).
5. No views in `views_readable` select the about-to-be-dropped columns
   from `canonical_operative_events_v1` — search via `SELECT sql FROM
   duckdb_views() WHERE database_name=? AND schema_name='views_readable'`
   and grep for each column name. If found, drop+recreate the offending
   views (`Surgery_Episode_Detail` was repointed in 362's Step 4 — should
   still be a `SELECT *` pass-through, but verify).

#### 7.3 Columns to DROP from `main.canonical_operative_events_v1`

```
gross_ete_flag, tracheal_involvement_flag,
esophageal_involvement_flag, local_invasion_flag
```

**DO NOT DROP** `strap_muscle_involvement_flag` (per Q3 — strap muscle
stays on 362 permanently).

Execute `ALTER TABLE ... DROP COLUMN ...` one column at a time with a
log line per drop. After all drops, re-run the Step 3 view CREATE OR
REPLACE to refresh `Surgery_Episode_Detail` and the new views (per
`feedback_alter_view_dependents.md` — DROP COLUMN doesn't cascade into
view bodies).

#### 7.4 Post-strip verification

- `canonical_operative_events_v1` row count unchanged.
- None of the dropped column names appear in
  `information_schema.columns` for that table.
- `canonical_invasion_events_v1` row count unchanged.
- `Surgery_Episode_Detail` view still resolves.

---

### Step 8 — Close-out summary

Print to stdout AND append to
`/Users/ros/THyroid 2026/script_363_closeout_<BUILD_TS>.md`:
- Row/patient counts on both new canonicals.
- Coverage census matrix (Step 0.b).
- Categorical vocab mapping coverage (Step 0.5; `unmapped_categorical_values`).
- LLM entity_type mapping coverage (Step 0.6; `unmapped_entity_types`).
- All 9 hard QA gate results + informational metrics.
- Cascade strip outcome (if `--phase 7` ran).
- Full SHA chain on `main` for 363 commits.
- Carry-forward items (sparse-coverage modalities, discordance rates,
  CPM repoint plan status).
- New patterns discovered during the build (additions to
  AGENTS.md / project memory).

---

## Gotchas (v2)

1. **`canonical_path_malignant_events_v1` has NO invasion columns.** Step 7
   does NOT touch this table. The synoptic/narrative path invasion data
   is sourced from pre361 archive snapshots — Pattern 8 (archive as
   permanent source dependency).
2. **LLM tables use `result_json`, not column-per-field.** Use
   `UNNEST(json_extract(result_json, '$.entities')::JSON[])` then
   `json_extract_string(entity_json, '$.<key>')`. Pattern 10.
3. **`'x'` is the modal value in many archive VARCHARs (~3K rows each).**
   It's a synoptic placeholder for "field unused" — map to
   `indeterminate`, NOT NULL/missing.
4. **strap_muscle_involvement_flag is NOT cross-modal.** Stays only on
   `canonical_operative_events_v1`. Do NOT include in Step 1 CTEs. Do
   NOT drop in Step 7.
5. **PHI: never store raw evidence spans.** Store
   `md5(entity.evidence_text)` only. Pattern: PHI hashing.
6. **VARCHAR date fields need TRY_CAST everywhere** (Pattern 3). LLM
   tables have `note_date :: VARCHAR`; archive tables have proper
   TIMESTAMP; operative has `surgery_date_native` (probe in Step 0.d).
7. **`COUNT(*) OVER (PARTITION BY ...)` for ambiguity counts**, never
   GROUP BY after rn=1 filter (Pattern 2).
8. **NULLIF(CONCAT_WS(...), '')** if you build any narrative column
   (Pattern 4).
9. **ALTER DROP COLUMN does NOT cascade into view bodies**
   (`feedback_alter_view_dependents.md`). Step 7 must CREATE OR REPLACE
   the views in `views_readable` that point at
   `canonical_operative_events_v1` after the drops.
10. **Imaging coverage IS sparse.** Per Q10 + Pattern 11: ultrasound /
    pet_ct / nucmed have zero LLM coverage in either invasion table.
    Drop to placeholder. Carry-forward item for upstream NLP.
11. **Registry schema drift** — use `information_schema.columns` to
    introspect before INSERT (per
    `reference_detail_table_registry_schema.md`). DELETE-first by
    `canonical_version` (Pattern 12).
12. **CPM feeder repoint is a HARD blocker for Step 7.** The cascade
    strip will break CPM feeders still reading from dropped columns.
    Gate 7.2.4 enforces this — do not override without `--force-strip`
    AND user acknowledgement.
13. **Surgical git add only.** Stage explicit paths: `git add
    scripts/363_invasion_canonical.py qa/qa_script_363_invasion.json
    invasion_coverage_census_*.md invasion_categorical_vocab_*.md
    invasion_llm_json_keys_*.md invasion_cpm_feeder_repoint_plan.md
    script_363_closeout_*.md`. Never `git add scripts/output/` or
    `git add -A`.
14. **PHI: research_id + aggregate counts only in stdout.** No clinical
    notes, no raw evidence text.
15. **Discordance is informational, not a bug.** A patient with gross_ete
    present in op_note but absent in synoptic_path is a legitimate
    clinical finding (op-note describes gross appearance; synoptic
    captures microscopic fields). Do NOT treat discordance as a QA
    failure. Log rates; don't gate.

---

## Reusable patterns inventory (cross-script)

The 7 reusable patterns from Script 361 are unchanged. Script 363
introduces 5 more:

| # | Name                                       | Origin     | Description |
|---|--------------------------------------------|------------|-------------|
| 1 | Mention-key partition                      | 362 hotfix | `COUNT(*) OVER (PARTITION BY mention_key)` for grain-stable counts |
| 2 | Window OVER vs GROUP BY rn=1               | 361        | Get ambiguity counts BEFORE rn=1 filter, never after |
| 3 | TRY_CAST date discipline                   | 361        | All VARCHAR date sources wrap in `TRY_CAST(... AS DATE)` |
| 4 | NULLIF(CONCAT_WS, '') for narratives       | 361        | Avoid empty-string narrative columns |
| 5 | Window-FILTER → SUM(CASE) over (PARTITION) | 362 hotfix | Avoid DuckDB FILTER-in-window pitfalls |
| 6 | Autonomous archive lookup                  | 361        | Look up archive by name+rowcount parity, idempotent skip |
| 7 | Pre-CTE column existence check             | 361        | Skip CTEs whose source columns are missing; placeholder log |
| **8** | **Archive as permanent source dependency** | **363**    | **Pre*N* archive snapshots can be permanent feeders for downstream Tier-2 canonicals when their data was intentionally stripped from live tables. Document in script docstring + close-out memory. Resolve archive name with `LIKE 'foo_pre361_%' ORDER BY name DESC LIMIT 1`** |
| **9** | **VARCHAR vocab → finding_status normalisation** | **363**    | **Top-of-script `dict[str, str]` mapping; cross-checked in Step 0.5 against actual column vocab via `unmapped_categorical_values` log. SQL CASE WHEN built programmatically from the dict — keeps work in MotherDuck** |
| **10**| **`result_json` UNNEST + key probing**     | **363**    | **For LLM tables encoding entity lists in JSON. Step 0.6 enumerates `entity_type` values from samples; design CTEs around real keys via `json_extract_string`. UNNEST + DUCKDB JSON casting for entity flattening** |
| **11**| **Modality coverage census → placeholder** | **363**    | **Census-fail (n_patients=0) drops modality to placeholder column on the events table comment + `placeholder_modalities VARCHAR[]`. Does NOT abort. Carry-forward item.** |
| **12**| **Idempotent registry DELETE-first**       | **363**    | **`DELETE FROM detail_table_registry_v1 WHERE canonical_version = ? ` before any INSERT. Prevents the 361/362 duplicate-row bug** |

---

## Git workflow (v2)

Per `feedback_commit_workflow.md` and `feedback_surgical_git_add.md`:

1. Lint with `ruff check scripts/363_invasion_canonical.py` before staging.
2. Stage by explicit path only. NEVER `git add scripts/output/` or `git add -A`.
3. Three-commit pattern:
   - After `--commit --skip-strip` + green QA: `Script 363: build invasion canonicals (2 new, N QA gates green, 8-modality cross-modal)`
   - After CPM feeder repoint script: `Script 363: repoint CPM feeders to invasion_patient_rollup_v1`
   - After `--commit --phase 7`: `Script 363: strip invasion columns from 362 canonical after green QA <build_sha>`
4. Push to `origin/main` after each commit.

---

## Success criteria (v2)

1. Script runs to completion idempotently across separate sessions.
2. `qa/qa_script_363_invasion.json` all 9 hard gates pass; informational
   metrics logged.
3. Registry clean (1 DELETE for `v1_0_script363`, 2 INSERTs).
4. `archive_pub_v1_0` contains `pre363strip` snapshot for
   `canonical_operative_events_v1` at time of Step 7.
5. Coverage census output documents which modalities were included vs
   excluded with rationale.
6. Categorical vocab + LLM entity_type mapping have ≥90% coverage of
   actual source values; `unmapped_*` lists are short (<10 entries
   each).
7. CPM feeder repoint plan written + applied (gate 7.2.4 satisfied or
   explicit `--force-strip` with documented consumer acknowledgement).
8. All 2 views resolve.
9. Strap muscle NOT touched on 362.
10. preservation_* gates exact — zero invasion data lost going from
    structured/archive/LLM sources to invasion_events rows.
11. Three clean git commits (or two + force-strip note).
