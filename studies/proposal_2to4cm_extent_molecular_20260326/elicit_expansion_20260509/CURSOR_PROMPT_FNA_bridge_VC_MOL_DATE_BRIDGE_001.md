# Cursor handoff: FNA key-type bridge (VC-MOL-DATE-BRIDGE-001)

**Recommendation: do this in Cursor.** Requires canonical-layer writes (build a bridge table + re-run date backfill arm). Read-only diagnostic on Cowork side confirmed the join is structurally broken (0/374 matches).

Closes `VC-MOL-DATE-BRIDGE-001` (`recDwv4CliD7MunoE`, Open/MISMATCH/medium).

## Background

`canonical_molecular_genetics_v2.linked_fna_episode_id` (STRING) holds **numeric episode tokens** like `"3580"`, `"7773"` — these are legacy DuckDB pipeline episode IDs.

`canonical_fna_events_v1.fna_event_id` (STRING) holds **32-character hex strings** — these are the current BQ-native UUIDs.

The mig_324 date backfill tried to join these directly:
```sql
LEFT JOIN canonical_fna_events_v1 fna ON fna.fna_event_id = g.linked_fna_episode_id
```
and got **0 rows out of 374** non-null `linked_fna_episode_id` values. The FNA-linkage backfill arm contributed nothing; the 903 dateless rows fell back to `imported_at_fallback` (file upload date) instead of getting real FNA-proximate dates.

## Goal

Build a token→hex bridge table that maps the legacy numeric episode IDs to the current BQ-native `fna_event_id` UUIDs, then re-run the date-backfill arm so the ~374 affected canonical rows can shift from `imported_at_fallback` provenance to `fna_linkage` provenance.

## Hard rules

1. **Snapshot first** of `canonical_molecular_genetics_v2` to `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_<YYYYMMDD>` before the backfill re-run.
2. **No PHI in committed code, Airtable, or Linear.** The bridge table is pure ID-to-ID mapping; safe.
3. **DFL row pre-edit** (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`). `change_type=data_correction`. Reference `VC-MOL-DATE-BRIDGE-001`.
4. **MFL row post-edit** (`MFL-<YYYYMMDD>-EXT2-4-FNA-BRIDGE`) linked to EXT2-4 (`rec1GJyrmKdKxjlaY`).
5. Update VC-MOL-DATE-BRIDGE-001 lifecycle Open → Verified after acceptance.
6. Skill version bump v2.3.1 → **v2.3.2** (patch — additive bridge table and refreshed date provenance).

## Phase 1 — Source-of-truth investigation

The numeric episode tokens almost certainly came from the legacy MotherDuck/DuckDB pipeline's `molecular_episode_id` sequence (per mig_320 / mig_321 code references). The 32-char hex UUIDs are the BQ-rebuild canonical IDs.

Find the bridge by checking:

```sql
-- Is there an existing bridge anywhere in pub_legacy_source_20260416 or pub_archive?
SELECT table_name FROM `thyroid-canonical-pub-2026.region-us-central1`.INFORMATION_SCHEMA.TABLES
WHERE LOWER(table_name) LIKE '%episode%' OR LOWER(table_name) LIKE '%bridge%' OR LOWER(table_name) LIKE '%link%';

-- Does canonical_fna_events_v1 have a legacy-id column?
SELECT column_name, data_type
FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'canonical_fna_events_v1'
  AND (LOWER(column_name) LIKE '%legacy%' OR LOWER(column_name) LIKE '%episode%'
       OR LOWER(column_name) LIKE '%token%' OR LOWER(column_name) LIKE '%duck%');

-- Sample a few numeric tokens from canonical_molecular_genetics_v2 and see if they
-- match any column on canonical_fna_events_v1 by value (not by name):
WITH tokens AS (
  SELECT DISTINCT linked_fna_episode_id AS tok
  FROM `pub_canonical.canonical_molecular_genetics_v2`
  WHERE linked_fna_episode_id IS NOT NULL
  LIMIT 50
)
SELECT t.tok, COUNT(*) AS n_matches
FROM tokens t
LEFT JOIN `pub_canonical.canonical_fna_events_v1` f
  ON CAST(f.fna_index AS STRING) = t.tok
     OR CAST(f.fna_seq_n AS STRING) = t.tok
     OR f.bethesda_provider = t.tok
     -- add other candidate columns here
GROUP BY t.tok;
```

If `canonical_fna_events_v1` carries a legacy-id column directly (e.g., `legacy_fna_episode_id`), the bridge is trivial — just use it. If not, the bridge must be built by joining on (research_id, FNA date proximity, Bethesda match).

## Phase 2 — Build the bridge

Two paths depending on Phase 1:

### Path A — column-level bridge (preferred)
If a legacy ID column exists on `canonical_fna_events_v1`, build:
```sql
CREATE OR REPLACE TABLE `pub_workspace.fna_episode_id_bridge_<YYYYMMDD>` AS
SELECT
  legacy_fna_episode_id AS token,
  fna_event_id AS uuid,
  research_id,
  'column_lookup' AS bridge_method
FROM `pub_canonical.canonical_fna_events_v1`
WHERE legacy_fna_episode_id IS NOT NULL;
```

### Path B — research_id + date proximity match
If no column-level bridge exists, build a heuristic bridge:
```sql
CREATE OR REPLACE TABLE `pub_workspace.fna_episode_id_bridge_<YYYYMMDD>` AS
WITH cmg_tokens AS (
  SELECT
    research_id,
    linked_fna_episode_id AS token,
    -- best-available date hint for the molecular test
    COALESCE(resolved_test_date, test_date_native, DATE(SAFE_CAST(NULL AS TIMESTAMP))) AS mol_date_hint
  FROM `pub_canonical.canonical_molecular_genetics_v2`
  WHERE linked_fna_episode_id IS NOT NULL
),
fna_candidates AS (
  SELECT
    research_id,
    fna_event_id,
    fna_date_resolved,
    bethesda_final_num,
    days_to_surgery
  FROM `pub_canonical.canonical_fna_events_v1`
),
joined AS (
  SELECT
    c.research_id,
    c.token,
    f.fna_event_id AS uuid,
    f.fna_date_resolved,
    ABS(DATE_DIFF(c.mol_date_hint, f.fna_date_resolved, DAY)) AS date_distance_days,
    ROW_NUMBER() OVER (
      PARTITION BY c.research_id, c.token
      ORDER BY ABS(DATE_DIFF(c.mol_date_hint, f.fna_date_resolved, DAY)) ASC NULLS LAST
    ) AS rn
  FROM cmg_tokens c
  LEFT JOIN fna_candidates f USING (research_id)
)
SELECT
  research_id, token, uuid, fna_date_resolved, date_distance_days,
  CASE
    WHEN uuid IS NULL THEN 'no_fna_event_for_patient'
    WHEN date_distance_days IS NULL THEN 'no_date_hint_available'
    WHEN date_distance_days <= 30 THEN 'date_match_within_30d'
    WHEN date_distance_days <= 90 THEN 'date_match_within_90d'
    ELSE 'date_match_loose'
  END AS bridge_confidence
FROM joined
WHERE rn = 1;
```

Acceptance: ≥ 70% of the 374 tokens should resolve to a `fna_event_id` (i.e., `uuid IS NOT NULL`); of those, ≥ 50% should be `date_match_within_30d` confidence tier.

## Phase 3 — Re-run date backfill arm using the bridge

Update the mig_324 date-backfill staging logic to JOIN through the bridge instead of directly:

```sql
CREATE OR REPLACE TABLE `pub_workspace.cmg_date_backfill_v2_<YYYYMMDD>` AS
SELECT
  g.molecular_episode_id,
  g.research_id,
  g.report_source_table,
  g.resolved_test_date AS current_resolved_test_date,
  g.resolved_test_date_source AS current_resolved_test_date_source,
  fna.fna_date_resolved AS proposed_date_from_fna,
  br.bridge_confidence AS fna_bridge_confidence,
  CASE
    WHEN g.resolved_test_date_source = 'native' THEN g.resolved_test_date
    WHEN br.bridge_confidence IN ('column_lookup','date_match_within_30d')
         AND fna.fna_date_resolved IS NOT NULL
      THEN fna.fna_date_resolved
    ELSE g.resolved_test_date  -- leave imported_at_fallback as-is
  END AS proposed_resolved_date,
  CASE
    WHEN g.resolved_test_date_source = 'native' THEN 'native'
    WHEN br.bridge_confidence IN ('column_lookup','date_match_within_30d')
         AND fna.fna_date_resolved IS NOT NULL
      THEN 'fna_linkage_via_bridge'
    ELSE g.resolved_test_date_source
  END AS proposed_resolved_test_date_source
FROM `pub_canonical.canonical_molecular_genetics_v2` g
LEFT JOIN `pub_workspace.fna_episode_id_bridge_<YYYYMMDD>` br
  ON br.token = g.linked_fna_episode_id AND br.research_id = g.research_id
LEFT JOIN `pub_canonical.canonical_fna_events_v1` fna
  ON fna.fna_event_id = br.uuid;
```

Then MERGE into `canonical_molecular_genetics_v2`:
- UPDATE `resolved_test_date` only where (a) current source is `imported_at_fallback` AND (b) proposed comes from `fna_linkage_via_bridge`
- UPDATE `resolved_test_date_source` to `fna_linkage_via_bridge` on those rows
- Never overwrite `native` source values

## Phase 4 — Verification

```sql
SELECT
  resolved_test_date_source,
  COUNT(*) AS n,
  ROUND(SAFE_DIVIDE(COUNT(*), (SELECT COUNT(*) FROM `pub_canonical.canonical_molecular_genetics_v2`)), 3) AS frac
FROM `pub_canonical.canonical_molecular_genetics_v2`
GROUP BY resolved_test_date_source
ORDER BY n DESC;
```

Expected post-fix distribution:
- `native`: 481 (unchanged at 35%)
- `fna_linkage_via_bridge`: 200–300 (new — was 0)
- `imported_at_fallback`: 600–700 (decreased from 903)
- `unresolvable`: still 0

Acceptance criteria:
- [ ] `n_with_fna_linkage_via_bridge` ≥ 200 (vs 0 pre-fix)
- [ ] Total `frac_with_date` still 1.0 (no regression on date coverage)
- [ ] Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_fna_bridge_<YYYYMMDD>`
- [ ] No regression: 0 previously-`native` values changed

## Phase 5 — Airtable + skill bump

- DFL pre-edit, MFL post-edit
- VC-MOL-DATE-BRIDGE-001 → Verified
- Update `NF-2026-05-13-canonical-molecular-date-coverage-with-fna-bridging-gap` (`recRPg7hWTWwRPzrV`) lifecycle to `Verified` with the new provenance distribution in the evidence_summary
- Skill v2.3.1 → v2.3.2; CHANGELOG entry

## Manuscript impact

- **EXT2-4 v3 / v4**: minimal — year-level era stratification is unaffected. Manuscript Limitations § item (f) can be amended to remove the "FNA-linkage arm yielded zero rows" caveat once this is done.
- **Future sub-year temporal analyses** (FNA-to-molecular-test interval, time-to-molecular-test post-FNA, etc.): become defensible after this fix.

## When done, hand back to Cowork for

- Limitations § amendment in executive_summary §2 item (f) and the manuscript draft
- Optional: file a Notable Finding noting the bridge construction methodology (if Path B / heuristic match was used, the methodology is publishable as a data-engineering disclosure)
