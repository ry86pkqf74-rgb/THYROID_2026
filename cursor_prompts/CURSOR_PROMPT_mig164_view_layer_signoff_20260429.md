# Cursor Prompt — mig_164 VIEW Layer Registration + Sign-off

**Lane:** 52 / mig_164
**Batch_id:** `mig_164_view_layer_registration_signoff_20260429`
**Generated:** 2026-04-29 (late evening)
**Type:** Registry-only writes + table_signoff flips. Path C apply via Cowork after Cursor SQL ships.

---

## §0 Governance — AGENTS doctrine

- **Read + author SQL only.** No `query_rw` from agent session. Logan / Cowork applies after Path C verification.
- Ship one SQL file: `qc_framework_v1/migrations/164_view_layer_registration_signoff_20260429.sql`.
- Pre-snapshot the registry slice for the 4 affected views before the registry INSERTs/UPDATEs (Section A of the SQL).

## §1 Why this lane

Independent Cowork probe (2026-04-29) found that the publication DB has 2 physical canonical_*_VIEW_v* views in `main` that are **NOT registered** in `canonical_table_signoff_registry_v1`:

1. `canonical_us_exam_master_VIEW_v2` — pass-through view over US exam master canonical
2. `canonical_us_patient_master_VIEW_v2` — pass-through view over US patient master canonical

Plus 2 already-in-registry molecular UNNEST views (status TBD per agent probe):

3. `molecular_fusions_unnested_VIEW_v2` — UNNEST of `canonical_molecular_genetics_v2.gene_fusions_list`
4. `molecular_variants_unnested_VIEW_v2` — UNNEST of `canonical_molecular_genetics_v2.gene_mutations_variants`

These are gate1 leakage (verified base canonicals exist; their compat views are unverified or unregistered). This lane registers and signs them off.

Per `reference_view_naming_convention.md`: any main.* VIEW must carry `_VIEW` in name; pattern `canonical_<domain>_<grain>_VIEW_v<N>`. All 4 already comply.

## §2 Required pre-flight probes (read-only, paste counts into SQL header)

```sql
-- §2a Confirm 4 views still exist in main with the expected names + table_type=VIEW
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name IN (
    'canonical_us_exam_master_VIEW_v2',
    'canonical_us_patient_master_VIEW_v2',
    'molecular_fusions_unnested_VIEW_v2',
    'molecular_variants_unnested_VIEW_v2'
  )
ORDER BY table_name;

-- §2b Per-view: column count + dtypes
SELECT table_name, COUNT(*) AS n_cols
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name IN (...)
GROUP BY 1 ORDER BY 1;

-- §2c Existing registry rows for these 4 views
SELECT table_name, table_status, n_columns_total, n_verified, n_na, n_not_started, n_failed
FROM main.canonical_table_signoff_registry_v1
WHERE table_name IN (...)
ORDER BY 1;

-- §2d Existing column-registry rows for these 4 views
SELECT table_name, COUNT(*) AS n_in_registry,
       SUM(CASE WHEN verification_status='verified' THEN 1 ELSE 0 END) AS n_verified,
       SUM(CASE WHEN verification_status='na' THEN 1 ELSE 0 END) AS n_na,
       SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started
FROM main.canonical_column_verification_registry_v1
WHERE table_name IN (...)
GROUP BY 1 ORDER BY 1;

-- §2e Pass-through correctness probe — for canonical_us_exam_master_VIEW_v2:
--   sample 10 rows, JOIN back to underlying base (probably canonical_us_exam_master_v2 or similar)
--   on the natural key, confirm zero IS DISTINCT FROM on every projected col.
--   Repeat for canonical_us_patient_master_VIEW_v2.
--   For molecular UNNEST views: confirm each parent row's array length equals the count of
--   unnest output rows for that research_id (by molecular_episode_id).
```

## §3 Verification methodology vocabulary (per `reference_canonical_naming_convention.md`)

For VIEW columns, the methodology vocabulary should distinguish:

- **`auto_view_passthrough_<base>_<col>`** — view simply selects the column verbatim from a verified base canonical → eligible for `verified`
- **`auto_view_unnest_<base>_<arrayfield>`** — view UNNESTs an array field → eligible for `verified` (cardinality check is the verification)
- **`auto_view_derived_<base>_<expression>`** — view applies a computed expression (e.g., `gene1 || '-' || gene2 AS fusion_pair`) → `verified` after expression sanity check
- **`auto_provenance_skip`** — for any audit/build_ts cols carried through the view

If a column derives from a base canonical that is **not yet verified**, set the view col to `not_started` and emit `CF-mig164-VIEW-COL-AWAITS-BASE-VERIFY-<col>`.

## §4 SQL structure expected in `164_view_layer_registration_signoff_20260429.sql`

### Section A — Pre-snapshots (registry-only)

```sql
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_table_signoff_registry_pre_mig164_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_table_signoff_registry_v1
WHERE table_name IN (
  'canonical_us_exam_master_VIEW_v2','canonical_us_patient_master_VIEW_v2',
  'molecular_fusions_unnested_VIEW_v2','molecular_variants_unnested_VIEW_v2'
);

CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig164_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig164_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name IN (...);
```

### Section B — INSERT registry rows for the 2 orphan VIEWs (us_exam_master, us_patient_master)

For each missing VIEW, do an `INSERT ... SELECT FROM information_schema.columns` so every physical column gets a row in `canonical_column_verification_registry_v1` with `schema_name='main'`, `verification_status='not_started'`, all metadata cols NULL, plus a `notes` seed referencing mig_164. Then `INSERT` a row in `canonical_table_signoff_registry_v1` with status `not_started`, `n_columns_total` = info_schema count.

### Section C — Per-VIEW UPDATEs flipping cols to verified / na

One UPDATE block per VIEW. Use the methodology vocabulary from §3. Set `batch_id`, `verified_by='logan'`, `verified_ts=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, `verification_method`, and a `notes` appendix.

### Section D — Resync `canonical_table_signoff_registry_v1` for the 4 VIEWs

Same pattern as mig_159 §159g — recompute n_verified / n_na / n_not_started / n_failed / n_columns_total from the column registry, flip table_status to `verified` if `n_not_started + n_failed = 0`.

### Section E — Required CFs

Open these CFs (registry note appendices) only if the corresponding finding occurs:

- `CF-mig164-US-VIEW-PASSTHROUGH-DRIFT-<col>` — if §2e probe finds any IS DISTINCT FROM rows on the pass-through views.
- `CF-mig164-MOLECULAR-UNNEST-CARDINALITY-DRIFT` — if UNNEST output rows ≠ parent array length sum.
- `CF-mig164-VIEW-COL-AWAITS-BASE-VERIFY-<col>` — if any view col depends on a not-yet-verified base.

## §5 5-gate audit expected after apply

`gate1` should increase by however many of the 4 VIEWs flip to `verified`. Other gates remain 0. If `gate5` jumps, that's a CF — VIEW projects a TIMESTAMP/VARCHAR-named-`*_date` column from a not-yet-retyped base; document and defer until the base is fixed.

Run the §11 audit query from `COWORK_HANDOFF_PROMPT_2026-04-29_v5.md` post-apply and paste pre/post counts into the SQL file header.

## §6 Git workflow

- File: `qc_framework_v1/migrations/164_view_layer_registration_signoff_20260429.sql`
- Commit: `qc: mig_164 VIEW layer registration + sign-off (4 canonical_*_VIEW_v*)`
- Push to `origin/main`.

## §7 Out of scope

- Do NOT modify VIEW bodies (`CREATE OR REPLACE VIEW`).
- Do NOT touch base canonicals.
- Do NOT register or sign off `note_entities_llm_presenting_symptoms` — that's mig_165's lane.
- Do NOT apply on MD; ship SQL only.
