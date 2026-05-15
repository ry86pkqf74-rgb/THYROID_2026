# pub_workspace Lifecycle Policy — thyroid-canonical-pub-2026

**Prepared:** May 14, 2026 · Cowork / BigQuery Studio Integration Plan
**Pain point addressed:** workspace sprawl — "hard to tell live from dead."

---

## The actual situation (live measurement, 2026-05-14)

`pub_workspace` holds **480 objects** totalling **1.03 GB**:

| Category | Count |
|---|---|
| Views | 79 |
| Tables | ~401 |
| `cohort_m*` cohort tables | 83 |
| `mig_*` / version-dated (`_vN_YYYYMMDD`) tables | 23 |
| snapshot-like (`snapshot` / `_snap_` / `_archived` / `_pre_`) | 16 |
| scratch (`dryrun` / `_test` / `_tmp` / `_bak` / `_old` / `scratch` / `sandbox`) | 5 |

**Key finding: this is not a storage problem — it's a legibility problem.** 1 GB is trivial; the cost is that nobody can tell which of 480 objects is live. Two compounding facts:

1. **`last_modified_time` is useless as a staleness signal here.** A recent bulk migration touched every table — 0 of 480 objects are "untouched in 14 days." You cannot find dead tables by age.
2. **Version duplication is modest, not rampant** — only `ete_manuscript_analytic` (v1–v7, 6 live versions) and `cohort_h2_pathology_outcome` (v1–v3) have 3+ versions. The sprawl is breadth (many one-off cohort tables), not deep version stacks.

So the fix is **not** a one-time mass delete. It's a policy that makes lifecycle state *explicit* going forward, plus a small targeted sweep.

---

## The policy

### 1. Every pub_workspace table gets a label at creation

BigQuery table labels, set in `OPTIONS(labels=[...])`:

- `lifecycle:active` — in use by a current cohort or pipeline.
- `lifecycle:scratch` — exploratory; auto-sweep candidate after 30 days.
- `lifecycle:superseded` — replaced by a newer version; keep only until the dependent work ships.
- `owner:<name>` — who to ask before deleting.
- `expires:<YYYYMMDD>` — optional hard sunset date.

A table with **no `lifecycle` label** is itself a finding — it shows up in the sweep query below as `unlabeled`.

### 2. Scratch and dry-run tables get a real BigQuery TTL

Set `OPTIONS(expiration_timestamp=...)` (or dataset-default partition expiration) so `dryrun` / `_tmp` / `_test` tables physically expire instead of accumulating. New scratch work should be created with a 30-day expiration by default.

### 3. Snapshots belong in pub_archive, not pub_workspace

The 16 snapshot-like objects in `pub_workspace` should be in `pub_archive` (which already holds ~175 of them and is the documented home). Move them; don't snapshot into the workspace.

### 4. A recurring sweep — query, don't guess

Run this monthly (or wire it into the QC pipeline as an info-level assertion). It surfaces sweep candidates by *pattern and label*, since age is not a usable signal:

```sql
SELECT
  table_id,
  CASE
    WHEN REGEXP_CONTAINS(table_id, r'(?i)dry.?run|_test|_tmp|_temp|_bak|_old|scratch|_sandbox') THEN 'scratch'
    WHEN REGEXP_CONTAINS(table_id, r'(?i)snapshot|_snap_|_archived|_pre_')                       THEN 'should_be_in_pub_archive'
    WHEN REGEXP_CONTAINS(table_id, r'(?i)^mig_|_premig|pre_mig|_v[0-9]+[a-z]?_20[0-9]{6}')        THEN 'migration_or_dated'
    ELSE 'review'
  END AS sweep_class,
  ROUND(size_bytes/POW(1024,2),2) AS size_mb,
  TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `thyroid-canonical-pub-2026.pub_workspace.__TABLES__`
WHERE REGEXP_CONTAINS(table_id, r'(?i)dry.?run|_test|_tmp|_temp|_bak|_old|scratch|_sandbox|snapshot|_snap_|_archived|_pre_|^mig_|_premig|pre_mig|_v[0-9]+[a-z]?_20[0-9]{6}')
ORDER BY sweep_class, size_mb DESC;
```

This returns the ~44 pattern-matched candidates (5 scratch + 16 snapshot-like + 23 migration/dated). Each should be confirmed with its owner, then deleted or moved — **not auto-dropped**, since this is a publication database.

### 5. Targeted first sweep (safe, do now)

- **5 scratch tables** — confirm with owners, then delete (or set a 7-day TTL).
- **16 snapshot-like tables** — move to `pub_archive`, then drop from `pub_workspace`.
- **`ete_manuscript_analytic` v1–v7** — confirm which version the ETE manuscript actually uses; label that one `lifecycle:active` and the rest `lifecycle:superseded`.

### 6. Governance hook

Add one info-level assertion to `cowork_qc_nonblocking_pipeline_v1`: *count of `pub_workspace` tables with no `lifecycle` label.* If that number grows, the policy is being ignored — the problem surfaces instead of silently re-accumulating.

---

## Why this closes the pain point durably

A one-time cleanup would just sprawl again. Labels + TTLs + a recurring labelled-vs-unlabelled check make lifecycle state **visible and self-policing** — the workspace stays legible because every new table declares what it is, and the QC pipeline notices when it doesn't.

*All counts from a live query of `pub_workspace.__TABLES__` on 2026-05-14.*
