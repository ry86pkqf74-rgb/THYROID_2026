# VIEW Labeling Pass — Rename All Tables-That-Are-Views With `_VIEW` Suffix

**Date:** 2026-04-21
**Owner:** Logan Glosser (Emory surgery)
**Db:** `thyroid_canonical_publication_v1_0` (+ archive db `"Thyroid 2026 UPdated"`)
**Goal:** Any object in `main.*` that is actually a VIEW (not a TABLE) must carry `_VIEW` in its name so the distinction is visible at the name level — same convention as `views_readable.<name>_VIEW_v1`. New target form: `canonical_<domain>_<grain>_VIEW_v<N>`.

---

## 0. Context You Need

- Prior work:
  - **Part B (TIRADS cleanup)** landed 2026-04-21 — converted `canonical_us_exam_master_VIEW_v2` and `canonical_us_patient_master_VIEW_v2` to VIEWS (commit `dab2f7e`), with `canonical_us_patient_master_VIEW_v2` backed by persistent snapshot `main.cupm_v2_canonical_backfill_v1`.
  - `views_readable` schema already uses `_VIEW_v1` suffix — we are extending this convention to `main.*` v2 artifacts that happen to be views.
- Logan's directive (verbatim): *"all VIEW tables need to be labeled as VIEW."*
- **This is a rename + reader migration pass, not a redesign.** View bodies and semantics stay identical. Only object names change, plus any code that references them.
- Pattern borrowed from Script 347/360 cleanups: archive → rename → migrate readers → QA → commit.

---

## 1. Phase 0 — Discovery (READ-ONLY, STOP GATE)

Before touching anything, enumerate the full set of renaming candidates.

### 1.1 Inventory all VIEWs in `main`

```sql
SELECT
  table_schema,
  table_name,
  table_type
FROM thyroid_canonical_publication_v1_0.information_schema.tables
WHERE table_schema = 'main'
  AND table_type  = 'VIEW'
ORDER BY table_name;
```

### 1.2 Inventory all VIEWs in `views_readable` (for reference / naming sanity check)

```sql
SELECT table_name
FROM thyroid_canonical_publication_v1_0.information_schema.tables
WHERE table_schema = 'views_readable'
  AND table_type  = 'VIEW'
ORDER BY table_name;
```

### 1.3 For every VIEW found in 1.1, compute its reader footprint

For each candidate `<V>`:

```bash
# SQL script readers (be strict — boundary-anchored)
grep -rEn --include='*.sql' "[^a-zA-Z0-9_]${V}[^a-zA-Z0-9_]|^${V}[^a-zA-Z0-9_]|[^a-zA-Z0-9_]${V}$" .

# Python readers
grep -rEn --include='*.py' "['\"\.\s]${V}['\"\s]|\.${V}\b" .

# Markdown / docs
grep -rEn --include='*.md' "\b${V}\b" .
```

Tally per-object: **# SQL hits, # Python hits, # Markdown hits, total unique files**.

### 1.4 Classify each VIEW

| Class | Meaning | Action |
|---|---|---|
| **KEEP-AS-VIEW** | Legit derived/convenience view, will stay a view after rename | Rename with `_VIEW` suffix |
| **SHOULD-BE-TABLE** | Was meant to be a persisted table but ended up a view by accident | Flag to Logan; separate fix |
| **ARCHIVE-ONLY** | Legacy / frozen / no active readers | Drop or move to `views_readable.legacy_*`; don't rename |

### 1.5 STOP GATE → Logan

Deliver one markdown report with:

1. Full table of every `main.*` VIEW: name, classification, reader counts (SQL / Py / MD), top 5 reader files by hit count.
2. Proposed rename map: `old_name → new_name` for every KEEP-AS-VIEW.
3. Any SHOULD-BE-TABLE or ambiguous entries flagged with one-line rationale — Logan decides per-object.
4. Estimated total reader files to edit (union across all KEEP-AS-VIEW views).
5. **DO NOT PROCEED** past Phase 0 without Logan's approval on the rename map + SHOULD-BE-TABLE decisions.

**Expected floor of candidates:**
- `canonical_us_exam_master_VIEW_v2` → `canonical_us_exam_master_VIEW_v2`
- `canonical_us_patient_master_VIEW_v2` → `canonical_us_patient_master_VIEW_v2`

If these two are the only `main.*` VIEWs, the pass is narrow. If there are more (likely 3–10), the report determines scope.

---

## 2. Phase 1 — Archive Current VIEW Definitions (PROVENANCE)

For each approved rename, capture the current `CREATE VIEW` SQL so the exact definition is preserved independently of the rename.

### 2.1 Pull DDL from DuckDB catalog

```sql
SELECT sql
FROM thyroid_canonical_publication_v1_0.duckdb_views()
WHERE schema_name = 'main'
  AND view_name   = '<V>';
```

### 2.2 Write to provenance archive

Save each DDL to: `scripts/archive/view_definitions_20260421/<V>.sql`

Header per file:
```sql
-- Archived: 2026-04-21
-- Source:   thyroid_canonical_publication_v1_0.main.<V>
-- New name: thyroid_canonical_publication_v1_0.main.<NEW_V>
-- Reason:   view labeling pass — add _VIEW suffix
```

Commit after writing, before renaming. Message: `archive: capture view DDLs before _VIEW rename pass (view_labeling_20260421)`.

---

## 3. Phase 2 — Rename Views (ATOMIC, PER-VIEW)

For each approved rename `<old> → <new>`:

### 3.1 Transactional swap

```sql
BEGIN TRANSACTION;

-- Rename VIEW
ALTER VIEW thyroid_canonical_publication_v1_0.main.<old>
  RENAME TO <new>;

-- Optional: create a compatibility VIEW pointing to the new name
-- ONLY if Phase 0 reader count is high (>20 files) and migration will span multiple commits.
-- Otherwise skip and rely on Phase 3 reader migration to land atomically.
CREATE OR REPLACE VIEW thyroid_canonical_publication_v1_0.main.<old> AS
  SELECT * FROM thyroid_canonical_publication_v1_0.main.<new>;
-- If created, add TODO comment: -- DROP after reader migration (view_labeling_20260421)

COMMIT;
```

### 3.2 Verify

```sql
-- New name exists and is a VIEW
SELECT table_type
FROM information_schema.tables
WHERE table_schema='main' AND table_name='<new>';
-- Expect: VIEW

-- Sanity: row counts match pre-rename snapshot (if captured)
SELECT COUNT(*) FROM main.<new>;
```

### 3.3 Commit per batch

If ≤3 renames: one commit. Otherwise one commit per rename. Message pattern:
`rename: <old> → <new> (VIEW labeling pass)`

---

## 4. Phase 3 — Reader Migration

For every file grep'd in Phase 0.3, update references.

### 4.1 Strategy

- **SQL files:** exact boundary-anchored substitution, one view at a time (not blanket sed — we got burned on Cat B in Part B when CTAS aliases shared names with column names).
- **Python files:** same — boundary-anchored. Watch for these reference forms:
  - `"main.canonical_us_patient_master_VIEW_v2"` (string literal)
  - `.canonical_us_patient_master_VIEW_v2` (attribute-style from some ORMs)
  - `FROM canonical_us_patient_master_VIEW_v2` (bare in f-string SQL)
  - `'canonical_us_patient_master_VIEW_v2'` in table_name dicts / manifests
- **Markdown:** run substitution; these are doc-only, low risk.

### 4.2 Per-file discipline

1. Open file → identify hits → apply substitution → re-read → visually confirm.
2. If file is SQL-only and runnable as a smoke test, run it.
3. If Python, lint: `python -m py_compile <file>`.
4. Do not bulk-apply across many files without diff review. Process in batches of ≤10 files per commit.

### 4.3 Compatibility-view cleanup

If Phase 2 created temporary `CREATE OR REPLACE VIEW <old> AS SELECT * FROM <new>` compat shims:
- After all readers migrated, `DROP VIEW main.<old>`.
- Verify no remaining grep hits for `<old>` before dropping.

### 4.4 Commits

Batch by domain. Message pattern: `migrate readers: <old> → <new> (<N> files)`.

---

## 5. Phase 4 — Update Downstream Documentation

Update any of the following that reference old names:
- `memory/` files (especially any that reference `canonical_us_patient_master_VIEW_v2` / `canonical_us_exam_master_VIEW_v2`).
- `scripts/README.md`, domain READMEs.
- Any dashboard/query catalog docs.
- `detail_table_registry_v1` rows, if the renamed view is listed there — do a `UPDATE ... SET detail_table_name = '<new>'`.

---

## 6. Phase 5 — Handle SHOULD-BE-TABLE Entries (If Any)

Separate issue, but surface in the final report. For each flagged entry:
- Does it need to be persisted as a real table? (Perf, snapshot semantics, upstream drops.)
- If yes: propose a `CREATE TABLE ... AS SELECT ...` backing + `CREATE OR REPLACE VIEW` on top (like `cupm_v2_canonical_backfill_v1` pattern).
- Do NOT execute inside this prompt — hand back a separate micro-script proposal.

---

## 7. Phase 6 — QA

### 7.1 Rename completeness

```sql
-- No main.* VIEWs without _VIEW in the name (excluding any Logan explicitly kept)
SELECT table_name
FROM information_schema.tables
WHERE table_schema='main'
  AND table_type='VIEW'
  AND table_name NOT LIKE '%\_VIEW\_%' ESCAPE '\';
-- Expect: empty, or only Logan-approved exceptions
```

### 7.2 No dangling old-name references in live code

```bash
# For each renamed <old>, across whole repo minus scripts/frozen and scripts/archive:
grep -rE --include='*.py' --include='*.sql' --include='*.md' \
  --exclude-dir=scripts/frozen --exclude-dir=scripts/archive \
  "\b<old>\b" .
# Expect: zero hits for every <old>, OR only hits in frozen/archive with FROZEN headers.
```

### 7.3 Functional smoke

For each renamed view, `SELECT COUNT(*) FROM main.<new>` should return the same count as the pre-rename snapshot captured at start of Phase 2.

### 7.4 Readers still run

Run one representative reader per renamed view (top-hit file from Phase 0.3 tally). No errors → pass.

---

## 8. Phase 7 — Commit & Report

### 8.1 Final commit sequence expected

1. `archive: capture view DDLs before _VIEW rename pass`
2. Per-view rename commits
3. Per-batch reader migration commits
4. `docs: update memory / README references to new VIEW names`
5. `cleanup: drop compatibility shims after reader migration` (if used)
6. `qa: verify VIEW labeling pass 2026-04-21`

### 8.2 Closeout report to Logan

One markdown summary:
- Views renamed (old → new), per-view row counts pre/post (should be identical).
- Files edited, counts by type (SQL / Py / MD).
- Commits made, SHA list.
- Compat shims used? If yes, when dropped.
- Any SHOULD-BE-TABLE items flagged for follow-up.
- QA results: all checks pass / any exceptions.

---

## 9. Hard Rules / Don'ts

1. **Do NOT** change view bodies. Rename only. Semantic change is a separate PR.
2. **Do NOT** blanket-sed across the repo. Boundary-anchored, per-view, reviewed.
3. **Do NOT** drop compat shims until Phase 6.2 passes (zero dangling refs).
4. **Do NOT** rename `views_readable.*_VIEW_v1` — those already follow the convention.
5. **Do NOT** touch `cupm_v2_canonical_backfill_v1` — that's a backing TABLE, not a view. Its existence is precisely why the view on top can survive source drops; leave alone.
6. **Do NOT** proceed past Phase 0 STOP gate without Logan's approval.
7. **Do NOT** skip the DDL archive in Phase 1 — it's cheap and future-proofs us if a rename ever needs to be reversed.

---

## 10. Expected Scope (rough)

- **Views to rename:** 2 known + likely 0–8 more from Phase 0 discovery.
- **Reader files to edit:** probably 20–60 (Part B cohort views, Python QA scripts, memory refs).
- **Time estimate:** ~1 session (Phase 0 report → approval → execute).
- **Risk:** Low. Renames are reversible via archived DDLs; view semantics unchanged.

---

## 11. Handoff Checkpoint

At Phase 0 STOP gate, pause and wait for Logan. After Phase 0 approval, proceed through Phase 7 end-to-end, pausing only if:
- A rename fails (FK / dependency error).
- A reader migration touches a file in `scripts/frozen/` (should not happen — flag and skip).
- Any SHOULD-BE-TABLE entry turns out to have live dependencies that require same-session fix.
