# Cowork handoff prompt — 2026-05-05 (post mig_309/311/312, pre mig_313/310v2)

> **PASTE THE BLOCK BELOW INTO THE NEW COWORK CHAT VERBATIM.** It's a self-contained brief.

---

## Continuing thyroid-2026 work — handoff brief

You are picking up an active research project (Logan Glosser, Emory thyroid surgery cohort, 1999–2025, n=10,871 patients). The repo lives at `/Users/loganglosser/THYROID_2026`. Pre-read this entire file end-to-end before responding, then do exactly what the **First Action** section says.

---

## Architecture

**Three runtimes, three roles, one repo:**

- **MotherDuck** (`thyroid_canonical_publication_v1_0`) — primary working surface. All canonical tables, cohort views, signoff registry. Connect via the MCP `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__*` tools (read-only `query`, write `query_rw`). Token in `.env.motherduck` and `.env`. Release tag `pub_v1_1_20260504`. 245 main objects, 279 manuscript_workspace objects, 87 cohort views (m001–m083).
- **Snowflake** (`THYROID_VALIDATION.PUBLIC`, account `qcc02515.us-east-1`) — Cortex services + flat mirrors. PAT in `.env` (`SNOWFLAKE_PAT`). **Important — handoff doc claim of "Cortex-only PAT" is STALE.** PAT has full warehouse access via the patched-PAT pattern; see `snowflake_trial/scripts/02_load_to_snowflake.py` and `snowflake_trial/scripts/load_m025_nodule_level_to_sf.py` for the connector patch (account_name + token injection during login). Use that template for all SF SQL/DDL/DML from Cowork. `cortex` CLI (`~/.local/bin/cortex`) supports `cortex analyst query --connection thyroid_2026 --model <localpath>` — local YAML semantic models work without a Snowsight bind.
- **Cursor** (Composer 2.0, GPT-5-class) — runs out-of-band, has its own MD + SF auth, picks up cursor prompts from `cursor_prompts/`. **DO NOT execute heavy/long-running migrations from Cowork** — write a clean `CURSOR_PROMPT_MIG_<N>_*.md` file and tell Logan to drop it into cursor.
- **GitHub** (`https://github.com/ry86pkqf74-rgb/THYROID_2026.git`) — main branch only. Cursor and Logan auto-push; Cowork's sandbox lacks GitHub auth, so Cowork uses **Desktop Commander MCP** (`mcp__Desktop_Commander__start_process`) to run git commands in Logan's actual zsh terminal at `/Users/loganglosser/THYROID_2026`. Always check `.git/index.lock` before write ops — cursor sometimes holds it.

**Migration / signoff convention:** every database change gets a `mig_<N>` ID. Insert a row into `main.signoff_migration` with `(mig_id, signed_off_at, by_actor, summary)`. Idempotent via `WHERE NOT EXISTS`. The "by_actor" tag identifies the runner (`cursor_composer_mig<N>`, `cowork_mig<N>_<purpose>`, etc.). Migration registry is the single source of truth for "did this land?"

**Cowork's job:**
- Probe data, surface bugs, write cursor prompts for big jobs
- Execute small/medium MotherDuck operations directly via `query_rw`
- Execute Snowflake operations via the patched-PAT Python pattern (`02_load_to_snowflake.py` template)
- Drive git commit/push via Desktop Commander, NOT bash sandbox (sandbox can't push)
- Insert signoffs after each landed migration
- Hand off heavy NLP / multi-hour jobs to cursor with a complete prompt

**Cowork's NOT job:**
- Drafting manuscript prose (Logan does in his M025 chat or directly)
- Running `cortex analyst` queries that need >5 sec compute (Logan does)
- Anything that needs Snowsight UI clicks

---

## Manuscript inventory

| ID | Topic | Cohort N | Status | Submission package | Notes |
|---|---|---|---|---|---|
| **M004** | Autoimmune × cancer | 10,871 (CPM-anchored) | Cohort built | `M004_submission_package_v1_0/` | mig_298 NLP-augmented Option 2 (Hashimoto 400 / Graves 1,656). Ready for writing brief. |
| **M019** | RAI outcomes | 862 RAI recipients | Analysis landed | none | Cursor commit `0f91f52`. Needs writing brief. |
| **M025 v1** | TI-RADS performance, patient grain | 3,375 operative | **Submission v1 frozen** (sister) | `M025_submission_package_v1_0/` | mig_292. Patient-level paper. |
| **M025 v2** | TI-RADS performance, **nodule grain** | 37,438 nodules / 6,523 pts / 3,687 strict | **ACTIVE — Logan drafting in another chat** | `M025_submission_package_v2_0/` + `M025_FINAL_PACKAGE/` | Headline: nodule-level TR4 ROM 18.7% [16.3-21.5] / TR5 26.1% [23.7-28.6] inside ACR bands; patient-level shows 47%/59% — 50-70% of operative-cohort ROM inflation is multinodular attribution error, not selection bias. AUC 0.6399 nodule vs 0.6478 patient. mig_306/307/307b/307c/307d. Cortex Analyst semantic model BOUND and operational (mig_311). **One of three published manuscripts already complete from this batch.** |
| **M029** | FNA cytology concordance | TBD | Analysis landed | none | Cursor commit `a9bc38c`. Sister to M025; **next-priority writing brief**. |
| **M032** | 25-yr descriptive | 10,871 | **Submission v1 frozen** | `M032_submission_package_v1_0/` | mig_290/290b. n_malig=4,019. Stage IV counts may need re-audit post-mig_313 M-stage fix. |
| **M033** | Afirma vs ThyroSeq | TBD | Analysis landed | none | Cursor commit `d693275`. |
| **M036** | ATA 2025 RSS reclassification | 4,019 malig | **v2 ran but distribution distorted** | `studies/m036_ata_rss_comparison/` | Cursor commit `421e4d3`. v2 fixed margin (R1 ≠ incomplete) but 1,642 patients still flagged `high:distant_metastasis` driven by upstream M-stage corruption. **Must re-run as v3 after mig_313 lands.** |
| **M037** | LN predictors | 2,234 | **Submission v1 frozen** | `M037_submission_package_v1_0/` | mig_291. |
| **M038** | Massive goiter | 10,871 (2,501 massive composite) | **Submission v1 frozen** ✅ ALREADY PUBLISHED-READY | `M038_submission_package_v1_0/` + `M038_OUTLINE.md` | mig_276. **Logan flagged this as one of three already built.** |
| **M043** | LN multivariate | TBD | Analysis landed | none | Cursor commit `a9b5940`. Sister to M037. |
| **M044** | mETE / AJCC ETE | 4,013 (3,572 strict-DTC analytic) | **v5 manuscript shipped** ✅ ALREADY PUBLISHED-READY | `M044_FINAL_PACKAGE/` (v5 docx + LaTeX + Synthesis Summary v2 + figures + all_stats.xlsx + per-research-id dataset) | Headline aOR gross-vs-micro 1.77 [1.15-2.71] p=0.009. Item-1 reconciliation closed: FU IQR 5.91 yr (locked Excel) wins over LaTeX v3's 5.89. **Logan flagged this as one of three already built.** STAGE IV CLAIMS NEED RE-AUDIT after mig_313 — Cowork to run delta check post-fix. |
| **M083** | BRAF dual-platform discordance | TBD | Cohort view exists in SF | none | Cursor prompt queued. |
| **m045–m082** | Various (multimodal risk, NIFTP, frozen-section trio, parathyroid, recurrence, etc.) | varies | Cohort views scaffolded only | none | 38 cohorts. Triage by clinical priority before analyzing. |

**Three already-published-ready** (Logan's note): M025 v2 (nodule-level TI-RADS — drafting in another chat), M038 (massive goiter), M044 v5 (mETE).

---

## What this prior Cowork session accomplished (2026-05-05)

**Live MotherDuck operations (Cowork-executed):**
- `mig_308` — backfilled 8 unsigned migrations (mig_264/274/276/278/279/301/301b/304)
- `mig_311` — Cortex Analyst bind for M025 nodule-level grain. Smoke test reproduced TR2 12.90% / TR3 9.13% / TR4 18.72% / TR5 26.11% locked Wilson CIs. CLI workflow: `cortex analyst query "..." --connection thyroid_2026 --model snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml`
- `mig_309` — Snowflake `VALIDATE_ALL_COHORTS_V3()` SP deployed. **24/24 PASS** (17 v2 baseline + 7 row-count drift). Closes `CF-mig_305-SP-V3-HANG`.
- `mig_312` — Rebuilt 5 broken VARIANT cohort flats in Snowflake (M025 patient + M032 + M037 + M038 + M044) using INFER_SCHEMA + ALTER TABLE RENAME COLUMN to uppercase. Same recipe as mig_311 nodule-level. Unblocks mig_309 drift checks AND enables Cortex Analyst binding for all 5 patient grains.
- `mig_305` retro signoff inserted.

**Cursor prompts delivered (running now per Logan):**
- `cursor_prompts/CURSOR_PROMPT_MIG_313_M_STAGE_CORRUPTION_FIX_20260505.md` — **P0**. Fix `m_stage_ajcc8_resolved` corruption (45.19% M1 in malignant cohort; PTC 44.23%, FC 57.82%, follicular adenoma 100% — all impossible). Recipe: NLP_M_STAGE_RESOLVED via Cortex EXTRACT_ANSWER over path notes; rebuild canonical_path_malignant_events_v1 with M0 default; cascade refresh of CPM + 6 cohort flats; re-run M036 ATA RSS as v3; audit M044 v5 Stage IV deltas.
- `cursor_prompts/CURSOR_PROMPT_MIG_310_V2_FNA_NLP_HP_CORPUS_20260505.md` — supersedes v1. Cowork `--pilot --dry-run` probe revealed: (a) no FNA-typed note exists; FNA content embedded in HP notes (top note_type for "bethesda" keyword); (b) SQL bug at line 187 of cursor's `36_pull_sf_nlp_fna_size.py`. v2 adds keyword-filter corpus + 60-day FNA event↔note linkage + SQL fix.

**Repo hygiene executed:**
- M044_FINAL_PACKAGE/ v5 committed
- M044 v1-v3 drafts retired to `archive/m044_ete_predecessors_20260505/`
- 5/4 cursor prompts committed
- `.gitignore` broadened (backups/ runs/ exports/ processed/remaining/ etc — 1.6 GB excluded)
- 2 commits pushed to origin/main this session: `bc87eec` (cohort flats + cursor prompts), `43f8f48` (mig_311), prior `2266cc2` (hygiene), `e34ee71` (infra docs)

**Open carry-forwards (closed by current cursor work):**
- `CF-MSTAGE-CORRUPTION` (Cowork-opened 2026-05-05) — closes when mig_313 lands
- `CF-FNA-SIZE-CM-NULL` — closes when mig_310 v2 lands

---

## First Action — Review cursor's mig_313 + mig_310 v2 work

Logan triggered both mig_313 (M-stage corruption fix) and mig_310 v2 (FNA NLP via HP-note corpus) in cursor. Your job:

### Step 1 — Check git for cursor commits since `bc87eec`

```bash
cd /Users/loganglosser/THYROID_2026 && git fetch origin main && git log --oneline bc87eec..origin/main
```

### Step 2 — Check MotherDuck for mig_310 / mig_313 signoff rows

```sql
SELECT mig_id, signed_off_at, by_actor, LEFT(summary, 200) AS summary
FROM main.signoff_migration
WHERE mig_id IN ('mig_310','mig_313','mig_314','mig_315')
   OR signed_off_at >= '2026-05-05 06:00:00'
ORDER BY signed_off_at DESC;
```

### Step 3 — Check that mig_313 actually corrected M-stage (acceptance gates from prompt)

```sql
SELECT histology_final, COUNT(*) AS n,
  SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) AS n_m1,
  ROUND(100.0 * SUM(CASE WHEN ajcc8_m_stage='M1' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_m1
FROM main.canonical_patient_master
WHERE is_malignant
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

Acceptance: PTC M1 ≤ 3% (was 44.23%); follicular carcinoma ≤ 10% (was 57.82%); follicular adenoma 0% (was 100%).

### Step 4 — Check mig_310 v2 deliverables

```sql
SELECT
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='manuscript_workspace' AND table_name LIKE 'fna_content_corpus%') AS corpus_built,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='manuscript_workspace' AND table_name LIKE 'fna_event_note_linkage%') AS linkage_built,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='manuscript_workspace' AND table_name='nlp_fna_size_rollup_v1') AS rollup_built,
  (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='manuscript_workspace' AND table_name='imaging_fna_linkage_v4') AS linkage_v4_built;
```

If linkage_v4_built = 1, smoke-test the rebuilt M025 nodule cohort:
```bash
cortex analyst query "what is the per-tr ROM in the strict eligible cohort, with counts" \
  --connection thyroid_2026 \
  --model snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml
```
Numbers should still hit TR2 12.90 / TR3 9.13 / TR4 18.72 / TR5 26.11, possibly with small drift (FNA-size covariate is informational, not in the per-TR aggregate).

### Step 5 — If mig_313 landed, audit M044 v5 Stage IV deltas

M044 was just packaged at v5 with locked numbers. If Stage IV row counts in `M044_FINAL_PACKAGE/M044_ETE_FINAL_all_stats.xlsx` changed by >5 patients post-fix, the manuscript needs a v6 numerical patch. Run:

```sql
-- M044 cohort Stage IV count, pre vs post (if pre snapshot exists)
SELECT
  ajcc8_stage_group,
  COUNT(*) AS n_post_mig313
FROM manuscript_workspace.cohort_m044_ajcc_ete_v1
GROUP BY 1
ORDER BY 1;
```

Compare to M044 Table 1 in the v5 manuscript (current Stage IV count is in the docx).

### Step 6 — Re-run M036 ATA RSS as v3 (post-mig_313)

```bash
cd /Users/loganglosser/THYROID_2026
.venv/bin/python scripts/m036_ata_2025_rss.py
```

Expected v3 distribution:
- high: 2,353 → ~600–900 (most M1-driven false positives drop)
- low: 23 → ~200–500
- intermediate: ~1,200 (minimal change)
- uncalculable: ~425 (driven by non-DTC histology, unchanged)

If distribution looks right, insert mig_314 signoff and write the M036 ready-for-writing brief.

### Step 7 — Comprehensive next-steps plan

After steps 1–6, write Logan a comprehensive next-steps plan covering:
- Cursor work that landed (and any failures to retry)
- Whether mig_313 + mig_310 closed their carry-forwards cleanly
- Whether M044 v5 needs a numerical patch
- What's the next manuscript to draft (Logan's call but you can recommend)
- Any new data-quality issues surfaced by the NLP runs

---

## Workflow recipes

### Probe MotherDuck
Use the MCP directly: `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__query` for read, `query_rw` for write. Database: `thyroid_canonical_publication_v1_0`.

### Execute Snowflake DDL/DML from Cowork
Use the patched-PAT pattern (template in `snowflake_trial/scripts/load_m025_nodule_level_to_sf.py`):
```python
import os, json
from dotenv import load_dotenv; load_dotenv('.env')
import snowflake.connector
import snowflake.connector.network as _net
PAT = os.environ['SNOWFLAKE_PAT']; DOTTED = 'qcc02515.us-east-1'
_orig_post = _net.SnowflakeRestful._post_request
def _patched_post(self, url, headers, body, *args, **kwargs):
    if '/session/v1/login-request' in url:
        try:
            d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
            d['data']['ACCOUNT_NAME'] = DOTTED
            if not d['data'].get('TOKEN'): d['data']['TOKEN'] = PAT
            body = json.dumps(d)
        except: pass
    return _orig_post(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched_post
ctx = snowflake.connector.connect(account='qcc02515', host=f'{DOTTED}.snowflakecomputing.com',
    user='LGLOSSE13', password=PAT, warehouse='COMPUTE_WH', database='THYROID_VALIDATION',
    schema='PUBLIC', role='ACCOUNTADMIN', authenticator='PROGRAMMATIC_ACCESS_TOKEN')
```

Run from Cowork via `mcp__Desktop_Commander__start_process` so it executes in Logan's terminal.

### Mirror MD cohort to SF (semantic model precondition)
Run `snowflake_trial/scripts/load_m025_nodule_level_to_sf.py` — it's a one-shot template. Adapt for other cohorts by changing the table name and view name.

### Cortex Analyst NL→SQL (no Snowsight bind required)
```bash
cortex analyst query "<question>" --connection thyroid_2026 \
  --model snowflake_trial/semantic_models/<grain>_semantic_model.yaml
```
Currently bound: `m025_nodule_level_semantic_model.yaml`. Add new YAMLs by copying the M025 template (mig_311 reference YAML lives in the same dir).

### Hand work to cursor
Write `cursor_prompts/CURSOR_PROMPT_MIG_<N>_<purpose>_20260505.md` with: problem statement, recipe, validation gates, signoff SQL. Tell Logan to drop it into cursor (he runs cursor in a separate IDE session out-of-band). Do NOT execute the cursor work yourself.

### Commit + push
```bash
# Cowork via Desktop Commander to Logan's terminal
[ -f .git/index.lock ] && rm -f .git/index.lock
git add <files>
git commit -m "<conventional message>"
git push origin main
```
The bash sandbox (`mcp__workspace__bash`) lacks GitHub auth — always go through Desktop Commander for git ops.

### Cursor prompt template
Every cursor prompt should have:
1. Title + agent + estimated time + supersedes
2. Problem statement (what's broken)
3. Step-by-step recipe with SQL/Python
4. Validation gates (what numbers must match)
5. Signoff SQL block (`INSERT INTO main.signoff_migration ...`)
6. Carry-forward references opened/closed
7. Out-of-scope notes

---

## Key locked numbers (validation baselines)

| Metric | Value |
|---|---|
| CPM rows | **10,871** |
| Malignant patients | **4,019** |
| M025 v1 patient cohort | 3,375 (1,479 malignant) |
| M025 v2 nodule cohort (strict-eligible) | 3,687 (631 malignant) |
| M025 nodule TR4 ROM | 18.72% (locked Wilson CI 16.3–21.5) |
| M025 nodule TR5 ROM | 26.11% (locked Wilson CI 23.7–28.6) |
| M025 nodule AUC | 0.6399 |
| M025 patient AUC | 0.6478 |
| M044 strict-DTC cohort | 3,572 (105 path-proven recurrence) |
| M044 primary aOR gross-vs-micro | 1.77 [1.15–2.71] p=0.009 |
| M032 cohort | 10,871 (4,019 malig) |
| M037 LN cohort | 2,234 |
| M038 massive goiter (composite) | 2,501 (23.0%) |

Any deviation from these post-mig_313 means the M-stage fix introduced regressions — flag immediately.

---

## Files Logan cares about most

- `M025_FINAL_PACKAGE/` and `M025_submission_package_v2_0/` — active drafting (other chat owns)
- `M025_v2_manuscript_DRAFT_v1_0.md` — currently untracked, his other chat's working draft
- `M044_FINAL_PACKAGE/M044_ETE_FINAL_Manuscript_v5.docx` — shipped
- `M038_submission_package_v1_0/` — shipped
- `cursor_prompts/CURSOR_PROMPT_MIG_313_M_STAGE_CORRUPTION_FIX_20260505.md` — running now in cursor
- `cursor_prompts/CURSOR_PROMPT_MIG_310_V2_FNA_NLP_HP_CORPUS_20260505.md` — running now in cursor

---

## Ground rules

1. **Always read this brief end-to-end first.** It encodes hard-won state.
2. **Never touch `M025_FINAL_PACKAGE/` or `M025_submission_package*/`** — Logan's other chat owns those. If you need M025 numbers, query MD `manuscript_workspace.m025_analytic_master_*_v1` directly.
3. **Always insert a signoff row after a migration lands.** No silent operations.
4. **Always validate against locked numbers before announcing success.** Reproducibility is the bedrock.
5. **Cursor handles long-running NLP / multi-hour pipelines.** Cowork handles probes, small-medium ops, repo hygiene, and writing prompts.
6. **Push via Desktop Commander, not the workspace bash.** Sandbox can't auth to GitHub.
7. **Don't over-edit Logan's manuscript prose.** Audit numbers, flag deltas, recommend changes — but don't rewrite his text.

---

**Begin with Step 1 of First Action above.** Don't ask clarifying questions until you've finished steps 1–6 and have the comprehensive review ready.
