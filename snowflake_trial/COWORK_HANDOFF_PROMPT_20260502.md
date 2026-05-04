# Cowork Handoff — Snowflake/MotherDuck/Cursor Workflow
**Generated:** 2026-05-02. Master document for resuming work in a new Cowork chat.

Paste sections of this into the next chat to bootstrap context. The new Cowork instance has access to the same memory files (auto-loaded), but this captures the live state + workflow that's not in static memory.

---

## §0 — Bootstrap message for new Cowork chat

> I'm Logan Glosser, Emory thyroid surgery researcher. I've been running a multi-week project where Cowork drives a Snowflake Cortex AI sidecar that validates the MotherDuck canonical thyroid cohort (10,871 patients) and feeds findings into Cursor migrations that fix data in MotherDuck per Protocol v2.
>
> The architecture is: **Cowork (you) ↔ Snowflake (validation/manuscripts) ↔ MotherDuck (canonical SOT) ↔ Cursor (DML executor)**. Read `snowflake_trial/COWORK_HANDOFF_PROMPT_20260502.md` for the full workflow + current state. Auto-memory at `reference_snowflake_access.md` and `reference_archive_pub_v1_0_location.md` cover auth + paths.
>
> Current focus: completing M044 (ETE & Outcomes) + M038 (Massive Goiter Definition Paper) manuscripts. Lower-priority manuscripts in flight: M025 (TIRADS Performance), M032 (25-yr Descriptive), M004 (Autoimmune+Cancer), M037 (LN Predictors).

---

## §1 — Architecture & roles

### Cowork (this chat)
- Authors Cursor mig prompts following Protocol v2 conventions
- Drives Snowflake Cortex AI for validation + manuscript outputs (Tables, regressions, figures-data, AI_EMBED clustering, AI_CLASSIFY, AI_AGG)
- Reads MotherDuck via Cowork's MotherDuck MCP (server-side authenticated, read-only by default; read-write requires Logan's per-call permission)
- Refreshes Snowflake from MotherDuck via local Mac scripts (uses MD service-account token from `motherduck.local.toml`)
- Writes reports + scripts to `snowflake_trial/`, prompts to `cursor_prompts/`, then commits + pushes

### Snowflake (validation sidecar; not source of truth)
- Account: `qcc02515.us-east-1` (AWS_US_EAST_1), user `LGLOSSE13`, role `ACCOUNTADMIN`, warehouse `COMPUTE_WH`
- Database: `THYROID_VALIDATION.PUBLIC`
- ~10 canonical tables loaded as VARIANT $1 + flat views built via `04_build_flat_views.py`
- Cortex AI SQL functions: `AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_AGG`, `AI_EMBED` — all work on trial despite `cortex --print` being blocked
- 30-day trial; converts to ~$25/mo on **2026-05-29**

### MotherDuck (canonical source of truth)
- Database: `thyroid_canonical_publication_v1_0`
- 10,871 patients × 1,630 columns on `canonical_patient_master`
- Pre-snapshot archives at `"Thyroid 2026 UPdated".archive_pub_v1_0.*` (separate DB, quoted path required)
- `main.signoff_migration` table tracks every applied mig

### Cursor (DML executor + manuscript text editor)
- Receives Cowork-authored prompts in `cursor_prompts/CURSOR_PROMPT_MIG_NNN_*.md`
- Two routing modes: **Composer** (mechanical apply) vs **Chat → Composer** (rule disambiguation needed first)
- Pushes to GitHub `origin/main`; Cowork pulls/refreshes from there

---

## §2 — The validate-fix-revalidate ratchet (the core pattern)

```
1. Cowork runs Snowflake validation prompt → finds N rows that violate a rule
2. Cowork drafts a Cursor mig prompt with live stats (cursor_prompts/CURSOR_PROMPT_MIG_NNN_...md)
3. Logan runs prompt in Cursor:
   - Composer-direct for mechanical migs
   - Chat (Sonnet 4 / GPT-5) first for rule disambiguation, then Composer
4. Cursor applies UPDATE in MotherDuck per Protocol v2:
   a. Pre-snapshot to archive_pub_v1_0
   b. Apply UPDATE
   c. Verify count = expected target
   d. INSERT main.signoff_migration row
   e. surgical git add (explicit paths only, never -A)
   f. commit + push
5. Cowork pulls + re-exports MD → Snowflake → flat views → re-runs validation
6. Counts drop to disposition target → CF closed
```

Round-trip latency: **~70 seconds** (export 5s + load 30s + flat 10s + validate 20s).

---

## §3 — Authentication (the two non-obvious blockers)

### Snowflake PAT auth
- PATs require an authentication policy because trial enforces MFA by default; without policy, all PAT auth fails as "invalid".
- The Python connector v4.4.0 has a bug — leaves `TOKEN` body field empty AND strips region from `ACCOUNT_NAME`. Workaround in `snowflake_trial/scripts/_sf_client.py`:
  ```python
  import snowflake.connector.network as _net
  _orig = _net.SnowflakeRestful._post_request
  def _patched(self, url, headers, body, *args, **kwargs):
      if "/session/v1/login-request" in url:
          d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
          d["data"]["ACCOUNT_NAME"] = "qcc02515.us-east-1"
          if not d["data"].get("TOKEN"):
              d["data"]["TOKEN"] = PAT
          body = json.dumps(d)
      return _orig(self, url, headers, body, *args, **kwargs)
  _net.SnowflakeRestful._post_request = _patched
  ```
- Trial blocks `cortex --print` headless mode. AI SQL functions still work via the connector — that's the path validation uses.

### MotherDuck token (Mac-side)
- Logan's `motherduck.local.toml` contains `MD_SA_TOKEN` (gitignored)
- Set both env vars before running scripts: `export MOTHERDUCK_TOKEN=$(grep MD_SA_TOKEN motherduck.local.toml | sed 's/.*"\(.*\)".*/\1/') && export motherduck_token=$MOTHERDUCK_TOKEN`
- If the script triggers SSO browser flow (`auth.motherduck.com/activate`), Logan must authenticate within ~60 seconds or the export aborts. The token-from-toml approach bypasses this.
- Cowork's MotherDuck MCP uses a separate server-side credential that doesn't need Logan's interaction — preferred for read-only verification.

### Snowflake INFER_SCHEMA quirk
- INFER_SCHEMA on view-derived Parquet returns 0 columns. When loading `canonical_us_patient_master_VIEW_v2`, the FLAT view must be hand-built with explicit `$1:field::TYPE AS field` projection.
- See `_sf_client.py` notes + the pattern used in scripts that handle `*_VIEW_*` Parquets.

---

## §4 — File conventions

```
/Users/ros/THyroid 2026/                          ← repo root
├── motherduck.local.toml                          ← MD service-account token (gitignored)
├── motherduck_client.py                           ← token resolver
├── scripts/
│   ├── _md_connect.py                             ← MD connection helper (locks DB context)
│   ├── mig_NNN_*.py                               ← Cursor-authored mig runners
│   └── output/                                    ← apply logs, dispositions, probe results
├── cursor_prompts/                                ← Cowork-authored mig dispatches
│   ├── CURSOR_PROMPT_MIG_NNN_*.md                 ← per-mig dispatch
│   └── SNOWFLAKE_ROUND_N_CURSOR_ROUTING_*.md      ← per-round routing summary
├── qc_framework_v1/migrations/                    ← versioned SQL files (Cursor-applied)
│   └── NNN_*.sql
├── manuscript_outputs/v1_0_20260501/              ← active manuscript drafts
│   ├── M032_25yr_descriptive_analysis_DRAFT_v1.md
│   ├── M044_ETE_manuscript_draft.md (top-level too)
│   └── M038_massive_goiter_manuscript_draft_v1.md (mig_276 will create)
├── M044_submission_package_v1_0/                  ← M044 production figures + tables
│   └── 06_figures/
└── snowflake_trial/                               ← Cowork's working area
    ├── COWORK_HANDOFF_PROMPT_20260502.md          ← (this file)
    ├── SCAFFOLD.md                                ← original operational scaffold
    ├── NEXT_RUNS_PLAN_V2_20260502.md              ← Tier 5-7 backlog
    ├── MANUSCRIPT_COMPLETION_ROADMAP_20260502.md  ← M044/M038 runway
    ├── FINDINGS.md / FINDINGS_ROUND5.md           ← finding catalogs
    ├── SESSION_SUMMARY_20260501.md                ← prior session summary
    ├── cortex_analyst/
    │   ├── thyroid_2026_semantic_model.yaml       ← Cortex Analyst semantic model
    │   └── UPLOAD_INSTRUCTIONS.md
    ├── scripts/
    │   ├── _sf_client.py                          ← Snowflake client w/ PAT bug patch
    │   ├── 01_export_md_to_parquet.py             ← MD → local Parquet
    │   ├── 02_load_to_snowflake.py                ← PUT + CTAS into Snowflake
    │   ├── 03_run_validation_prompt1.py
    │   ├── 04_build_flat_views.py                 ← VARIANT $1 → flat views
    │   ├── 05_prompt2_molecular.py through 14_prompt8_complications.py
    │   ├── 08_cohort_views.py                     ← M004/M032/M037 cohort builders
    │   ├── 09_m037_table1.py                      ← M037 Table 1
    │   ├── 10_generate_cursor_prompts.py          ← Snowflake → Cursor prompt drafter
    │   ├── 15-18_*                                ← Prompts 9-12 (round 6)
    │   ├── 19_m044_table1.py                      ← M044 Table 1
    │   ├── 20-22_*                                ← M032/M004 Tables + M037 Table 2
    │   ├── 23_ai_embed_phenotype_clustering.py    ← AI_EMBED + KMeans
    │   ├── 24_m044_cox_ph.py                      ← M044 Cox PH
    │   ├── 25_m037_sensitivity_ln_both.py
    │   ├── 26_prompt11_reverify_mig265.py
    │   ├── 27_m025_tirads_performance.py          ← M025 TIRADS sens/spec/PPV/NPV
    │   ├── 28_m032_braf_year_trend.py
    │   ├── 29_m044_cox_sensitivity_ln_clean.py
    │   ├── 30_m044_km_forest_data.py              ← M044 Figure 2 + 3 data
    │   ├── 31_m038_massive_goiter_table1.py       ← M038 cohort scaffold + Table 1
    │   ├── 32_m044_race_disparity.py
    │   └── 33_m044_cox_interactions.py
    ├── reports/                                   ← Cowork-generated outputs
    │   ├── 01_demographics_validation.md through 12_synoptic_validation.md
    │   ├── m037_table1.md, m037_table2_logreg.md, m037_sensitivity_ln_both.md
    │   ├── m044_table1.md, m044_cox_ph.md, m044_cox_sensitivity_ln_clean.md
    │   ├── m044_km_curves_data.csv, m044_forest_plot_data.csv
    │   ├── m044_race_disparity.md, m044_cox_interactions.md
    │   ├── m025_tirads_performance.md, m032_braf_year_trend.md, m032_table1.md
    │   ├── m004_table1.md, m038_table1_massive_goiter.md
    │   ├── ai_embed_phenotype_clustering.md
    │   └── 11_comorbidity_revalidation_post_mig265.md
    └── parquet/                                   ← gitignored intermediate Parquet
```

---

## §5 — Cursor mig prompt template (use this verbatim for new prompts)

```markdown
# Cursor Composer Dispatch — mig_NNN: <one-line title>

**Generated:** YYYY-MM-DD by Cowork.
**Lane:** mig_NNN — <what the mig does + why it exists>.
**Recommended agent:** **Cursor Composer** | **Cursor Chat (Sonnet 4 / GPT-5) → Composer**.
**Estimated runtime:** N min.
**Triggered by:** <validation prompt / mig_X close-out / manuscript need>.
**Severity:** LOW | MED | HIGH.
**Closes carry-forward:** CF-...

---

## §0 — First message to paste into Cursor

> mig_NNN dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_NNN_*.md` end-to-end. ...

## §1 — Why this lane exists
<live stats from Snowflake; cite exact n's, p-values, Cohort sizes>

## §2 — Pre-task probes (read-only)
<SQL probes Logan/Cursor runs first to confirm pre-state; surface to Logan if Chat-first>

## §3 — Apply
- §3a Pre-snapshot to "Thyroid 2026 UPdated".archive_pub_v1_0.<table>_pre_migNNN_YYYYMMDD
- §3b UPDATE main.<table> SET ... WHERE ...
- §3c Registry signoff: INSERT INTO main.signoff_migration ...

## §4 — Verify
<verify queries; expected counts>

## §5 — Snowflake re-verify (if affects CPM)
<bash + python script invocations>

## §6 — Carry-forwards
- CF-... → CLOSED on apply
- CF-... → OPEN if scope deferred

## §7 — Surgical git add (explicit paths only, never -A)
```
qc_framework_v1/migrations/NNN_*.sql
scripts/output/mig_NNN_*.txt
```
```

---

## §6 — Current state (2026-05-02 EOD)

### Migs applied & verified

| Mig | What | Apply commit | Snowflake verified |
|---|---|---|---|
| 254 | M1+Stage II reconcile (40 flips: 29 MTC + 2 ATC + 9 PDTC) | 531bd74 | ✓ M1+II 1058→1018 |
| 254b | rid 9600 MTC residual | 28fa4a7 | ✓ MTC IVB 59→60 |
| 255 | Recurrence flag/timing reconcile (B′+A′ hybrid) | 06a41b9 | ✓ mismatch=0, flag=TRUE=560 |
| 256 | 6→8 benign+recurrence dispositioned | 05c38f9 | adjudication done; DML pending |
| 257 | followup>survival clamped | a25b539 | ✓ |
| 258→259file | LN status_source Rule C (column added) | c981d26 | ✓ both=1126/staging=1509/NULL=8236 |
| 260 | TIRADS re-point (downstream code) | d7e7fbc | ✓ TIRADS×ROM table working |
| 261 | path_synoptics CAP norm (typos + DATE) | ca82d8a | ✓ on MD |
| 262 | LN flag rebuild + imaging YY-typos | b30510f+aad47d2+aa0c2ac+470439a | ✓ suspicious_ln 8→1733 |
| 263 | AJCC overlay Option B (IVA/IVC→IVB collapse) | 5351070 | ✓ stage_group_resolved + _v2 cols |
| 264 | Bethesda-2 audit (read-only) | 4aa7940 | docs only |
| 264b | Bethesda-2 obvious-fix (NIFTP+FA+neg-FNA) | (running today) | ✓ via 4,113 cohort verify |
| 265 | PMH _definitive rule | 5f05f9a | ✓ all 9 conds at parity |
| 266 | Bulk manuscript footnotes | a4762e4+e9bad1b | text-only |
| 267 | canonical_histology_lookup_v1 SSOT | (Logan ran) | ✓ 38 mappings loaded |
| 268 | Focality residual cleanup | (verified) | ✓ |
| 270 | Snowflake re-point to histology SSOT | c31407d | ✓ |
| 271 | NIFTP+AJCC sweep (post-264b) | (Logan ran) | ✓ 4,113 / 37.83% |
| 273 | M038 cohort view in MD | (Logan ran) | ✓ same 4 buckets as Snowflake |

### Migs in flight (Logan running now)
- mig_274 — M044 figure render pipeline (KM Fig 2 + Forest Fig 3)
- mig_275 — M038 surgical complexity column scaffold (op time / EBL / LOS)
- mig_276 — M038 manuscript draft scaffold (methods + results)

### Migs queued / proposed
- mig_252 — comp_*_confirmed rollup fix (Logan-internal queue; gates M038)
- mig_269 — canonical_recurrence_events_v1 SSOT (optional)
- mig_272 — NLP refresh batch coordinator (defer to post-trial)
- mig_277+ — TBD based on next round

### Cohort numbers (post-mig_271)
- Total cohort: 10,871 patients
- Malignant: **4,113** (37.83% / 37.8% per ROUND(,1))
- mig_252 ≥200g focal cohort: 475 patients
- mig_258/259 ln_status_source: both=1,126 / staging=1,509 / NULL=8,236

### Open issues / Logan-decisions pending
- **NIFTP+IS_MALIGNANT=TRUE = 95** (mig_264b/271 only handled the 22 with Bethesda 2; 73 NIFTPs at other Bethesda values stay malignant per AJCC pre-2017 convention). Decide: scope a follow-on mig that reclassifies all NIFTPs, or document as known limitation.
- **Mig_269 priority** — only worth doing if M044 needs cleaner recurrence inputs.
- **NLP refresh batch (mig_272) scope** — closes 749 vasc invasion under-fires + smoking/family-hx coverage gaps. Big effort; defer to post-trial unless M032/M037/M044 manuscripts are blocked without it.

---

## §7 — Manuscript completion runway

### M044 ETE & Outcomes (~85% complete)
**Done:** Table 1 baseline, Cox PH multivariable + sensitivity (cleaner LN), KM curves data, Forest plot data, race-disparity sub-analysis, Cox interactions (all NS — robust null effect), Methods footnotes (mig_266).

**Remaining: 1-2 Snowflake runs + mig_274 figure render + Logan text editing**
1. Post-mig_264b/271 cohort refresh (~24 NIFTP/FA shift the denominator) — 30 min
2. Time-to-RAI subgroup — 30 min (optional)
3. mig_274 (running now) — produces Fig 2 (KM by ETE) + Fig 3 (Forest plot)

**Submission-ready: 1-2 more sessions.**

### M038 Massive Goiter Definition Paper (~40% complete)
**Done:** Cohort scaffold (≥200g=475 / 50-199g=2,467 / <50g=6,188), Table 1 by weight strata, MD-mirror cohort view (mig_273).

**Remaining: 4-5 Snowflake runs + 2-3 Cursor migs + Logan text**
1. mig_252 lands → strict complications by weight strata available (1 Snowflake run)
2. mig_275 lands → surgical complexity proxies populated (op time/EBL/LOS in Table 1; 1 Snowflake run)
3. M038 logreg: massive-goiter as primary exposure for any-strict-complication (1 Snowflake run)
4. M038 era × massive-goiter rate trend (1 Snowflake run)
5. M038 cohort flow figure data (1 Snowflake run)
6. Optional: M038 propensity-matched analysis (1 Snowflake run)
7. mig_276 (running now) — manuscript draft scaffold

**Submission-ready: 3-4 more sessions.**

### Other manuscripts (lower priority)
- **M025 TIRADS Performance** — 1 Snowflake run done; manuscript footnote drafted; ready for Logan to ship as-is
- **M032 25-yr Descriptive** — Table 1 + BRAF year trend done; era × outcomes pending (1 run)
- **M037 LN Predictors** — Table 1 + Table 2 + sensitivity all done; race-disparity sub-analysis pending
- **M004 Autoimmune+Cancer** — Table 1 done; might need M044-style Cox PH if survival is in scope

---

## §8 — Open questions / explicit Logan decisions

When the new chat starts, raise these:

1. **mig_274/275/276 status** — what came back? Any execution issues to verify?
2. **NIFTP residual 95 patients** — scope a mig to reclassify all, or document as limitation?
3. **mig_252 timeline** — when does that close? Gates M038's strict-complications table.
4. **mig_269 (recurrence SSOT)** — do or skip? M044 Cox PH uses one-event recurrence; SSOT only matters if multi-event analysis is in scope.
5. **NLP refresh batch (mig_272)** — in scope this trial or post-trial workstream?
6. **PAT rotation** — current PAT expires 2026-05-08 (may have been rotated by then). Generate a 30-day in Snowsight for the next session.

---

## §9 — Calendar / cost

- **PAT expires 2026-05-08** — rotate via Snowsight (Admin → Users & Roles → LGLOSSE13 → Generate token, set 30-day next time)
- **Trial converts 2026-05-29** — set cancel reminder if not converting
- **Credits used so far:** ~$8-10 of $40 trial budget
- **Round 11+ estimated:** another $8-15 for remaining manuscript work + AI_AGG over synoptic_diagnosis if we go that route

---

## §10 — Quick-start commands for new chat

After reading §1-§9 above:

```bash
# 1. Check git state
cd "/Users/ros/THyroid 2026"
git pull --rebase

# 2. Verify mig state on MotherDuck
# (Use Cowork's MD MCP query tool — server-side authed; no SSO needed)
# Quick health check:
SELECT COUNT(*) AS n_total, COUNT_IF(is_malignant) AS n_malig
FROM main.canonical_patient_master;
# Expected: 10871 / 4113 (post-mig_271)

# 3. Refresh Snowflake with latest MD state (Mac-side; needs token)
source .venv/bin/activate
export MOTHERDUCK_TOKEN=$(grep MD_SA_TOKEN motherduck.local.toml | sed 's/.*"\(.*\)".*/\1/')
export motherduck_token=$MOTHERDUCK_TOKEN
export SNOWFLAKE_PAT='<rotate via Snowsight first>'
rm -f snowflake_trial/parquet/canonical_patient_master.parquet
python snowflake_trial/scripts/01_export_md_to_parquet.py
python snowflake_trial/scripts/02_load_to_snowflake.py
python snowflake_trial/scripts/04_build_flat_views.py

# 4. Sanity check
python -c "
import sys; sys.path.insert(0,'snowflake_trial/scripts')
from _sf_client import get_cursor
ctx, cur = get_cursor()
cur.execute('SELECT COUNT(*), COUNT_IF(IS_MALIGNANT) FROM CANONICAL_PATIENT_MASTER_FLAT')
print(cur.fetchone())  # expect (10871, 4113)
ctx.close()
"
```

---

## §11 — Where to find prior session details

- `snowflake_trial/SCAFFOLD.md` — operational scaffold (auth recipes, AI Studio paths)
- `snowflake_trial/SESSION_SUMMARY_20260501.md` — first-day comprehensive summary
- `snowflake_trial/NEXT_RUNS_PLAN_V2_20260502.md` — Tier 5-7 backlog + Cursor mig backlog
- `snowflake_trial/MANUSCRIPT_COMPLETION_ROADMAP_20260502.md` — M044/M038 runway estimates
- `snowflake_trial/FINDINGS.md` — round 2 finding catalog
- `snowflake_trial/FINDINGS_ROUND5.md` — round 5 findings (Bethesda-2, complications)
- `cursor_prompts/SNOWFLAKE_ROUND*_CURSOR_ROUTING_*.md` — per-round Cursor routing summaries
- Auto-memory: `reference_snowflake_access.md` + `reference_archive_pub_v1_0_location.md`

---

## §12 — Methodology rules to maintain (Logan-ratified during this session)

1. **MotherDuck stays canonical** — Snowflake is a validation sidecar, not a write target. All DML flows through Cursor migs.
2. **Protocol v2 enforced for every mig** — pre-snapshot → apply → verify → registry signoff → surgical git add.
3. **`archive_pub_v1_0` lives in `"Thyroid 2026 UPdated"` DB**, not the publication DB. Quoted-DB-name path required.
4. **Surgical git add only** — never `-A`; explicit paths only (per `feedback_surgical_git_add.md`).
5. **2-digit-year convention** (Logan-ratified 2026-04-27): all YY → 20YY. Applies to date derivations.
6. **Clinical event dates = DATE type, never TIMESTAMP** (mig_261 fixed surg_date; build_ts cols exempt).
7. **Cohort malignancy rate is 37.8%** post-mig_271 (4,113 / 10,871; ROUND(,1) gives 37.8 not 37.9).
8. **Manuscript footnotes per mig_266** (F1-F6 conventions for ln_status_source / AJCC IVA-IVC collapse / ETE label norm / LN flag rebuild / Bethesda enrichment / NLP coverage).
9. **Snowflake INFER_SCHEMA returns 0 cols on view-derived Parquet** — hand-build flat view with explicit `$1:field::TYPE` projection for any `*_VIEW_*` source.
10. **AI_EMBED phenotype clusters** are reproducible (random_state=42 in KMeans; 768-d via snowflake-arctic-embed-m-v1.5).
