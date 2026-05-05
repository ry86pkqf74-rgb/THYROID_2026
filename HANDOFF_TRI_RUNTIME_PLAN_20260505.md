# Tri-runtime next-steps plan — post-mig_313/314 + mig_310 v2 Phase A0

> Live as of 2026-05-05 ~07:00 UTC. Heads: `9ceaa5e` on origin/main. Latest signoffs: mig_314 (Cowork, M036 v3 cascade), mig_310_phaseA0 (Cowork progress marker).

---

## Where mig_310 v2 actually stands

The cursor commit `9ceaa5e` shipped Phase A0 only. The `Closes CF-FNA-SIZE-CM-NULL (pending Cortex run + QA pass)` line in the commit body confirms this was intended as a partial landing. Five phases remain before the carry-forward closes and `imaging_fna_linkage_v4` is queryable.

| Phase | Runtime | Status | What it does |
|---|---|---|---|
| **A0** | MD | ✅ DONE | Build `fna_content_corpus_v1` (3,432) + `fna_event_note_linkage_v1` (2,756/8,050 = 34.2%) views |
| **B** | MD→SF | PENDING | Export MD parquet (~2,756 linked notes) via `COPY (...) TO '<parq>'` |
| **C** | SF | PENDING | `PUT` parquet to `@COWORK_STAGE`, `COPY INTO FNA_NOTES_MIG310_V2` |
| **D** | SF (Cortex) | PENDING | 4× `SNOWFLAKE.CORTEX.EXTRACT_ANSWER` per note → `NLP_FNA_SIZE_FULL_RESULTS_v1` + `NLP_FNA_SIZE_PATIENT_ROLLUP_v1`. Approx **11,024 Cortex calls** |
| **E** | SF | PENDING | Sample-200 validation probe with precision estimates |
| **F** | SF→MD | PENDING | Mirror SF rollup → `manuscript_workspace.nlp_fna_size_rollup_v1` |
| **G** | MD | PENDING | `scripts/mig_310_fna_size_mirror.py --md --signoff` builds `imaging_fna_linkage_v4` and inserts the formal `mig_310` signoff |

A signed-off `mig_310_phaseA0` progress marker is in `main.signoff_migration` so the registry reflects partial state.

---

## Three lanes — what runs where, why

### Lane 1 — Snowflake (one-shot, cost-bearing, owner = Cursor or Cowork patched-PAT)

**Phase A0→G of mig_310 v2 — single pipeline invocation.**

```bash
SNOWFLAKE_PAT=$SNOWFLAKE_PAT \
  .venv/bin/python snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md
# When happy with sample-200 precision:
.venv/bin/python scripts/mig_310_fna_size_mirror.py --md --signoff
```

- **Cost:** ~11k EXTRACT_ANSWER calls. Per-call cost is on the order of fractions of a credit; total cost is bounded but not free. **Run with `--pilot` first** (limits to 200 random notes, ~$0.20 worst case) to confirm precision and shape before committing to the full run.
- **Wall time:** EXTRACT_ANSWER on a warehouse can take several minutes per thousand notes; full run plausibly 10–30 min on COMPUTE_WH.
- **Owner choice:**
  - **Cursor** (default per ground rule #5: long-running NLP pipelines are cursor's domain): hand off via a cursor prompt that includes pilot first, then full run, then signoff.
  - **Cowork** (alternative): the patched-PAT pattern works fine for this. The script is self-driving — invoke once, it handles every phase. Cowork can do the pilot + full run if Logan wants the audit trail in this session and is OK leaving the chat open for the duration.
- **Recommendation:** Cursor. The ground rule exists for a reason — these pipelines occasionally retry, fail mid-run, or need a transient fix. A separate cursor agent that lives only inside that workflow is cheaper than letting Cowork's cross-cutting context get tangled with a 30-minute Cortex job.

### Lane 2 — Cursor (heavy migrations + structural rebuilds)

| Job | Why cursor, not Cowork |
|---|---|
| **mig_310 v2 full pipeline** (above) | NLP pipeline + Cortex compute time |
| **mig_315 — `cohort_m044_ajcc_ete_v1` rebuild** | Table currently has duplicate columns (every column appears twice — JOIN that wasn't column-projected). Needs explicit column selection rebuild + dependency cascade audit (Cortex Analyst will choke on dupes). Multi-step DDL with verification. |
| **mig_316 — `cohort_m037_ln_predictors_v1`** | **DONE** — TABLE materialized (n=2,234) = M043 filtered by mig_280 LN eligibility; matches `cohort_m037_ln_metastasis_v1` rids exactly. |
| **M032 era × stage refresh** | M032 is shipped; era-stratified counts had pre-2008 IVB inflation. Re-run produces a v2 numerical patch to the submission package (potentially deltas to multiple table cells + figure regeneration). |

For each, deliver via `cursor_prompts/CURSOR_PROMPT_MIG_<N>_*.md` with the standard template (problem, recipe, validation gates, signoff SQL).

### Lane 3 — Cowork (small/medium ops + audits + writing briefs)

| Job | Why Cowork |
|---|---|
| **M044 v6 Table 1 numerical patch** | Stage IVB went 684→61 (−91%) but **regression unchanged** (T+N stage covariates, not stage_group). docx surgical edit + LaTeX fragment regeneration. Fits inside one Cowork turn. |
| **M036 v3 manuscript prose draft** | Brief at `studies/m036_ata_rss_comparison_v3/M036_READY_FOR_WRITING_v3.md`. Numbers locked. ~3 hour writing job. |
| **M029 + M019 ready-for-writing briefs** | Analyses already landed (cursor commits `a9bc38c` + `0f91f52`). Each needs a brief in the M036 v3 mold. |
| **86 vs 114 M1 audit** | 86 patients fired `high:distant_metastasis` rule but CPM has 114 M1. Probable explanation: 28 non-DTC malignancies (anaplastic, MTC, PDTC, FTUMP) drop out of the ATA classifier. Confirm with one query before M036 prose claims "true M1 ≈2.1%". |
| **M036 KM curves + reclassification crosstab interpretation** | Read CSV/PNG outputs from `studies/m036_ata_rss_comparison_v3/`, write the Results section interpretation. |
| **Cortex Analyst smoke tests** post-mig_310 Phase G | Re-run `cortex analyst query "what is the per-tr ROM..."` against M025 nodule semantic model; confirm TR2/TR3/TR4/TR5 within tolerance. |
| **m045–m082 cohort triage** | 38 scaffolded cohorts. Walk the list, flag clinical priority, recommend which deserve next analyses. |
| **Repo hygiene + git push** | Standard ongoing work. |

---

## Recommended order of operations

### This session (or next Cowork turn)

1. **Cowork — M044 v6 Table 1 patch.** Smallest path to closing a published-ready manuscript. Keep regression sections verbatim; only update Table 1 stage rows, eMethods stage paragraph, and any Stage IV mention in Results §1. Ship as `M044_FINAL_PACKAGE/M044_ETE_FINAL_Manuscript_v6.docx` + locked stats xlsx update.
2. **Cowork — 86-vs-114 M1 audit query** (5 minutes). Resolves a footnote-level question for M036 prose.

### Next cursor session

3. **Cursor — mig_310 v2 Phase A→G pipeline.** Pilot first (200 random notes), inspect precision, then full run + signoff. Closes `CF-FNA-SIZE-CM-NULL`.
4. **Cursor — mig_315** (`cohort_m044_ajcc_ete_v1` rebuild without dup columns). Should land with explicit `SELECT col1, col2, ...` projection, no `SELECT *` or `SELECT a.*, b.*`.

### After mig_310 v2 Phase G

5. **Cowork — Cortex Analyst smoke test on M025 nodule semantic model.** Confirm TR-level ROM still hits locked numbers; FNA-size covariate is informational and shouldn't shift the per-TR aggregates more than ±0.1 pp.
6. **Cowork — re-bind FNA-size into M025 nodule cohort if material.** If size-resolved coverage moves substantially (v3 vs v4), Cowork rebuilds the M025 nodule analytic master and Logan's other chat re-checks the manuscript.

### Deferred

7. **Cursor — M032 v2 era × stage refresh.** Required before any republication of M032; not urgent if M032 isn't being touched this cycle.
8. **Cursor — mig_316** (M037 cohort materialization or naming alignment). Required before M037 is touched again.

---

## Carry-forward register (current)

| Carry-forward | State | Owner | Closes when |
|---|---|---|---|
| `CF-MSTAGE-CORRUPTION` | CLOSED | (cursor mig_313, cowork mig_314) | already closed |
| `CF-FNA-SIZE-CM-NULL` | OPEN | cursor (mig_310 Phase A→G) | imaging_fna_linkage_v4 built + signed off |
| `CF-mig_305-SP-V3-HANG` | CLOSED | (cowork mig_309) | already closed |
| `CF-M044-DUP-COLS` | OPEN (new) | cursor (mig_315) | cohort flat rebuilt with explicit projection |
| `CF-M037-COHORT-MISSING` | **CLOSED** (mig_316) | cursor | `cohort_m037_ln_predictors_v1` TABLE = M037 LN-eligible subset of M043 |

---

## Decision rules to keep

1. **Cursor owns long-running, retry-prone, NLP/Cortex/multi-table-rebuild pipelines.** If a job runs >5 minutes or touches >3 tables, it goes to cursor.
2. **Cowork owns audits, probes, single-cohort-flat operations, manuscript-writing briefs, and small docx/LaTeX patches.** If a job is "look, conclude, write a paragraph" or "rebuild one Table 1 from a corrected cohort," it stays in Cowork.
3. **Snowflake compute is invoked only by scripts checked into the repo.** No ad-hoc Cortex calls — the SQL/Python pair is the audit trail.
4. **Every landing gets a signoff row.** Partial landings get a `mig_<N>_phase<X>` progress marker and an explicit "PARTIAL" prefix in the summary so the registry isn't ambiguous.
5. **Locked numbers are the regression test.** CPM=10,871 / malig=4,019 / M025 nodule TR-ROM (12.90/9.13/18.72/26.11) / M044 aOR 1.77 [1.15–2.71] — any drift after a migration is a stop-the-line moment.

---

## Files written this turn

- Inserted: `main.signoff_migration` row for `mig_310_phaseA0`
- Wrote: `HANDOFF_TRI_RUNTIME_PLAN_20260505.md` (this file)

---

## What the next Cowork session should do first

Run the M044 v6 Table 1 patch. Everything else in Lane 3 is sequence-flexible; M044 is the only one that closes a published-ready manuscript with a single ~1-hour edit pass.
