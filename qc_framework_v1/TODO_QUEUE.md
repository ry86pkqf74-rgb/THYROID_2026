# Thyroid Canonical Publication v1.0 — TODO Queue

**Last updated:** 2026-04-30 (post-mig_203 apply)
**Current state:** 5-gate audit **v11 `172 / 0 / 0 / 0 / 0`**; PM **~1,606 v / 24 na / 0 not_started** (registry resynced to physical cols); 62/62 Tier-2 canonicals verified; cohort parity 10,871 / 10,871 ✓
**Manuscript readiness verdict:** ✅ **READY** for survival/recurrence/outcomes analyses; ~5% residual cleanup work remaining for "fully verified to every CF"

---

## §1 Pending Logan ratifications (BLOCKERS for downstream apply)

| Item | Decision needed | Default if no answer | Affects |
|---|---|---|---|
| **mig_194 unblock path** | Pick A (build NLP source) / **B (shell-only, Cowork+Cursor recommend)** / C (cancel) | B | mig_189 → CF-117-US-GLAND-PARENCHYMA closure |
| **r1c bucket-3 review** | 50 ambiguous PM-only-size patients — keep as `t_resolution_source='ambiguous_pm_size_only_logan_pending'` indefinitely OR hand-curate in CSV | Keep as-is for manuscript; flag in supplement | path_malignant_events_v1 r1c residual |
| **r1d T4 invasion adjudication** | 374 events flagged as candidate T4a/T4b — Logan reviews CSV, ratifies any auto-overrides for cases where mig_188b §D didn't catch | Auto-resolved already (mig_188b §D `t4_invasion` CTE caught 15 invasion-positive events) | path_malignant_events_v1 T4-resolved cohort |
| **r1e mixed-histology stage_group** | 168 events with multi-component histology (e.g., 'MTC \| PTC') — confirm Rule #5 most-aggressive component | mig_188b §G already applies Rule #5 | PM stage_group_resolved for mixed cohort |
| **Methods section voice pass** | mig_195 starter has ~12 `[N]` / `[start year]` / `[cite IRB]` placeholders | Defer until manuscript writing phase | manuscript |

---

## §2 Cowork-direct apply queue (no ratification needed)

| ID | Lane | Effort | Status | Notes |
|---|---|---:|---|---|
| **mig_201** | Apply mig_190 disposition-C closures (4 stale CF tags) | 15 min | Queued | Append `CLOSED by mig_<N>` to CF-mig156-COHORT-UNIFORM-FALSE-prm_high_risk_marker_any (closed by 156b), CF-mig156-ANY-RECURRENCE- (closed by 163b), CF-mig134-PM-LAB-DATE-ANCHOR (closed by 160), CF-mig154-MARGIN-MM-VARCHAR-RETYPE (closed by 154). Registry-only. |
| **mig_202** | Run all 5 mig_196 analytic templates + Table 1 + cohort flow SQLs against MD; populate CSVs | 30 min | Queued | Cowork executes the SELECTs from mig_195 + mig_196 deliverables and exports CSVs (placeholders currently). Verifies templates work end-to-end. |
| **mig_203** | gate5 → 0: extend audit allowlist for `_built_at`/`_derived_at`/`_resolved_at` suffix patterns + `_confidence` substring | 20 min | **Applied** | Applied 2026-04-30: `203_gate5_zero_audit_allowlist_extension_20260430.sql` + `queries/cleanliness_audit_v11.sql`. CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION **CLOSED**. |
| **mig_204** | PM signoff registry refresh (account for ~10 new `*_resolved` cols added by mig_188b) | 15 min | **Superseded** | Delivered as part of mig_203 (ten registry INSERTs + PM rollup resync). |

Total Cowork-direct effort: ~80 min.

---

## §3 Cursor/Cline-authored work pending dispatch

These prompts were authored in prior rounds but not yet dispatched. They have conditional pre-flight gates that defer if prerequisites aren't met. **All prerequisites are now met (chain applied),** so they can be dispatched.

| ID | Prompt path | Tool recommendation | Effort | What it does |
|---|---|---|---:|---|
| **mig_191** | `cursor_prompts/CURSOR_PROMPT_mig191_post_apply_manuscript_readiness_v11_20260430.md` | **Cursor Composer** | ~1 hr | Post-apply audit + v11 Cowork handoff doc + v1.0 manuscript readiness report. Heavy templating off existing v10 doc. |
| **mig_193** | `cursor_prompts/CURSOR_PROMPT_mig193_r1bde_logan_review_csv_unblock_20260430.md` | **Cline + GPT-5.5** | ~2 hr | Diagnose r1b 0-row return; regenerate r1b/r1d/r1e + r1c CSVs from post-apply state. Investigation + SQL authoring. |

---

## §4 Next-round Cursor/Cline prompts (4-6 to author this round)

See §6 below.

---

## §5 Carry-forward CFs (informational — manuscript appendix candidates)

| CF tag | n_cols | Disposition | Manuscript treatment |
|---|---:|---|---|
| **CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION** | — | NEW (chain). Cowork patched live VIEW; Python source still has bug. | Source-review lane; not data-quality issue (live VIEW correct) |
| **CF-mig186-WHO-2017-NIFTP-RECLASS** | 13 | OPEN by mig_186b. 220 events excluded; preserved in indeterminate landing. | Manuscript methods/limitations footnote |
| **CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION** | 1 | OPEN by mig_186b. ~115 patients with biopsy-only/imaging-only malignancy evidence. | Manuscript methods footnote OR sensitivity analysis |
| **CF-mig185-EVENT-GRAIN-SOURCE-DISTINCT-PRESERVED** | — | OPEN by mig_185b. 525 source-distinct dups preserved on events; analytic SQL must use COUNT DISTINCT for tumor counts. | Manuscript methods footnote |
| **CF-mig160b-AUDIT-ALLOWLIST-PATTERN-EXTENSION** | 6 | **CLOSED** by mig_203 (2026-04-30). | None — operational only |
| CF-117-US-GLAND-PARENCHYMA | 28 | mig_198 (mig_194 Option B apply) closes. | None if Option B; manuscript footnote if Option C |
| CF-117-US-EXAM-ID-PORTABILITY | 53 | partially closed via mig_171b/187; remaining = US-nodule rebuild (separate lane) | Manuscript footnote: US-nodule v2 is future work |
| CF-87-AJCC | 36 | **CLOSED** by mig_188b | — |
| CF-mig171b-EXAM-MASTER-REBUILD | 77 | **CLOSED** by mig_187 R-A | — |
| CF-mig58 / CF-mig136 / CF-mig145 / CF-mig151 / CF-mig156-N- / CF-mig166 / CF-PMH | 7 tags / ~99 cols | mig_190 disposition B (tag-only / retain for trace) | Manuscript supplementary appendix candidates |

---

## §6 Master checklist to "fully verified, statistical-analysis-ready"

### Already done ✅
- [x] All 62 Tier-2 canonical tables verified (100%)
- [x] Patient master backbone 100% verified (~1,606 v / 24 na post-mig_203 registry sync)
- [x] AJCC `*_resolved` cols populated on path_malignant + PM
- [x] T0 cohort transparently labeled (60 events; 13 no-primary + 50 ambiguous + dups)
- [x] NIFTP/UMP exclusion with full audit trail (220 events)
- [x] Source-distinct duplicate-grain flag on path_malignant_events
- [x] LN-NLP exam-date integration (G9 PASS, 0 fallback IDs)
- [x] Cohort parity 10,871 / 10,871
- [x] PM date cols all DATE type (mig_160b); gate5 v11 audit **0** (mig_203 allowlist + suffix patterns)
- [x] Manuscript Table 1 SQL + cohort flow SQL authored (mig_195)
- [x] 5 manuscript analytic SQL templates authored (mig_196)
- [x] Per-canonical methods footnotes for ~83 tables (mig_197)
- [x] Data dictionary CSV/SQL exported (mig_197)
- [x] Cleanliness audit gates 1-4 fully clean

### Remaining ~5% before "fully verified to every CF"
- [ ] mig_198 — mig_194 Option B apply (shell-only US gland v2 events/rollup; closes CF-117-US-GLAND-PARENCHYMA)
- [ ] mig_201 — disposition-C 4-CF closure (registry-only)
- [ ] mig_202 — populate Table 1 + cohort flow + analytic template CSVs from MD
- [x] mig_203 — gate5 audit allowlist extension → 0 + PM registry refresh (supersedes mig_204 scope)
- [x] mig_204 — merged into mig_203
- [ ] mig_191 dispatch — post-apply audit + v11 doc (Cursor)
- [ ] mig_193 dispatch — r1b/r1d/r1e Logan-review CSV regen (Cline GPT-5.5)
- [ ] r1c bucket-3 (50 events) Logan review OR ratify "leave as ambiguous_pending"
- [ ] r1d/r1e CSVs Logan adjudication (~542 events)
- [ ] CF-mig187-SCRIPT-366-EXAM-DATE-FILTER-REGRESSION Python source fix
- [ ] Methods section Logan voice pass

### Gating items for actual manuscript submission (separate from data-quality)
- [ ] IRB approval / data-use agreement reference in manuscript methods
- [ ] Final cohort definition Logan-ratified
- [ ] Analytic plan (specific endpoints, primary/secondary outcomes, multivariable approach)
- [ ] Statistical software/version locked
- [ ] Sensitivity analyses scoped (NIFTP exclusion impact, source-distinct dup impact, T0-as-T1 impact)

### Estimated time to "fully verified, statistical-analysis-ready"
- Cowork-direct work: ~80 min (mig_201/202/203/204)
- Cursor/Cline work: ~3 hr (mig_198 + mig_191 + mig_193)
- Logan review work: ~4-8 hr (r1c/r1d/r1e CSVs, methods voice pass)
- **Total: 1-2 working days of mostly automated work + Logan review time**

---

End of TODO queue.
