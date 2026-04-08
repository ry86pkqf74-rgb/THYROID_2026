# Recommendations: generic safety net closure (2026-04-08)

## Decision

**New bespoke validation engines are not necessary** for the four pillars at this time. Existing surfaces (`112`, `119`, `29`, CI metric/uniqueness checks, offline multimodal/provenance tests, contract doc) already cover most intent.

**What is necessary** is **thin integration**: wire existing tests and optionally surface existing `val_*` outputs in CI or on a schedule **without** duplicating logic.

---

## Priority 1 — CI wiring (lowest effort, highest leverage)

1. **Add offline tests to existing pytest jobs in `.github/workflows/ci.yml`**
   - Include `tests/test_linkage_confidence.py` in an appropriate job (alongside multimodal-tests or llm-extraction-gold; both are offline-safe).
   - Include `tests/test_lab_canonical.py` only if CI can supply a **read-only** `thyroid_master.duckdb` artifact or skip remains acceptable; otherwise document as intentional local gate and add a **scheduled** or **manual** job with a checked-in minimal fixture DB (future thin work).

2. **Document the split** in `docs/motherduck_database_contract_v1.md` or `AGENTS.md` (one paragraph): default **push** CI runs formalization `116→112→119` plus offline suites; **full** linkage/chronology `val_*` from `29_validation_engine.py` runs on **episode pipeline** / operator workflows.

---

## Priority 2 — Thin wrapper (optional; still no new validators)

3. **Read-only `29 --md` summary in CI or cron**  
   Add a job that runs `python scripts/29_validation_engine.py --md` then **aggregates only** row counts / FAIL flags from `val_*` tables (no note text), fails on configurable thresholds. Reuses script 29 entirely; no new SQL.

4. **Single source for lab bounds**  
   If drift is observed between `tests/test_lab_canonical.py::PLAUSIBILITY` and `VAL_LAB_CANONICAL_SQL` in script 29, extract shared constants into one module (refactor, not new validation).

---

## Priority 3 — Schema drift artifact (only if product asks for PR-level DDL parity)

5. **Frozen column manifest diff**  
   Generate a checked-in JSON manifest from `docs/motherduck_database_contract_v1.md` table list (or SQL INFORMATION_SCHEMA export run offline in CI) and fail CI on unexpected column drops. This is **new automation** but **thin** (diff only); defer until Priority 1–2 are done.

---

## Explicit non-recommendations

- **Do not** add fuzzy patient matching or note-body sampling to CI for these pillars.
- **Do not** duplicate `112` G2 column logic in a second script; extend 112/111 if column rules change.
- **Default push CI should not** require MotherDuck **writes** beyond what already exists in `lint-and-syntax` and `motherduck-formalization` (per current design).

---

## Summary line

**Implementation:** not required for core safety-net goals. **CI wiring + documentation alignment** are the appropriate next steps; optional read-only aggregation over `29` if teams want chronology/linkage regressions on every merge.
