---
type: feedback
description: Never repoint frozen CPM cols to live canonicals during a cleanup cycle; frozen-at-publication is the correct model for a published cohort
---

# CPM frozen at publication — never repoint during cleanup

**Rule**: When a Tier-2 cleanup proposes dropping a legacy source and
the only literal readers (a) target a different DB OR (b) are
historical one-shots not part of any active pipeline, **SKIP the
repoint phase**. Drop the legacy source with a safety audit; leave
the CPM col values frozen.

## Why

This is the Script 365b Phase 2 decision (2026-04-22). The literal-
source-reader audit found 78 CPM cols sourced from
`note_entities_problem_list` + `note_entities_medications` via
2 historical scripts (`scripts/212_nlp_entity_rollup.py` +
`scripts/215_deep_nlp_entity_integration.py`). **Both scripts target
`thyroid_ete_fix_20260413` (a different DB)**, and were historical
one-shots whose outputs were promoted to publication CPM at prior
cutover.

If Phase 2 had repointed those 78 cols to the new
`canonical_pmh_*_v1` / `canonical_medications_*_v1` rollup
phenotype-BOOL triads, the cleanup would have **OVERWRITTEN 78 frozen
CPM col values with current canonical state** — breaking
reproducibility for any published analysis already run against those
cols. For a published cohort, "frozen at publication" is the correct
model. The cohort version IS the cohort identity; mutating it without
a `canonical_version` bump is a silent semantic rewrite, not a cleanup.

## How to apply

Before authoring a CPM feeder-repoint script in any future cleanup
cycle, audit:

1. Do the literal readers target the **publication DB**
   (`thyroid_canonical_publication_v1_0`)? If they target a different
   namespace, the frozen cols don't refresh from the legacy source on
   the publication side anyway — repoint is unnecessary.
2. Are the literal readers **part of an active build pipeline** (run
   on every CPM rebuild / publication snapshot)? If they're
   one-shots that ran historically and aren't in any cron / Makefile /
   GitHub Actions cascade, the cols are frozen — repoint is
   unnecessary.

If **EITHER (1) or (2) is false**, **skip the repoint**. Document the
Option-C decision in the Phase-3 commit body so future-you can trace
why the drop was safe without a repoint. The cleanup commit then has
two artifacts:

- The **safety audit findings** (4 checks: views / registry /
  script-level SQL deps / refresh jobs).
- The **Option-C rationale** explaining that the cols are frozen and
  refreshing them would be a semantic rewrite, not a cleanup.

If someone later genuinely wants live tier-driven CPM signals (e.g.,
"latest diabetes assertion across all sources"), the right
architecture is a **VIEW joining the new rollup phenotype-BOOL triads
onto CPM** — NOT a materialised overwrite of the frozen cols. The
view preserves both signals: the frozen historical value stays on
CPM (back-compat for prior analyses), and the live rollup-derived
value is available alongside via the view's projected columns.

## Anti-pattern: silent CPM mutation

DO NOT:

- Repoint a CPM col during a "cleanup" without a `canonical_version`
  bump on CPM. The repoint is a publication-tier change, not a
  Tier-2 cleanup, and warrants its own publication-version branch.
- Treat the CPM col as a live re-derivable value when it was
  populated as a frozen snapshot at cutover.
- Drop the legacy source, then later re-run the feeder script
  expecting to refresh the col — the source is gone, the col stays
  at the frozen value, but now the lineage (which the user thinks
  is live) is silently broken.

## Reference precedent

- **Phase 3 commit body**: `0a2ec27` — explicit Option-C rationale
  + 4-check safety-audit findings inline.
- **Phase 3 close-out doc**: `docs/script_365b_phase3_close_out_20260422.md`
  — full safety-audit results in tabular form.
- **Frozen-source lineage**: `reference_thyroid_ete_fix_20260413_namespace.md`
  — the historical DB that fed the 78 cols, and the operational
  guardrail that protects it from accidental cleanup.

## Related memory

- `project_script_365b_close_out.md` — complete cascade narrative
- `reference_thyroid_ete_fix_20260413_namespace.md` — the historical
  DB source-of-truth
- `feedback_dryrun_signoff_before_build.md` — companion rule for
  preventing the build-side defect that necessitated remediation
