# Cursor Composer Dispatch — mig_290b: M032 25-yr submission package (mig_290 retry)

**Generated:** 2026-05-04 by Cowork at HEAD `170ee3d`.
**Lane:** mig_290b — Original mig_290 (M032 submission package) was the only prompt from the prior round of 4 that did NOT land. Retry as mig_290b. Same scope as `cursor_prompts/CURSOR_PROMPT_MIG_290_M032_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md`.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through table structure before generating .docx files.
**Estimated runtime:** 2-3 hours.
**Severity:** MED. Unblocks M032 writing chat.

---

## §0 — First message to paste into Cursor Chat

> mig_290b dispatch. This is the retry of mig_290 (which was authored 2026-05-03 but did not land in the prior Cursor batch). Read `cursor_prompts/CURSOR_PROMPT_MIG_290_M032_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md` for the full scope. Same expected deliverable. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

---

## §1 — Why this dispatch exists

mig_290 was authored at `46a4557` but signoff_migration check 2026-05-04 confirmed it never landed. mig_287/288/289 all completed. Since M032 ready-for-writing is now blocked on this single deliverable, retry it explicitly as mig_290b to make the dependency visible.

## §2 — Same scope as mig_290

See `cursor_prompts/CURSOR_PROMPT_MIG_290_M032_SUBMISSION_PACKAGE_SCAFFOLD_20260504.md` §2-§7. Builds:
- `M032_submission_package_v1_0/` directory mirroring M044/M038
- Tables 1-5 + Supp S1-S3
- 4 figures (cohort flow, era × malig, TNM stage, smoking trends)
- Reproducibility SQL package
- Build scripts
- 156-cell validation report
- CLOSEOUT_NOTES.md

## §3 — Locked input numbers (post-mig_281/285/287)

From `manuscript_outputs/v1_0_20260501/M032_READY_FOR_WRITING_BRIEF.md`:

- Cohort n=10,871 / malig=4,019 (37.0%)
- Smoking known: 3,022 (current 215 / former 504 / never 2,303 — clean enum post-mig_287)
- Family hx thyroid: 366 present / 2,652 absent (3,018 known, 12.1% prevalence)
- Era stratification: 905 (1999-04) → 3,935 (2020-25); malig rate 29% → 41%

## §4 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_290b', CURRENT_TIMESTAMP, 'cursor_composer_mig290b_retry_of_290',
 'mig_290b: M032 25-yr Descriptive submission package v1.0 built (mig_290 retry). See mig_290 prompt for full scope. Closes M032 ready-for-writing gate.');
```

---

**End of mig_290b dispatch.**
