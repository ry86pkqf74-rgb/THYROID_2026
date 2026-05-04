# Cursor Composer Dispatch — mig_300: Tag pub v1.1 release on MD signoff registry

**Generated:** 2026-05-04 by Cowork at HEAD `e590e40`.
**Lane:** mig_300 — Since mig_277 (NIFTP carve-out → cohort 4012) and mig_269 (recurrence SSOT) and mig_281 (NLP promotion) and mig_287 (smoking enum) and mig_288 (TIRADS enum) and mig_295 (M044 .docx patch), the publication state has materially advanced from `pub_v1_0_20260430` to a new milestone. Tag this milestone as `pub_v1_1_20260504` so future references can pin to the post-NLP-augment state.
**Recommended agent:** **Cursor Composer** — registry-only mig + signoff_migration tag.
**Estimated runtime:** 15 min.
**Severity:** LOW (versioning hygiene).

---

## §0 — First message

> mig_300 dispatch. Add a release tag row to signoff_migration documenting the pub_v1_1 milestone. MotherDuck DB is `thyroid_canonical_publication_v1_0`.

## §1 — Apply

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('release_pub_v1_1_20260504', CURRENT_TIMESTAMP, 'cowork_release_tag',
 'pub_v1_1_20260504 release tag. Cumulative changes since pub_v1_0_20260430:
  - mig_277 NIFTP carve-out (cohort 4128->4018, malig rate 37.83->36.96%)
  - mig_269 canonical_recurrence_events_v1 SSOT
  - mig_281 NLP promotion (smoking 13->3022, family_hx 163->3018, vasc invasion 1172->1220)
  - mig_287 smoking taxonomy normalization (clean current/former/never enum)
  - mig_288 tirads_resolved enum (TR1-5 + NULL; supersedes dirty nlp_tirads_max_category)
  - mig_283 12 cohort views BinderException fix
  - mig_284 legacy recurrence canonicals deprecated
  - mig_290/291/292 M032/M037/M025 submission packages
  - mig_295 M044 v1.1 .docx patch
  - SF AI infrastructure: VALIDATE_ALL_COHORTS, COHORT_SUMMARY_DASHBOARD, THYROID_NOTES_SEARCH, Cortex Analyst staged.');
```

## §2 — Verify

```sql
SELECT mig_id, signed_off_at, by_actor, summary
FROM main.signoff_migration
WHERE mig_id = 'release_pub_v1_1_20260504';
```

## §3 — Update referenced docs

In header of `M044_submission_package_v1_0/00_README.md`, `M038_submission_package_v1_0/00_README.md`, `M025/M032/M037 README.md`:
- Replace `release `pub_v1_0_20260430`` with `release `pub_v1_1_20260504``
- Add note: "post-NLP-augmentation milestone"

## §4 — Surgical git add

```
qc_framework_v1/migrations/300_release_tag_pub_v1_1_20260504.sql
M044_submission_package_v1_0/00_README.md  (release tag header)
M038_submission_package_v1_0/00_README.md
M032_submission_package_v1_0/00_README.md
M037_submission_package_v1_0/00_README.md
M025_submission_package_v1_0/00_README.md
cursor_prompts/CURSOR_PROMPT_MIG_300_PUB_V1_1_RELEASE_TAG_20260504.md
```

---

**End of mig_300 dispatch.**
