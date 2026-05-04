# Cursor Composer Dispatch — mig_303: Refresh all submission package READMEs to pub_v1_1

**Generated:** 2026-05-04 by Cowork at HEAD `17baa2b`.
**Lane:** mig_303 — mig_300 tagged `release_pub_v1_1_20260504` on signoff_migration but the 7 submission package `00_README.md` files (M044, M038, M032, M037, M025, M038-Definition planning doc, future M004) still reference `pub_v1_0_20260430`. Find/replace all references.
**Recommended agent:** **Cursor Composer** — mechanical sed-style edit.
**Estimated runtime:** 15 min.
**Severity:** LOW.

---

## §0 — First message

> mig_303 dispatch. Update all submission package READMEs and manuscript drafts to reference release `pub_v1_1_20260504` instead of `pub_v1_0_20260430`. Add the post-NLP-augment milestone note.

## §1 — Find/replace

```bash
cd "/Users/ros/THyroid 2026"
grep -rln "pub_v1_0_20260430\|release `pub_v1_0`\|pub v1.0" \
  M044_submission_package_v1_0/ M038_submission_package_v1_0/ \
  M032_submission_package_v1_0/ M037_submission_package_v1_0/ \
  M025_submission_package_v1_0/ \
  manuscript_outputs/v1_0_20260501/ 2>/dev/null
```

For each hit: replace `pub_v1_0_20260430` → `pub_v1_1_20260504` and add to the README header:

> **Release:** `pub_v1_1_20260504` (post-NLP-augmentation milestone; was `pub_v1_0_20260430` before mig_281–mig_300)

## §2 — Apply

Per file. Expected ~5-10 files touched.

## §3 — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_303', CURRENT_TIMESTAMP, 'cursor_composer_mig303',
 'mig_303: Refreshed N submission package READMEs and manuscript drafts to reference pub_v1_1_20260504 release tag. Closes pub_v1_1 versioning hygiene.');
```

## §4 — Surgical git add

Per touched .md / README files + `qc_framework_v1/migrations/303_release_readme_refresh_20260504.sql` + cursor prompt.

---

**End of mig_303 dispatch.**
