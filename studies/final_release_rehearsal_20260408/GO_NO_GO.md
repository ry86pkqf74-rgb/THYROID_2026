# GO / NO-GO — final master release rehearsal

**TAG:** `20260408`  
**Session:** `MOTHERDUCK_SESSION_HINT=thyroid_final_release_20260408`  
**User agent:** See `preflight_probe.txt` (pattern `thyroid_final_release/20260408/...`)

## Verdict: **HOLD**

Publication **GO** is **not** supported until QA **119 --release-mode** is clean. Current QA state fails **5** checks (see `blocker_checklist.md` and `qa_release_mode/validation_report.md`).

### What passed in-repo (rehearsal mechanics)

| Step | Result |
|------|--------|
| Token probe + DB names + git SHA | `preflight_probe.txt` |
| `119` QA release-mode | **FAIL** (blockers above) |
| `126` dry-run + MRQ/decisions preflight | **PASS** (tier-policy gate CSVs) |
| `137 promote` (rehearsal, no `--execute`) | **PARTIAL** — 130/printed SQL OK; embedded 119 failed; long-running stage (136/124) not completed in-session after duplicate 119 |

### PROD_WRITE gate

The exact phrase **`PROCEED_PROD_WRITE`** was **not** present in the user prompt. **No live mutations** were executed (130 clone, 126 live, 124 live, 136 writer/reader).

### Preconditions (from task) vs actual

| Precondition | Actual |
|--------------|--------|
| Live-state audit free of release blockers | **Not satisfied** on QA formalization |
| Specimen/FHIR release-mode passing | Check 13 skipped (prereq table absent) — not a green “full” gate |
| MRQ human-reviewed | **Repo** tier-policy MRQ is admissible for 126 preflight; **QA DB** still holds synthetic MRQ rows |
| Promotion decisions substantive | CSV present; **QA** rows lack `decision_batch_id` |
| Final non-Tg lab wave | No lab CSV in `exports/incoming/` — not appended |

## If you proceed to GO later

1. Re-hydrate QA `manual_review_queue` from `studies/20260407_tier_policy_review_gate` (or newer signoff gate) via **114** / **126**.
2. Run **126** (non-dry) far enough to append decisions and `UPDATE` NULL `decision_batch_id`, or run equivalent SQL on QA.
3. Load `molecular_testing` and rerun canonical/linkage scripts per Check 12b remediation text.
4. Re-run **119 --release-mode** on QA until **0 FAIL**.
5. Only then: `PROCEED_PROD_WRITE` in prompt + `130 --execute` + **126** live + **124** + **136** per runbook.
