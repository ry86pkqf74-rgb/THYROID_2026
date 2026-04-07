<!-- Copy from here into docs/motherduck_release_runbook_v2.md or RC checklist -->

## Manual review queue — release criteria (`qa.manual_review_queue`)

### A. Infrastructure / CI validation (`119 --release-mode`)

- [ ] `SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL` **= 0**
- [ ] `qa.manual_review_queue_summary_v` reflects expected `run_label` scope for the RC
- [ ] No gate regression: promotion scorecard **8/8 PASS** on the promoting run (see contract §4)

### B. Manuscript-grade adjudication (clinical defensibility)

Treat this block as **independent** of (A). Validator (A) does **not** reject synthetic placeholder strings.

- [ ] **Discordant closure:** Every `algorithm_status = discordant_existing` row has `verification_status ∈ {confirmed_correct, confirmed_incorrect}` and playbook **second review** where required.
- [ ] **No synthetic placeholders for publication:**  
      `SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status = 'SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF'` **= 0**  
      OR PI-signed waiver listing **exact row count**, domains affected, remediation date, and acknowledgment that metrics may shift after real adjudication.
- [ ] **Tier governance:** Critical-tier bulk acceptance only after documented sample + `qa.promotion_review_decisions` entry (policy: `docs/domain_mapping_rules.md`).
- [ ] **Reject / limit fields:** For `source_limited`, `not_promotable`, `confirmed_incorrect`: non-null `reason_code` + `reviewer_comment` + `promotion_approved=false`.
- [ ] **Promotion alignment:** For approving statuses, `promotion_approved` is `true` unless explicitly documented exception.

### C. Operational artifacts (audit trail)

- [ ] Frozen **reviewed** `manual_review_queue.csv` in study folder (authoritative human decisions).
- [ ] Operational summary refreshed: `studies/20260407_manual_review_queue_rc_ops/QUEUE_SUMMARY.md` (or successor dated folder).
- [ ] SQL spot checks run from `sql/qa_manual_review_queue_ops_summaries.sql`.

### D. Post-RC / final release (labs & presentation)

- [ ] Longitudinal non-Tg lab extract milestones satisfied (institutional feed) — **orthogonal** to queue but blocks full biomarker manuscript claims.
- [ ] Presentation views (`125_master_verified_views.py`) reviewed if cited in downstream tables.

### Sign-off

| Role | Infrastructure RC (A) | Manuscript RC (B–D) |
|------|----------------------|---------------------|
| Informatics lead | signature | |
| Clinical PI | | signature |

<!-- End copy -->
