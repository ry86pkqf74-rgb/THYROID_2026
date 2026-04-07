# Operational memo — THYROID_2026 manual review queue (RC readiness)

**Audience:** study ops, informatics, and clinical leads jointly accountable for promotion governance.  
**Scope:** Turn `qa.manual_review_queue` from an infrastructure checkpoint into an **auditable, reviewer-ready work package** with explicit release criteria.  
**Authoritative playbooks:**  
`studies/v2_domain_promotion_gate_formalization_20260406_v3/MANUAL_REVIEW_PLAYBOOK.md`,  
`docs/domain_mapping_rules.md` (fill-candidate tier policy),  
`docs/motherduck_database_contract_v1.md`.

---

## 1. Current MotherDuck snapshot (live read, 2026-04-07 UTC)

Connection: `scripts/120_review_queue_triage.py --md` / `utils/md_connect.connect_md_fail_closed` against `md:Thyroid 2026`.

| Metric | Value |
|--------|------:|
| Total `qa.manual_review_queue` rows | 16,866 |
| Pending (`verification_status` **IS NULL**) | **0** → **`119 --release-mode` infrastructure gate passes** |
| `discordant_existing` | 6 — all **`confirmed_correct`**, `promotion_approved=true` |
| Placeholder `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | **5,620** |

### 1.1 Critical interpretation

- **Infrastructure RC** (validator `scripts/119_md_formalization_validate.py`): only tests **NULL vs non-NULL** `verification_status`. Any non-null string—including the synthetic placeholder—clears the check.
- **Clinical / manuscript defensibility** is **not** established for the **5,620** rows still carrying the synthetic status inserted by `scripts/126_final_master_release.py` via `--synthetic-fill-mrq-verification` (explicitly **NON-PUBLICATION** in script help text).
- **Operational consequence:** Treat the queue as **two-layer:** (A) *pipeline unblock* vs (B) *publication-grade adjudication*. Layer B is **not complete** until synthetic rows are replaced with closed-vocabulary decisions (`MANUAL_REVIEW_PLAYBOOK.md`) or covered by a **signed, quantitative waiver** (see §3).

### 1.2 Schema note (auditability)

- **`qa.manual_review_queue.domain`** mirrors the gate CSV **`source_domain`** (v2 promotable domain). The v1 **`comparison_domain`** is **not** stored on this table after hydration (`scripts/114_qa_schema_setup.py` column mapping). For pairwise concordance lineage, retain or join to the gate **`manual_review_queue.csv`** in the study folder.

### 1.3 Queue summary artifact

Aggregated breakdowns (reason code, domain, registry **QA tier**, **promotability class**, **reviewer channel**) are in:

- `QUEUE_SUMMARY.md`
- `queue_summary_by_slice.csv`

Regenerate after re-hydrate: rerun the summarization job captured in the commit that introduces this study folder, or extend `scripts/120_review_queue_triage.py` with the same group-bys.

---

## 2. Release categories (explicit)

### 2.1 Must be **zero** to unblock **infrastructure** RC

| Category | Rule |
|----------|------|
| Pending adjudication | `verification_status IS NULL` count **= 0** for all rows evaluated under strict validation. |

### 2.2 Must be **zero** (or explicitly re-classed) for **manuscript / clinical** RC

| Category | Rule |
|----------|------|
| Unresolved discordance | `algorithm_status = discordant_existing` MUST NOT share visit with `verification_status` in `defer`, `needs_second_reviewer`, or NULL. Must be `confirmed_correct` or `confirmed_incorrect` with mandated **second review** (playbook). |
| Synthetic placeholder | `verification_status = SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` **= 0** OR batch-replaced with rubric statuses (`auto_accepted_*`, `confirmed_*`, `source_limited`, `not_promotable`) **without** using bulk shortcuts forbidden in playbook. |
| High-risk auto-promotion | **No** bulk acceptance of `discordant_existing`. **No** `auto_accepted_critical_sample_ok` without documented sample + `qa.promotion_review_decisions` trail (domain policy). |

### 2.3 May remain with **documented waiver** (PI / governance)

| Category | Rule |
|----------|------|
| Informational `defer` | Playbook: **≤2** `defer` on informational `source_domain`s, else PI waiver in release notes. |
| Post-hoc pipeline limitation | Known duplicate-rate domains flagged in playbook (`labs`, `tg_kinetics`, etc.) — waiver must cite **domain**, **row count**, and **mitigation** (`not_promotable` vs dedup milestone). |

### 2.4 **Source-limited / not-promotable** (closed statuses, not “pending”)

| Category | Rule |
|----------|------|
| `source_limited`, `not_promotable`, `confirmed_incorrect` | Acceptable **terminal** states when `promotion_approved` aligns with rubric (`false` for rejects). **`reason_code`** and **`reviewer_comment`** required per playbook. |

### 2.5 **Needs second reviewer**

Per playbook: all `discordant_existing`; RAI / thyroid-hormone / medication triangle; selected pathology↔genetics fills. Track locally with optional `second_reviewer_id` columns or `qa.promotion_review_decisions` entries.

---

## 3. Work prioritization (burn-down order)

1. **`discordant_existing` first** — template cross-domain semantics (e.g. RAI vs levothyroxine) before scale.
2. **Highest-impact fill candidates** — critical QA tier × volume (`staging`, `pathology`, `rai_detailed`, `vascular_invasion`, `recurrence`, …) per `QUEUE_SUMMARY.md` and playbook domain ordering.
3. **Source-limited / non-promotable tail** — sparse domains and truncated spans last; batch **marking** only where playbook allows (not discordant).

---

## 4. Batching strategy (summary)

| Mode | What can batch | What still needs human adjudication |
|------|----------------|--------------------------------------|
| Tier bulk acceptance | `existing_missing_fill_candidate` on **standard** / **informational** tiers via `scripts/127_qa_tier_batch_adjudicate.py` + governance sign-off | **Never** `discordant_existing` |
| Critical tier | Sample 10% (min 20); **>90%** pass → remainder batch per policy; log sample in `qa.promotion_review_decisions` | Failed sample → row-level review |
| Duplicate-heavy domains | Prefer `not_promotable` + reason code after human spot check | Contradictory duplicates without dedup rule |
| Synthetic remediation | Scripted **status replacement** only where upstream CSV already contains defensible decisions; otherwise **human** pass | Blind overwrite of synthetic without note review |

**Suggested batch size:** time-boxed **250–400** rows per analyst shift for standard/informational; **≤25** for critical clinician blocks; **every** discordant pair as its own mini-batch with dual sign-off.

---

## 5. Staffing / work split

| Role | Owns | Artifacts |
|------|------|-----------|
| **Lead analyst** | Tier policy execution, `127` batch runs, exports (`120_review_queue_triage.py`), SQL dashboards, merge gate CSV columns into hydrate folders | `exports/` + study `manifest.json` references |
| **Clinical reviewer (thyroid/endo)** | Discordant rows, RAI vs suppression, recurrence, cervical nodal pathology semantics | `reviewer_evidence_span`, `reason_code`, second sign-off |
| **Pathology-literate reviewer** | Pathology / staging / genetics collision rows, synoptic tails | Same |
| **PI / governance** | Waivers on residuals, approval of critical sample outcomes | Release notes + `qa.release_manifest` linkage |

**Minimum viable:** 1 analyst + 1 clinical reviewer + 1 second reviewer pool (can be the same clinician on a different day for non-overlapping rows—**not** for the same row).

---

## 6. Sign-off rules

| Gate | Requirement |
|------|-------------|
| **RC infrastructure unblock** | `119 --release-mode` **PASS** (NULL pending cleared). Document known limitations if synthetic placeholders remain. |
| **Manuscript / external-facing RC** | **Zero** synthetic placeholder OR PI waiver with **exact counts** and timeline to full rubric closure. **Zero** unresolved discordant. Critical-tier sample rules **satisfied**. |
| **Final release after labs** | Independent longitudinal lab extract milestones (per `README.md` institutional non-Tg labs) **plus** presentation-layer review metrics where applicable (`125_master_verified_views.py`). |

---

## 7. Auditability checklist (per-row)

Maintain in `qa.manual_review_queue` (or gate CSV before hydrate):

- `reviewer_evidence_span` when it differs from machine `evidence_span` (machine span unchanged).
- `reviewer_comment` for rejects, deferrals, limitations.
- `reason_code` compact machine tag (`DOMAIN_MISMATCH`, `RAI_VS_THYROID_HORMONE`, …).
- `verification_status` from **closed vocabulary** in playbook—not synthetic strings for publication paths.
- **`promotion_approved`** consistent with status; empty `promotion_approved` on placeholder rows is a **red flag** for manuscript readiness.

---

## 8. Recommended follow-on engineering (optional)

- Extend **`119`** with a **`--manuscript-mode`** flag that **FAIL**s on `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` and on non-whitelist `verification_status` values.
- Add `comparison_domain` as optional nullable column on `qa.manual_review_queue` for cloud-side triage without CSV joins.

---

*Memo version 1.0 — aligned with database contract v1 and formalization gate playbook.*
