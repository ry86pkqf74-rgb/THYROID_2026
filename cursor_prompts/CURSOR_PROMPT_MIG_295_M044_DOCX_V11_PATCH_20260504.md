# Cursor Composer Dispatch — mig_295: M044 manuscript .docx patch (v1.0 → v1.1 numbers)

**Generated:** 2026-05-04 by Cowork at HEAD `7279f23`.
**Lane:** mig_295 — `M044_submission_package_v1_0/02_manuscript.docx` was generated against frozen v1.0 numbers (cohort 4,128 / aOR 1.80 / Cox HR 2.34). Cowork v1.1 regenerate (round 11b at `c3ef965`) updated the underlying tables.xlsx + figures + parquet to v1.1 numbers (cohort 4,012 / aOR 2.08 / Cox HR 0.91). The .docx body was NOT re-patched. mig_295 = run a find/replace pass on the .docx to reflect v1.1 numbers + add the Cox HR caveat per `M044_READY_FOR_WRITING_BRIEF_v1_1.md`.
**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through the specific cells that drift before find/replace.
**Estimated runtime:** 60 min.
**Severity:** HIGH (M044 is the closest-to-submission paper; .docx body must reflect locked v1.1 numbers before Logan ships).
**Closes:** CF-M044-DOCX-V11-PATCH (newly opened).

---

## §0 — First message to paste into Cursor Chat

> mig_295 dispatch. Patch `M044_submission_package_v1_0/02_manuscript.docx` from v1.0 numbers to v1.1. Source of truth: `M044_submission_package_v1_0/04_tables.xlsx` (already v1.1) + `studies/m044_validation/m044_validation_summary.md` + `manuscript_outputs/v1_0_20260501/M044_READY_FOR_WRITING_BRIEF_v1_1.md`. Walk me through the specific cells that drifted before find/replace.

---

## §1 — Specific drifts to patch

| Cell | v1.0 frozen | v1.1 (current) |
|---|---:|---:|
| Total cohort n | 4,128 | **4,012** |
| Strict-DTC + no-RAI primary | 3,789 | **3,750** |
| Primary path-proven events | 139 | **193** |
| Cox subset n | 2,025 | **2,511** |
| Cox subset events | (not reported) | **178** |
| Gross vs Microscopic ETE — primary aOR | 1.80 (1.22-2.67), p=0.003 | **2.08 (1.48-2.91), p=2.5×10⁻⁵** |
| Gross vs Microscopic ETE — Cox HR | 2.34 (1.35-4.06), p=0.003 | **0.91 (0.48-1.73), p=0.77** ← FLIPPED |
| Pseudo-R² | (not reported) | 0.140 |
| LR vs null χ² | (not reported) | 213.7 |

## §2 — Discussion section addition required

Per `M044_READY_FOR_WRITING_BRIEF_v1_1.md` §"Logan Decision Required":

> The Cox HR for gross-vs-microscopic flipped from 2.34 (v1.0) → 0.91 (v1.1). This is from a 24% larger Cox-eligible subset (2,025 → 2,511), not from biochemical contamination — the event column is still path-proven. The primary logistic specification is unaffected and is in fact **stronger** at v1.1 (aOR 2.08 vs 1.80).

Logan picked **Option A**: report both logistic + Cox with spec-sensitivity discussion.

Add a Discussion paragraph (template):
> "In sensitivity analyses, the time-to-event Cox proportional-hazards model showed no significant difference between gross and microscopic ETE (HR 0.91, 95% CI 0.48–1.73, p=0.77, n=2,511 / 178 events). This contrasts with the primary logistic regression on path-proven recurrence (aOR 2.08, p=2.5×10⁻⁵). The discrepancy reflects the difference in eligibility criteria: the Cox model includes patients with shorter follow-up windows in which late path-proven recurrences accumulate disproportionately in the microscopic stratum (median follow-up X.X years), diluting the time-to-event signal. The primary logistic specification, which treats path-proven recurrence as a binary outcome over the entire observation period, is the pre-specified primary analysis."

## §3 — Apply

### §3a — Walk through find/replace cells with Logan
Surface a 9-row diff table showing each old-text → new-text replacement. Get sign-off before touching .docx.

### §3b — Apply via python-docx
```python
from docx import Document
doc = Document('M044_submission_package_v1_0/02_manuscript.docx')
patches = [
    ('4,128', '4,012'),
    ('3,789', '3,750'),
    ('139 events', '193 events'),
    ('aOR 1.80', 'aOR 2.08'),
    ('1.22–2.67', '1.48–2.91'),
    ('p = 0.003', 'p = 2.5×10⁻⁵'),  # tweak per context
    ('HR 2.34', 'HR 0.91'),
    # ... full list per §1
]
for para in doc.paragraphs:
    for old, new in patches:
        if old in para.text:
            for run in para.runs:
                if old in run.text:
                    run.text = run.text.replace(old, new)
doc.save('M044_submission_package_v1_0/02_manuscript.docx')
```

### §3c — Add Discussion paragraph
Insert the §2 template paragraph after the existing primary-result Discussion paragraph.

### §3d — Verify
- Open .docx in LibreOffice or Word; visually confirm v1.1 numbers throughout
- Check that NO v1.0 numbers (4,128 / 1.80 / 2.34) remain anywhere in body or supplement
- Re-render hash if package SHA256 tracking is in place

### §3e — Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_295', CURRENT_TIMESTAMP, 'cursor_composer_mig295',
 'mig_295: Patched M044_submission_package_v1_0/02_manuscript.docx from v1.0 to v1.1 numbers. 9 cells updated (cohort 4128->4012; aOR 1.80->2.08; Cox HR 2.34->0.91; etc.). Added Discussion paragraph on Cox-vs-logistic spec-sensitivity per Logan Option A disposition. Closes CF-M044-DOCX-V11-PATCH.');
```

---

## §4 — Surgical git add

```
M044_submission_package_v1_0/02_manuscript.docx
M044_submission_package_v1_0/03_supplement.docx  (if any v1.0 numbers there too)
qc_framework_v1/migrations/295_m044_docx_v11_patch_20260504.sql
scripts/output/mig_295_diff_table.md
scripts/output/mig_295_apply_log.txt
cursor_prompts/CURSOR_PROMPT_MIG_295_M044_DOCX_V11_PATCH_20260504.md
```

---

**End of mig_295 dispatch.**
