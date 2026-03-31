# Executive Summary

## Canonical numbers to use
- Expanded total cohort: 3,278 PTC patients.
- Expanded complete-case ordinal model: 3,269 patients.
- ETE distribution: 724 no ETE, 1,736 microscopic ETE, 818 gross ETE.
- Stage migration: 1,241 of 1,736 microscopic ETE tumors downstaged on T stage (71.5%); 1,872 overall downstaged (57.3%).
- Cross-validated AUCs: base 0.851, full 0.876, delta 0.025.
- Frozen matched structural analysis: 711 matched pairs, OR 1.4339, Fisher p=0.030.
- Interaction terms: mETE x age p=0.258; mETE x N1 p=0.006.
- CT timing: 3,018 PTC CT rows among 650 patients; 1,245 pathologic exams among 331 patients; 581 preop/perioperative pathologic exams (508 preop, 73 perioperative), 664 >=30 day postoperative.

## Confirmed consistent items
- The final revision packet, frozen analysis metadata, and frozen audit tables agree on the primary expanded cohort N=3,278 and the ETE distribution 724/1,736/818.
- The main manuscript package aligns with the frozen PSM headline result of 711 matched pairs, OR 1.43, and p=0.030, while the supplement labels the 712-pair replay as sensitivity.
- The main package and revision packet align on the expanded N1 prevalence values 56.9%, 67.2%, and 74.7% and on the interaction p-values 0.258 and 0.006.
- CT timing language in the revision packet correctly favors the PTC/pathologic exam denominators rather than the institutional 7,701-row export.
- No legacy S7/S8 supplementary numbering was confirmed in the final package inputs reviewed for this audit.
- Supplementary Figure S4 and Figure S6 legends are present in the final supplement and align with the package assets for Tg trajectory and expanded OR forest content.
- No explicit whole-specimen or pathology lymph-node completeness overstatement was found in the final package text search.

## Confirmed inconsistent items
- The forensics JSON reports complete_case_ordinal=523, psm_matched=1006, and primary_classic_ptc=589, which conflict with the frozen manuscript-facing analysis spine.
- The forensics metric crosswalk still records a stale 503-pair PSM reproduction for MET08.
- The blinded PSM replay in revision_rerun_20260326 differs from the frozen package result: 712 pairs, OR 1.3044, p=0.132 versus the frozen 711 pairs, OR 1.4339, p=0.030.

## Items requiring manuscript text edit only
- Keep tumor-1-centric wording explicit in Methods and avoid implying whole-specimen multi-tumor capture.
- Do not cite the 7,701 institutional CT export row count or any mistaken 701 count in manuscript prose.
- If the classic cohort is discussed, keep 596 as the frozen classic export and footnote the deduplicated 589 only if necessary.
- If pathology lymph-node completeness is described, keep the wording conservative and avoid implying complete whole-specimen node ascertainment beyond what the audits support.

## Items requiring analysis/output regeneration
- Regenerate or explicitly quarantine the stale forensics JSON cohort_size block if that artifact will continue circulating with the submission package.
- Refresh or quarantine final_metric_crosswalk.csv so the stale 503-pair MET08 claim does not remain adjacent to the frozen package.
- If strict journal file validation matters, re-export package figures whose `.png` extension does not match the binary image format.
- Any attempt to replace the frozen PSM headline with the 712-pair rerun would require an intentional regeneration decision, not a silent refresh.

## Submission Readiness
- Is the package numerically consistent enough for journal submission as-is? Yes.
- Rationale: the final submission package itself is internally aligned with the frozen audit spine; the remaining high-severity inconsistencies are stale collateral artifacts elsewhere in the repo, not contradictions inside the package text that is currently staged for submission.
- High-severity out-of-sync items still present outside the frozen package: 13 rows in discrepancy_report.csv.
- Medium-severity wording/denominator guardrails: 4 rows in discrepancy_report.csv.
