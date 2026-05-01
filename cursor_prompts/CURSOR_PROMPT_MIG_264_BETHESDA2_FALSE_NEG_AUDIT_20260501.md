# Cursor Composer Dispatch — mig_264: Bethesda-2 false-negative audit (385 patients with benign cytology + malignant final histology)

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex round 5).
**Lane:** mig_264 — `BETHESDA_FINAL = 2` (benign cytology) but `IS_MALIGNANT = TRUE` for **385 patients** (18.9% of all Bethesda-2 patients = 2,033). Published Bethesda 2023 expected ROM for Cat 2 is **0–3%**. The 7× over-rate is a major data-quality finding. Either the cytology was wrong (true false-negative; literature supports ~3%, not 19%), the FNA didn't sample the index nodule (separate-nodule scenario), or `BETHESDA_INDEX_NODULE_LINKAGE_SOURCE` mismaps the cytology to wrong nodule.
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for the linkage probe + disposition decision → **Cursor Composer** to apply per-patient corrections (only for the linkage-mismap subset).
**Estimated runtime:** 2–3 hrs (decision pass + per-pattern review)
**Triggered by:** Round 5 Prompt 7 (TIRADS / Bethesda diagnostic accuracy).
**Severity:** HIGH for any manuscript citing Bethesda accuracy. M025/M027 (FNA performance papers) cannot publish until this is dispositioned.
**Opens carry-forward:** CF-mig264-BETHESDA-LINKAGE-MISMAP.

---

## §0 — First message to paste into Cursor Chat (decision pass)

> mig_264 decision pass. 385 patients with `BETHESDA_FINAL = 2` (benign cytology) on `canonical_patient_master` ended up with `IS_MALIGNANT = TRUE`. Run §2 probes to disambiguate three causes: (a) true false-negative cytology (~3% expected per literature); (b) FNA sampled non-index nodule and the malignancy was elsewhere; (c) `BETHESDA_INDEX_NODULE_LINKAGE_SOURCE` is mismapping which nodule the Bethesda value belongs to. Surface to Logan: count of each cause + a per-pattern disposition. No UPDATEs in this pass.

---

## §1 — Why this lane exists

Round-5 Prompt 7 surfaced Bethesda × ROM mismatch:

| Bethesda | N | Observed ROM | Expected (Bethesda 2023) | Verdict |
|---|---|---|---|---|
| 1 (non-diagnostic) | 233 | 30.5% | 5–10% | WAY ABOVE |
| **2 (benign)** | **2,033** | **18.9%** | **0–3%** | **MAJOR FINDING — 385 false negatives** |
| 3 (AUS/FLUS) | 642 | 50.9% | 6–18% | WAY ABOVE |
| 4 (FN) | 624 | 49.0% | 10–40% | Slightly above |
| 5 (susp malig) | 273 | 89.0% | 45–60% | WAY ABOVE |
| 6 (malig) | 1,221 | 87.6% | 94–96% | Slightly below |

The general direction (Cat 1-5 over-malignant) is **operative-cohort enrichment** — these are surgically resected nodules, not a screening cohort. That's a manuscript footnote, not a data bug.

But Cat 2 at 18.9% (385/2,033) is too far above the expected 0-3% to be entirely cohort enrichment. Even at 7× community rate, 18.9% suggests a structural issue.

## §2 — Pre-task probes (decision pass)

### 2a. How many patients have multiple FNA episodes?
```sql
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND is_malignant = TRUE
)
SELECT
  COUNT_IF(n_fna = 1) AS single_fna,
  COUNT_IF(n_fna = 2) AS two_fna,
  COUNT_IF(n_fna >= 3) AS three_plus_fna
FROM (
  SELECT b.research_id, COUNT(*) AS n_fna
  FROM bethesda2_malig b
  LEFT JOIN main.canonical_fna_events_v1 f USING (research_id)
  GROUP BY b.research_id
);
```

### 2b. Is `BETHESDA_INDEX_NODULE_LINKAGE_SOURCE` populated?
```sql
SELECT bethesda_index_nodule_linkage_source, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE
GROUP BY 1 ORDER BY n DESC;
-- Look for: 'unlinked' / 'fallback' / 'multi-nodule-uncertain' patterns
```

### 2c. Are these patients multi-nodule (Bethesda was on a different nodule than the malignancy)?
```sql
WITH bethesda2_malig AS (
  SELECT research_id FROM main.canonical_patient_master
  WHERE bethesda_final = 2 AND is_malignant = TRUE
)
SELECT
  COUNT_IF(ps.tumor_2_size_greatest_dimension_cm IS NOT NULL) AS multi_tumor,
  COUNT_IF(ps.tumor_2_size_greatest_dimension_cm IS NULL) AS single_tumor
FROM bethesda2_malig b
JOIN main.path_synoptics ps USING (research_id);
```

### 2d. What histology types end up malignant in the Bethesda-2 cohort?
```sql
SELECT histology_final, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE bethesda_final = 2 AND is_malignant = TRUE
GROUP BY 1 ORDER BY n DESC LIMIT 25;
-- Look for: NIFTP (border malignancy / 2017 reclass), follicular variant PTC,
-- tiny incidental microcarcinomas, aggressive variants
```

### 2e. FNA-to-surgery interval — was there time for the disease to evolve?
```sql
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_fna_to_surg) AS median_days,
  COUNT_IF(days_fna_to_surg < 30) AS within_30d,
  COUNT_IF(days_fna_to_surg BETWEEN 30 AND 365) AS one_to_12mo,
  COUNT_IF(days_fna_to_surg > 365) AS over_1yr
FROM (
  SELECT research_id, MIN(DATEDIFF('day', f.fna_date, cpm.first_surgery_date)) AS days_fna_to_surg
  FROM main.canonical_patient_master cpm
  JOIN main.canonical_fna_events_v1 f USING (research_id)
  WHERE cpm.bethesda_final = 2 AND cpm.is_malignant = TRUE
  GROUP BY 1
);
```

## §3 — Disposition framework

Surface a disposition table to Logan based on §2 results:

| Pattern | Likely cause | Disposition |
|---|---|---|
| Single FNA, single tumor, NIFTP/follicular-variant PTC | True false-negative (cytology can't distinguish FV-PTC from FA reliably) | Manuscript footnote; no DML |
| Single FNA, multi-tumor (T2+ populated), index-nodule linkage = unlinked | Linkage mismap | Run linkage probe; consider re-mapping bethesda_final to truly-index nodule |
| Multi-FNA, latest is non-Bethesda-2 | Latest FNA changed but bethesda_final stuck on old | UPDATE bethesda_final to use most-recent malignancy-suggestive Bethesda value |
| FNA-to-surgery > 1 year + new malignancy on path | Disease evolved between FNA and surgery | Manuscript footnote; correct as historical (no DML) |
| Other / unclear | Case-by-case | Logan adjudicates |

## §4 — Apply (only after Logan ratifies which subset to fix)

```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig264_20260501 AS
SELECT research_id, bethesda_final, bethesda_final_name, bethesda_index_nodule,
       bethesda_index_nodule_linkage_source, is_malignant, histology_final
FROM main.canonical_patient_master WHERE bethesda_final = 2 AND is_malignant = TRUE;

-- Pattern-specific UPDATEs (after Logan disposition)
-- e.g. if Pattern X = "use latest non-2 Bethesda from FNA events":
UPDATE main.canonical_patient_master cpm
SET bethesda_final = (
  SELECT MAX(bethesda_value)
  FROM main.canonical_fna_events_v1 f
  WHERE f.research_id = cpm.research_id AND f.bethesda_value > 2
)
WHERE bethesda_final = 2 AND is_malignant = TRUE
  AND research_id IN (<RIDs from disposition Pattern X>);
```

## §5 — Verify
```sql
-- Re-check Bethesda-2 ROM
SELECT bethesda_final, COUNT(*) AS n,
       COUNT_IF(is_malignant) AS n_malig,
       ROUND(100.0 * COUNT_IF(is_malignant) / COUNT(*), 1) AS rom_pct
FROM main.canonical_patient_master
WHERE bethesda_final = 2 GROUP BY 1;
-- Expect: rom_pct drops toward published 0-3% if linkage mismap was a major cause;
-- if still ~10-15%, the residual is true false-negative + cohort enrichment
```

## §6 — Manuscript footnote requirement
Regardless of how much DML this mig applies, **M025 / M027 / M037 require a manuscript-methods footnote** explaining:
- This is an operative cohort with intrinsic enrichment for malignancy
- Bethesda 2 ROM of 18.9% reflects (a) cohort enrichment + (b) any residual linkage/methodology limitations
- Compare to Bethesda 2 ROM in published *operative* cohorts (typically 5-10%) — even there, our 18.9% is high; honest disclosure required

## §7 — Carry-forwards
- CF-mig264-BETHESDA-LINKAGE-MISMAP → CLOSED if §2b probe finds populated linkage and we fix it; OPEN otherwise
- CF-mig264-MANUSCRIPT-FOOTNOTE → OPEN until M025/M027/M037 add the footnote in their methods sections

## §8 — Surgical git add
```
scripts/output/mig_264_*.md
scripts/output/mig_264_disposition_table.csv
qc_framework_v1/migrations/264_*.sql
```
