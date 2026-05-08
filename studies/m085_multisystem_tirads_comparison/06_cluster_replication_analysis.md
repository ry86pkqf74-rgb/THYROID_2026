# Pre-registration: 11-System TIRADS Cluster Replication Analysis

**Study:** M085 — Multi-system TIRADS Comparison  
**Pre-registration date:** 2026-05-08  
**Output directory:** `studies/m085_multisystem_tirads_comparison/`  
**Status:** Pre-registered (analysis pending Step 5 disagreement-queue build)  
**BQ destination:** `pub_workspace.qc_tirads_multisystem_cluster_v1`

---

## Motivation

During Phase A.3 (Steps 2–3 of the Path A patch, commit 3c727e6), the cross-Phase-B
benign-vs-suspicious binary concordance across the 4 deterministic systems (ACR 2017,
Kwak 2011, K-TIRADS 2021, C-TIRADS 2020) revealed a **cluster structure**:

| System pair | Binary concordance |
|---|---|
| ACR 2017 ↔ K-TIRADS 2021 | 96.9% |
| Kwak 2011 ↔ C-TIRADS 2020 | 99.5% |
| ACR/K-TIRADS ↔ Kwak/C-TIRADS (cross-cluster) | 77–78% |
| 4-system unanimous agreement | **76.06%** |
| 4-system mean pairwise | 84.6% |

**Interpretation (pre-specified):** ACR and K-TIRADS form a "points-accumulation" cluster
requiring multiple independent suspicious features before upstaging; Kwak and C-TIRADS form
a "single-suspicious-feature" cluster that counts solid composition alone as an upstaging
criterion, systematically classifying solid+isoechoic nodules as ≥ Category 4A vs. the
ACR/K-TIRADS pair that would retain them at TR3. This is a real systems-level design
difference, not a scoring bug.

**Observable consequence:** On this cohort (25-year American surgical enrichment), the
Kwak/C-TIRADS cluster drives substantially higher FNA recommendation rates for nodules with
solid composition but otherwise benign echogenic features, compared to the ACR/K-TIRADS cluster.

---

## Hypotheses (pre-specified)

**H1 (Primary — cluster persistence):** After all 11 systems are scored, hierarchical
clustering on the 11×11 pairwise concordance matrix (benign-vs-suspicious binary, cut at
suspicion ordinal ≥ 3 on the normalized 1–5 scale) will reproduce the ACR/K-TIRADS vs.
Kwak/C-TIRADS split as two distinct subclusters, plus a third subcluster for pattern-based
systems (Horvath/ATA/EU-TIRADS) and a fourth for clinical-pathway systems (SRU/BTA/AACE).

**H2 (Effect size — cluster gap):** The between-cluster concordance (ACR/K-TIRADS vs.
Kwak/C-TIRADS) will be ≤ 80%, at least 10 percentage points below the within-cluster
concordance of each cluster.

**H3 (Clinical impact — solid+isoechoic nodules):** In the subgroup of solid, isoechoic
nodules (the driver of the disagreement), the Kwak/C-TIRADS cluster will assign ≥ Category
4A at a rate ≥ 1.5× the rate assigned by the ACR/K-TIRADS cluster (pre-specified multiplier
derived from the ~77% cross-cluster rate vs. ~98% within-cluster rate at 4-system level).

**H4 (Pattern systems):** Horvath 2009 will cluster closer to ACR 2017 (European
origin, multi-feature decision trees) than to Kwak/C-TIRADS (Asian-origin, composition-first
rules), given Horvath's colloid-type pattern structure which implicitly requires multi-feature
pattern support for higher-category assignment.

---

## Analysis plan

### Step 1 — Pairwise concordance matrix (all 11 systems)

After the disagreement queue is built (Step 5 of the patch prompt), compute the 11×11
pairwise binary concordance matrix using the `suspicion_spread` normalized scores from
`pub_workspace.qc_tirads_multisystem_disagreement_v1` plus all nodules (not just disagreements):

```sql
CREATE OR REPLACE TABLE `pub_workspace.qc_tirads_multisystem_cluster_v1`
CLUSTER BY research_id AS
WITH normalized AS (
  SELECT
    nodule_id, research_id,
    -- Benign-vs-suspicious binary per system (cut at normalized ordinal >= 3)
    CASE WHEN sus_acr     >= 3 THEN 1 ELSE 0 END AS susp_acr,
    CASE WHEN sus_kwak    >= 3 THEN 1 ELSE 0 END AS susp_kwak,
    CASE WHEN sus_ktirads >= 3 THEN 1 ELSE 0 END AS susp_ktirads,
    CASE WHEN sus_ctirads >= 3 THEN 1 ELSE 0 END AS susp_ctirads,
    CASE WHEN sus_eu      >= 3 THEN 1 ELSE 0 END AS susp_eu,
    CASE WHEN sus_ata     >= 3 THEN 1 ELSE 0 END AS susp_ata,
    CASE WHEN sus_bta     >= 3 THEN 1 ELSE 0 END AS susp_bta,
    CASE WHEN sus_aace    >= 3 THEN 1 ELSE 0 END AS susp_aace,
    CASE WHEN sus_park2009>= 3 THEN 1 ELSE 0 END AS susp_park2009,
    CASE WHEN sus_park_cohort >= 3 THEN 1 ELSE 0 END AS susp_park_cohort,
    CASE WHEN sus_horvath >= 3 THEN 1 ELSE 0 END AS susp_horvath,
    n_systems_scored
  FROM `pub_workspace.qc_tirads_multisystem_disagreement_v1`
  WHERE n_systems_scored >= 8
  UNION ALL
  -- Include non-disagreement nodules (spread < 2 or n_systems < 8)
  SELECT ... FROM `pub_canonical.canonical_us_nodule_tirads_multisystem_v1`
  WHERE nodule_id NOT IN (SELECT nodule_id FROM `pub_workspace.qc_tirads_multisystem_disagreement_v1`)
)
SELECT * FROM normalized;
```

Then compute the 11×11 concordance matrix in Python:
```python
systems = ['acr','kwak','ktirads','ctirads','eu','ata','bta','aace','park2009','park_cohort','horvath']
# For each pair (i,j): COUNTIF(susp_i == susp_j, both non-null) / COUNT(both non-null)
```

### Step 2 — Hierarchical clustering

Apply Ward's linkage (distance = 1 − concordance) to the 11×11 matrix.
Output: dendrogram + cluster assignment at k=2, 3, 4 cut levels.

### Step 3 — Subgroup analysis (solid+isoechoic)

Restrict to nodules with `composition='solid' AND echogenicity IN ('isoechoic','hyperechoic')`.
Compute per-cluster FNA-recommendation rate (defined as ≥ Category 4A on the system's
native scale).

### Step 4 — Path-outcome validation (where available)

For labeled nodules in `pub_workspace.us_nodule_path_outcome_v1`, compute AUC per cluster
(using the cluster's majority-vote binary label). Determine whether the clustering structure
maps onto diagnostic performance differences.

### Step 5 — Reporting

Primary table: 11×11 concordance matrix (symmetric, diagonal = 1.0).
Figure: Hierarchical dendrogram with cluster color-coding + summary AUCs.
Key numbers for M085 Results section:
- 4-system Phase B unanimous rate: 76.06% (pre-established, from commit 3c727e6)
- 11-system unanimous rate (post-Horvath): TBD
- Within-cluster vs. between-cluster gap: TBD
- Solid+isoechoic FNA uplift (Kwak/C-TIRADS vs. ACR/K-TIRADS): TBD

---

## Notable Finding cross-links

- `NF-2026-05-07-tirads-pipeline-version-state-mismatch` — evidence summary will include
  the 76.06% unanimous rate and cluster pattern as a publishable observation discovered
  during the infrastructure patch (Step 7 of the patch prompt).
- `NF-2026-05-07-park2009-noncalibration` — Park 2009's null discrimination may partly
  reflect its cluster position (whether it co-clusters with the American-origin or
  Asian-origin systems).

---

## Phase A.3 coverage context

The 4-system Phase B concordance analysis was computed on a denominator of
`n_feasible_rows` (rows where all 4 systems could be scored), not total rows,
because primitive coverage constraints at Phase A.3 reduced full-feature availability:
- Echogenic foci: 24.8% coverage → ACR strict requires all 5 features
- Shape (taller-than-wide): 58% coverage → affects K-TIRADS and C-TIRADS
- Margins: 60% coverage → affects Kwak and K-TIRADS

The 76.06% unanimous rate applies to the feasible denominator. The 11-system cluster
analysis will use the same feasible-denominator convention, stratifying results by
primitive-coverage completeness tier.

---

## Deliverable

`pub_workspace.qc_tirads_multisystem_cluster_v1` — per-nodule cluster data  
`exports/phase_e_audit_adjudication_20260507/cluster_analysis/` — concordance matrix CSV,
dendrogram PNG, subgroup table CSV  
Script: `scripts/429_tirads_cluster_replication.py` (to be authored after Step 5 queue build)
