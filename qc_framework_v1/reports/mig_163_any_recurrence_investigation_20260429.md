# mig_163 ANY-RECURRENCE investigation (read-only profile)

**Batch_id:** `mig_163_any_recurrence_investigation_20260429`  
**Lane:** 51 / mig_163  
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck, read-only probes)  
**Generated:** 2026-04-29  
**Source prompt:** `cursor_prompts/CURSOR_PROMPT_mig163_any_recurrence_investigation_20260429.md` (repo; mirrors GitHub `main` path)

---

## 1. Executive summary

- **PM vs canonical 2×2 reconciles exactly** to the Cowork snapshot (165 / 219 / 349 / 10,138). The **349 “canon-only”** patients (`ARF=FALSE` but `canonical_recurrence_v1.recurrence_confirmed=TRUE`) are dominated by **`surgical_pathology` + `structural_confirmed`** (320 + 29 FNA-confirmed + 3 Bethesda V); PM often has **wide proxy flags** (`structural_recurrence_flag` / `distant_mets_proxy`) **ON** while **`any_recurrence_flag` stays FALSE** — a **builder / precedence bug or definitional gap**, not “no signal in data.”
- **Proxy-to-canonical map (§3.1):** among **1,818** patients with `structural_recurrence_flag=TRUE` (same count for `distant_mets_proxy`), **1,467** fall in **no Tier-2 overlap** with `recurrence_confirmed`, `path_proven`, or `imaging_only_unconfirmed` in the join pattern used — i.e. **imaging-suspicion / NLP envelope** without resolved canonical rows. **Biochemical** proxy is much smaller (**128** patients) with **88** lacking those canonical joins.
- **Tier-1 LLM JSON (`note_entities_llm_recurrence`):** deterministic sample (**20** patients from `struct_no_canonical_source` structural flags) showed **persisted rows per patient** (`result_json`), but **`entities` arrays were uniformly empty** — the **Tier-1 layer is not presently encoding recurrence phenotypes for these IDs** (empty extractor output), so **PM structural flags are not inferable from this JSON** for the sampled set.
- **Definitions:** **STRICT** (= **514**) and **Cowork HYBRID** (`recurrence_confirmed` **OR** `resolved.path_proven`) are **numerically identical** in live data: **0** `path_proven` patients exist outside **`recurrence_confirmed=TRUE`** (path ⊆ confirmed). **WIDE** (naive OR of proxies + canonical) **= 2,187** (+**1,803** vs current **384** `ARF` TRUE), matching the “over-correction” warning.

---

## 2. The 2×2 reconcile + 349 canon-only profile

| Cell | Count | Interpretation |
|------|------:|----------------|
| ARF=T / `recurrence_confirmed`=T | **165** | Aligned positives |
| ARF=T / `recurrence_confirmed`=F | **219** | PM-only (wider envelope) |
| ARF=F / `recurrence_confirmed`=T | **349** | **Canon-only / derivation gap** |
| Neither | **10,138** | No recurrence |

**349 canon-only — `recurrence_type` (canonical_recurrence_v1)**

| recurrence_type | n |
|------------------|---|
| structural_confirmed | 320 |
| fna_confirmed | 29 |

**349 canon-only — `recurrence_definition`**

| recurrence_definition | n |
|----------------------|---|
| surgical_pathology | 320 |
| fna_bethesda_vi_malignant | 26 |
| fna_bethesda_v_suspicious | 3 |

---

## 3. §3.1 Source-distribution matrix (3 PM proxies × 5 columns)

Overlaps in columns 2–4 are **non-exclusive** (a patient may match multiple tiers). Column 5 is **mutually exclusive** by construction (`no_canonical_source`).

| PM proxy (`TRUE` row count) | A: in `canonical_recurrence_v1.recurrence_confirmed` | B: in resolved `path_proven` | C: in resolved `imaging_only_unconfirmed` | D: no canonical join (columns A–C all absent) |
|----------------------------|------:|-----:|-----:|-----:|
| `structural_recurrence_flag` (1,818) | 244 | 98 | 131 | **1,467** |
| `distant_mets_proxy` (1,818) | 244 | 98 | 131 | **1,467** |
| `biochemical_recurrence_flag` (128) | 29 | 12 | 15 | **88** |

*Join keys: `research_id` cast to `VARCHAR` throughout; Tier-2 filters as in probe SQL.*

**Interpretation.** **Structural / distant proxies** coincide in **row counts** — likely **shared derivation** in PM. **`D` is dominant** (~81% of proxy-TRUE cohort), consistent with **`imaging + suspicion / NLP`** without **path-proven / imaging-tier** alignment in **`canonical_recurrence_resolved_v1`** under the predicates used.

---

## 4. §3.2 Tier-1 LLM probe (20 deterministic `rid`s)

**Selection:** Patients with **`structural_recurrence_flag=TRUE`** **and** no overlap with **recurrence_confirmed**, **`path_proven`**, **`imaging_only_unconfirmed`**; ordered **`hash(rid)`**, **`LIMIT 20`**.

**Observation:** **`note_entities_llm_recurrence`** returned **1–4 rows per `research_id`**, but every persisted **`result_json`** inspected was `{"entities": []}` (**empty entity list**).

**Aggregate over sample:** **20 / 20** showed **zero parsed `entity_type` entries** → **Tier-1 LLM JSON cannot explain structural PM positives** on this slice; upstream PM builder must pull from **other feeders** or **pre-join flags**.

---

## 5. §3.3 Reverse-trace (10 deterministic `rid`s — canon-only cohort)

Hashes used: same pattern as **`ORDER BY hash(rid)`** on **`ARF=FALSE ∧ recurrence_confirmed=TRUE`**.

| rid | PM: ARF | bioch | struct | distant | malignant | Canonical: type / definition / evidence_src | Resolved: status_final |
|-----|---------|-------|--------|---------|-----------|--------------------------------------------|---------------------------|
| 7978 | F | F | **T** | **T** | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 9533 | F | F | **T** | **T** | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 6670 | F | NULL | NULL | F | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 7065 | F | **T** | F | F | T | fna_confirmed / fna_bethesda_vi_malignant / fna_cytology | **path_proven** |
| 3523 | F | NULL | NULL | F | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 2827 | F | NULL | NULL | F | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 9850 | F | F | **T** | **T** | T | structural_confirmed / surgical_pathology / reoperation_pathology | path_proven |
| 10959 | F | F | **T** | **T** | T | structural_confirmed / surgical_pathology / reoperation_pathology | path_proven |
| 2942 | F | NULL | NULL | F | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |
| 5770 | F | NULL | NULL | F | T | structural_confirmed / surgical_pathology / reoperation_pathology | none |

**Pattern:** Canonical **already confirms recurrence** (`recurrence_confirmed=TRUE`) with **surgical / FNA definitions**, while **`any_recurrence_flag` stays FALSE** despite repeated **proxy TRUE** on many rows — supports **ARF builder bug / rule gap**, not absence of recurrence. Several resolved rows show **`path_proven`** or **`none`** inconsistently vs canonical type; **alignment work** belongs in **mig_163b** after Logan’s definition.

---

## 6. §3.4 Three-option counts + deltas

| Definition | ARF=TRUE patients (N) | Malignant among positive (N) | vs current PM `any_recurrence_flag` | Drops from current | Adds from current |
|------------|------------------------:|-----------------------------:|--------------------------------------|--------------------|--------------------|
| **(a) STRICT** — `recurrence_confirmed=TRUE` only | **514** | **508** / 514 | **94.8%** agreement (10,303 / 10,871) | **219** PM-only | **349** canon-only |
| **(b) WIDE** — `bioch ∨ struct ∨ distant ∨ recurrence_confirmed` | **2,187** | **2,168** / 2,187 | **83.4%** agreement (9,064 / 10,871) | *(net −1,803 who would flip to TRUE)* | Embeds **~1,803** net flips from current 384 ARF-T |
| **(c) HYBRID** — `recurrence_confirmed ∨ resolved.path_proven` | **514** | **508** / 514 | **94.8%** agreement | **219** PM-only | **349** canon-only |

**Critical live-data fact:** **`path_proven` ⊆ `recurrence_confirmed`** — **0** patients have **`path_proven` without **`recurrence_confirmed`** in the current rebuild. Therefore **HYBRID == STRICT** at the **patient level** today. If Logan chooses HYBRID for **future-proofing** (in case tiers diverge), specify that as **policy**, not a distinct N.

**Naive OR vs prompt:** +**1,803** ARF-T vs current **384** (matches **2,187 − 384**).

---

## 7. §3.5 Sanity gates

| Gate | Result |
|------|--------|
| `canonical_patient_master` rows / distinct `research_id` | **10,871 / 10,871** |
| `canonical_recurrence_v1` `recurrence_confirmed=TRUE` | **514** |
| 2×2 (§1) | **165 / 219 / 349 / 10,138** |

---

## 8. Decision request for Logan (STRICT / WIDE / HYBRID)

**Manuscript / analytic posture**

- **STRICT (514):** Aligns **`any_recurrence_flag`** with **Tier-1 SSOT** **`canonical_recurrence_v1`**, **fixes 349 under-counts**, **drops 219 PM-only** (often **imaging-NLP / suspicion**). **Conservative, protocol-defensible** “confirmed recurrence.”
- **WIDE (2,187):** Maximizes sensitivity; **captures suspicion** at the cost of **1,803** flips — **unsafe** for headline “recurrence prevalence” unless methods explicitly define **suspected vs confirmed** strata.
- **HYBRID (live = 514):** **Operationally identical to STRICT today** because **path-proven ⊆ confirmed**. Use if you want **explicit OR** with **resolved pathology tier** for **future** SSOT splits; otherwise **redundant**.

**Recommendation (data-driven, Logan decides):** Prefer **STRICT** (or **HYBRID** as equivalent policy wrapper) **+** manuscript **dual reporting** — e.g. **confirmed** (`recurrence_confirmed`) **vs imaging/proxy suspicion** (`structural_recurrence_flag` without canonical confirmation) — instead of collapsing into one noisy boolean.

---

## 9. Open carry-forwards

- **`CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT`** — remain open until **mig_163b** apply after Logan picks.
- **Empty `entities` arrays** in **`note_entities_llm_recurrence`** for **struct-no-source** IDs — schedule **Tier-1 pipeline / join audit** separately from PM boolean.
- **PM proxy flags ON + `ARF` FALSE** — treat as **explicit regression test** for **mig_163b builder**.

---

## 10. SQL / provenance

Read-only probes recorded in **`qc_framework_v1/migrations/163_any_recurrence_investigation_probes_20260429.sql`**.
