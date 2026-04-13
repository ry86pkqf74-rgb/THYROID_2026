# Clinician review packet — ultrasound / FNA / Bethesda linkage

**Audit date:** 2026-04-13  
**Purpose:** Close documentation and data-linkage gaps without guessing which nodule was biopsied.

---

## A. Ultrasound nodules and TI-RADS

- Structured ultrasound nodule rows from the **COMPLETE multi-sheet workbook** match the database **one-for-one** for that corpus.
- TI-RADS: when the radiologist assigned a category and ACR criteria were available, the database holds **both** the reported category and the ACR re-calculation. A one-point difference between them is **not automatically an error**—radiologists sometimes down- or up-grade borderline nodules.

**Action:** None for “missing TR” on sufficient-source COMPLETE rows per audit.

---

## B. Lymph nodes on ultrasound

- Automated audit found **no** missed positive/suspicious lymph-node statements and **no** missed explicit “negative” preservation issues in the strict lists.

**Action:** None required from this packet for LN capture.

---

## C. FNA Bethesda category

- Many FNA episodes have a Bethesda class in one system (e.g. cytology table) and a **different number** in another (episode index vs Excel-derived history). **~1900** such mismatches are listed machine-readable in `fna_bethesda_conflicts.csv`.

**Action:** For each patient episode in conflict, confirm the **correct Bethesda class** from the diagnostic report you trust (usually the pathology/cytology report), then record the adjudicated value. Do **not** assume the highest or lowest number is correct.

---

## D. Which nodule matches which FNA (and later pathology)

- **128** specific ultrasound-defined nodules have at least one FNA dated **within 90 days after** the ultrasound, but the automated multimodal linker did **not** create a primary link—often because **side**, **size**, or **specimen site** did not match automated rules.

**Action:** For each listed nodule in `human_review_packet.csv` (domain `nodule_FNA_linkage`), confirm whether the FNA in that window targeted **that** nodule. If yes, a data steward can approve a primary link; if no, document “different lesion” so the row is **not** forced into a false link.

---

## E. What this packet does *not* ask you to do

- Fix narrative-only ultrasound that was never structured as nodule rows.
- Resolve conflicting Bethesda numbers **without** looking at source reports.

---

*Supporting CSV:* `human_review_packet.csv` (2027 rows: 128 linkage + 1899 Bethesda conflicts).
