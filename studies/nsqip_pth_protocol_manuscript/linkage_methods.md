# NSQIP Linkage — Methods Section Text

## Data Source

ACS-NSQIP thyroidectomy data were obtained from the institutional NSQIP
Case Details and Custom Fields Report (December 14, 2025 extract),
comprising 1,281 thyroid-specific cases (CPT codes 60220–60271) with
prospectively collected 30-day outcomes including the NSQIP
thyroidectomy-specific module fields.

## Linkage Methodology

NSQIP cases were linked to the institutional thyroid cancer research
database using a multi-step deterministic matching protocol. No
probabilistic or fuzzy matching was employed.

**Step 1 — MRN-based matching:** The NSQIP institutional identifier
(IDN) was matched against hospital medical record numbers (EUH_MRN,
TEC_MRN) extracted from four independent institutional source files
(synoptic pathology, operative details, clinical notes, and
complications). Matches were confirmed by requiring exact surgery date
concordance between the NSQIP operation date and the master cohort
surgery date.

**Step 2 — Multi-surgery resolution:** For patients whose MRN was
confirmed across ≥3 independent source files with concordant sex, age,
and date of birth, but whose NSQIP operation date did not match the
master cohort primary surgery date, the institutional surgical timeline
was queried to identify multi-surgery patients where NSQIP captured an
earlier or different procedure.

**Step 3 — Verification:** All matches were independently verified via
sex concordance, age concordance (within 1 year), and — where available
— date of birth matching against source files.

## Linkage Results

Of 1,281 NSQIP Case Details records, 1,275 (99.5%) were successfully
linked to 1,261 unique patients in the master cohort. Match methods:
reuse of prior verified linkage (n=1,086), MRN + exact date (n=185),
MRN disambiguation by date (n=2), and MRN + DOB multi-surgery
resolution (n=2). Six cases (0.5%) were unmatched: one MRN collision
(confirmed different patient), one MRN absent from all sources, and four
patients not present in the institutional data extract (predominantly
2024 cases). Fourteen patients (1.1%) had two thyroidectomy surgeries
captured in NSQIP; the index (chronologically first) procedure was used
for patient-level analyses.
