# NSQIP PTH Protocol — Manuscript-Ready Statistics

**Source:** NSQIP Case Details linkage (N=1,261 patients, first surgery per patient)
**Generated:** 2026-03-10
**Linkage rate:** 1,275/1,281 (99.5%)

---

## Demographics

| Variable | Value |
|----------|-------|
| Total patients | 1,261 |
| Total surgical cases | 1,275 |
| BMI | median 28.9, IQR [25.0–34.3] |

### ASA Classification

| ASA Class | n (%) |
|-----------|-------|
| ASA I — Normal/Healthy | 59 (4.7%) |
| ASA II — Mild systemic disease | 578 (45.8%) |
| ASA III — Severe systemic disease | 582 (46.2%) |
| ASA IV — Severe systemic disease, threat to life | 42 (3.3%) |

## Comorbidities

| Comorbidity | n (%) |
|-------------|-------|
| Diabetes mellitus | 189 (15.0%) |
|   — Non-insulin | 124 (9.8%) |
|   — Insulin | 65 (5.2%) |
| Tobacco/Nicotine use | 145 (11.5%) |
| Hypertension requiring medication | 582 (46.2%) |

## Operative Details

| Variable | Value |
|----------|-------|
| Operative duration | median 109 min, IQR [84–152] |
| Inpatient | 188 (14.9%) |
| Outpatient | 1,073 (85.1%) |
| Central neck dissection | 248/946 (26.2%) |
| Lateral neck dissection | 39/257 (15.2%) |
| Drain usage | 284/950 (29.9%) |
| RLN monitoring | 730/944 (77.3%) |

### CPT Code Distribution

| CPT | Description | n (%) |
|-----|-------------|-------|
| 60240 | Thyroidectomy, total or complete | 689 (54.6%) |
| 60252 | Total for malignancy + limited neck dissection | 222 (17.6%) |
| 60271 | Including substernal, cervical approach | 168 (13.3%) |
| 60260 | Completion thyroidectomy | 141 (11.2%) |
| 60254 | Total for malignancy + radical neck dissection | 22 (1.7%) |
| 60270 | Including substernal, sternal split | 19 (1.5%) |

## Outcomes

### Length of Stay & Discharge

| Variable | Value |
|----------|-------|
| Same-day discharge (LOS=0) | 319 (25.3%) |
| Hospital LOS median | 1.0 day |
| Hospital LOS IQR | [0–1] |
| Hospital LOS mean | 1.29 days |
| Hospital LOS max | 28 days |

### 30-Day Events

| Event | n/N (%) |
|-------|---------|
| 30-day readmission | 29/1,261 (2.3%) |
| 30-day mortality | 1/1,245 (0.1%) |
| Any SSI | 7/1,261 (0.6%) |
| VTE (DVT or PE) | 4/1,261 (0.3%) |

### Hypocalcemia

| Variable | n/N (%) |
|----------|---------|
| Postoperative hypocalcemia (overall) | 82/939 (8.7%) |
| Hypocalcemia pre-discharge | 31/945 (3.3%) |
| Hypocalcemia post-discharge | 82/943 (8.7%) |

### Calcium/Vitamin D Replacement

| Category | n/N (%) |
|----------|---------|
| Both calcium and vitamin D | 478/945 (50.6%) |
| Calcium only | 207/945 (21.9%) |
| Vitamin D only | 85/945 (9.0%) |
| None | 175/945 (18.5%) |

### Surgical Complications

| Complication | n/N (%) |
|-------------|---------|
| RLN injury/dysfunction | 88/931 (9.5%) |
| Neck hematoma/bleeding | 16/936 (1.7%) |

## Preoperative Labs (median [IQR])

| Lab | Median [IQR] | Unit | n |
|-----|-------------|------|---|
| Sodium | 139.0 [138.0–140.0] | mEq/L | 1,081 |
| Creatinine | 0.8 [0.7–1.0] | mg/dL | 1,089 |
| Albumin | 4.0 [3.7–4.2] | g/dL | 702 |
| WBC | 6.4 [5.2–7.9] | K/uL | 1,029 |
| Hematocrit | 39.5 [36.9–42.0] | % | 1,029 |
| Platelets | 252.5 [210.2–300.0] | K/uL | 1,030 |

## Data Source Notes

- Thyroidectomy-specific module fields (hypocalcemia, Ca/VitD, RLN) available for ~74–75% of patients (939–945/1,261)
- General NSQIP fields (readmission, LOS, ASA, comorbidities, labs) available for all 1,275 matched cases
- Multi-surgery patients (n=14, 1.1%) analyzed using index (first) procedure
- All statistics from `exports/nsqip/nsqip_patient_summary.parquet`
