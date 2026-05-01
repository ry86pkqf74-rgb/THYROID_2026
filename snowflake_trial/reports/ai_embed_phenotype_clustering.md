# AI_EMBED Phenotype Clustering — Malignant Cohort (n=500 sample)
**Generated:** 2026-05-01 18:02:21
**Method:** Snowflake AI_EMBED ('snowflake-arctic-embed-m-v1.5') → 768-d vectors → sklearn KMeans (k=6)

## Cluster sizes

| cluster | n |
| --- | --- |
| 0 | 175 |
| 1 | 44 |
| 2 | 110 |
| 3 | 31 |
| 4 | 28 |
| 5 | 112 |

## Per-cluster phenotype profile

### Cluster 0 (n=175)

- **Mean age:** 48.0
- **Top histologies:** PTC (157), MTC (8), NIFTP (8)
- **Top stages:** I (74), II (72), IVB (29)
- **BRAF+:** 8.6%
- **Recurrence:** 7.4%

### Cluster 1 (n=44)

- **Mean age:** 54.9
- **Top histologies:** follicular carcinoma (40), poorly differentiated thyroid carcinoma (2), anaplastic carcinoma (2)
- **Top stages:** II (21), IVB (12), I (11)
- **BRAF+:** 6.8%
- **Recurrence:** 11.4%

### Cluster 2 (n=110)

- **Mean age:** 54.0
- **Top histologies:** PTC (98), NIFTP (4), FTUMP (3)
- **Top stages:** I (46), II (40), IVB (23)
- **BRAF+:** 10.9%
- **Recurrence:** 20.0%

### Cluster 3 (n=31)

- **Mean age:** 55.5
- **Top histologies:** follicular carcinoma (29), metastatic follicular carcinoma (1), high grade carcinoma with focal squamous features (1)
- **Top stages:** II (18), I (7), IVB (6)
- **BRAF+:** 6.5%
- **Recurrence:** 19.4%

### Cluster 4 (n=28)

- **Mean age:** 51.0
- **Top histologies:** metastatic PTC (18), PTC (6), MTC (1)
- **Top stages:** II (15), I (10), IVB (2)
- **BRAF+:** 0.0%
- **Recurrence:** 28.6%

### Cluster 5 (n=112)

- **Mean age:** 48.4
- **Top histologies:** PTC (99), MTC (7), metastatic PTC (5)
- **Top stages:** II (53), I (42), IVB (17)
- **BRAF+:** 7.1%
- **Recurrence:** 17.0%

## Cluster centroid representatives

| cluster | rid | phenotype |
| --- | --- | --- |
| 0 | 950 | Age 46; Sex female; Histology PTC; Stage I; T T1a; N N0; M M0; Size 0.7cm; ETE microscopic; LN+ 0; Surgery total_thyroid... |
| 1 | 1526 | Age 67; Sex female; Histology follicular carcinoma; Stage I; T T2; N N0; M M0; Size 2.4cm; ETE microscopic; LN+ 0; Surge... |
| 2 | 5358 | Age 49; Sex female; Histology PTC; Stage I; T T2; N N1a; M M0; Size 4cm; ETE microscopic; LN+ NULL; Surgery hemithyroide... |
| 3 | 10522 | Age 59; Sex female; Histology follicular carcinoma; Stage IVB; T T2; N N1a; M M1; Size 2.2cm; ETE microscopic; LN+ NULL;... |
| 4 | 531 | Age 47; Sex female; Histology metastatic PTC; Stage I; T T1a; N N1a; M M0; Size 0.2cm; ETE none; LN+ 5; Surgery other; R... |
| 5 | 11083 | Age 46; Sex female; Histology PTC; Stage II; T T3b; N N1a; M M1; Size 2.2cm; ETE gross; LN+ 9; Surgery total_thyroidecto... |
