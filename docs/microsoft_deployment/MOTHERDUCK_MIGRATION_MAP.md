# MotherDuck Migration Map
## THYROID_2026 Cloud-to-Local Transition Guide

Complete mapping of every MotherDuck operation to its Microsoft 365 / local DuckDB replacement.

**Scope:** 307 files, 1,786 MotherDuck references
**Target:** Zero cloud database dependency by Phase 4E (Day 10)
**Migration Model:** Cloud materialized views → Local Parquet + Power BI DAX measures

---

## 1. Database Connection & Authentication

### MotherDuck (Current)

```python
import motherduck
conn = motherduck.connect(token=MOTHERDUCK_TOKEN)
```

**Issues:**
- Cloud token required; exposed in .env, GitHub Secrets, CI/CD
- Cloud dependency for every query (network latency, cloud outage risk)
- Token rotation/invalidation breaks production

### Microsoft Replacement

**Option A: Local DuckDB (Recommended)**
```python
import duckdb
conn = duckdb.connect('THYROID_2026.db')  # Local SQLite-like file
```

**Option B: Local Parquet + Pandas (For Pure Analytics)**
```python
import pandas as pd
import pyarrow.parquet as pq

df = pd.read_parquet('01_SILVER_DEID_PARQUET/patient_demographics.parquet/')
```

**Option C: DuckDB + Parquet (Hybrid)**
```python
import duckdb
conn = duckdb.connect('THYROID_2026.db')
conn.execute("CREATE OR REPLACE TABLE demographics AS SELECT * FROM 'file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet'")
```

**Benefits:**
- Zero credentials needed
- 100% offline-capable
- 10x faster (local disk vs. cloud)
- No token rotation
- FileVault-encrypted by default

---

## 2. Cloud Table Queries

### MotherDuck (Current)

```python
# Query MotherDuck cloud table
df = pd.read_sql(
    "SELECT research_id, age_at_diagnosis, sex FROM patient_demographics WHERE age_at_diagnosis > 50",
    conn
)
```

**Issues:**
- Each query is a cloud RPC (latency, cost)
- Cloud schema must be maintained separately
- No local caching

### Microsoft Replacement

**Option A: Direct Parquet Read (Fastest)**
```python
import pandas as pd

df = pd.read_parquet(
    'file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet/',
    columns=['research_id', 'age_at_diagnosis', 'sex'],
    filters=[('age_at_diagnosis', '>', 50)]
)
```

**Option B: DuckDB SQL (More Familiar)**
```python
import duckdb

result = duckdb.sql("""
    SELECT research_id, age_at_diagnosis, sex
    FROM 'file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet/'
    WHERE age_at_diagnosis > 50
""")
df = result.to_df()
```

**Option C: Power BI (For Business Users)**
- Data imported via Power Query M script
- DAX measures replace SQL WHERE clauses
- Visual filtering via slicers (no code needed)

**Benefits:**
- No cloud latency
- Parquet partitioning optimizes reads (partition by year)
- DuckDB supports SQL without Postgres/MySQL overhead
- Power BI makes queries visual/interactive

---

## 3. Database Shares & Access Control

### MotherDuck (Current)

```python
# Create shareable link to cloud database
share_token = conn.get_share_token()
# Share link: motherduck://org/share/<token>
```

**Issues:**
- Expiring tokens; re-sharing needed
- No fine-grained permissions (all-or-nothing access)
- Cloud-dependent for collaboration

### Microsoft Replacement

**Option A: Parquet Export → SharePoint (Read-Only)**
```python
# Export de-identified data to CSV
df.to_csv('03_DEID_EXPORTS/demographics_public.csv', index=False)

# Upload to SharePoint via Power Automate
# Share link: https://emory.sharepoint.com/...
# Permissions: Read-only, expires per policy (7 years minimum)
```

**Option B: Power BI Web (Phase 5)**
```
# Publish .pbix to Power BI Cloud
# RLS enforces row-level permissions per user
# Share: app.powerbi.com/groups/[workspace]/reports/[reportId]
```

**Option C: OneDrive Shared Folder**
```
# Metadata-only exports to OneDrive (no PHI)
# Share link with selective sync (data never leaves Mac in full form)
# Emory has unlimited OneDrive storage
```

**Benefits:**
- Granular permissions (read-only, edit, viewer)
- Audit trail (who accessed, when)
- No token expiry (permissions-based)
- Version control (auto-retain 93+ days)

---

## 4. Materialized Views (Cloud)

### MotherDuck (Current)

```sql
-- Create materialized view in MotherDuck cloud
CREATE VIEW thyroid_summary AS
SELECT
    EXTRACT(YEAR FROM diagnosis_date) AS year,
    COUNT(DISTINCT research_id) AS patient_count,
    AVG(age_at_diagnosis) AS avg_age,
    SUM(CASE WHEN outcome_status = 'deceased' THEN 1 ELSE 0 END) AS deaths
FROM episodes
GROUP BY EXTRACT(YEAR FROM diagnosis_date);

-- Cloud auto-refreshes on schedule
-- Query via: SELECT * FROM thyroid_summary
```

**Issues:**
- Materialized view lives in cloud (requires cloud infra)
- Manual refresh scheduling
- Cloud refresh latency
- Maintenance burden

### Microsoft Replacement

**Option A: Power Query Transform (Recommended)**
```m
// Power Query M script in Excel / Power BI
let
    Source = Parquet.Contents("file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/episode_facts.parquet/"),
    #"Converted to Table" = Table.FromRecords(Source),
    #"Grouped by Year" = Table.Group(
        #"Converted to Table",
        {"year"},
        {
            {"patient_count", each Table.RowCount(_), type number},
            {"avg_age", each List.Average([age_at_diagnosis]), type number},
            {"deaths", each List.Count(List.Select([outcome_status], each _ = "deceased")), type number}
        }
    )
in
    #"Grouped by Year"
```
- Executes on local machine during Power BI refresh
- No cloud storage
- Parameterized (dynamic by date range, stage, etc.)

**Option B: DuckDB View (SQL Compatibility)**
```python
import duckdb

conn = duckdb.connect('THYROID_2026.db')
conn.execute("""
    CREATE OR REPLACE VIEW thyroid_summary AS
    SELECT
        EXTRACT(YEAR FROM diagnosis_date_shifted) AS year,
        COUNT(DISTINCT research_id) AS patient_count,
        AVG(age_at_diagnosis) AS avg_age,
        SUM(CASE WHEN outcome_status = 'deceased' THEN 1 ELSE 0 END) AS deaths
    FROM 'file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/episode_facts.parquet/'
    GROUP BY EXTRACT(YEAR FROM diagnosis_date_shifted)
""")
df = conn.execute("SELECT * FROM thyroid_summary").to_df()
```
- Persisted in local DuckDB
- Refreshed on script execution
- Familiar SQL syntax

**Option C: DAX Measures (Power BI)**
```dax
// DAX in Power BI Desktop
Measure_Patient_Count_by_Year =
    VAR YearTable = SUMMARIZE(Fact_Episodes, Fact_Episodes[diagnosis_year], "Count", DISTINCTCOUNT(Fact_Episodes[research_id]))
    RETURN RETURN MAXX(YearTable, [Count])

Measure_Avg_Age = AVERAGE(Dim_Patient[age_at_diagnosis])
Measure_Deaths = CALCULATE(DISTINCTCOUNT(Fact_Outcomes[research_id]), Fact_Outcomes[outcome_status] = "deceased")
```
- Interactive (slicers refine calculations in real-time)
- No manual refresh needed
- Embedded in Power BI reports

**Benefits:**
- All local (zero cloud dependency)
- Power BI DAX = most powerful for analytics
- DuckDB views = SQL compatibility with ETL scripts
- Power Query = seamless Excel/Power BI integration

---

## 5. Dashboard & Reporting

### MotherDuck (Current)

```python
# Streamlit app querying MotherDuck cloud database
import streamlit as st
import motherduck
import pandas as pd

conn = motherduck.connect(token=MOTHERDUCK_TOKEN)

# Page: Patient Demographics
st.title("THYROID_2026 Patient Demographics")

# Slider: age filter (queries cloud on every change)
min_age = st.slider("Minimum Age", 0, 100, 50)
df = pd.read_sql(
    f"SELECT * FROM patient_demographics WHERE age_at_diagnosis >= {min_age}",
    conn
)
st.dataframe(df)

# Chart: patients by stage (cloud query on every page load)
stage_counts = pd.read_sql(
    "SELECT staging_code, COUNT(*) as count FROM episodes GROUP BY staging_code",
    conn
)
st.bar_chart(stage_counts)
```

**Issues:**
- Streamlit app hosted on cloud (Streamlit Cloud or custom server)
- MotherDuck cloud queries for every user interaction
- Slow (cloud RPC overhead)
- MotherDuck token exposure
- No offline capability

### Microsoft Replacement

**Power BI Desktop (Recommended)**
```
Local .pbix file: /Users/lhglosser/02_GOLD_POWERBI/THYROID_2026_SEMANTIC_MODEL.pbix

Features:
1. Import Parquet data into Power BI model on refresh
2. Build star schema (13 fact tables, 8 dimensions)
3. Define 20+ DAX measures (Total Patients, Recurrence Rate, Mortality, etc.)
4. Create 6 report pages:
   - Dashboard (KPI cards, overview charts)
   - Labs (TSH trends, distributions)
   - Imaging (modality breakdown, findings summary)
   - Pathology (TNM matrix, grade × survival)
   - Treatment (treatment type × outcome)
   - Data Quality (QC metrics, audit trail)
5. Add slicers (Date, Stage, Treatment Intent) for interactive filtering
6. Drill-through pages for patient cohort analysis

Why Power BI over Streamlit:
- 10x faster (local computation vs. cloud RPC)
- No token management
- Offline-capable (fully local)
- Professional visualizations (matrix, KPI cards, funnels)
- DAX = more powerful than SQL for complex analytics
- RLS ready (prepare for Phase 5 multi-user scenario)
```

**Alternative: Local Jupyter Notebook**
```python
# Jupyter running on Mac (jupyter notebook)
# Imports Parquet locally; visualizations via matplotlib/plotly
# Can be converted to HTML for sharing (no cloud, no dynamic interaction)
```

**Benefits:**
- Power BI Desktop: Professional dashboards, enterprise-grade, local-first
- Jupyter: Flexible, exploratory analysis, easy for data scientists
- No cloud cost, no latency, no token expiry

---

## 6. Cloud Database Credentials & Secrets

### MotherDuck (Current)

```yaml
# .env file (DO NOT COMMIT)
MOTHERDUCK_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# GitHub Secrets (exposed if repo becomes public)
MOTHERDUCK_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# CI/CD pipeline
env:
  MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
```

**Issues:**
- Token is a secret key (not meant to be environment-variable-public)
- Risk: token leaked → cloud database access compromised
- Token rotation = re-deploy all apps
- No per-repo/per-environment token granularity

### Microsoft Replacement

**No Credentials Needed (Zero-Trust Local)**
```python
# Python script: zero credentials
import duckdb
import pandas as pd

# Connect to local file (no auth)
conn = duckdb.connect('/Users/lhglosser/THYROID_SECURE_2026/THYROID_2026.db')

# Read Parquet (no token)
df = pd.read_parquet('/Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet/')

# All access controlled by macOS FileVault + local file permissions
```

**Benefits:**
- Zero secrets to manage
- FileVault encryption = physical data protection
- No token expiry
- No risk of accidental exposure
- Git can safely commit scripts (no credentials embedded)

---

## 7. Scheduled Refresh / Data Pipeline

### MotherDuck (Current)

```yaml
# CI/CD pipeline (e.g., GitHub Actions)
name: "Refresh MotherDuck Cloud Database"
on:
  schedule:
    - cron: "0 9 * * 1"  # Monday 9 AM UTC

jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - name: Run ETL
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
        run: |
          python3 SCRIPTS/00_ingest_demographics.py
          python3 SCRIPTS/01_ingest_episodes.py
          # ... 8 more scripts
          # All write to MotherDuck cloud
```

**Issues:**
- CI/CD runs on cloud server (not on owner's Mac)
- MotherDuck token exposed to GitHub
- Cloud uptime dependency
- Data flows through GitHub Actions → MotherDuck (compliance risk for PHI)

### Microsoft Replacement

**Power Automate Desktop Robot (Local RPA)**
```
Monday 9 AM UTC (adjusted for local TZ):

1. Power Automate Cloud Flow triggers: "Weekly THYROID_2026 Refresh"
2. Robot invocation: Power Automate Desktop on Mac
3. RPA Steps:
   a. Open Terminal (osascript)
   b. Execute: python3 SCRIPTS/00_deid_gateway.py --table=all
   c. Execute: python3 SCRIPTS/09_validate_relationships.py
   d. Open Power BI Desktop (osascript)
   e. Trigger: Cmd+Shift+R (Power BI refresh)
   f. Close Power BI & Terminal
   g. Copy VALIDATION_AUDITS/ to OneDrive (metadata only)
4. Send Teams alert: #thyroid-research channel "Silver refresh complete"
5. All logs stored in VALIDATION_AUDITS/ (local)
```

**Alternative: Local macOS Cron Job**
```bash
# /etc/cron.d/thyroid_refresh
0 9 * * 1 /Users/lhglosser/THYROID_SECURE_2026/SCRIPTS/weekly_refresh.sh
```

**Benefits:**
- Runs on owner's Mac (not cloud VM)
- Zero GitHub token exposure
- Data never leaves local disk
- Compliance-friendly (PHI stays on FileVault-encrypted Mac)
- Full control over execution environment

---

## 8. Data Warehouse Tables

### MotherDuck (Current)

```sql
-- Cloud materialized tables
CREATE TABLE episodes (
    research_id INT,
    episode_id INT,
    diagnosis_date DATE,
    admission_type VARCHAR,
    PRIMARY KEY (research_id, episode_id)
);

CREATE TABLE labs (
    research_id INT,
    lab_id INT,
    lab_date DATE,
    test_name VARCHAR,
    result_value DECIMAL(10,2),
    PRIMARY KEY (research_id, lab_id)
);

-- ... 8 more tables in MotherDuck cloud
```

**Issues:**
- Proprietary cloud schema (MotherDuck-specific)
- Cloud-dependent for every query
- No version control (schema changes untracked)

### Microsoft Replacement

**Local Parquet Partition Tables**
```
/Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/

├── episode_facts.parquet/
│   ├── year=2020/[parquet files]
│   ├── year=2021/[parquet files]
│   └── year=2022/[parquet files]
├── lab_facts.parquet/
│   ├── year=2020/[parquet files]
│   └── ...
├── imaging_facts.parquet/
├── pathology_facts.parquet/
├── treatment_facts.parquet/
├── nsqip_facts.parquet/
├── outcome_facts.parquet/
├── dim_patient.parquet/
├── dim_date.parquet/
├── dim_staging.parquet/
├── dim_treatment.parquet/
└── dim_outcome.parquet/
```

**Access via:**
```python
import duckdb
import pandas as pd

# Option 1: DuckDB (SQL interface)
df = duckdb.sql("SELECT * FROM 'episode_facts.parquet' WHERE year = 2021").to_df()

# Option 2: Pandas (direct read)
df = pd.read_parquet('01_SILVER_DEID_PARQUET/episode_facts.parquet/year=2021/')

# Option 3: Power Query (Excel/Power BI)
# M script: Parquet.Contents("file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/episode_facts.parquet/")
```

**Benefits:**
- Open format (Parquet); not vendor-locked
- Partitioned by year (optimized queries)
- Version-controlled via DVC (data lineage tracked)
- Queries execute locally (no cloud RPC)
- Metadata stored in git (schema evolution traceable)

---

## 9. User Access & Permissions

### MotherDuck (Current)

```
MotherDuck Cloud:
- Organization: Emory
- Users: Logan (admin, full access)
- Access method: MotherDuck token (all-or-nothing)
- Audit: Limited (no row-level audit trail)
```

**Issues:**
- No role-based access control (RBAC)
- No row-level security (RLS)
- All users see all data if token shared
- Audit trail not compliance-grade

### Microsoft Replacement

**Phase 4: Single-User Local (Logan)**
```
FileVault Encryption:
- /Users/lhglosser/THYROID_SECURE_2026/ (chmod 700)
- Only Logan can access (authentication via Mac login)
- All file access logged by FileVault
```

**Phase 5: Multi-User Cloud (Power BI Web + RLS)**
```
Power BI Service:
- Publish .pbix to cloud
- Enable Row-Level Security (RLS):
  - Logan: sees all research_ids
  - Co-author 1: sees subset (e.g., 2020-2021 data only)
  - Co-author 2: sees different subset
- Audit trail: Power BI activity log (7-year retention)
- User access: managed via Emory AAD (Outlook/Teams login)
```

**Benefits:**
- Phase 4: Simplicity (single user, FileVault-encrypted)
- Phase 5: Enterprise-grade (RLS, audit, multi-user)
- No token sharing; credential-less access (SSO via Emory)

---

## 10. Backup & Disaster Recovery

### MotherDuck (Current)

```
Cloud Backup:
- MotherDuck maintains cloud backups (proprietary)
- No user-controlled backup mechanism
- Restore process: unknown (proprietary)
- Risk: cloud provider failure; data inaccessibility
```

**Issues:**
- No local copy of data
- Disaster recovery depends on MotherDuck uptime
- HIPAA requirement: 7-year data retention (cloud provider compliance unclear)

### Microsoft Replacement

**Local Weekly Encrypted Backups**
```bash
# Weekly backup script: SCRIPTS/weekly_backup.sh
cd /Users/lhglosser/THYROID_SECURE_2026

# Create full snapshot
zip -r -e 05_ARCHIVE_BACKUPS/$(date +%Y-%m-%d)_FULL_BACKUP.zip \
    01_SILVER_DEID_PARQUET/ \
    02_GOLD_POWERBI/THYROID_2026_SEMANTIC_MODEL.pbix \
    VALIDATION_AUDITS/ \
    SCRIPTS/ \
    DOCUMENTATION/

# Copy encrypted backup to OneDrive (metadata backup location)
cp 05_ARCHIVE_BACKUPS/$(date +%Y-%m-%d)_FULL_BACKUP.zip \
   /Users/lhglosser/OneDrive\ -\ Emory/THYROID_BACKUPS/
```

**Disaster Recovery Procedure**
```bash
# Restore from backup
unzip -e 05_ARCHIVE_BACKUPS/2026-04-03_FULL_BACKUP.zip \
      -d /Users/lhglosser/THYROID_SECURE_2026/

# Verify integrity
python3 SCRIPTS/09_validate_relationships.py

# Refresh Power BI
open -a "Microsoft Power BI" 02_GOLD_POWERBI/THYROID_2026_SEMANTIC_MODEL.pbix
```

**Benefits:**
- User-controlled backups (not dependent on cloud provider)
- Encrypted locally (FileVault) + at-rest (OneDrive encryption)
- 7-year retention (DVC + immutable archives)
- HIPAA-compliant (metadata only in cloud; PHI encrypted)
- Restore in < 1 hour (local SSD fast)

---

## 11. Cost Comparison

| Metric | MotherDuck (Cloud) | Microsoft (Local) | Savings |
|---|---|---|---|
| **Database Storage** | $0.20/GB/month | $0 (local SSD) | 100% |
| **Query Costs** | $0.003 per GB scanned | $0 (local queries) | 100% |
| **Cloud Token Management** | Labor (rotation, security) | $0 (no tokens) | 100% |
| **Backup Infrastructure** | Included (opaque) | OneDrive (included in Enterprise) | 100% |
| **Data Egress (if any)** | Variable (cloud-dependent) | $0 (local-only) | 100% |
| **Annual Cost Estimate (11.7 GB data)** | ~$100-500 | $0 | 100% |
| **Compliance Audit Labor** | Higher (cloud provider audit required) | Lower (local audit trail) | ~30% savings |

**ROI:** Switching to local architecture saves ~$100-500/year + eliminates cloud dependency risk + improves HIPAA compliance.

---

## 12. Migration Checklist (307 Files)

### Phase 4A: Foundation (Day 1)

**Credentials & Secrets:**
- [ ] Remove MOTHERDUCK_TOKEN from .env
- [ ] Remove MOTHERDUCK_TOKEN from GitHub Secrets
- [ ] Update .gitignore (add *.env, secrets/)
- [ ] CI/CD: Disable MotherDuck token injection (GitHub Actions, GitLab CI, etc.)
- [ ] Verify: grep -r "MOTHERDUCK_TOKEN" . | wc -l = 0

**Package Dependency:**
- [ ] Remove `motherduck` from requirements.txt
- [ ] Add: `duckdb==1.0.0`, `pyarrow==15.0.0`
- [ ] Reinstall: pip install -r requirements.txt
- [ ] Test: `python3 -c "import duckdb; print(duckdb.__version__)"`

---

### Phase 4B: ETL Refactoring (Days 2-3)

**High-Priority Scripts (35 files):**
- [ ] `SCRIPTS/00_ingest_demographics.py`: Replace motherduck → duckdb
- [ ] `SCRIPTS/01_ingest_episodes.py`: Replace motherduck → Parquet read
- [ ] `SCRIPTS/02_ingest_labs.py`: Refactor
- [ ] ... (8 more)
- [ ] `SCRIPTS/09_validate_relationships.py`: Update to validate local Parquet
- [ ] `SCRIPTS/10_generate_deid_audit.py`: Update to reference local files

**Test Execution:**
- [ ] Run refactored scripts locally on sample data
- [ ] Verify output Parquet files created
- [ ] Compare row counts: MotherDuck cloud → local Parquet (should match)

---

### Phase 4C: Streamlit Removal (Days 4-5)

**Streamlit Apps (50 files):**
- [ ] Archive: mv streamlit_apps/ ARCHIVE/streamlit_apps_deprecated/
- [ ] Or Delete: rm -rf streamlit_apps/
- [ ] Update docs: Remove Streamlit installation/usage instructions
- [ ] Update DOCUMENTATION/: Reference Power BI Desktop instead

**Power BI Setup:**
- [ ] Create THYROID_2026_SEMANTIC_MODEL.pbix
- [ ] Import Parquet tables via Power Query
- [ ] Define relationships
- [ ] Create 6 report pages
- [ ] Test: All visualizations render, slicers work

---

### Phase 4D: Configuration & CI/CD (Days 6-7)

**Configuration Files (30 files):**
- [ ] Remove MotherDuck connection strings from config.yaml
- [ ] Add local Parquet paths: `SILVER_LAYER_PATH=/Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/`
- [ ] Remove: motherduck credentials, cloud DB names
- [ ] Update: paths to reference /Users/lhglosser/... (absolute paths)

**Tests (20 files):**
- [ ] Update unit test fixtures: Remove MotherDuck mocks
- [ ] Add: Local DuckDB fixtures (in-memory or temp database)
- [ ] Example: `@pytest.fixture def mock_db(): return duckdb.connect(':memory:')`

**CI/CD Pipeline (15 files):**
- [ ] GitHub Actions: Remove MOTHERDUCK_TOKEN secret injection
- [ ] Update: script to run locally (not cloud queries)
- [ ] Example: Instead of `motherduck query`, run `duckdb THYROID_2026.db < query.sql`

---

### Phase 4E: Documentation & Cleanup (Days 8-10)

**Documentation (50 files):**
- [ ] README.md: Update architecture section (remove MotherDuck, add local architecture)
- [ ] SETUP.md: Replace MotherDuck token setup with "No credentials needed"
- [ ] Architecture diagrams: Remove cloud database, add local Parquet + DuckDB
- [ ] SCRIPTS/config_example.yaml: Remove motherduck examples

**Notebooks (30 files):**
- [ ] Jupyter notebooks: Remove MotherDuck cell execution or mark as "deprecated/archived"
- [ ] Convert: Active notebooks to Python scripts if necessary
- [ ] Or Archive: Move unused notebooks to ARCHIVE/notebooks_deprecated/

**Comments & References (27 files):**
- [ ] Global find-and-replace: "motherduck" → "duckdb" (in comments)
- [ ] Example: `# MotherDuck query` → `# Local DuckDB query`
- [ ] Final audit: `grep -r "motherduck" . | wc -l` (should be 0)

---

### Phase 4F: Validation & Sign-Off (Days 11-14)

**Data Reconciliation:**
- [ ] Row count comparison: MotherDuck cloud DB → local Parquet (all tables)
- [ ] Record sample: Pick 10 random research_ids, verify all columns match
- [ ] Date shifts: Verify all dates shifted by documented amount

**Functional Testing:**
- [ ] Run all refactored ETL scripts end-to-end
- [ ] Verify Power BI model loads and calculates measures
- [ ] Test Power Automate Desktop robot (manual trigger)
- [ ] Verify audit logs created in VALIDATION_AUDITS/

**Compliance Sign-Off:**
- [ ] Logan + Emory IRB/Privacy Officer: Review VALIDATION_AUDITS/
- [ ] Sign-off: VALIDATION_AUDITS/migration_complete_sign_off.md
- [ ] Archive MotherDuck docs (historical reference)
- [ ] Final git commit: "Phase 4F: MotherDuck migration complete (307 files, 1,786 refs)"

---

## Migration Commands Reference

### Convert a Single Python Script

**Before:**
```python
import motherduck
import pandas as pd

conn = motherduck.connect(token=MOTHERDUCK_TOKEN)
df = pd.read_sql("SELECT * FROM demographics WHERE age > 50", conn)
print(df.shape)
```

**After:**
```python
import duckdb

result = duckdb.sql("""
    SELECT * FROM 'file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet/'
    WHERE age_at_diagnosis > 50
""")
df = result.to_df()
print(df.shape)
```

### Verify Migration Success

```bash
# Check: zero MotherDuck references
grep -r "motherduck" /Users/lhglosser/THYROID_SECURE_2026/SCRIPTS/ 2>/dev/null | wc -l
# Expected output: 0

# Check: local DuckDB connectivity
python3 << 'EOF'
import duckdb
conn = duckdb.connect('/Users/lhglosser/THYROID_SECURE_2026/THYROID_2026.db')
result = conn.execute("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema='memory'").to_df()
print(f"DuckDB is functional: {result['table_count'].values[0]} tables present")
EOF

# Check: Parquet file integrity
python3 << 'EOF'
import pandas as pd
df = pd.read_parquet('/Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/patient_demographics.parquet/')
print(f"Parquet read successful: {len(df)} rows, {len(df.columns)} columns")
EOF
```

---

## Troubleshooting Common Migration Issues

| Issue | Cause | Solution |
|---|---|---|
| `ModuleNotFoundError: No module named 'motherduck'` | motherduck still imported after uninstall | Check imports; remove `import motherduck` lines |
| `FileNotFoundError: /Users/lhglosser/...parquet` | Path incorrect or Parquet file missing | Verify path is absolute; check 01_SILVER_DEID_PARQUET/ exists |
| `DuckDB Error: IO Error: Could not open file` | Parquet partition path wrong | Use full path with `year=XXXX/` subdirs; test read with Pandas first |
| Power BI Import: "Parquet not found" | Power Query path syntax issue | Escape backslashes: `file:///C:/path/to/file.parquet` (Windows) or `file:///Users/.../file.parquet` (Mac) |
| Script timeout on first execution | DuckDB loading Parquet partitions | Expected on first load; subsequent queries cached; use smaller partition for testing |
| Teams alert not sending | Power Automate Desktop robot failed silently | Check Power Automate Desktop execution logs; add try/catch in RPA steps |

---

## Sustainability & Future-Proofing

**Long-term Strategy:**
1. **Phase 4 (Now):** Local DuckDB + Parquet (zero cloud dependency)
2. **Phase 5 (Q2 2026):** Optional Power BI Web cloud publishing (read-only, metadata only)
3. **Phase 6 (Q3 2026):** Consider Emory data warehouse (if available) for multi-user collaboration

**Technology Stack Lock-In:**
- **Parquet:** Open format; can be read by any tool (Spark, DuckDB, Pandas, Power BI, Tableau, etc.)
- **DuckDB:** Open-source; no vendor lock-in; can switch to Postgres/SQLite if needed
- **Power BI:** Microsoft proprietary, but 6-month data export policy (can move to Tableau if needed)
- **Git + DVC:** Version control data and code; migration history preserved

**Conclusion:**
By eliminating MotherDuck and using local Microsoft 365 + DuckDB + Parquet, THYROID_2026 achieves:
- **Zero cloud dependency** for research data
- **HIPAA compliance** (air-gapped architecture)
- **Cost savings** (~$100-500/year)
- **Technology independence** (open formats; no vendor lock-in)
- **Enterprise scalability** (Power BI Web ready for Phase 5)

---

**Migration Map Version:** 1.0
**Last Updated:** 2026-03-27
**Prepared By:** Implementation Team
**Review Cadence:** Phase-end (4A, 4B, 4C, 4D, 4E, 4F)
