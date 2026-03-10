# MotherDuck Trial Utilization Log

## Dashboard v3 Upgrade — 2026-03-10

**Script:** `scripts/12_update_streamlit_dashboard.py`

### What was added
- 5 new Streamlit tabs: Timeline, Events, QA, Survival, Advanced Features v3
- Sidebar filters: surgery count, QA flag
- MotherDuck compute-tier controls (Business trial → Jumbo toggle)
- Kaplan-Meier survival plots via lifelines

### Tables/views used (all require script 10 + 11)
- `master_timeline` — multi-surgery patient timelines
- `extracted_clinical_events_v4` — NLP-extracted events with relative days
- `qa_issues` — cross-validation inconsistencies
- `advanced_features_v3` — 60+ engineered features
- `survival_cohort_ready_mv` — time-to-event data
- `recurrence_risk_features_mv` — mutation + Tg slope features

### Connection instructions
```bash
export MOTHERDUCK_TOKEN='your_token'
streamlit run dashboard.py
# Opens at http://localhost:8501
```

### Trial utilization
- Compute: MotherDuck Business trial (large instances + replicas)
- All new tabs use `@st.cache_data(ttl=300)` for query caching
- Jumbo compute toggle available in sidebar
