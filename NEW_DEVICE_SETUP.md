# NEW DEVICE SETUP — THYROID_2026

> **Purpose.** Bring up a fresh machine so you can continue manuscript work against the canonical BigQuery layer, with Airtable + Linear integration intact. End state: every smoke test in §8 passes.
>
> **Last verified:** 2026-05-13. BigQuery project `thyroid-canonical-pub-2026` confirmed (9 schemas: `pub_archive`, `pub_canonical`, `pub_legacy_source_20260416`, `pub_raw`, `pub_semantic`, `pub_signoff`, `pub_staging`, `pub_views_readable`, `pub_workspace`). Airtable bases and Linear team verified.
>
> **Scope.** This doc covers environment + auth only. The *workflow* (Session Opening Protocol, lifecycle gates, feedback logs, daily sync) lives in `.cowork/skills/thyroid-integration/SKILL.md` — read that second, after the environment is up.

---

## 1. What you're setting up

| Layer | What it is | Auth |
|---|---|---|
| **BigQuery** (canonical) | `thyroid-canonical-pub-2026` project. Datasets: `pub_canonical.*`, `pub_workspace.*`, `pub_signoff.*`. This is the only canonical data source. | gcloud ADC |
| **Airtable** | Workspace `wspDGHtW2HNuT20GQ` ("My First Workspace"). Bases: `appTGeB1jIizZbjnw` (THYROID_DATA_REGISTRY), `appJYOnUb7KrHKwpV` (THYROID_MANUSCRIPT), `app0iWn2bdpJnOAke` (Thyroid Research Manuscripts). | Personal Access Token (PAT) |
| **Linear** | Team `THY` ("Thyroid Database"), team UUID `c4afb51b-8bca-413a-a53e-15eb825cffbd`. Daily sync anchor = `THY-6`. | API key |
| **MotherDuck** *(legacy reference)* | Cloud trial expired; databases retained as historical snapshots only. **Not** the SSOT. Optional install if you want to read `archive_pub_v1_0` or legacy objects. | Token (optional) |
| **GitHub** | `ry86pkqf74-rgb/THYROID_2026` (this repo). Reference for scripts, prompts, parquets. | gh CLI / SSH |
| **Claude / Cowork** | MCP connectors to BQ, Airtable, Linear (and optionally MotherDuck, Google Drive). | OAuth via Claude Settings |

---

## 2. Prerequisites

Install these once on the new machine:

| Tool | Version | Why |
|---|---|---|
| Python | 3.11 or 3.12 | matches `runtime.txt` and CI |
| git | any recent | clone + sync |
| `gh` (GitHub CLI) | any recent | HTTPS auth via osxkeychain on macOS |
| `gcloud` CLI | any recent | BigQuery ADC + project switching |
| Cursor *(optional)* | latest | per-prompt agent execution (THYROID_2026 verification protocol) |
| Claude Desktop or Claude.ai | latest | MCP connector host |

macOS one-liner:
```bash
brew install python@3.12 git gh google-cloud-sdk
```

---

## 3. Clone the repo

```bash
gh auth login                          # interactive, choose HTTPS + browser
cd ~/Documents                          # or wherever you keep projects
gh repo clone ry86pkqf74-rgb/THYROID_2026
cd THYROID_2026
```

Verify clean state:
```bash
git status            # expect: clean, on main
git log --oneline -3  # confirm recent commits look familiar
```

---

## 4. Python environment

The repo pins are in `requirements.txt`. Use a venv per the existing convention (no Poetry / Pipenv yet).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt   # ruff + mypy for lint/type
```

Sanity:
```bash
python -c "import google.cloud.bigquery, pandas, duckdb, statsmodels, lifelines; print('ok')"
```

---

## 5. BigQuery authentication

The canonical layer is BigQuery; everything else flows from this. Auth is **Application Default Credentials (ADC)** — no service-account key files on personal devices.

```bash
# 1. Authenticate the gcloud CLI itself (opens browser)
gcloud auth login

# 2. Set the default project
gcloud config set project thyroid-canonical-pub-2026

# 3. Create ADC so Python's `google.cloud.bigquery` client picks them up
gcloud auth application-default login
```

Verify:
```bash
gcloud config get-value project
# → thyroid-canonical-pub-2026

bq query --use_legacy_sql=false \
  'SELECT COUNT(*) AS n FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.TABLES`'
# → returns a row with the table count (currently dozens)
```

Python smoke test:
```bash
python -c "
from google.cloud import bigquery
c = bigquery.Client(project='thyroid-canonical-pub-2026')
r = list(c.query('SELECT schema_name FROM \`thyroid-canonical-pub-2026.INFORMATION_SCHEMA.SCHEMATA\` ORDER BY 1'))
print([row.schema_name for row in r])
"
# → ['pub_archive', 'pub_canonical', 'pub_legacy_source_20260416', 'pub_raw',
#    'pub_semantic', 'pub_signoff', 'pub_staging', 'pub_views_readable', 'pub_workspace']
```

If you see `DefaultCredentialsError`, re-run `gcloud auth application-default login`.

---

## 6. Airtable Personal Access Token (PAT)

1. Sign in to Airtable as the account that owns workspace `wspDGHtW2HNuT20GQ`.
2. Go to https://airtable.com/create/tokens → **Create new token**.
3. Name it something like `THYROID_2026 — <hostname>`.
4. **Scopes** (minimum set the integration needs):
   - `data.records:read`
   - `data.records:write`
   - `data.recordComments:read`
   - `data.recordComments:write`
   - `schema.bases:read`
5. **Access**: explicitly add all three bases — `THYROID_DATA_REGISTRY`, `THYROID_MANUSCRIPT`, `Thyroid Research Manuscripts`. (Workspace-level access is fine if simpler.)
6. Copy the token (`patXXXXXXXXXXXXXX...`) — Airtable shows it once.
7. Add to your shell or `.env`:

```bash
# in ~/.zshrc or repo .env (gitignored)
export AIRTABLE_TOKEN="pat_REPLACE_ME"
```

Verify with curl:
```bash
curl -s -H "Authorization: Bearer $AIRTABLE_TOKEN" \
  "https://api.airtable.com/v0/meta/bases" | jq '.bases[] | {id, name}'
# → should list the three bases above
```

> **Hard rule reminder (skill §Hard rules #1):** no PHI in Airtable. `research_id` only.

---

## 7. Linear API key

1. Sign in to Linear → **Settings → API → Personal API keys → New API key**.
2. Name: `THYROID_2026 — <hostname>`.
3. Scope: full read/write is fine for solo use; the integration writes issues, labels, and comments under team `THY`.
4. Copy the key (`lin_api_...`).
5. Add to env:

```bash
export LINEAR_API_KEY="lin_api_REPLACE_ME"
```

Verify:
```bash
curl -s -X POST https://api.linear.app/graphql \
  -H "Authorization: $LINEAR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ teams(filter:{key:{eq:\"THY\"}}){nodes{id name key}} }"}' | jq
# → should return one team with key="THY", name="Thyroid Database"
```

---

## 8. MotherDuck *(legacy — optional)*

The cloud MotherDuck trial expired; databases there are historical-reference only. Install only if you need to query `archive_pub_v1_0` or other legacy snapshots, or run any of the older `scripts/1xx_md_*.py` tooling.

```bash
# tokens live in a gitignored file at repo root
cp .env.motherduck.example .env.motherduck
# edit .env.motherduck and uncomment + fill MOTHERDUCK_TOKEN=...
# OR put them in motherduck.local.toml (see motherduck.local.toml.example)
```

Resolution order is documented in `motherduck_client.py` and `docs/motherduck_database_contract_v1.md` §8. Shell-exported env wins over file values.

> **Reminder:** writes belong in BigQuery now. MotherDuck is read-only reference. Treat any MotherDuck write outside `archive_*` as a mistake.

---

## 9. Claude / Cowork MCP connectors

The integration runs through Claude with MCP connectors. On the new device, sign into Claude.ai (or Claude Desktop / Cowork) with the same account, then in **Settings → Connectors** confirm these are connected:

| Connector | Required? | Why |
|---|---|---|
| **Google Cloud BigQuery** | yes | canonical data reads, smoke tests |
| **Airtable** | yes | manuscripts table, feedback logs, override decisions, lifecycle reads/writes |
| **Linear** | yes | daily sync, issue creation/transition, Issue Ledger |
| **Google Drive** | recommended | manuscript packages, Excel master data, figures |
| **MotherDuck** | optional | only if you need legacy reads |
| **Microsoft 365** | optional | for Fabric/OneLake work; not core to manuscripts |

The first time you use each connector in a fresh session, Claude prompts an OAuth flow — accept once per connector.

If you keep a Cowork **skill** for the workflow (recommended), the existing skill lives at `.cowork/skills/thyroid-integration/` in this repo. Open the project in Cowork from this clone and the skill auto-loads.

---

## 10. Verification — full smoke test

Run these in order. If any fail, fix that layer before moving on.

```bash
# A. Repo + Python
git rev-parse --short HEAD && python --version

# B. BigQuery
python - <<'PY'
from google.cloud import bigquery
c = bigquery.Client(project="thyroid-canonical-pub-2026")
n = list(c.query("""
  SELECT COUNT(*) AS n
  FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.TABLES`
"""))[0].n
print(f"pub_canonical tables: {n}")
PY

# C. Airtable
curl -s -H "Authorization: Bearer $AIRTABLE_TOKEN" \
  "https://api.airtable.com/v0/meta/bases/appJYOnUb7KrHKwpV/tables" \
  | jq '.tables | length'
# → expect 7+ tables (Manuscripts, Sections, Tables & Figures, References,
#                    Co-Authors, Submission Targets, Manuscript Feedback Log,
#                    Data Feedback Log, Notable Findings)

# D. Linear
curl -s -X POST https://api.linear.app/graphql \
  -H "Authorization: $LINEAR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ issue(id:\"THY-6\"){identifier title state{name}} }"}' | jq
# → should return the daily-sync anchor issue THY-6

# E. (Optional) MotherDuck
# python -c "import duckdb; con=duckdb.connect('md:'); print([r[0] for r in con.execute('SHOW DATABASES').fetchall()])"
```

In Claude (one-shot end-to-end test):

> "Open THYROID_2026. Run the Session Opening Protocol, then report which manuscripts in Airtable have lifecycle=Active and no open Linear blocker."

A clean response means: connectors alive, skill loaded, BQ/Airtable/Linear all reachable, you're ready to work.

---

## 11. Where to start working

Once §10 is clean, read these in order:

1. **`CLAUDE.md`** *(this repo)* — top-level hard rules, ID map, session opening protocol summary.
2. **`.cowork/skills/thyroid-integration/SKILL.md`** *(v2.1.1)* — authoritative workflow. Read fully on first device setup; skim on returns.
3. **`MANUSCRIPT_DATA_START_HERE.md`** — analyst-facing views and row-count citation rule. Note that the live SSOT is BigQuery; older sections of this doc reference the MotherDuck `Thyroid 2026` database from before the BQ migration. Treat BQ as canonical.
4. **`MANUSCRIPT_TRACKER.md`** — legacy text inventory of active manuscripts.
5. **`docs/REPO_ARCHITECTURE_V2.md`** — directory map.
6. **`AGENTS.md`** — handoff context for agent-driven sessions.

For per-manuscript work, the canonical inventory is the Airtable **Manuscripts** table in `THYROID_MANUSCRIPT` (`appJYOnUb7KrHKwpV`), not any file in this repo.

---

## 12. Known issues & gotchas

- **MotherDuck vs. BigQuery in older docs.** Many docs in `docs/` and several top-level `HANDOFF_*` files predate the BQ migration and still talk about `thyroid_canonical_publication_v1_0` and the `Thyroid 2026` MotherDuck DB as canonical. **They are wrong as of 2026-05.** Skill v2.1.1 and `bq_migrations/` reflect current truth.
- **Desktop Commander timeouts on big commits.** When committing batches with binary files (PNGs, large CSVs, parquet), DC's MCP can hang. Workaround: stage with DC, then run `git commit` and `git push` manually in terminal. Small text-only commits are fine.
- **Cursor self-reports are not verification.** Per the project's verification protocol: after any Cursor agent prompt, always re-verify by (a) querying BigQuery directly for the invariants the prompt claimed to enforce, and (b) `git pull` + reading the actual committed files. Never accept a Cursor "phase complete" without primary-source confirmation.
- **PHI hard rule.** Airtable and Linear see `research_id` and Claude-summarized 1–2 sentence evidence only. No pathology text, no operative-note excerpts, no MRN, no DOB-month. If a script seems to be writing raw text to Airtable, stop and audit.
- **`Manuscript-Locked` records.** Cannot be edited without an explicit unlock instruction that names the manuscript and reason. The skill enforces this; do not work around it.
- **gcloud quota project.** If you see `User does not have permission to project` errors despite being logged in, your ADC has the wrong quota project. Fix with `gcloud auth application-default set-quota-project thyroid-canonical-pub-2026`.

---

## 13. Quick-reference commands

```bash
# refresh the repo at the start of every session
cd ~/Documents/THYROID_2026 && git pull --rebase

# list pub_canonical tables
bq ls thyroid-canonical-pub-2026:pub_canonical | head -30

# run an analyst view
bq query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`'

# rotate gcloud ADC (e.g. on a new device or after long inactivity)
gcloud auth application-default login

# show the active token/account for each
echo "gcloud:" && gcloud config get-value account
echo "Airtable:" && [[ -n "$AIRTABLE_TOKEN" ]] && echo "set" || echo "MISSING"
echo "Linear:"   && [[ -n "$LINEAR_API_KEY" ]] && echo "set" || echo "MISSING"
```

---

## 14. When this doc goes stale

Update this file (and bump the date at the top) when any of the following change:

- BigQuery project ID, dataset names, or canonical schema list
- Airtable base IDs or workspace ID
- Linear team UUID or key
- Required MCP connector list
- A new auth scope is needed for any service
- Python pinned versions in `requirements.txt` change in a way that breaks the smoke test

The `thyroid-integration` skill version (`.cowork/skills/thyroid-integration/SKILL.md` frontmatter) is the higher-authority document for workflow changes; bump it per the rules in that file and append a CHANGELOG entry.
