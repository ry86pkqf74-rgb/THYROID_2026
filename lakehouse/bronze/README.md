# Bronze layer

Immutable or minimally transformed institutional source data.

- **Repository location:** [`raw/`](../raw/) (gitignored; PHI). Do not commit patient identifiers or full clinical note text exports here into git.
- **Typical contents:** Excel workbooks, vendor exports, and other line-of-system drops prior to snake_case harmonization and research_id spine alignment.
- **Policy:** Bronze stays outside git; this folder documents the contract and optional local-only staging. ETL entry: [`scripts/01_ingest_all_files.py`](../scripts/01_ingest_all_files.py) and related ingest scripts.
