# Microsoft Fabric Lakehouse / OneLake — notes entity pipeline

This repo can land **note entity Parquet** files in a Fabric Lakehouse **Files** area on OneLake, then register **Delta** tables from a Fabric notebook (Spark + `mssparkutils`).

## Install (local uploader)

```bash
pip install azure-storage-file-datalake azure-identity pyarrow pandas adlfs openai
```

Pinned list: see `requirements-fabric-onelake.txt`.

## OneLake authentication

### Option A — Interactive developer (`DefaultAzureCredential`)

1. Ensure your user has **Storage Blob Data Contributor** (or Owner) on the workspace capacity / lakehouse storage.
2. Sign in with Azure CLI:

   ```bash
   az login --tenant <YOUR_TENANT_ID>
   ```

3. Set the lakehouse workspace id (filesystem name on OneLake):

   ```bash
   export FABRIC_LAKEHOUSE_WORKSPACE_ID='<lakehouse-workspace-id-guid>'
   ```

   In Fabric: open the Lakehouse → **Properties** / URL — the id is the Lakehouse artifact GUID (also used as the `abfss://` filesystem segment).

4. Run the uploader:

   ```bash
   python scripts/09b_fabric_upload_notes_entities.py --domain all
   ```

### Option B — Service principal (CI / headless)

1. Register an app in Microsoft Entra ID; create a **client secret**.
2. Grant the app **Storage Blob Data Contributor** on the storage backing the workspace (or the Lakehouse via workspace IAM, depending on tenant policy).
3. Export:

   ```bash
   export AZURE_TENANT_ID='<tenant>'
   export AZURE_CLIENT_ID='<app-id>'
   export AZURE_CLIENT_SECRET='<secret>'
   export FABRIC_LAKEHOUSE_WORKSPACE_ID='<lakehouse-guid>'
   ```

4. Run `scripts/09b_fabric_upload_notes_entities.py` as above.

### Option C — `adlfs` + fsspec (optional reads)

`adlfs` registers `abfss://` URLs for pandas/pyarrow in environments where fsspec is configured with `DefaultAzureCredential`. The upload script uses `azure-storage-file-datalake` directly for clarity and append/overwrite control.

## Paths

- Local outputs: `processed/note_entities_<domain>.parquet` (from `notes_extraction/run_extraction.py` or `notes_extraction_new/run_extraction_local.py`).
- Canonical fact long (clean + quarantine) and extraction run log (same repo, DVC-tracked when enabled):
  - `processed/canonical_extracted_fact_long_v1.parquet` — analysis-ready facts (`scripts/103_fact_lineage_materialize.py`)
  - `processed/canonical_fact_quarantine_v1.parquet` — conservative exclusions from clean canonical
  - `processed/note_extraction_runs.parquet` — one row per `run_extraction.py` invocation
- OneLake layout:

  `abfss://<FABRIC_LAKEHOUSE_WORKSPACE_ID>@onelake.dfs.core.windows.net/Files/note_entities_<domain>/part-000.parquet`

  Register optional folders `canonical_extracted_fact_long_v1`, `canonical_fact_quarantine_v1`, `note_extraction_runs` beside `note_entities_*` if you mirror the full release artefacts to Fabric.

## Delta tables (`mssparkutils` / Spark)

`mssparkutils` is only available **inside a Fabric notebook** attached to the Lakehouse. After upload, create Delta tables using the snippet printed by `09b_fabric_upload_notes_entities.py`, or run:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

lakehouse_id = "<FABRIC_LAKEHOUSE_WORKSPACE_ID>"
domain = "staging"
parquet_path = f"abfss://{lakehouse_id}@onelake.dfs.core.windows.net/Files/note_entities_{domain}/"
spark.read.format("parquet").load(parquet_path).write.format("delta").mode("overwrite").saveAsTable(
    f"note_entities_{domain}"
)
```

Adjust `domain` per folder. Register tables in the **Lakehouse** default semantic model as needed.

## Provenance

- **Preprocess remaining workbooks:** `source_workbook`, `preprocess_batch_id`, `preprocessed_at_utc`, `preprocess_script_version` (see `scripts/preprocess_remaining_excels.py`).
- **Fabric upload:** optional `fabric_upload_run_id`, `fabric_uploaded_at_utc`, `fabric_source_local_uri`, `fabric_upload_script_version` (default on; use `--skip-provenance-columns` for byte-identical uploads).

## vLLM local extraction

```bash
export VLLM_MODEL=<served-model-name>
export VLLM_OPENAI_BASE_URL=http://localhost:8000/v1   # default
python notes_extraction_new/run_extraction_local.py
```

## Troubleshooting

| Symptom | Check |
|--------|--------|
| `403` / authorization | RBAC: app or user needs blob write on OneLake; tenant allow policies |
| Wrong filesystem | `FABRIC_LAKEHOUSE_WORKSPACE_ID` must be the **Lakehouse** id, not workspace tenant id |
| Delta fails in notebook | Path must be folder URL; run notebook attached to same Lakehouse |
| Empty LLM output | `VLLM_MODEL` must match vLLM; confirm `http://localhost:8000/v1/models` |
