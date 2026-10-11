# Banking Data Engineering Pipeline

An end-to-end banking data engineering pipeline built on Databricks using a medallion architecture (Bronze → Silver → ODS → DWH → Reporting). The pipeline ingests source data, transforms it through progressively cleaner layers, builds dimensional model tables, and produces 360-degree customer/account analytics — all orchestrated by a cascading Lakeflow Jobs hierarchy defined as a Declarative Automation Bundle (DAB).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Catalog & Schema Layout](#catalog--schema-layout)
- [Bundle Structure](#bundle-structure)
- [Setup Phase (One-Time)](#setup-phase-one-time)
- [Ingestion Pipeline](#ingestion-pipeline)
- [Job Hierarchy](#job-hierarchy)
- [Batch Control & Watermarking](#batch-control--watermarking)
- [DAB Configuration](#dab-configuration)
- [Deploying the Bundle](#deploying-the-bundle)
- [Running the Pipeline](#running-the-pipeline)
- [Configuration Parameters](#configuration-parameters)

---

## Architecture Overview

The pipeline follows a **medallion architecture** with the following data flow:

```
data_source  →  bronze  →  silver  →  ods  →  gold (DWH)  →  platinum (Reporting)
                                                                       ↓
                                                                360° Analysis
```

| Layer      | Schema        | Purpose                                                                   |
| ---------- | ------------- | ------------------------------------------------------------------------- |
| Source     | `data_source`  | Raw source tables (simulated operational data: branch, account, etc.)     |
| Bronze     | `bronze`       | Raw ingested data from source, with audit columns appended                 |
| Silver     | `silver`       | Cleaned, deduplicated, type-cast data with SCD-2 support                   |
| ODS        | `ods`          | Operational data store — business-ready conformed tables                 |
| DWH (Gold) | `gold`         | Dimensional model: fact tables, dimension tables, and bridge tables       |
| Reporting  | `platinum`     | Governed reporting datasets with point-in-time metrics                    |
| Analysis   | —              | Account 360 and Customer 360 views from DWH facts and dimensions          |
| Metadata   | `metadata`     | Control tables: table registry, batch tracking, watermarks, parameters    |

### Source Tables

The pipeline processes 8 source tables in `data_source`:

1. `branch` — bank branch master data
2. `customer_history` — customer records with SCD-2 history
3. `account` — customer accounts
4. `account_customer_history` — account-customer relationships (many-to-many)
5. `product` — product catalog
6. `transaction` — transaction headers
7. `transaction_line` — transaction line items
8. `daily_account_balance` — daily balance snapshots

---

## Catalog & Schema Layout

All tables live under a single catalog named `{environment}_banking` (e.g., `dev_banking` for dev, `prod_banking` for prod). Switching environments changes the catalog prefix — no code changes required.

```
dev_banking
├── data_source/     ← Source tables (operational data)
├── bronze/          ← Raw ingested data
├── silver/          ← Cleaned & conformed data
├── ods/             ← Operational data store
├── gold/            ← DWH: facts, dimensions, bridges
├── platinum/        ← Reporting datasets
└── metadata/        ← Control & governance tables
    ├── tables                  ← Table registry (which tables to ingest, load order)
    ├── table_parameters        ← Per-table config (load_type, primary_key, watermark_column)
    ├── batch_control           ← Ingestion run tracking (run_id, status, record_count)
    └── table_watermarks        ← Last-processed timestamp per table
```

---

## Bundle Structure

```
dev-banking-data-engineering/
├── databricks.yml                          # Main DAB config: variables, sync, targets
├── README.md                                # This file
├── resources/                               # DAB resource definitions (excluded from sync)
│   ├── banking_master_job.yml               # Parent orchestrator job
│   ├── banking_source_to_target.yml         # Child: generic layer ingestion
│   └── banking_source_to_target_inner.yml   # Grandchild: per-table conditional ingestion
└── src/                                     # All notebook source code (synced to workspace)
    ├── 01_setup_tables/
    │   ├── 01_setup_control_tables           # Create metadata control tables
    │   ├── 02_setup_source_tables            # Create data_source tables
    │   ├── 03_insert_source_tables           # Register tables in metadata.tables
    │   ├── 04_setup_bronze_layer             # Create bronze schema tables
    │   ├── 05_setup_silver_layer             # Create silver schema tables
    │   ├── 06_setup_ods_report_layer          # Create ODS & reporting schema tables
    │   └── 07_setup_dwh_report_layer         # Create DWH & reporting tables
    ├── 02_source_to_bronze/
    │   ├── 01_read_tables_list               # Read active tables from metadata (driver)
    │   ├── 02_table_configuration            # Get per-table load config
    │   ├── 03_source_to_bronze               # Bronze ingestion logic (APPEND/MERGE)
    │   └── 04_validate_metadata_info         # Validate metadata tables
    ├── 03_ods_layer/
    │   ├── 01_bronze_to_silver               # Silver transformation logic
    │   ├── 02_ods_layer                      # ODS transformation logic
    │   └── 03_ods_reports                    # ODS-level reporting
    ├── 04_dwh_layer/
    │   ├── 01_dwh_tables                    # Build DWH facts/dimensions from ODS
    │   ├── 03_reporting_tables              # Build platinum reporting datasets
    │   └── 04_analysis_360_degree           # Account 360 & Customer 360 analytics
    └── utils/
        ├── __init__.py                      # Package marker
        └── helper.py                        # BatchControlHelper class
```

---

## Setup Phase (One-Time)

Before running the pipeline for the first time, execute the setup notebooks in order. These create the catalog schemas, source tables, metadata control tables, and target-layer tables.

| Order | Notebook                                  | What It Does                                                                   |
| ----- | ----------------------------------------- | ------------------------------------------------------------------------------ |
| 1     | `01_setup_tables/01_setup_control_tables` | Creates `metadata.tables`, `metadata.table_parameters`, `metadata.batch_control`, `metadata.table_watermarks`, `metadata.pipeline_runs` |
| 2     | `01_setup_tables/02_setup_source_tables`  | Creates 8 Delta tables under `data_source` schema (branch, account, etc.)      |
| 3     | `01_setup_tables/03_insert_source_tables` | Inserts table metadata into `metadata.tables` (table name, source, target layer, load order) |
| 4     | `01_setup_tables/04_setup_bronze_layer`   | Creates empty bronze schema target tables                                      |
| 5     | `01_setup_tables/05_setup_silver_layer`   | Creates empty silver schema target tables                                      |
| 6     | `01_setup_tables/06_setup_ods_report_layer`| Creates empty ODS and reporting schema tables                                   |
| 7     | `01_setup_tables/07_setup_dwh_report_layer`| Creates empty DWH (gold) and reporting (platinum) tables                        |

Each setup notebook accepts an `environment` widget parameter (default: `dev`).

---

## Ingestion Pipeline

The main pipeline is orchestrated by `banking_master_job`, which cascades through layers sequentially:

### Layer 1–3: Source → Bronze → Silver → ODS

Each layer is processed by the same generic job (`banking_source_to_target`) with a different `target_layer` parameter. The flow within each layer is:

1. **Read table list** — `01_read_tables_list` queries `metadata.tables` for all active tables matching the target layer, ordered by `load_order`. The result is emitted as a task value (`tables_metadata`).
2. **Fan out (for_each)** — `banking_source_to_target` iterates over each table's metadata with concurrency 4, calling `banking_source_to_target_inner` once per table.
3. **Get table config** — `02_table_configuration` reads `metadata.table_parameters` to get `load_type` (APPEND or MERGE), `primary_key`, and `watermark_column` for the specific table.
4. **Conditional ingestion** — Based on `target_layer`, the inner job routes to the appropriate notebook:
   - `bronze` → `03_source_to_bronze` (appends raw source data with audit columns)
   - `silver` → `01_bronze_to_silver` (cleans, deduplicates, applies SCD-2 MERGE)
   - `ods` → `02_ods_layer` (transforms to ODS conformed tables)

### Layer 4: DWH (Gold)

`01_dwh_tables` builds dimensional model tables from ODS:
- Fact tables: `fact_transactions`, `fact_account_balance`
- Dimension tables: `dim_branch`, `dim_customer`, `dim_product`, `dim_date`
- Bridge tables: `bridge_account_customer`, `bridge_transaction_product`

Uses incremental MERGE based on `processing_date` to avoid full reloads.

### Layer 5: Reporting & Analysis

Runs in parallel after DWH:
- `03_reporting_tables` — Creates governed reporting datasets in `platinum` schema with point-in-time metrics (as-of-date, period filters)
- `04_analysis_360_degree` — Builds Account 360 and Customer 360 views combining current ownership, as-of customer/KYC context, latest balance, transaction activity, product usage, and risk flags
- `03_ods_reports` — ODS-level reporting (runs in parallel after ODS, before DWH completes)

---

## Job Hierarchy

The pipeline uses a 3-level cascading job structure. All cross-job references use `${resources.jobs.<key>.id}` so DAB resolves them at deploy time.

```
banking_master_job (resources/banking_master_job.yml)
│
├── bronze_layer    → run_job(banking_source_to_target, target_layer=bronze)
│   └── banking_source_to_target (resources/banking_source_to_target.yml)
│       ├── get_source_table_metadata  → notebook(01_read_tables_list)
│       └── ingest_table_source_to_silver  → for_each(concurrency=4)
│           └── ingest_table_source_to_target_iteration
│               → run_job(banking_source_to_target_inner, table_metadata={{input}})
│               └── banking_source_to_target_inner (resources/banking_source_to_target_inner.yml)
│                   ├── get_table_configurations  → notebook(02_table_configuration)
│                   ├── conditional_bronze_layer  → if target_layer == bronze
│                   │   └── source_to_bronze_ingestion → notebook(03_source_to_bronze)
│                   ├── conditional_silver_layer  → if target_layer == silver
│                   │   └── bronze_to_silver_ingestion → notebook(01_bronze_to_silver)
│                   └── conditional_ods_layer     → if target_layer == ods
│
├── silver_layer    → run_job(banking_source_to_target, target_layer=silver)  [after bronze]
├── ods_layer       → run_job(banking_source_to_target, target_layer=ods)     [after silver]
│
├── dwh_layer          → notebook(01_dwh_tables)              [after ods]
├── analysis_360_layer → notebook(04_analysis_360_degree)    [after dwh]
├── reporting_layer     → notebook(03_reporting_tables)      [after dwh]
└── ods_report_layer   → notebook(03_ods_reports)            [after ods]
```

### Execution Order

```
bronze_layer → silver_layer → ods_layer ┬→ dwh_layer ┬→ analysis_360_layer
                                          │            └→ reporting_layer
                                          └→ ods_report_layer
```

---

## Batch Control & Watermarking

All ingestion notebooks use `BatchControlHelper` from `src/utils/helper.py` to ensure idempotent, resumable loads.

### Batch Control (`metadata.batch_control`)

Every table ingestion creates a batch record with a unique `ingestion_run_id`:

| Column           | Description                                          |
| ---------------- | ---------------------------------------------------- |
| `ingestion_run_id` | UUID per ingestion run                              |
| `batch_id`       | e.g., `bronze-20241231`, `silver-20241231`, `ods-20241231` |
| `source_table`   | Source table name                                    |
| `target_table`   | Target table name                                    |
| `target_layer`   | bronze, silver, or ods                               |
| `load_type`      | APPEND or MERGE                                       |
| `status`         | RUNNING → SUCCESS or FAILED                          |
| `start_time`     | Run start timestamp                                   |
| `end_time`       | Run end timestamp (null while running)               |
| `record_count`   | Records processed                                    |
| `error_message`  | Error details (if failed)                            |

### Watermarking (`metadata.table_watermarks`)

For incremental loads, the pipeline tracks the last processed timestamp per table:

- `get_watermark(table_name, process_name)` — Returns the last source timestamp processed
- `update_watermark(process_name, table_name, source_max_timestamp, run_id)` — Updates the watermark after a successful load
- On subsequent runs, only rows with `watermark_column > previous_watermark` are loaded

### Idempotency

- `force_reload=false` (default): Skips tables already marked SUCCESS for the current `batch_id`
- `force_reload=true`: Reprocesses all tables regardless of prior status

---

## DAB Configuration

### Variables (`databricks.yml`)

The bundle defines three variables that flow into all job parameters via `${var.*}` references:

| Variable           | Default       | Description                                    |
| ------------------ | ------------- | ---------------------------------------------- |
| `environment`      | `dev`         | Catalog prefix (`{env}_banking`)               |
| `force_reload`     | `false`       | Force reprocessing of all tables               |
| `processing_date`  | `2024-12-31`  | As-of date for initial/incremental loads       |

### Sync Configuration

The `sync` section controls which local files are deployed to the workspace:

```yaml
sync:
  include:
    - "src/"          # All notebooks and utils
  exclude:
    - "resources/"    # Config-only YAML files, not deployed
```

`src/utils/` is **included** in the sync because notebooks import `from src.utils.helper import BatchControlHelper` — excluding it would break imports at runtime.

### Targets

| Target | Mode         | Description                              |
| ------ | ------------ | ---------------------------------------- |
| `dev`  | development  | Default target. Jobs are prefixed with the developer's username. |
| `prod` | production   | Deploys to a shared root path. Runs as `pasrichashivam@gmail.com`. |

### Resource Files

Each job is defined in a separate YAML file under `resources/`. Notebook paths use `../src/` prefix (relative from `resources/` to the bundle root):

```yaml
notebook_task:
  notebook_path: ../src/04_dwh_layer/01_dwh_tables
```

---

## Deploying the Bundle

### Prerequisites

- Databricks CLI v0.218.0+ installed and configured
- Appropriate permissions on the target workspace
- Source data tables already created (run setup notebooks first)

### Validate

```bash
databricks bundle validate --strict --target dev
```

### Deploy

```bash
# Deploy to dev (default target)
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

### Override Variables at Deploy Time

```bash
databricks bundle deploy --target dev --var="processing_date=2025-01-15" --var="force_reload=true"
```

---

## Running the Pipeline

### Via DAB

```bash
databricks bundle run banking_master_job --target dev
```

### With Parameter Overrides

```bash
databricks bundle run banking_master_job --target dev --var="processing_date=2025-01-15"
```

### Via Databricks UI

After deployment, navigate to **Lakeflow Jobs** in the workspace and run `banking_master_job` with desired parameters.

---

## Configuration Parameters

### Job-Level Parameters

All three jobs share these parameters (defaults come from bundle variables):

| Parameter          | Default      | Description                                    |
| ------------------ | ------------ | ---------------------------------------------- |
| `environment`      | `${var.environment}`     | Catalog prefix                       |
| `force_reload`     | `${var.force_reload}`    | Reprocess even if SUCCESS             |
| `processing_date`  | `${var.processing_date}` | As-of date (YYYY-MM-DD)              |
| `target_layer`     | `bronze`     | Which layer to process (child & grandchild only) |
| `table_metadata`   | `""`         | Per-table metadata JSON (grandchild only, set by for_each) |

### How Parameters Cascade

1. **Variables** in `databricks.yml` → defaults for **job parameters**
2. **Parent job** passes `target_layer` to child via `run_job_task.job_parameters`
3. **Child job** passes `table_metadata` to grandchild via for_each `{{input}}`
4. **Notebooks** receive parameters as **widgets** (`dbutils.widgets.get`)

### Notebook Widgets

Each ingestion notebook defines these widgets:

- `environment` (text)
- `table_metadata` (text — JSON string)
- `table_config` (text — JSON string)
- `processing_date` (text)
- `force_reload` (dropdown: true/false)

DWH and reporting notebooks use:
- `environment` (text)
- `processing_date` (text)
- `as_of_date`, `from_date`, `to_date` (text — reporting notebooks)
