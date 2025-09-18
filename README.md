# Azure Databricks Training - Medicare Capstone Project

- This databricks training project is showcased using an [Azure Free account](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) as of 8th September, 2025.

![azure-free-account](docs/images/azure-free-account.png)

## Problem Statement - Healthcare Claims Quality & Compliance Platform (Databricks Lakehouse)

- MediSure, a national healthcare insurer, ingests claims and reference data from heterogeneous sources (batch CSV files, streaming EHR events, JDBC legacy systems, and nested JSON provider directories).
- The current estate suffers from long processing delays (often >24 hours), pervasive data-quality defects (duplicates, invalid codes, mismatched member/provider IDs), fragmented analytics, and limited auditability/governance—creating operational inefficiency, fraud exposure, and compliance risk.
- The organization needs a governed, near-real-time claims platform on the Databricks Lakehouse to standardize ingestion, enforce quality rules, enable ACID time-travel/rollback, and deliver reliable analytics for fraud scoring and regulatory reporting.

The full problem statement is described at [Capstone Project Statement - Healthcare Claims Quality & Compliance Platform.pdf](<problem/Capstone Project Statement - Healthcare Claims Quality & Compliance Platform.pdf>).

## Repository Structure

```text
databricks-training
├── data
│   ├── claims_batch.csv
│   ├── claims_stream.json
│   ├── diagnosis_ref.csv
│   ├── members.csv
│   └── providers.json
├── docs
│   ├── images
│   └── architecture.md
├── functions
│   └── utils.py
├── notebooks
│   ├── 00_init
│   ├── 01_bronze
│   ├── 02_silver
│   └── 03_gold
├── problem
│   └── Capstone Project Statement - Healthcare Claims Quality & Compliance Platform.pdf
├── resources
│   ├── 00_job_medisure_init.yml
│   └── 01_job_medisure_orchestrate.yml
├── setup
│   ├── 01_create_resource_groups.ps1
│   ├── 02_create_storage.ps1
│   ├── 03_create_databricks.ps1
│   ├── 04_create_azure_sql.ps1
│   ├── 05_create_databricks_connector.ps1
│   ├── 06_create_storage_container.ps1
│   ├── 07_create_key_vault.ps1
│   ├── 08_create_databricks_objects.ipynb
│   ├── 09_load_members_to_azure_sql.ipynb
│   └── azure-cli-usage.md
├── .gitignore
├── config.yml
├── databricks.yml
├── poetry.lock
├── pyproject.toml
└── README.md
```

### Repo Description

- **data**/: sample inputs for batch/stream (claims, members, providers, diagnosis).

- **docs**/: project docs and diagrams.

- **functions**/: reusable Python helpers imported by notebooks.

- **notebooks**/: lakehouse pipelines by layer (00_init, 01_bronze, 02_silver, 03_gold).

- **problem**/: capstone problem statement.

- **resources**/: Databricks Jobs YAML (init + orchestration).

- **setup**/: Infrastructure as Code scripts (PowerShell + notebooks) to provision Azure (RG, ADLS, Databricks, SQL, Key Vault) and load seed data.

- **Root files**: project config (config.yml, databricks.yml), packaging (pyproject.toml, poetry.lock), and repo docs (README.md).

## Getting Started

I used VSCode as my IDE to develop and run the setup scripts.

### 1. Clone the repo

```bash
git clone https://github.com/bank-r/databricks-training.git
cd databricks-training
```

### 2. Setup environment using Poetry

[Poetry](https://python-poetry.org/docs/) is a tool for dependency management and packaging in Python. It allows you to declare the libraries your project depends on and it will manage (install/update) them for you. Poetry offers a lockfile to ensure repeatable installs, and can build your project for distribution.

```bash
poetry install
```

This will create a new environment based on all dependencies declared with poetry.

### 3. Using the environment with your notebooks for setup

For more info refer to [VSCode Docs](https://code.visualstudio.com/docs/datascience/jupyter-kernel-management#_python-environments)

### 4. Install az cli for infrastructure powershell scripts

Download the [Azure CLI for Windows](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows?view=azure-cli-latest&pivots=zip)

### 5. Install ODBC Driver for SQL Server

Download the [ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17). The one natively installed with windows is obsolete.

## Set up Azure resources

## Set up Databricks resources

## Macro Architecture

![macro architecture](docs/images/macro-architecture.excalidraw.png)

## High level ETL Workflow

![high level etl workflow](docs/images/high-level-etl-workflow.excalidraw.png)
