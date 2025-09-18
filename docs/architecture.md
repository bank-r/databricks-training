# Architecture Overview

- **Azure Databricks** workspace (public networking) for dev/POC
- **ADLS Gen2** storage with containers: landing, checkpoint, bronze, silver, gold, uc-metastore
- **Delta Lake** for ACID tables, time travel, versioning
- **DLT** for streaming + batch incremental pipelines
- **Azure SQL Database (Serverless)** for serving/reporting
- **Unity Catalog (optional)** for governance, external locations & RBAC

## Best Practices

- Auto Loader for efficient discovery & schema evolution.
- Bronze/Silver/Gold separation; minimal logic in Bronze.
- OPTIMIZE/VACUUM schedules; Z-ORDER on query keys.
- PK/FK checks & DQ flags in Silver; error quarantine.
- Parametrize via tfvars & config YAML.
- For prod: private networking, service principals, UC hardening.
