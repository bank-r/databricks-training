# Databricks notebook source
from pyspark.sql import functions as F, Window

spark.sql("USE CATALOG medisure_dev")
spark.sql("USE SCHEMA bronze")

src = "/Volumes/medisure_dev/bronze/landing/diagnosis"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/medisure_dev/ops/checkpoints/schemas/diagnosis")
    .load(src)
    .withColumnRenamed("Code", "code")
    .withColumnRenamed("Description", "description")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
)
df = df.drop("_rescued_data")

q= (df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/medisure_dev/ops/checkpoints/streams/reference/diagnosis")
  .trigger(availableNow=True)
  .toTable("medisure_dev.bronze.diagnosis_ref"))

q.awaitTermination()
# COMMAND ----------
# Read the bronze diagnosis table (columns: code, description, _ingest_ts)
df = spark.table("medisure_dev.bronze.diagnosis_ref")

# Trim and normalise fields
clean_df = (
    df
      .withColumn("code",        F.upper(F.trim("code")))
      .withColumn("description", F.trim("description"))
      .withColumn("_ingest_ts", F.col("_ingest_ts"))
)

# Validation rules:
#  - code must follow ICD-10 pattern (e.g. A00, A15.0, etc.)
#  - description must not be null or empty
clean_df = (
    clean_df
      .withColumn("code_valid",
                  F.col("code").rlike("^[A-TV-Z][0-9][A-Z0-9](?:\\.[A-Z0-9]{1,4})?$"))
      .withColumn("description_valid",
                  F.col("description").isNotNull() & (F.length(F.col("description")) > 0))
)

# Collect errors; assign an empty array if no failures:contentReference[oaicite:2]{index=2}
clean_df = clean_df.withColumn(
    "errors",
    F.when(
        F.col("code_valid") & F.col("description_valid"),
        F.array().cast("array<string>")
    ).otherwise(
        F.array_remove(F.array(
            F.when(~F.col("code_valid"),        F.lit("INVALID_CODE_PATTERN")),
            F.when(~F.col("description_valid"), F.lit("MISSING_DESCRIPTION"))
        ), None)
    )
)

# Split into valid and invalid rows
valid_cols_to_drop = [c for c in clean_df.columns if c.endswith("_valid")]
valid_df   = clean_df.filter(F.size("errors") == 0).drop("errors", *valid_cols_to_drop)
invalid_df = clean_df.filter(F.size("errors") > 0).drop(*valid_cols_to_drop)

# Write invalid rows to quarantine with an audit timestamp
(invalid_df
    .withColumn("audit_ts", F.current_timestamp())
    .write.mode("append").saveAsTable("medisure_dev.silver.diagnosis_ref_invalid")
)

# Deduplicate valid rows by code, keeping the most recent ingestion_ts if present
if "_ingest_ts" in valid_df.columns:
    window = Window.partitionBy("code").orderBy(F.col("_ingest_ts").desc_nulls_last())
    deduped = (valid_df.withColumn("rn", F.row_number().over(window))
                        .filter(F.col("rn") == 1)
                        .drop("rn"))
else:
    deduped = valid_df.dropDuplicates(["code"])

# Write the clean data to the silver table
deduped.write.format("delta").mode("overwrite").saveAsTable("medisure_dev.silver.diagnosis_ref")