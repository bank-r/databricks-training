# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG medisure_dev")
spark.sql("USE SCHEMA bronze")

storage_account = "samedisuredev"

df_raw = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text")
  .option("cloudFiles.schemaLocation", "/Volumes/medisure_dev/ops/checkpoints/schemas/providers")
  .load("/Volumes/medisure_dev/bronze/landing/providers")
  .select(
    F.col("value").alias("raw_json"),
    F.col("_metadata.file_path").alias("_source_file"),
    F.current_timestamp().alias("_ingest_ts"),
  )
)
q = (df_raw.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/medisure_dev/ops/checkpoints/streams/providers")
  .trigger(availableNow=True)
  .toTable("medisure_dev.bronze.providers"))

q.awaitTermination()
# COMMAND ----------

# Read the raw bronze providers table
df = spark.table("medisure_dev.bronze.providers")

# Define a schema that matches the JSON keys (case‑sensitive)
json_schema = (
    "ProviderID STRING, Name STRING, "
    "Specialties ARRAY<STRING>, "
    "Locations ARRAY<STRUCT<Address:STRING, City:STRING, State:STRING>>, "
    "IsActive BOOLEAN, TIN STRING, LastVerified STRING"
)

# Parse the raw JSON and extract fields
parsed_df = (
    df.withColumn("parsed", F.from_json("raw_json", json_schema))
      .select(
          F.col("parsed.ProviderID").alias("provider_id"),
          F.col("parsed.Name").alias("name"),
          F.col("parsed.Specialties").alias("specialties"),
          F.col("parsed.Locations").alias("locations"),
          F.col("parsed.IsActive").alias("is_active"),
          F.col("parsed.TIN").alias("tin"),
          F.to_date(F.col("parsed.LastVerified")).alias("last_verified"),
          F.col("_ingest_ts").alias("_ingest_ts")
      )
)

# Flatten nested arrays: explode specialties and locations
flat_df = (
    parsed_df
      .withColumn("specialty", F.explode("specialties"))
      .withColumn("location",  F.explode("locations"))
      .select(
          "provider_id",
          "name",
          "specialty",
          F.col("location.Address").alias("address"),
          F.col("location.City").alias("city"),
          F.col("location.State").alias("state"),
          "is_active",
          "tin",
          "last_verified",
          "_ingest_ts"
      )
)

# Data quality checks: TIN length, non‑null IDs, etc.
clean_df = (
    flat_df
      .withColumn("provider_valid", F.col("provider_id").isNotNull())
      .withColumn("name_valid",     F.col("name").isNotNull())
      .withColumn("tin_valid",      F.col("tin").rlike("^[0-9]{9}$"))
      .withColumn("specialty_valid",F.col("specialty").isNotNull())
      .withColumn("location_valid", F.col("address").isNotNull() & F.col("city").isNotNull() & F.col("state").isNotNull())
      .withColumn(
          "errors",
          F.array_remove(F.array(
              F.when(~F.col("provider_valid"),  F.lit("MISSING_PROVIDERID")),
              F.when(~F.col("name_valid"),      F.lit("MISSING_NAME")),
              F.when(~F.col("tin_valid"),       F.lit("INVALID_TIN")),
              F.when(~F.col("specialty_valid"), F.lit("MISSING_SPECIALTY")),
              F.when(~F.col("location_valid"),  F.lit("INCOMPLETE_LOCATION"))
          ), None)
      )
)

# Separate valid and invalid records
valid_cols_to_drop = [c for c in clean_df.columns if c.endswith("_valid")]
valid_df = clean_df.filter(
    F.col("errors").isNull() | (F.size("errors") == 0)
).drop("errors", *[c for c in clean_df.columns if c.endswith("_valid")])

invalid_df = clean_df.filter(
    F.col("errors").isNotNull() & (F.size("errors") > 0)
)

# Write invalid rows to the quarantine table
(invalid_df
    .withColumn("audit_ts", F.current_timestamp())
    .write.mode("append").saveAsTable("medisure_dev.silver.providers_invalid")
)

# There are no duplicates on ProviderID in your sample, but for completeness you can deduplicate:
window = Window.partitionBy("provider_id").orderBy(F.col("last_verified").desc_nulls_last())
deduped = valid_df.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

# Write the clean, deduplicated records to the silver table
deduped.write.format("delta").mode("overwrite").saveAsTable("medisure_dev.silver.providers")
