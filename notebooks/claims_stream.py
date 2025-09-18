# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG medisure_dev")
spark.sql("USE SCHEMA bronze")

storage_account = "samedisuredev"

df_raw = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "text")
  .option("cloudFiles.schemaLocation", "/Volumes/medisure_dev/ops/checkpoints/schemas/claims/stream")
  .load("/Volumes/medisure_dev/bronze/landing/claims/stream")
  .select(
    F.col("value").alias("raw_json"),
    F.col("_metadata.file_path").alias("_source_file"),
    F.current_timestamp().alias("_ingest_ts"),
  )
)
q = (df_raw.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/medisure_dev/ops/checkpoints/streams/claims/stream")
  .trigger(availableNow=True)
  .toTable("medisure_dev.bronze.claims_stream"))

q.awaitTermination()
# COMMAND ----------

# read bronze table
df = spark.table("medisure_dev.bronze.claims_stream")

json_schema = (
    "ClaimID STRING, MemberID STRING, ProviderID STRING, "
    "ClaimDate STRING, Amount DOUBLE, Status STRING, "
    "ICD10Codes STRING, CPTCodes STRING, EventTimestamp STRING"
)

# Parse JSON and flatten fields
parsed_df = (
    df
      .withColumn("parsed", F.from_json(F.col("raw_json"), json_schema))
      .select(
          F.col("parsed.ClaimID").alias("claim_id"),
          F.col("parsed.MemberID").alias("member_id"),
          F.col("parsed.ProviderID").alias("provider_id"),
          F.to_date(F.col("parsed.ClaimDate")).alias("claim_date"),
          F.col("parsed.Amount").cast("decimal(18,2)").alias("amount"),
          F.col("parsed.Status").alias("status"),
          F.col("parsed.ICD10Codes").alias("icd10_codes"),
          F.col("parsed.CPTCodes").alias("cpt_codes"),
          F.to_timestamp(F.col("parsed.EventTimestamp")).alias("event_timestamp"),
          F.col("_ingest_ts").alias("_ingest_ts"),
          F.col("_source_file").alias("_source_file")
      )
)
clean_df = (parsed_df
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("status", F.upper("status"))
    .withColumn("icd10_codes", F.split("icd10_codes", ";"))
    .withColumn("cpt_codes", F.split("cpt_codes", ";"))
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("amount_valid", F.col("amount").isNotNull() & (F.col("amount") >= 0))
    .withColumn("status_valid", F.col("status").isin("SUBMITTED","PENDING","PAID","REJECTED","APPROVED"))
    .withColumn("member_valid", F.col("member_id").isNotNull())
    .withColumn("provider_valid", F.col("provider_id").isNotNull())
    .withColumn("icd10_codes_valid", F.col("icd10_codes").isNotNull())
    .withColumn("cpt_codes_valid", F.col("cpt_codes").isNotNull())
)

# Collect names of failed checks into an array
clean_df = clean_df.withColumn(
    "failures",
    F.array(
        *[
            F.when(~F.col("amount_valid"), F.lit("INVALID_AMOUNT")).otherwise(F.lit(None)),
            F.when(~F.col("status_valid"), F.lit("INVALID_STATUS")).otherwise(F.lit(None)),
            F.when(~F.col("member_valid"), F.lit("MISSING_MEMBERID")).otherwise(F.lit(None)),
            F.when(~F.col("provider_valid"), F.lit("MISSING_PROVIDERID")).otherwise(F.lit(None)),
            F.when(~F.col("icd10_codes_valid"), F.lit("MISSING_ICD10_CODES")).otherwise(F.lit(None)),
            F.when(~F.col("cpt_codes_valid"), F.lit("MISSING_CPT_CODES")).otherwise(F.lit(None)),
        ]
    )
)

# Flatten out None values
clean_df = clean_df.withColumn(
    "errors",
    F.expr("filter(failures, x -> x is not null)")
).drop("failures")

valid_cols_to_drop = [c for c in clean_df.columns if c.lower().endswith("_valid")]

valid_df = clean_df.filter(
    F.col("errors").isNull() | (F.size("errors") == 0)
).drop("errors", *[c for c in clean_df.columns if c.endswith("_valid")])

invalid_df = clean_df.filter(
    F.col("errors").isNotNull() & (F.size("errors") > 0)
)

# isolate invalid rows
(invalid_df
    .withColumn("audit_ts", F.current_timestamp())
    .write.mode("append").saveAsTable("medisure_dev.silver.claims_stream_invalid")
    )

# deduplicate
window = Window.partitionBy("claim_id").orderBy(F.col("event_timestamp").desc_nulls_last())
deduped = valid_df.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

# write to silver
deduped.createOrReplaceTempView("stg_claims_stream")

spark.sql(f"""
MERGE INTO medisure_dev.silver.claims_stream AS t
USING stg_claims_stream AS s
ON t.claim_id = s.claim_id
WHEN MATCHED AND (s.event_timestamp > t.event_timestamp OR t.event_timestamp IS NULL) THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")