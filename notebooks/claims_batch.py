# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG medisure_dev")
spark.sql("USE SCHEMA bronze")

src = "/Volumes/medisure_dev/bronze/landing/claims/batch"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/medisure_dev/ops/checkpoints/schemas/claims/batch")
    .option("mode", "FAILFAST")  # fail the batch on bad rows instead of rescuing
    .load(src)
    .withColumnRenamed("ClaimID", "claim_id")
    .withColumnRenamed("MemberID", "member_id")
    .withColumnRenamed("ProviderID", "provider_id")
    .withColumnRenamed("ClaimDate", "claim_date")
    .withColumnRenamed("ServiceDate", "service_date")
    .withColumnRenamed("Amount", "amount")
    .withColumnRenamed("Status", "status")
    .withColumnRenamed("ICD10Codes", "icd10_codes")
    .withColumnRenamed("CPTCodes", "cpt_codes")
    .withColumnRenamed("ClaimType", "claim_type")
    .withColumnRenamed("SubmissionChannel", "submission_channel")
    .withColumnRenamed("Notes", "notes")
    .withColumnRenamed("IngestTimestamp", "ingest_timestamp")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
)
df = df.drop("_rescued_data")

q = (df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/medisure_dev/ops/checkpoints/streams/claims/batch")
  .trigger(availableNow=True)
  .toTable("medisure_dev.bronze.claims_batch"))

q.awaitTermination()
# COMMAND ----------
# read bronze table
df = spark.table("medisure_dev.bronze.claims_batch")

clean_df = (df
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("service_date", F.to_date("service_date"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("status", F.upper("status"))
    .withColumn("icd10_codes", F.split(F.upper("icd10_codes"), ";"))
    .withColumn("cpt_codes", F.split("cpt_codes", ";"))
    .withColumn("ingest_timestamp", F.to_timestamp("ingest_timestamp"))
    .withColumn("amount_valid", F.col("amount") >= 0)
    .withColumn("dates_valid", F.col("service_date") <= F.col("claim_date"))
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
            F.when(~F.col("amount_valid"), F.lit("AMOUNT_NEGATIVE")).otherwise(F.lit(None)),
            F.when(~F.col("dates_valid"), F.lit("SERVICE_AFTER_CLAIM_DATE")).otherwise(F.lit(None)),
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
    .write.mode("append").saveAsTable("medisure_dev.silver.claims_batch_invalid")
    )

# deduplicate
window = Window.partitionBy("claim_id").orderBy(F.col("ingest_timestamp").desc_nulls_last())
deduped = valid_df.withColumn("rn", F.row_number().over(window)).filter("rn = 1").drop("rn")

# write to silver
deduped.write.format("delta").mode("overwrite").saveAsTable("medisure_dev.silver.claims_batch")