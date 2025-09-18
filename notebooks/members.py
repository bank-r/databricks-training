# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG medisure_dev")
spark.sql("USE SCHEMA bronze")

jdbc_url = "jdbc:sqlserver://sql-medisure-dev.database.windows.net:1433;database=db_medisure_dev"
connection_props = {
  "user": dbutils.secrets.get("medisure-dev", "sql-user"),
  "password": dbutils.secrets.get("medisure-dev", "sql-pass"),
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "fetchsize": "1000"
}

df = (spark.read.format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "dbo.Members")  # or use the subquery form below
      .options(**connection_props)
      .load()
      .withColumnRenamed("Name", "name")
      .withColumnRenamed("MemberID", "member_id")
      .withColumnRenamed("Name", "name")
      .withColumnRenamed("DOB", "dob")
      .withColumnRenamed("Gender", "gender")
      .withColumnRenamed("Region", "region")
      .withColumnRenamed("PlanType", "plan_type")
      .withColumnRenamed("EffectiveDate", "effective_date")
      .withColumnRenamed("Email", "email")
      .withColumnRenamed("IsActive", "is_active")
      .withColumnRenamed("LastUpdated", "last_updated")
      .withColumn("_ingest_ts", F.current_timestamp())
)

(df.write
   .format("delta")
   .mode("append")
   .saveAsTable("medisure_dev.bronze.members"))

# COMMAND ----------

# Read the bronze members table
df = spark.table("medisure_dev.bronze.members")

# Initial normalisation and type conversions
clean_df = (
    df
      .withColumn("member_id",      F.trim("member_id"))
      .withColumn("name",           F.trim("name"))
      .withColumn("dob",            F.to_date("dob"))
      .withColumn("gender",         F.upper(F.trim("gender")))
      .withColumn("region",         F.trim("region"))
      .withColumn("plan_type",      F.trim("plan_type"))
      .withColumn("effective_date", F.to_date("effective_date"))
      .withColumn("email",          F.lower(F.trim("email")))
      .withColumn("is_active",      F.col("is_active").cast("boolean"))
      .withColumn("last_updated",   F.to_timestamp("last_updated"))
      .withColumn("_ingest_ts",   F.col("_ingest_ts"))
)

# Define validation rules
clean_df = (
    clean_df
      .withColumn("member_valid",       F.col("member_id").isNotNull())
      .withColumn("dob_valid",          (F.col("dob").between(F.lit("1900-01-01"), F.current_date())) &
                                        (F.months_between(F.current_date(), F.col("dob")) / 12 <= 120))
      .withColumn("email_valid",        F.when(F.col("email").isNull(), True)
                                         .otherwise(F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")))
      .withColumn("effective_date_valid", F.col("effective_date").isNotNull() &
                                          (F.col("effective_date") <= F.current_date()))
      .withColumn("gender_valid",       F.col("gender").isin("M","F","O","U"))
)

clean_df = clean_df.withColumn(
    "errors",
    F.when(
        F.col("member_valid") &
        F.col("dob_valid") &
        F.col("email_valid") &
        F.col("effective_date_valid") &
        F.col("gender_valid"),
        F.array().cast("array<string>")
    ).otherwise(
        F.array_remove(F.array(
            F.when(~F.col("member_valid"),        F.lit("MISSING_MEMBERID")),
            F.when(~F.col("dob_valid"),           F.lit("INVALID_DOB")),
            F.when(~F.col("email_valid"),         F.lit("INVALID_EMAIL")),
            F.when(~F.col("effective_date_valid"),F.lit("INVALID_EFFECTIVE_DATE")),
            F.when(~F.col("gender_valid"),        F.lit("INVALID_GENDER"))
        ), None)
    )
)

# Split into valid and invalid sets
valid_cols_to_drop = [c for c in clean_df.columns if c.endswith("_valid")]
valid_df   = clean_df.filter(F.size("errors") == 0).drop("errors", *valid_cols_to_drop)
invalid_df = clean_df.filter(F.size("errors") > 0).drop(*valid_cols_to_drop)

# Write invalid rows to quarantine with audit timestamp
(invalid_df
    .withColumn("audit_ts", F.current_timestamp())
    .write.mode("append").saveAsTable("medisure_dev.silver.members_invalid")
)

# Deduplicate by member_id, keeping the most recent last_updated
window = Window.partitionBy("member_id").orderBy(F.col("last_updated").desc_nulls_last())
deduped = valid_df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

# Persist cleaned data to silver.members
deduped.write.format("delta").mode("overwrite").saveAsTable("medisure_dev.silver.members")
