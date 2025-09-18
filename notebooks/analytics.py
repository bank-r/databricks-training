# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM medisure_dev.gold.claims_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     provider_id,
# MAGIC     month_start,
# MAGIC     SUM(CASE WHEN status = 'SUBMITTED' THEN 1 ELSE 0 END) AS submitted_count,
# MAGIC     SUM(CASE WHEN status = 'APPROVED'      THEN 1 ELSE 0 END) AS approved_count,
# MAGIC     SUM(CASE WHEN status = 'REJECTED'  THEN 1 ELSE 0 END) AS rejected_count,
# MAGIC     SUM(total_amount)                                           AS total_amount
# MAGIC FROM medisure_dev.gold.provider_monthly_summary
# MAGIC GROUP BY provider_id, month_start;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM medisure_dev.gold.claims_with_fraud_score