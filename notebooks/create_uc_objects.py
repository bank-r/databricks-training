# Databricks notebook source
# DBTITLE 1,create schema
# MAGIC %sql
# MAGIC USE CATALOG medisure_dev;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw landing / bronze zone';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed & conformed';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Analytics marts';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ops COMMENT 'Operational';

# COMMAND ----------

# DBTITLE 1,create table diagnosis_ref
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.bronze.diagnosis_ref (
# MAGIC   code               STRING,
# MAGIC   description        STRING,
# MAGIC   -- Ingestion metadata
# MAGIC   _source_file       STRING,
# MAGIC   _ingest_ts         TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.silver.diagnosis_ref (
# MAGIC   code               STRING,
# MAGIC   description        STRING,
# MAGIC   _source_file       STRING,
# MAGIC   _ingest_ts         TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,create table members
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.bronze.members (
# MAGIC   member_id       STRING,
# MAGIC   name            STRING,
# MAGIC   gender          STRING,
# MAGIC   dob             TIMESTAMP,
# MAGIC   region          STRING,
# MAGIC   plan_type       STRING,
# MAGIC   effective_date  TIMESTAMP,
# MAGIC   email           STRING,
# MAGIC   is_active       BOOLEAN,
# MAGIC   last_updated    TIMESTAMP,
# MAGIC   _ingest_ts      TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- Clean members table
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.silver.members (
# MAGIC     member_id      STRING    NOT NULL,
# MAGIC     name           STRING,
# MAGIC     dob            DATE,
# MAGIC     gender         STRING,
# MAGIC     region         STRING,
# MAGIC     plan_type      STRING,
# MAGIC     effective_date DATE,
# MAGIC     email          STRING,
# MAGIC     is_active      BOOLEAN,
# MAGIC     last_updated   TIMESTAMP,
# MAGIC     _ingest_ts      TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,create table claims_batch
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.bronze.claims_batch (
# MAGIC   claim_id             STRING,
# MAGIC   member_id            STRING,
# MAGIC   provider_id          STRING,
# MAGIC   claim_date           STRING,
# MAGIC   service_date         STRING,
# MAGIC   amount               STRING,
# MAGIC   status               STRING,
# MAGIC   icd10_codes          STRING,
# MAGIC   cpt_codes            STRING,
# MAGIC   claim_type           STRING,
# MAGIC   submission_channel   STRING,
# MAGIC   notes                STRING,
# MAGIC   ingest_timestamp     STRING,
# MAGIC   -- Ingestion metadata
# MAGIC   _source_file         STRING,
# MAGIC   _ingest_ts           TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.silver.claims_batch (
# MAGIC     claim_id           STRING,
# MAGIC     member_id          STRING,
# MAGIC     provider_id        STRING,
# MAGIC     claim_date         DATE,
# MAGIC     service_date       DATE,
# MAGIC     amount             DECIMAL(18,2),
# MAGIC     status             STRING,
# MAGIC     icd10_codes        ARRAY<STRING>,
# MAGIC     cpt_codes          ARRAY<STRING>,
# MAGIC     claim_type         STRING,
# MAGIC     submission_channel STRING,
# MAGIC     notes              STRING,
# MAGIC     ingest_timestamp   TIMESTAMP,
# MAGIC     _source_file       STRING,
# MAGIC     _ingest_ts         TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,create table claims_stream
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.bronze.claims_stream (
# MAGIC   raw_json                STRING,
# MAGIC   -- ingestion metadata
# MAGIC   _source_file           STRING,
# MAGIC   _ingest_ts             TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.silver.claims_stream (
# MAGIC     claim_id         STRING    NOT NULL,
# MAGIC     member_id        STRING    NOT NULL,
# MAGIC     provider_id      STRING    NOT NULL,
# MAGIC     claim_date       DATE,
# MAGIC     amount           DECIMAL(18,2),
# MAGIC     status           STRING,
# MAGIC     icd10_codes      ARRAY<STRING>,
# MAGIC     cpt_codes        ARRAY<STRING>,
# MAGIC     event_timestamp  TIMESTAMP,
# MAGIC     _source_file       STRING,
# MAGIC     _ingest_ts     TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,create table providers
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.bronze.providers (
# MAGIC   raw_json STRING,
# MAGIC   -- ingestion metadata
# MAGIC   _source_file STRING,
# MAGIC   _ingest_ts TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS medisure_dev.silver.providers (
# MAGIC     provider_id   STRING    NOT NULL,
# MAGIC     name          STRING,
# MAGIC     specialty     STRING,
# MAGIC     address       STRING,
# MAGIC     city          STRING,
# MAGIC     state         STRING,
# MAGIC     is_active     BOOLEAN,
# MAGIC     tin           STRING,
# MAGIC     last_verified DATE,
# MAGIC     _source_file  STRING,
# MAGIC     _ingest_ts  TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,create volume
# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS medisure_dev.bronze.landing
# MAGIC LOCATION 'abfss://landing@samedisuredev.dfs.core.windows.net/';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS medisure_dev.ops.checkpoints
# MAGIC COMMENT 'Streaming checkpoints, schema logs, bad records';

# COMMAND ----------

# DBTITLE 1,mask PII
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION medisure_dev.silver.mask_email(email STRING)
# MAGIC   RETURN CASE
# MAGIC            WHEN is_member('admins') THEN email
# MAGIC            ELSE
# MAGIC              CONCAT(
# MAGIC                SUBSTRING(email, 1, 1),
# MAGIC                '*****',
# MAGIC                SUBSTRING(email, instr(email, '@'))
# MAGIC              )
# MAGIC          END;
# MAGIC
# MAGIC -- Apply the mask to the email column of the members table.
# MAGIC ALTER TABLE medisure_dev.silver.members
# MAGIC ALTER COLUMN email SET MASK medisure_dev.silver.mask_email;
# MAGIC
# MAGIC -- Mask DOB to the first day of the same year for non-admins
# MAGIC CREATE OR REPLACE FUNCTION medisure_dev.silver.mask_dob(dob DATE)
# MAGIC   RETURN CASE
# MAGIC            WHEN dob IS NULL THEN NULL
# MAGIC            WHEN is_member('admins') THEN dob
# MAGIC            ELSE make_date(year(dob), 1, 1)   -- stays a DATE
# MAGIC          END;
# MAGIC
# MAGIC -- Apply it to the members table (adjust column name if needed)
# MAGIC ALTER TABLE medisure_dev.silver.members
# MAGIC ALTER COLUMN dob SET MASK medisure_dev.silver.mask_dob;

# COMMAND ----------

# DBTITLE 1,create gold views
# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS medisure_dev.gold.claims_enriched AS
# MAGIC WITH unified_claims AS (
# MAGIC   SELECT claim_id, member_id, provider_id, claim_date, service_date,
# MAGIC          amount, status, icd10_codes, cpt_codes
# MAGIC   FROM medisure_dev.silver.claims_batch
# MAGIC   UNION ALL
# MAGIC   SELECT claim_id, member_id, provider_id, claim_date, NULL AS service_date,
# MAGIC          amount, status, icd10_codes, cpt_codes
# MAGIC   FROM medisure_dev.silver.claims_stream
# MAGIC ),
# MAGIC -- explode ICD‑10 codes and attach descriptions
# MAGIC codes_flat AS (
# MAGIC   SELECT
# MAGIC     c.claim_id,
# MAGIC     explode_outer(c.icd10_codes) AS dx_code
# MAGIC   FROM unified_claims c
# MAGIC ),
# MAGIC codes_agg AS (
# MAGIC   SELECT
# MAGIC     cf.claim_id,
# MAGIC     concat_ws(
# MAGIC       '\n',
# MAGIC       collect_list(concat(cf.dx_code, ': ', coalesce(d.description, '')))
# MAGIC     ) AS icd10_code_descriptions
# MAGIC   FROM codes_flat cf
# MAGIC   LEFT JOIN medisure_dev.silver.diagnosis_ref d
# MAGIC     ON d.code = cf.dx_code
# MAGIC   GROUP BY cf.claim_id
# MAGIC )
# MAGIC SELECT
# MAGIC   uc.claim_id,
# MAGIC   uc.claim_date,
# MAGIC   uc.amount,
# MAGIC   uc.status,
# MAGIC   m.member_id,
# MAGIC   m.name      AS member_name,
# MAGIC   m.region    AS member_region,
# MAGIC   m.plan_type,
# MAGIC   p.provider_id,
# MAGIC   p.name      AS provider_name,
# MAGIC   p.specialty,
# MAGIC   ca.icd10_code_descriptions
# MAGIC FROM unified_claims uc
# MAGIC LEFT JOIN codes_agg            ca ON uc.claim_id    = ca.claim_id
# MAGIC LEFT JOIN medisure_dev.silver.members   m  ON uc.member_id   = m.member_id
# MAGIC LEFT JOIN medisure_dev.silver.providers p  ON uc.provider_id = p.provider_id;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medisure_dev.gold.provider_monthly_summary AS
# MAGIC SELECT
# MAGIC     provider_id,
# MAGIC     date_trunc('month', claim_date) AS month_start,
# MAGIC     status,
# MAGIC     COUNT(*)                        AS claim_count,
# MAGIC     SUM(amount)                     AS total_amount,
# MAGIC     AVG(amount)                     AS avg_amount
# MAGIC FROM medisure_dev.gold.claims_enriched
# MAGIC GROUP BY provider_id, month_start, status;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW medisure_dev.gold.claims_with_fraud_score AS
# MAGIC SELECT
# MAGIC     ce.*,
# MAGIC     CASE
# MAGIC         WHEN ce.amount > 5000 THEN 0.9
# MAGIC         WHEN ce.status = 'REJECTED' AND ce.amount > 1000 THEN 0.8
# MAGIC         ELSE 0.1
# MAGIC     END AS fraud_score,
# MAGIC     CASE
# MAGIC         WHEN ce.amount > 5000 OR ce.status = 'REJECTED' THEN 'HIGH_RISK'
# MAGIC         ELSE 'NORMAL'
# MAGIC     END AS fraud_flag
# MAGIC FROM medisure_dev.gold.claims_enriched ce;
# MAGIC