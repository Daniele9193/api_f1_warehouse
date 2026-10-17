# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Silver: meetings
# MAGIC Da `f1.brz.meetings`: tipizzazione, deduplicazione per `meeting_key`, ricarica per anno.

# COMMAND ----------

# MAGIC %run ./00_slv_utils

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SLV}.meetings (
        meeting_key INT NOT NULL,
        year INT NOT NULL,
        meeting_name STRING,
        meeting_official_name STRING,
        date_start TIMESTAMP,
        date_end TIMESTAMP,
        gmt_offset STRING,
        is_cancelled BOOLEAN,
        circuit_key INT,
        circuit_short_name STRING,
        circuit_type STRING,
        circuit_image STRING,
        circuit_info_url STRING,
        country_key INT,
        country_code STRING,
        country_name STRING,
        country_flag STRING,
        location STRING,
        _ingested_at TIMESTAMP,
        _processed_at TIMESTAMP
    )
""")

# COMMAND ----------

select_sql = f"""
SELECT * FROM (
    SELECT
        {c_int('meeting_key')}  AS meeting_key,
        {c_int('year')}         AS year,
        {c_str('meeting_name')} AS meeting_name,
        {c_str('meeting_official_name')} AS meeting_official_name,
        {c_ts('date_start')}    AS date_start,
        {c_ts('date_end')}      AS date_end,
        {c_str('gmt_offset')}   AS gmt_offset,
        {c_bool('is_cancelled')} AS is_cancelled,
        {c_int('circuit_key')}  AS circuit_key,
        {c_str('circuit_short_name')} AS circuit_short_name,
        {c_str('circuit_type')} AS circuit_type,
        {c_str('circuit_image')} AS circuit_image,
        {c_str('circuit_info_url')} AS circuit_info_url,
        {c_int('country_key')}  AS country_key,
        {c_str('country_code')} AS country_code,
        {c_str('country_name')} AS country_name,
        {c_str('country_flag')} AS country_flag,
        {c_str('location')}     AS location,
        _ingested_at,
        current_timestamp()     AS _processed_at
    FROM {BRZ}.meetings
)
WHERE meeting_key IS NOT NULL AND year IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY meeting_key ORDER BY _ingested_at DESC) = 1
"""

load_by_year("meetings", select_sql)

# COMMAND ----------

run_dq(
    "meetings",
    bronze_where="1 = 1",
    silver_where="1 = 1",
    casts={"meeting_key": "INT", "year": "INT", "date_start": "TIMESTAMP", "date_end": "TIMESTAMP",
           "is_cancelled": "BOOLEAN", "circuit_key": "INT", "country_key": "INT"},
    key_cols=("meeting_key", "year"),
)
