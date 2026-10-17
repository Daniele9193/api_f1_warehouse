# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Silver: sessions
# MAGIC Da `f1.brz.sessions`: tipizzazione, deduplicazione per `session_key`, ricarica per anno.

# COMMAND ----------

# MAGIC %run ./00_slv_utils

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SLV}.sessions (
        session_key INT NOT NULL,
        meeting_key INT,
        year INT NOT NULL,
        session_name STRING,
        session_type STRING,
        date_start TIMESTAMP,
        date_end TIMESTAMP,
        gmt_offset STRING,
        is_cancelled BOOLEAN,
        circuit_key INT,
        circuit_short_name STRING,
        country_key INT,
        country_code STRING,
        country_name STRING,
        location STRING,
        _ingested_at TIMESTAMP,
        _processed_at TIMESTAMP
    )
""")

# COMMAND ----------

select_sql = f"""
SELECT * FROM (
    SELECT
        {c_int('session_key')}  AS session_key,
        {c_int('meeting_key')}  AS meeting_key,
        {c_int('year')}         AS year,
        {c_str('session_name')} AS session_name,
        {c_str('session_type')} AS session_type,
        {c_ts('date_start')}    AS date_start,
        {c_ts('date_end')}      AS date_end,
        {c_str('gmt_offset')}   AS gmt_offset,
        {c_bool('is_cancelled')} AS is_cancelled,
        {c_int('circuit_key')}  AS circuit_key,
        {c_str('circuit_short_name')} AS circuit_short_name,
        {c_int('country_key')}  AS country_key,
        {c_str('country_code')} AS country_code,
        {c_str('country_name')} AS country_name,
        {c_str('location')}     AS location,
        _ingested_at,
        current_timestamp()     AS _processed_at
    FROM {BRZ}.sessions
)
WHERE session_key IS NOT NULL AND year IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY _ingested_at DESC) = 1
"""

load_by_year("sessions", select_sql)

# COMMAND ----------

run_dq(
    "sessions",
    bronze_where="1 = 1",
    silver_where="1 = 1",
    casts={"session_key": "INT", "year": "INT", "meeting_key": "INT", "date_start": "TIMESTAMP",
           "date_end": "TIMESTAMP", "is_cancelled": "BOOLEAN", "circuit_key": "INT", "country_key": "INT"},
    key_cols=("session_key", "year"),
)
