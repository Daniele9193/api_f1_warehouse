# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Silver: drivers
# MAGIC Da `f1.brz.drivers`: una riga per sessione e pilota. Scartato `country_code` (deprecato in OpenF1).
# MAGIC Caricamento per sessione con `REPLACE WHERE`.

# COMMAND ----------

# MAGIC %run ./00_slv_utils

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SLV}.drivers (
        session_key INT NOT NULL,
        driver_number INT NOT NULL,
        meeting_key INT,
        full_name STRING,
        first_name STRING,
        last_name STRING,
        broadcast_name STRING,
        name_acronym STRING,
        team_name STRING,
        team_colour STRING,
        headshot_url STRING,
        _ingested_at TIMESTAMP,
        _processed_at TIMESTAMP
    )
""")

# COMMAND ----------

select_sql = f"""
SELECT * FROM (
    SELECT
        {c_int('session_key')}   AS session_key,
        {c_int('driver_number')} AS driver_number,
        {c_int('meeting_key')}   AS meeting_key,
        {c_str('full_name')}     AS full_name,
        {c_str('first_name')}    AS first_name,
        {c_str('last_name')}     AS last_name,
        {c_str('broadcast_name')} AS broadcast_name,
        {c_str('name_acronym')}  AS name_acronym,
        {c_str('team_name')}     AS team_name,
        {c_str('team_colour')}   AS team_colour,
        {c_str('headshot_url')}  AS headshot_url,
        _ingested_at,
        current_timestamp()      AS _processed_at
    FROM {BRZ}.drivers
    WHERE _session_key IN (__KEYS_STR__)
)
WHERE session_key IS NOT NULL AND driver_number IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY _ingested_at DESC) = 1
"""

keys = pending_sessions("drivers")
if keys:
    load_by_sessions("drivers", select_sql, keys)

# COMMAND ----------

if keys:
    ints = ",".join(str(k) for k in keys)
    strs = ",".join(f"'{k}'" for k in keys)
    run_dq(
        "drivers",
        bronze_where=f"_session_key IN ({strs})",
        silver_where=f"session_key IN ({ints})",
        casts={"session_key": "INT", "driver_number": "INT", "meeting_key": "INT"},
        key_cols=("session_key", "driver_number"),
    )
else:
    print("Niente da elaborare.")
