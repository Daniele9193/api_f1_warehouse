# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Silver: utilità condivise
# MAGIC Si usa con `%run ./00_slv_utils` all'inizio di ogni notebook silver.
# MAGIC - Widget `catalog` e `mode` (`incremental` = solo sessioni nuove o ricaricate in bronze, `full` = tutte).
# MAGIC - Helper per i cast SQL sicuri (`try_cast`), la scelta delle sessioni da elaborare,
# MAGIC   la scrittura con `INSERT ... REPLACE WHERE` e i controlli di qualità (`slv._dq_results`).

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

dbutils.widgets.text("catalog", "f1")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])

CATALOG = dbutils.widgets.get("catalog")
MODE = dbutils.widgets.get("mode")
BRZ = f"{CATALOG}.brz"
SLV = f"{CATALOG}.slv"
DQ_TABLE = f"{SLV}._dq_results"
RUN_TS = datetime.now(timezone.utc)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SLV}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
        run_ts TIMESTAMP, table_name STRING, check_name STRING,
        bad_rows BIGINT, total_rows BIGINT)
""")

# COMMAND ----------

# Espressioni SQL per tipizzare le colonne stringa del bronze.
# try_cast: un valore anomalo diventa NULL invece di far fallire il notebook.
def c_str(c):  return f"nullif(trim({c}), '')"
def c_int(c):  return f"try_cast({c} AS INT)"
def c_dbl(c):  return f"try_cast({c} AS DOUBLE)"
def c_ts(c):   return f"try_cast({c} AS TIMESTAMP)"
def c_bool(c): return f"try_cast(lower(trim({c})) AS BOOLEAN)"

# COMMAND ----------

def pending_sessions(table):
    """Sessioni da elaborare: mai elaborate, oppure ricaricate in bronze dopo l'ultima elaborazione."""
    bronze = f"{BRZ}.{table}"
    if not spark.catalog.tableExists(bronze):
        raise RuntimeError(f"Tabella bronze mancante: {bronze}")
    if MODE == "full":
        q = f"SELECT DISTINCT try_cast(_session_key AS INT) AS k FROM {bronze}"
    else:
        q = f"""
            SELECT b.k FROM (
                SELECT try_cast(_session_key AS INT) AS k, MAX(_ingested_at) AS i
                FROM {bronze} GROUP BY 1) b
            LEFT JOIN (
                SELECT session_key AS k, MAX(_processed_at) AS p
                FROM {SLV}.{table} GROUP BY 1) s ON b.k = s.k
            WHERE b.k IS NOT NULL AND (s.p IS NULL OR b.i > s.p)
        """
    keys = sorted(r["k"] for r in spark.sql(q).collect())
    print(f"{table}: {len(keys)} sessioni da elaborare ({MODE})")
    return keys


def load_by_sessions(table, select_sql, keys, chunk=50):
    """Sostituisce in modo atomico le righe delle sessioni indicate (REPLACE WHERE).
    Nel select_sql: __KEYS__ = lista di interi, __KEYS_STR__ = lista di stringhe (per _session_key)."""
    for i in range(0, len(keys), chunk):
        part = keys[i:i + chunk]
        ints = ",".join(str(k) for k in part)
        strs = ",".join(f"'{k}'" for k in part)
        sql = select_sql.replace("__KEYS__", ints).replace("__KEYS_STR__", strs)
        spark.sql(f"INSERT INTO {SLV}.{table} REPLACE WHERE session_key IN ({ints}) {sql}")
        print(f"{table}: scritte sessioni {part[0]}..{part[-1]} ({len(part)})")


def load_by_year(table, select_sql):
    """Per tabelle piccole con chiave non per sessione (sessions, meetings): sostituisce per anno."""
    years = [r[0] for r in spark.sql(
        f"SELECT DISTINCT try_cast(year AS INT) FROM {BRZ}.{table}").collect() if r[0] is not None]
    ints = ",".join(str(y) for y in sorted(years))
    spark.sql(f"INSERT INTO {SLV}.{table} REPLACE WHERE year IN ({ints}) {select_sql}")
    print(f"{table}: scritti gli anni {ints}")

# COMMAND ----------

def run_dq(table, bronze_where, silver_where, casts, key_cols=()):
    """Registra in slv._dq_results:
    - rows_dropped: righe bronze non arrivate in silver (duplicati scartati + chiavi nulle)
    - cast_failure:<col>: valori presenti in bronze che non si convertono nel tipo atteso
    - invalid_key:<col>: chiavi nulle o non convertibili
    `casts` e' un dict colonna -> tipo SQL (INT, DOUBLE, TIMESTAMP, BOOLEAN)."""
    exprs = []
    for col, typ in casts.items():
        if col in key_cols:
            exprs.append(f"SUM(CASE WHEN try_cast({col} AS {typ}) IS NULL THEN 1 ELSE 0 END) AS `invalid_key:{col}`")
        else:
            exprs.append(f"SUM(CASE WHEN {col} IS NOT NULL AND try_cast({col} AS {typ}) IS NULL "
                         f"THEN 1 ELSE 0 END) AS `cast_failure:{col}`")
    row = spark.sql(
        f"SELECT COUNT(*) AS total, {', '.join(exprs)} FROM {BRZ}.{table} WHERE {bronze_where}"
    ).first().asDict()
    total = row.pop("total")
    silver_rows = spark.sql(f"SELECT COUNT(*) FROM {SLV}.{table} WHERE {silver_where}").first()[0]

    results = [("rows_dropped", total - silver_rows, total)]
    results += [(name, int(bad or 0), total) for name, bad in row.items()]

    schema = StructType([
        StructField("run_ts", TimestampType()), StructField("table_name", StringType()),
        StructField("check_name", StringType()), StructField("bad_rows", LongType()),
        StructField("total_rows", LongType())])
    rows = [(RUN_TS, table, name, int(bad), int(tot)) for name, bad, tot in results]
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").saveAsTable(DQ_TABLE)

    display(spark.sql(
        f"SELECT check_name, bad_rows, total_rows FROM {DQ_TABLE} "
        f"WHERE table_name = '{table}' AND run_ts = TIMESTAMP '{RUN_TS.strftime('%Y-%m-%d %H:%M:%S.%f')}' "
        f"ORDER BY check_name"))
