# Databricks notebook source
# Databricks notebook source
from datetime import datetime
import time
from pyspark.sql import functions as F

# ============================================================
# CONFIG (single source of truth)
# ============================================================
CFG = {
    "catalog": "hive_metastore",
    "db": {
        "bronze": "pv_bronze",
        "silver": "pv_silver",
        "gold":   "pv_gold",
        "audit":  "pv_audit",
    },
    "paths": {
        "aact_raw":  "dbfs:/FileStore/tables/aact/bronze/raw",
        "faers_raw": "dbfs:/FileStore/tables/faers/bronze/raw",
    },
}

CATALOG   = CFG["catalog"]
BRONZE_DB = CFG["db"]["bronze"]
SILVER_DB = CFG["db"]["silver"]
GOLD_DB   = CFG["db"]["gold"]

AACT_RAW_PATH  = CFG["paths"]["aact_raw"]
FAERS_RAW_PATH = CFG["paths"]["faers_raw"]

AUDIT_DB    = CFG["db"]["audit"]
AUDIT_TABLE = f"{AUDIT_DB}.etl_run_log"

RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# ============================================================
# COMMON HELPERS
# ============================================================
def now_ts_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def canon(c):
    return F.upper(F.trim(F.regexp_replace(c, r"\s+", " ")))

def add_row_audit_cols(df, source_system: str, ingest_file: str = "NA"):
    return (
        df.withColumn("source_system", F.lit(source_system))
          .withColumn("ingest_file",   F.lit(ingest_file))
          .withColumn("ingest_ts",     F.current_timestamp())
    )

def add_table_audit_cols(df, source_system: str):
    return (
        df.withColumn("run_id",        F.lit(RUN_ID))
          .withColumn("source_system", F.lit(source_system))
          .withColumn("created_ts",    F.current_timestamp())
          .withColumn("updated_ts",    F.current_timestamp())
    )

def sk_from_key(key_col: str):
    return F.abs(F.hash(F.col(key_col)))

# ============================================================
# AUDIT LOGGING (single schema for all layers)
# ============================================================
def log_run(
    layer: str,
    entity: str,
    status: str,
    started_at: str = None,
    ended_at: str = None,
    duration_sec: float = None,
    rows_read: int = 0,
    rows_written: int = 0,
    message: str = None
):
    payload = [(
        RUN_ID,
        layer,
        entity,
        status,
        started_at,
        ended_at,
        float(duration_sec) if duration_sec is not None else None,
        int(rows_read),
        int(rows_written),
        message,
        datetime.utcnow()
    )]

    schema = """
        run_id STRING,
        layer STRING,
        entity STRING,
        status STRING,
        started_at STRING,
        ended_at STRING,
        duration_sec DOUBLE,
        rows_read LONG,
        rows_written LONG,
        message STRING,
        log_ts TIMESTAMP
    """

    (spark.createDataFrame(payload, schema)
          .write.format("delta")
          .mode("append")
          .saveAsTable(AUDIT_TABLE))

# ============================================================
# STANDARD RUN WRAPPER (error handling + time tracker)
# ============================================================
def run_with_audit(layer: str, entity: str, build_fn, write_fn, source_system: str = None):
    started_at = now_ts_str()
    t0 = time.perf_counter()

    status = "SUCCESS"
    rows_read = 0
    rows_written = 0
    msg = None

    try:
        result = build_fn()
        # build_fn returns either df OR (df, rows_read, rows_written)
        if isinstance(result, tuple):
            df, rows_read, rows_written = result
        else:
            df = result

        write_fn(df)
        return df

    except Exception as e:
        status = "FAILED"
        msg = f"{type(e).__name__}: {str(e)[:2000]}"
        raise

    finally:
        ended_at = now_ts_str()
        duration_sec = round(time.perf_counter() - t0, 3)
        if msg is None:
            msg = f"duration_sec={duration_sec}"
        log_run(
            layer=layer,
            entity=entity,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            duration_sec=duration_sec,
            rows_read=rows_read,
            rows_written=rows_written,
            message=msg
        )

# ============================================================
# STANDARD WRITERS
# ============================================================
def write_delta_overwrite(df, full_table: str, overwrite_schema: bool = True):
    w = (df.write.format("delta").mode("overwrite"))
    if overwrite_schema:
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(full_table)

def write_delta_append(df, full_table: str):
    df.write.format("delta").mode("append").saveAsTable(full_table)

def write_gold_overwrite(df, table_name: str, source_system: str):
    full_table = f"{GOLD_DB}.{table_name}"
    df2 = add_table_audit_cols(df, source_system)
    write_delta_overwrite(df2, full_table, overwrite_schema=True)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

