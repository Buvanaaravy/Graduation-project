# Databricks notebook source
# MAGIC %run /Users/buvanaa.ravi@agilisium.com/FAERS_AACT_Project/00_config
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup audit infrastructure and helper functions
from datetime import datetime

# Create audit schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_DB}")

# Create audit table if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
  run_id STRING,
  layer STRING,
  entity STRING,
  status STRING,
  started_at STRING,
  ended_at STRING,
  duration_sec DOUBLE,
  rows_read BIGINT,
  rows_written BIGINT,
  message STRING,
  log_ts TIMESTAMP
)
USING DELTA
""")

print(f"✅ Audit infrastructure ready: {AUDIT_TABLE}")

# Define helper functions needed by cell 2
def now_ts_str():
    return datetime.utcnow().isoformat()

def add_row_audit_cols(df, source, ingest_file=None):
    return add_audit_cols(df, source, ingest_file)

def write_delta_overwrite(df, full_table, overwrite_schema=False):
    writer = df.write.format("delta").mode("overwrite")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full_table)

def write_delta_append(df, full_table):
    df.write.format("delta").mode("append").saveAsTable(full_table)

def run_with_audit(layer, entity, build_fn, write_fn):
    started_at = now_ts_str()
    status = "SUCCESS"
    message = None
    rows_read = 0
    rows_written = 0
    
    try:
        df, rows_read, rows_written = build_fn()
        write_fn(df)
    except Exception as e:
        status = "FAILED"
        message = str(e)
        raise
    finally:
        ended_at = now_ts_str()
        duration_sec = (datetime.fromisoformat(ended_at) - datetime.fromisoformat(started_at)).total_seconds()
        
        audit_row = [(
            RUN_ID, layer, entity, status,
            started_at, ended_at, duration_sec,
            int(rows_read), int(rows_written),
            message, datetime.utcnow()
        )]
        schema = "run_id string, layer string, entity string, status string, started_at string, ended_at string, duration_sec double, rows_read long, rows_written long, message string, log_ts timestamp"
        spark.createDataFrame(audit_row, schema).write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)

print("✅ Helper functions defined")

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# ============================================================
# BRONZE INGESTION
# - AACT: snapshot overwrite (exact filename match)
# - FAERS: incremental-by-file (idempotent using ingest_file)
# - Adds row audit columns to every row
# - Logs to AUDIT_TABLE with duration + errors
# ============================================================

# -----------------------------
# File utils
# -----------------------------
def list_files_safe(path: str) -> list:
    try:
        return [x.path for x in dbutils.fs.ls(path)]
    except Exception:
        return []

def match_exact_filename(files: list, filename: str) -> list:
    fn = filename.lower()
    return [p for p in files if p.lower().endswith("/" + fn)]

def table_exists(full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False

def file_already_loaded(full_table: str, file_path: str) -> bool:
    if not table_exists(full_table):
        return False
    return (
        spark.table(full_table)
        .filter(F.col("ingest_file") == file_path)
        .limit(1)
        .count() > 0
    )

# -----------------------------
# Read + contract enforcement
# -----------------------------
def read_csv(paths: list, sep: str):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", sep)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "true")
        .csv(paths)
    )

def enforce_required_columns(df, required_cols: list):
    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast("string"))
    return df.select(*required_cols)

# -----------------------------
# Loaders
# -----------------------------
def load_snapshot(entity: dict):
    # build_fn
    def build():
        files = list_files_safe(entity["raw_path"])
        hits = match_exact_filename(files, entity["filename"])
        if not hits:
            raise FileNotFoundError(f"File not found: {entity['filename']} in {entity['raw_path']}")

        df_raw = read_csv(hits, sep=entity["sep"])
        df = enforce_required_columns(df_raw, entity["required_cols"])
        ingest_file = "MULTI" if len(hits) > 1 else hits[0]
        df = add_row_audit_cols(df, entity["source"], ingest_file=ingest_file)

        rows_read = df_raw.count()
        rows_written = rows_read
        return df, rows_read, rows_written

    # write_fn
    def write(df):
        full_table = f"{BRONZE_DB}.{entity['table']}"
        write_delta_overwrite(df, full_table, overwrite_schema=True)

    return run_with_audit(layer="BRONZE", entity=entity["table"], build_fn=build, write_fn=write)

def load_incremental_by_file(entity: dict):
    files = list_files_safe(entity["raw_path"])
    hits = sorted([p for p in files if entity["keyword"].lower() in p.lower()])
    if not hits:
        # log as skipped without throwing
        log_run(layer="BRONZE", entity=entity["table"], status="SKIPPED",
                started_at=now_ts_str(), ended_at=now_ts_str(), duration_sec=0.0,
                message=f"No files for keyword={entity['keyword']}")
        return

    full_table = f"{BRONZE_DB}.{entity['table']}"

    for fp in hits:
        if file_already_loaded(full_table, fp):
            continue

        def build():
            df_raw = read_csv([fp], sep=entity["sep"])
            df = enforce_required_columns(df_raw, entity["required_cols"])
            df = add_row_audit_cols(df, entity["source"], ingest_file=fp)

            rows_read = df_raw.count()
            rows_written = rows_read
            return df, rows_read, rows_written

        def write(df):
            write_delta_append(df, full_table)

        # one audit entry per file (clean + traceable)
        run_with_audit(layer="BRONZE", entity=f"{entity['table']}|{fp}", build_fn=build, write_fn=write)

def run_entities(entities: list):
    for e in entities:
        mode = e["mode"].lower()
        if mode == "snapshot":
            load_snapshot(e)
        elif mode == "incremental_by_file":
            load_incremental_by_file(e)
        else:
            log_run(layer="BRONZE", entity=e.get("table","unknown"), status="FAILED",
                    started_at=now_ts_str(), ended_at=now_ts_str(), duration_sec=0.0,
                    message=f"Unknown mode: {e.get('mode')}")


# COMMAND ----------


# -----------------------------
# Metadata
# -----------------------------
ENTITIES = [
    # AACT (snapshot)
    {
        "name": "AACT Studies", "source": "AACT",
        "raw_path": AACT_RAW_PATH, "filename": "studies.txt",
        "table": "aact_studies", "sep": "|", "mode": "snapshot",
        "required_cols": ["nct_id","overall_status","phase","study_type","enrollment","start_date","completion_date","last_update_posted_date"]
    },
    {
        "name": "AACT Interventions", "source": "AACT",
        "raw_path": AACT_RAW_PATH, "filename": "interventions.txt",
        "table": "aact_interventions", "sep": "|", "mode": "snapshot",
        "required_cols": ["nct_id","intervention_type","name"]
    },
    {
        "name": "AACT Conditions", "source": "AACT",
        "raw_path": AACT_RAW_PATH, "filename": "conditions.txt",
        "table": "aact_conditions", "sep": "|", "mode": "snapshot",
        "required_cols": ["nct_id","name"]
    },

    # FAERS (incremental)
    {
        "name": "FAERS Demo", "source": "FAERS",
        "raw_path": FAERS_RAW_PATH, "keyword": "demo",
        "table": "faers_demo", "sep": "$", "mode": "incremental_by_file",
        "required_cols": ["primaryid","caseid","caseversion","sex","age","age_cod","event_dt","occr_country","reporter_country"]
    },
    {
        "name": "FAERS Drug", "source": "FAERS",
        "raw_path": FAERS_RAW_PATH, "keyword": "drug",
        "table": "faers_drug", "sep": "$", "mode": "incremental_by_file",
        "required_cols": ["primaryid","caseid","drug_seq","role_cod","drugname","prod_ai","route"]
    },
    {
        "name": "FAERS Reac", "source": "FAERS",
        "raw_path": FAERS_RAW_PATH, "keyword": "reac",
        "table": "faers_reac", "sep": "$", "mode": "incremental_by_file",
        "required_cols": ["primaryid","caseid","pt"]
    },
    {
        "name": "FAERS Outc", "source": "FAERS",
        "raw_path": FAERS_RAW_PATH, "keyword": "outc",
        "table": "faers_outc", "sep": "$", "mode": "incremental_by_file",
        "required_cols": ["primaryid","caseid","outc_cod"]
    },
    {
        "name": "FAERS Indi", "source": "FAERS",
        "raw_path": FAERS_RAW_PATH, "keyword": "indi",
        "table": "faers_indi", "sep": "$", "mode": "incremental_by_file",
        "required_cols": ["primaryid","caseid","indi_drug_seq","indi_pt"]
    },
]
