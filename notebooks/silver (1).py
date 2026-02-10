# Databricks notebook source
# MAGIC %run /Users/buvanaa.ravi@agilisium.com/FAERS_AACT_Project/00_config
# MAGIC

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

LAYER = "SILVER"

def to_int(col):
    return F.regexp_replace(F.col(col).cast("string"), r"[^0-9\-]", "").cast("int")

def to_date_safe(col):
    return F.to_date(F.col(col).cast("string"))

def run_silver_table(table_name: str, build_fn):
    def write(df):
        full_table = f"{SILVER_DB}.{table_name}"
        write_delta_overwrite(df, full_table, overwrite_schema=True)
    return run_with_audit(layer=LAYER, entity=table_name, build_fn=build_fn, write_fn=write)


# COMMAND ----------


# -----------------------------
# AACT
# -----------------------------
run_silver_table("aact_studies", lambda: (
    spark.table(f"{BRONZE_DB}.aact_studies")
        .select(
            F.trim("nct_id").alias("nct_id"),
            F.upper(F.trim("overall_status")).alias("overall_status"),
            F.upper(F.trim("phase")).alias("phase"),
            F.upper(F.trim("study_type")).alias("study_type"),
            to_int("enrollment").alias("enrollment"),
            to_date_safe("start_date").alias("start_date"),
            to_date_safe("completion_date").alias("completion_date"),
            to_date_safe("last_update_posted_date").alias("last_update_posted_date")
        )
        .filter(F.col("nct_id").isNotNull() & (F.trim(F.col("nct_id")) != ""))
        .dropDuplicates(["nct_id"])
))

run_silver_table("aact_interventions", lambda: (
    spark.table(f"{BRONZE_DB}.aact_interventions")
        .select(
            F.trim("nct_id").alias("nct_id"),
            F.upper(F.trim("intervention_type")).alias("intervention_type"),
            F.trim("name").alias("name")
        )
        .filter(F.col("nct_id").isNotNull() & (F.trim(F.col("nct_id")) != ""))
        .filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
        .filter(F.col("intervention_type") == "DRUG")
        .dropDuplicates(["nct_id","intervention_type","name"])
))

run_silver_table("aact_conditions", lambda: (
    spark.table(f"{BRONZE_DB}.aact_conditions")
        .select(
            F.trim("nct_id").alias("nct_id"),
            F.trim("name").alias("name"),
            canon(F.col("name")).alias("condition_key")
        )
        .filter(F.col("nct_id").isNotNull() & (F.trim(F.col("nct_id")) != ""))
        .filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
        .dropDuplicates(["nct_id","name"])
))


# COMMAND ----------


# -----------------------------
# FAERS
# -----------------------------
w_demo = Window.partitionBy("caseid").orderBy(F.col("caseversion").desc_nulls_last())

run_silver_table("faers_demo", lambda: (
    spark.table(f"{BRONZE_DB}.faers_demo")
        .select(
            F.trim("primaryid").alias("primaryid"),
            F.trim("caseid").alias("caseid"),
            to_int("caseversion").alias("caseversion"),
            F.upper(F.trim("sex")).alias("sex"),
            to_int("age").alias("age"),
            F.upper(F.trim("age_cod")).alias("age_cod"),
            to_date_safe("event_dt").alias("event_dt"),
            F.upper(F.trim("occr_country")).alias("occr_country"),
            F.upper(F.trim("reporter_country")).alias("reporter_country")
        )
        .filter(F.col("caseid").isNotNull() & (F.trim(F.col("caseid")) != ""))
        .withColumn("rn", F.row_number().over(w_demo))
        .filter(F.col("rn") == 1)
        .drop("rn")
))

run_silver_table("faers_drug", lambda: (
    spark.table(f"{BRONZE_DB}.faers_drug")
        .select(
            F.trim("primaryid").alias("primaryid"),
            F.trim("caseid").alias("caseid"),
            F.col("drug_seq").cast("int").alias("drug_seq"),
            F.upper(F.trim("role_cod")).alias("role_cod"),
            F.trim("drugname").alias("drugname"),
            F.trim("prod_ai").alias("prod_ai"),
            F.upper(F.trim("route")).alias("route")
        )
        .filter(F.col("primaryid").isNotNull() & (F.trim(F.col("primaryid")) != ""))
        .filter(F.col("role_cod").isin("PS", "SS", "I"))
        .dropDuplicates()
))

run_silver_table("faers_reac", lambda: (
    spark.table(f"{BRONZE_DB}.faers_reac")
        .select(
            F.trim("primaryid").alias("primaryid"),
            F.trim("caseid").alias("caseid"),
            F.trim("pt").alias("pt"),
            canon(F.col("pt")).alias("reaction_key")
        )
        .filter(F.col("primaryid").isNotNull() & (F.trim(F.col("primaryid")) != ""))
        .filter(F.col("pt").isNotNull() & (F.trim(F.col("pt")) != ""))
        .dropDuplicates(["primaryid","caseid","pt"])
))

run_silver_table("faers_outc", lambda: (
    spark.table(f"{BRONZE_DB}.faers_outc")
        .select(
            F.trim("primaryid").alias("primaryid"),
            F.trim("caseid").alias("caseid"),
            F.upper(F.trim("outc_cod")).alias("outc_cod")
        )
        .filter(F.col("primaryid").isNotNull() & (F.trim(F.col("primaryid")) != ""))
        .dropDuplicates(["primaryid","caseid","outc_cod"])
))

run_silver_table("faers_indi", lambda: (
    spark.table(f"{BRONZE_DB}.faers_indi")
        .select(
            F.trim("primaryid").alias("primaryid"),
            F.trim("caseid").alias("caseid"),
            to_int("indi_drug_seq").alias("indi_drug_seq"),
            F.trim("indi_pt").alias("indi_pt"),
            canon(F.col("indi_pt")).alias("indication_key")
        )
        .filter(F.col("primaryid").isNotNull() & (F.trim(F.col("primaryid")) != ""))
        .filter(F.col("indi_pt").isNotNull() & (F.trim(F.col("indi_pt")) != ""))
        .dropDuplicates(["primaryid","caseid","indi_drug_seq","indi_pt"])
))


# COMMAND ----------


# -----------------------------
# Minimal DQ checks (final)
# -----------------------------
SILVER_TABLES = [
    "aact_studies","aact_interventions","aact_conditions",
    "faers_demo","faers_drug","faers_reac","faers_outc","faers_indi"
]
for t in SILVER_TABLES:
    full_table = f"{SILVER_DB}.{t}"
    if not spark.catalog.tableExists(full_table):
        raise Exception(f"DQ FAILED: Missing table {full_table}")
    if spark.table(full_table).limit(1).count() == 0:
        raise Exception(f"DQ FAILED: Empty table {full_table}")
