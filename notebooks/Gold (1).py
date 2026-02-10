# Databricks notebook source
# MAGIC %run /Users/buvanaa.ravi@agilisium.com/FAERS_AACT_Project/00_config

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from datetime import datetime, timedelta

LAYER = "GOLD"

def run_gold_table(table_name: str, source_system: str, build_fn):
    def write(df):
        write_gold_overwrite(df, table_name, source_system)
    return run_with_audit(layer=LAYER, entity=table_name, build_fn=build_fn, write_fn=write)

# -----------------------------
# Validate SILVER sources (fast check)
# -----------------------------
REQUIRED_SILVER = [
    "aact_studies","aact_interventions","aact_conditions",
    "faers_demo","faers_drug","faers_reac","faers_indi","faers_outc"
]
for t in REQUIRED_SILVER:
    if not spark.catalog.tableExists(f"{SILVER_DB}.{t}"):
        raise ValueError(f"Missing required table: {SILVER_DB}.{t}")


# COMMAND ----------


# -----------------------------
# DIM: dim_drug (FAERS priority)
# -----------------------------
def build_dim_drug():
    drug_faers = (
        spark.table(f"{SILVER_DB}.faers_drug")
            .filter(F.col("drugname").isNotNull() & (F.trim(F.col("drugname")) != ""))
            .select(
                canon(F.col("drugname")).alias("drug_key"),
                F.col("drugname").alias("drug_name"),
                F.col("prod_ai").alias("active_ingredient"),
                F.col("route").alias("route"),
                F.lit(1).alias("source_priority"),
                F.lit("FAERS").alias("source")
            )
    )

    drug_aact = (
        spark.table(f"{SILVER_DB}.aact_interventions")
            .filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
            .select(
                canon(F.col("name")).alias("drug_key"),
                F.col("name").alias("drug_name"),
                F.lit(None).cast("string").alias("active_ingredient"),
                F.lit(None).cast("string").alias("route"),
                F.lit(2).alias("source_priority"),
                F.lit("AACT").alias("source")
            )
    )

    w = Window.partitionBy("drug_key").orderBy(F.col("source_priority").asc())

    dim = (
        drug_faers.unionByName(drug_aact)
            .filter(F.col("drug_key").isNotNull() & (F.trim(F.col("drug_key")) != ""))
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn","source_priority")
            .withColumn("drug_sk", sk_from_key("drug_key"))
            .select("drug_sk","drug_key","drug_name","active_ingredient","route","source")
            .dropDuplicates(["drug_sk"])
    )
    return dim

run_gold_table("dim_drug", "FAERS_AACT", build_dim_drug)


# COMMAND ----------


# -----------------------------
# DIM: dim_reaction
# -----------------------------
run_gold_table("dim_reaction", "FAERS", lambda: (
    spark.table(f"{SILVER_DB}.faers_reac")
        .filter(F.col("pt").isNotNull() & (F.trim(F.col("pt")) != ""))
        .select(
            canon(F.col("pt")).alias("reaction_key"),
            F.col("pt").alias("reaction_name")
        )
        .dropDuplicates(["reaction_key"])
        .withColumn("reaction_sk", sk_from_key("reaction_key"))
        .select("reaction_sk","reaction_key","reaction_name")
))


# COMMAND ----------


# -----------------------------
# DIM: dim_condition (FAERS priority)
# -----------------------------
def build_dim_condition():
    cond_faers = (
        spark.table(f"{SILVER_DB}.faers_indi")
            .filter(F.col("indi_pt").isNotNull() & (F.trim(F.col("indi_pt")) != ""))
            .select(
                canon(F.col("indi_pt")).alias("condition_key"),
                F.col("indi_pt").alias("condition_name"),
                F.lit(1).alias("source_priority"),
                F.lit("FAERS").alias("source")
            )
    )
    cond_aact = (
        spark.table(f"{SILVER_DB}.aact_conditions")
            .filter(F.col("name").isNotNull() & (F.trim(F.col("name")) != ""))
            .select(
                canon(F.col("name")).alias("condition_key"),
                F.col("name").alias("condition_name"),
                F.lit(2).alias("source_priority"),
                F.lit("AACT").alias("source")
            )
    )

    w = Window.partitionBy("condition_key").orderBy(F.col("source_priority").asc())

    dim = (
        cond_faers.unionByName(cond_aact)
            .filter(F.col("condition_key").isNotNull() & (F.trim(F.col("condition_key")) != ""))
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn","source_priority")
            .withColumn("condition_sk", sk_from_key("condition_key"))
            .select("condition_sk","condition_key","condition_name","source")
            .dropDuplicates(["condition_sk"])
    )
    return dim

run_gold_table("dim_condition", "FAERS_AACT", build_dim_condition)


# COMMAND ----------


# -----------------------------
# DIM: dim_patient (from FAERS demo)
# -----------------------------
run_gold_table("dim_patient", "FAERS", lambda: (
    spark.table(f"{SILVER_DB}.faers_demo")
        .select(
            F.coalesce(F.col("sex"), F.lit("UNK")).alias("sex"),
            F.coalesce(F.col("age_cod"), F.lit("UNK")).alias("age_cod"),
            F.coalesce(F.col("reporter_country"), F.lit("UNK")).alias("reporter_country"),
            F.coalesce(F.col("occr_country"), F.lit("UNK")).alias("occr_country")
        )
        .withColumn("patient_key", canon(F.concat_ws("|","sex","age_cod","reporter_country","occr_country")))
        .dropDuplicates(["patient_key"])
        .withColumn("patient_sk", sk_from_key("patient_key"))
        .select("patient_sk","patient_key","sex","age_cod","reporter_country","occr_country")
))


# COMMAND ----------


# -----------------------------
# DIM: dim_study (AACT)
# -----------------------------
run_gold_table("dim_study", "AACT", lambda: (
    spark.table(f"{SILVER_DB}.aact_studies")
        .withColumn("study_id", F.trim(F.col("nct_id")))
        .filter(F.col("study_id").isNotNull() & (F.trim(F.col("study_id")) != ""))
        .dropDuplicates(["study_id"])
        .withColumn("study_sk", sk_from_key("study_id"))
        .select(
            "study_sk",
            "study_id",
            F.col("overall_status"),
            F.col("phase"),
            F.col("study_type"),
            F.col("enrollment"),
            F.col("completion_date"),
            F.col("last_update_posted_date")
        )
))


# COMMAND ----------


# -----------------------------
# DIM: dim_date
# -----------------------------
def build_dim_date():
    start_date = datetime(2000, 1, 1)
    end_date   = datetime(2026, 12, 31)

    rows = []
    cur = start_date
    while cur <= end_date:
        rows.append((
            int(cur.strftime("%Y%m%d")),
            cur.date(),
            cur.year,
            f"{cur.year}Q{(cur.month-1)//3 + 1}",
            cur.month,
            cur.strftime("%B"),
            cur.day,
            cur.strftime("%A"),
            int(cur.weekday() >= 5)
        ))
        cur += timedelta(days=1)

    return spark.createDataFrame(
        rows,
        ["date_key","full_date","year","quarter","month","month_name","day","day_of_week","is_weekend"]
    )

run_gold_table("dim_date", "SYSTEM", build_dim_date)

# 

# COMMAND ----------

#-----------------------------
# FACT: fact_faers_event
# -----------------------------
def build_fact_faers_event():
    dim_drug_lu = broadcast(spark.table(f"{GOLD_DB}.dim_drug").select("drug_key","drug_sk"))
    dim_reac_lu = broadcast(spark.table(f"{GOLD_DB}.dim_reaction").select("reaction_key","reaction_sk"))
    dim_pat_lu  = broadcast(spark.table(f"{GOLD_DB}.dim_patient").select("patient_key","patient_sk"))
    dim_cond_lu = broadcast(spark.table(f"{GOLD_DB}.dim_condition").select("condition_key","condition_sk"))

    demo_latest = (
        spark.table(f"{SILVER_DB}.faers_demo")
            .select(
                F.col("caseid").cast("int").alias("caseid"),
                F.col("primaryid").cast("bigint").alias("primaryid"),
                F.col("event_dt").cast("date").alias("event_dt"),
                F.date_format(F.col("event_dt").cast("date"), "yyyyMMdd").cast("int").alias("date_key"),
                F.coalesce(F.col("sex"), F.lit("UNK")).alias("sex"),
                F.coalesce(F.col("age_cod"), F.lit("UNK")).alias("age_cod"),
                F.coalesce(F.col("reporter_country"), F.lit("UNK")).alias("reporter_country"),
                F.coalesce(F.col("occr_country"), F.lit("UNK")).alias("occr_country")
            )
            .dropDuplicates(["caseid","primaryid"])
            .withColumn("patient_key", canon(F.concat_ws("|","sex","age_cod","reporter_country","occr_country")))
    )

    drug = (
        spark.table(f"{SILVER_DB}.faers_drug")
            .select(
                F.col("caseid").cast("int").alias("caseid"),
                F.col("primaryid").cast("bigint").alias("primaryid"),
                canon(F.col("drugname")).alias("drug_key")
            )
            .filter(F.col("drug_key").isNotNull() & (F.trim(F.col("drug_key")) != ""))
            .dropDuplicates(["caseid","primaryid","drug_key"])
    )

    reac = (
        spark.table(f"{SILVER_DB}.faers_reac")
            .select(
                F.col("caseid").cast("int").alias("caseid"),
                F.col("primaryid").cast("bigint").alias("primaryid"),
                canon(F.col("pt")).alias("reaction_key")
            )
            .filter(F.col("reaction_key").isNotNull() & (F.trim(F.col("reaction_key")) != ""))
            .dropDuplicates(["caseid","primaryid","reaction_key"])
    )

    indi = (
        spark.table(f"{SILVER_DB}.faers_indi")
            .select(
                F.col("caseid").cast("int").alias("caseid"),
                F.col("primaryid").cast("bigint").alias("primaryid"),
                canon(F.col("indi_pt")).alias("condition_key")
            )
            .filter(F.col("condition_key").isNotNull() & (F.trim(F.col("condition_key")) != ""))
            .dropDuplicates(["caseid","primaryid","condition_key"])
    )

    outc = (
        spark.table(f"{SILVER_DB}.faers_outc")
            .select(
                F.col("caseid").cast("int").alias("caseid"),
                F.col("primaryid").cast("bigint").alias("primaryid"),
                F.upper(F.trim(F.col("outc_cod"))).alias("outc_cod")
            )
            .filter(F.col("outc_cod").isNotNull() & (F.trim(F.col("outc_cod")) != ""))
            .dropDuplicates(["caseid","primaryid","outc_cod"])
    )

    outc_flags = (
        outc.groupBy("caseid","primaryid").agg(
            F.max(F.when(F.col("outc_cod") == "DE", 1).otherwise(0)).alias("outc_death"),
            F.max(F.when(F.col("outc_cod") == "HO", 1).otherwise(0)).alias("outc_hosp"),
            F.max(F.when(F.col("outc_cod") == "DS", 1).otherwise(0)).alias("outc_disability"),
            F.max(F.when(F.col("outc_cod") == "LT", 1).otherwise(0)).alias("outc_life_threat"),
            F.max(F.when(F.col("outc_cod") == "OT", 1).otherwise(0)).alias("outc_other_serious")
        )
        .withColumn(
            "outc_any_serious",
            (F.col("outc_death") + F.col("outc_hosp") + F.col("outc_disability") +
             F.col("outc_life_threat") + F.col("outc_other_serious") > 0).cast("int")
        )
    )

    base = (
        drug.join(reac, ["caseid","primaryid"], "inner")
            .join(indi, ["caseid","primaryid"], "left")
            .join(demo_latest.select("caseid","primaryid","event_dt","date_key","patient_key"), ["caseid","primaryid"], "left")
            .join(outc_flags, ["caseid","primaryid"], "left")
            .fillna(0, subset=["outc_death","outc_hosp","outc_disability","outc_life_threat","outc_other_serious","outc_any_serious"])
            .join(dim_drug_lu, ["drug_key"], "left")
            .join(dim_reac_lu, ["reaction_key"], "left")
            .join(dim_pat_lu,  ["patient_key"], "left")
            .join(dim_cond_lu, ["condition_key"], "left")
            .withColumn(
                "date_key",
                F.when((F.col("date_key") >= 19000101) & (F.col("date_key") <= 20991231), F.col("date_key"))
                 .otherwise(F.lit(None).cast("int"))
            )
    )

    fact = (
        base.filter(F.col("drug_sk").isNotNull() & F.col("reaction_sk").isNotNull() & F.col("patient_sk").isNotNull())
            .select(
                "caseid","primaryid",
                "drug_sk","reaction_sk","condition_sk","patient_sk",
                "event_dt","date_key",
                "outc_death","outc_hosp","outc_disability","outc_life_threat","outc_other_serious","outc_any_serious",
                F.lit(1).alias("event_count")
            )
            .dropDuplicates(["caseid","primaryid","drug_sk","reaction_sk"])
    )
    return fact

run_gold_table("fact_faers_event", "FAERS", build_fact_faers_event)


# COMMAND ----------


# -----------------------------
# FACT: fact_trial_drug_condition
# -----------------------------
def build_fact_trial_drug_condition():
    dim_study_lu = broadcast(spark.table(f"{GOLD_DB}.dim_study").select("study_id","study_sk").dropDuplicates(["study_id"]))
    dim_drug_lu  = broadcast(spark.table(f"{GOLD_DB}.dim_drug").select("drug_key","drug_sk"))
    dim_cond_lu  = broadcast(spark.table(f"{GOLD_DB}.dim_condition").select("condition_key","condition_sk"))

    trial_drug = (
        spark.table(f"{SILVER_DB}.aact_interventions")
            .select(F.trim(F.col("nct_id")).alias("study_id"),
                    canon(F.col("name")).alias("drug_key"))
            .filter(F.col("study_id").isNotNull() & (F.trim(F.col("study_id")) != ""))
            .filter(F.col("drug_key").isNotNull() & (F.trim(F.col("drug_key")) != ""))
            .dropDuplicates(["study_id","drug_key"])
    )

    trial_condition = (
        spark.table(f"{SILVER_DB}.aact_conditions")
            .select(F.trim(F.col("nct_id")).alias("study_id"),
                    canon(F.col("name")).alias("condition_key"))
            .filter(F.col("study_id").isNotNull() & (F.trim(F.col("study_id")) != ""))
            .filter(F.col("condition_key").isNotNull() & (F.trim(F.col("condition_key")) != ""))
            .dropDuplicates(["study_id","condition_key"])
    )

    fact = (
        trial_drug.join(trial_condition, ["study_id"], "inner")
                  .join(dim_study_lu, ["study_id"], "left")
                  .join(dim_drug_lu,  ["drug_key"], "left")
                  .join(dim_cond_lu,  ["condition_key"], "left")
                  .filter(F.col("study_sk").isNotNull() & F.col("drug_sk").isNotNull() & F.col("condition_sk").isNotNull())
                  .select("study_sk","drug_sk","condition_sk", F.lit(1).alias("trial_mapping_count"))
                  .dropDuplicates(["study_sk","drug_sk","condition_sk"])
    )
    return fact

run_gold_table("fact_trial_drug_condition", "AACT", build_fact_trial_drug_condition)
