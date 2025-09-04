# Databricks notebook or job: Ingesta y Calidad -> Bronze/Silver con bitácora
# Databricks: %run compatible (.py)
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable
import yaml, os, time
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# --- Config ---
base_dir = os.path.dirname(os.path.dirname(__file__))
with open(os.path.join(base_dir, "config", "params.yaml"), "r") as f:
    params = yaml.safe_load(f)

with open(os.path.join(base_dir, "config", "mapping.yaml"), "r") as f:
    mapping = yaml.safe_load(f)["canonical_to_source"]

p = params["paths"]
q = params["quality_rules"]

def log_event(message, level="INFO", counts=None, extras=None):
    schema = T.StructType([
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("level", T.StringType(), False),
        T.StructField("message", T.StringType(), False),
        T.StructField("counts", T.StringType(), True),
        T.StructField("extras", T.StringType(), True),
    ])
    df = spark.createDataFrame(
        [(datetime.utcnow(), level, message, str(counts) if counts else None, str(extras) if extras else None)],
        schema=schema
    )
    df.write.format("delta").mode("append").save(p["dq_log_table"].replace("delta.`", "").replace("`",""))

# Ensure log table exists
spark.createDataFrame([], "event_ts timestamp, level string, message string, counts string, extras string") \
    .write.format("delta").mode("append").save(p["dq_log_table"].replace("delta.`", "").replace("`",""))

# --- Ingest raw (Bronze) ---
bronze_path = p["bronze_table"].replace('delta.`','').replace('`','')
raw_csv = p["raw_csv"]

df_raw = spark.read.option("header", True).option("inferSchema", True).csv(raw_csv)

# Rename to canonical
rename_exprs = [F.col(mapping.get(k, k)).alias(k) for k in ["bet_id","user_id","timestamp","amount","ip_address","location","label"] if mapping.get(k, None)]
df = df_raw.select(*rename_exprs)

# Casts
df = df.withColumn("amount", F.col("amount").cast("double")) \
       .withColumn("timestamp", F.to_timestamp("timestamp"))

df.write.format("delta").mode("overwrite").save(bronze_path)
log_event("Bronze written", counts={"rows": df.count()})

# --- Quality to Silver ---
silver_path = p["silver_table"].replace('delta.`','').replace('`','')

# Required columns present?
missing = [c for c in q["required_columns"] if c not in df.columns]
if missing:
    log_event(f"Missing required columns: {missing}", level="ERROR")
    raise ValueError(f"Missing required columns: {missing}")

# Drop duplicates by key
before = df.count()
df = df.dropDuplicates([q["require_unique_by"]])
after = df.count()
log_event("Deduplicated", counts={"before": before, "after": after, "removed": before - after})

# Null handling: drop rows with nulls in required columns
before = df.count()
for c in q["required_columns"]:
    df = df.filter(F.col(c).isNotNull())
after = df.count()
log_event("Dropped rows with nulls in required columns", counts={"before": before, "after": after, "removed": before - after})

# Range validation for amount
min_v = float(q["amount_min"]); max_v = float(q["amount_max"])
df_valid = df.filter((F.col("amount") >= min_v) & (F.col("amount") <= max_v))
df_invalid = df.exceptAll(df_valid)

if df_invalid.count() > 0:
    # Log invalid rows count and sample
    sample = df_invalid.limit(5).toJSON().collect()
    log_event("Out-of-range amounts found", level="WARN", counts={"invalid_rows": df_invalid.count()}, extras={"sample": sample})

df_valid.write.format("delta").mode("overwrite").save(silver_path)
log_event("Silver written", counts={"rows": df_valid.count()})

print("Ingest & Quality completed.")
