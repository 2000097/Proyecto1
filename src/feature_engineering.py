# Databricks notebook or job: Feature Engineering
from pyspark.sql import SparkSession, functions as F, types as T
from datetime import timedelta
import yaml, os

spark = SparkSession.builder.getOrCreate()

base_dir = os.path.dirname(os.path.dirname(__file__))
with open(os.path.join(base_dir, "config", "params.yaml"), "r") as f:
    params = yaml.safe_load(f)

p = params["paths"]
feat = params["features"]

silver = spark.read.format("delta").load(p["silver_table"].replace('delta.`','').replace('`',''))

# Window for 7-day features
w_user_7d = (
    F.window("timestamp", f"{feat['lookback_days']} days").alias("w")
)

# We will compute per user_id, by week_start
df = silver.withColumn("week_start", F.date_trunc("week", F.col("timestamp")))

features = (
    df.groupBy("user_id", "week_start")
      .agg(
          F.avg("amount").alias("avg_amount_7d"),
          F.count(F.lit(1)).alias("bets_7d")
      )
)

# IP risk score: share of past fraud by IP over last 30 days (if label exists)
if "label" in silver.columns:
    ip_risk = (
        silver.withColumn("day", F.to_date("timestamp"))
              .groupBy("ip_address")
              .agg(F.avg(F.col("label").cast("double")).alias("ip_fraud_rate_30d"))
    )
    features = features.join(ip_risk, on=[], how="cross") if "ip_address" not in features.columns else features

# Persist features
features_path = p["features_table"].replace('delta.`','').replace('`','')
features.write.format("delta").mode("overwrite").save(features_path)

print("Features computed and stored.")
