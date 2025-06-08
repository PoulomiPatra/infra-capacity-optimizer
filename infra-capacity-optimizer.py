# Databricks notebook source
# MAGIC %md
# MAGIC ### STARTING...

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flattening JSON data file and create RAW table for Bronze Layer

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import json

# Step 1: Read the JSON file directly
df = spark.read.option("multiline", "true").json("/FileStore/tables/infra_health.json")

# Step 2: Flatten nested structure dynamically
def flatten_df(nested_df):
    flat_cols = []
    nested_cols = []
    
    for column in nested_df.schema.fields:
        if isinstance(column.dataType, StructType):
            nested_cols.append(column.name)
        else:
            flat_cols.append(column.name)
    
    flat_df = nested_df.select(
        *[col(c) for c in flat_cols] +
        [col(n + "." + f.name).alias(n + "_" + f.name)
         for n in nested_cols
         for f in nested_df.select(n + ".*").schema.fields]
    )
    
    # Recurse if further nesting remains
    if any(isinstance(f.dataType, StructType) for f in flat_df.schema.fields):
        return flatten_df(flat_df)
    else:
        return flat_df

flat_df = flatten_df(df)

# Step 3: Show the flattened data
#flat_df.show(truncate=False)


# COMMAND ----------

flat_df.write.format('delta').mode("overwrite").partitionBy('location').saveAsTable('infra_health_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Curated table for Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC - Drop any Duplicate records if available
# MAGIC - Add derived Columns:
# MAGIC 1. Free CPU percentage
# MAGIC 2. Free Memory Percentage
# MAGIC 3. Free Storage percentage
# MAGIC - Rename existing columns with more user friendly ways

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, round, when

cur_df= spark.read.table ("infra_health_raw")
cur_df= cur_df.dropDuplicates()
new_df= cur_df.select(
    to_timestamp('timestamp').alias('event_timestamp'),
    'location',
    'server_id',
    'uptime_days',
	col('metrics_cpu_cores_used').alias('used_cpu_cores'),
    col('metrics_cpu_temperature_celsius').alias('cpu_temp'),
    'alerts_cpu_threshold_breach',
    'status',
    col('metrics_cpu_usage_percent').alias('cpu_usage'),
    round(100-col('metrics_cpu_usage_percent'),3).alias('cpu_free_perct'),
    'alerts_memory_threshold_breach',
    col('metrics_memory_total_gb').alias('total_memory_gb'),
    col('metrics_memory_used_gb').alias('used_memory_gb'),
    round(col('metrics_memory_total_gb')-col('metrics_memory_used_gb'),3).alias('free_memory_gb'),
    col('metrics_memory_usage_percent').alias('memory_usage'),
    'alerts_storage_threshold_breach',
    col('metrics_storage_total_gb').alias('total_storage_gb'),
    col('metrics_storage_used_gb').alias('used_storage_gb'),
    round(col('metrics_storage_total_gb')- col('metrics_storage_used_gb'),3).alias('free_storage_gb'),
    col('metrics_storage_usage_percent').alias('storage_usage'),
    col('metrics_storage_read_iops').alias('read_ios'),
    col('metrics_storage_write_iops').alias('write_ios')           
)

risky_df= new_df.withColumn(
    'risk_level',
    when(
            (col('cpu_usage') >=90) |
            (col('memory_usage')>= 80) |
            (col('storage_usage')>=80),
            "High"
        ).when(
           (col('cpu_usage')>70) & (col('cpu_usage')<90) |
           (col('memory_usage')>65) & (col('memory_usage')<80) |
           (col('storage_usage')>65) & (col('storage_usage')<80),
           "Moderate"
        ).when(
            (col('cpu_usage') <=30) |
            (col('memory_usage')<= 30) |
            (col('storage_usage')<=30),
            "Low"
        ).otherwise("Normal")
        
)

risky_df.write.format('delta').mode('overwrite').saveAsTable('infra_health_curated')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Consumption table for Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC | Gold Output Table / View           | Description                                        |
# MAGIC | ---------------------------------- | -------------------------------------------------- |
# MAGIC | `infra_daily_summary`              | Daily average usage and alert counts per location  |
# MAGIC | `high_risk_servers`                | List of servers tagged as `High` risk with metrics |
# MAGIC | `resource_alert_summary`           | Aggregated count of alerts per resource type       |
# MAGIC | `server_risk_trends`               | Time-series view of risk levels per server         |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Infra Daily Summary

# COMMAND ----------

from pyspark.sql.functions import date_trunc, col, count, avg, round, date_format

inf_df= spark.read.table('infra_health_curated')
inf_df= inf_df.groupBy(
  'location', 
  #date_format(date_trunc('day','event_timestamp'), 'YYYY-MM-DD').alias('day')
  date_format(date_trunc("day", "event_timestamp"), "yyyy-MM-dd").alias("day")
  ).agg(
  round(avg(col('cpu_usage')),3).alias('avg_cpu_usage'),
  round(avg(col('memory_usage')),3).alias('avg_memory_usage'),
  round(avg(col('storage_usage')),3).alias('avg_storage_usage'),
  count(when(col("alerts_cpu_threshold_breach") == True, 1)).alias("cpu_alerts_count"),
  count(when(col("alerts_memory_threshold_breach") == True, 1)).alias("memory_alerts_count"),
  count(when(col("alerts_storage_threshold_breach") == True, 1)).alias("storage_alerts_count")
)
inf_df.write.format('delta').mode('overwrite').saveAsTable('infra_daily_summary')

# COMMAND ----------

# MAGIC %md
# MAGIC high_risk_servers

# COMMAND ----------

df= spark.read.table('infra_health_curated')
df= df.filter(col('risk_level')=='High')
risky_df= df.select(
    date_format(date_trunc('day','event_timestamp'), 'yyyy-MM-dd').alias('day'),
    'location',
    'server_id',
    'cpu_usage',
    'memory_usage',
    'storage_usage',
    'risk_level',
    'cpu_temp',
    'status'
)
risky_df.write.format('delta').mode('overwrite').saveAsTable('high_risk_servers')

# COMMAND ----------

# MAGIC %md
# MAGIC resource_alert_summary

# COMMAND ----------

df= spark.read.table('infra_health_curated')
summary_df = df.groupBy("location").agg(
    count(when(col("alerts_cpu_threshold_breach") == True, 1)).alias("cpu_alerts_count"),
    count(when(col("alerts_memory_threshold_breach") == True, 1)).alias("memory_alerts_count"),
    count(when(col("alerts_storage_threshold_breach") == True, 1)).alias("storage_alerts_count")
)

summary_df.write.format("delta").mode("overwrite").saveAsTable("resource_alert_summary")


# COMMAND ----------

# MAGIC %md
# MAGIC server_risk_trends

# COMMAND ----------

df= spark.read.table('infra_health_curated')
trend_df = df.groupBy("server_id", "location", "risk_level").count()

trend_df.write.format("delta").mode("overwrite").saveAsTable("server_risk_trends")


# COMMAND ----------

