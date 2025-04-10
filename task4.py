# === Task 4: Window Function ===

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("Task 4 -  Window Function").getOrCreate()

# Load data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Avg temp per sensor
sensor_avg_df = df.groupBy("sensor_id") \
                  .agg(round(avg("temperature"), 2).alias("avg_temp"))

# Define window and rank
window_spec = Window.orderBy(sensor_avg_df["avg_temp"].desc())
ranked_df = sensor_avg_df.withColumn("rank_temp", dense_rank().over(window_spec))

# Top 5 sensors
top5_df = ranked_df.limit(5)
top5_df.show()
top5_df.write.csv("task4_output.csv", header=True)
