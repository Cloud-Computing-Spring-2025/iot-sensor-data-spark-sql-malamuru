# === Task 3: Time-Based Analysis ===

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("Task 3 - Time-Based Analysis").getOrCreate()

# Load data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Convert timestamp to proper format
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Re-register temp view after timestamp update
df.createOrReplaceTempView("sensor_readings")

# Average temperature by hour
hourly_df = spark.sql("""
    SELECT HOUR(timestamp) AS hour_of_day, 
           ROUND(AVG(temperature), 2) AS avg_temp
    FROM sensor_readings
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")

hourly_df.show()
hourly_df.write.csv("task3_output.csv", header=True)