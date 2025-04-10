# === Task 5: Pivot & Interpretation ===

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("Task 5 - Pivot & Interpretation").getOrCreate()

# Load data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)


# Add hour_of_day column
df = df.withColumn("hour_of_day", hour("timestamp"))

# Re-register temp view after adding hour_of_day
df.createOrReplaceTempView("sensor_readings")

# Pivot table
pivot_df = df.groupBy("location") \
    .pivot("hour_of_day", list(range(24))) \
    .agg(round(avg("temperature"), 1))

pivot_df.show()
pivot_df.write.csv("task5_output.csv", header=True)

# Find max temp across (location, hour)
pivot_df.createOrReplaceTempView("pivot_table")
spark.sql("""
    SELECT location, stack(24,
        '0', `0`, '1', `1`, '2', `2`, '3', `3`, '4', `4`, '5', `5`, 
        '6', `6`, '7', `7`, '8', `8`, '9', `9`, '10', `10`, '11', `11`,
        '12', `12`, '13', `13`, '14', `14`, '15', `15`, '16', `16`, '17', `17`, 
        '18', `18`, '19', `19`, '20', `20`, '21', `21`, '22', `22`, '23', `23`)
        AS (hour_of_day, avg_temp)
    ORDER BY avg_temp DESC
    LIMIT 1
""").show()
