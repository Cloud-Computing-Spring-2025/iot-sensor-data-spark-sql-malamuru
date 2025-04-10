# === Task 1: Load & Basic Exploration ===

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("IoT Sensor Data").getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create initial temp view
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
df.show(5)

# Total number of records
spark.sql("SELECT COUNT(*) AS total_records FROM sensor_readings").show()

# Distinct locations
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

# Save output
df.write.csv("task1_output.csv", header=True)