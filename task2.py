
# === Task 2: Filtering & Simple Aggregations ===
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# Initialize Spark
spark = SparkSession.builder.appName("Task 2 - Filtering and Aggregation").getOrCreate()

# Load data
df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

# Create Temp View
df.createOrReplaceTempView("sensor_readings")

# Filter and count in/out-of-range
spark.sql("""
    SELECT 
        CASE 
            WHEN temperature < 18 OR temperature > 30 THEN 'out_of_range' 
            ELSE 'in_range' 
        END AS temp_status
    FROM sensor_readings
""").groupBy("temp_status").count().show()

# Aggregate avg temp/humidity per location
agg_df = spark.sql("""
    SELECT location, 
           ROUND(AVG(temperature), 1) AS avg_temperature, 
           ROUND(AVG(humidity), 1) AS avg_humidity
    FROM sensor_readings
    GROUP BY location
    ORDER BY avg_temperature DESC
""")

# Show and save
agg_df.show()
agg_df.write.csv("task2_output.csv", header=True)
