# iot-sensor-data-spark-sql

## **README – IoT Sensor Data Analysis Using Apache Spark SQL**

### **Overview**

This assignment focuses on analyzing IoT sensor data using Apache Spark SQL. The goal is to process environmental sensor readings (temperature, humidity, timestamp, and location), perform data exploration, filtering, aggregation, time-based analysis, ranking, and pivoting.

---

##  **Prerequisites**

Before running this project, ensure the following tools are installed:

1. **Python 3.x**  
   ```bash
   python --version
   ```

2. **PySpark**  
   ```bash
   pip install pyspark
   ```

---

## **Project Structure**

| File Name     | Purpose                                        |
|---------------|------------------------------------------------|
| `task1.py`    | Load data, explore schema, basic queries       |
| `task2.py`    | Filter temperatures and aggregate by location  |
| `task3.py`    | Perform time-based hourly analysis             |
| `task4.py`    | Use window functions to rank sensors           |
| `task5.py`    | Create pivot table by location and hour        |
| `sensor_data.csv` | Input IoT sensor dataset                  |
| `task1_output.csv/` | Output folders for CSV results (1)    |
| `task2_output.csv/` | Output folders for CSV results (2)    |
| `task3_output.csv/` | Output folders for CSV results (3)    |
| `task4_output.csv/` | Output folders for CSV results (4)    |
| `task5_output.csv/` | Output folders for CSV results (5)    |

---

##  **Running Each Task**

```bash
python task1.py
python task2.py
python task3.py
python task4.py
python task5.py
```

Each script reads from `sensor_data.csv`, processes it using Spark SQL, and writes output to a corresponding CSV folder (e.g., `task1_output.csv/`).

---

## **Dataset Schema**

| Field        | Type      | Description                           |
|--------------|-----------|---------------------------------------|
| sensor_id    | Integer   | Unique ID for each sensor             |
| timestamp    | Timestamp | Time of the reading                   |
| temperature  | Float     | Temperature in °C                     |
| humidity     | Float     | Relative humidity percentage          |
| location     | String    | Location of the sensor (building/floor)|
| sensor_type  | String    | Type/category of the sensor           |

---

## **Task Summaries**

---

### **Task 1 – Load & Basic Exploration**

- Load `sensor_data.csv` into Spark.
- Shows first 5 records, count total entries.
- Display distinct sensor locations.
- Output: `task1_output.csv/`

**Output**
```
|sensor_id   |timestamp               |temperature|humidity|location        |sensor_type|
|------------|------------------------|-----------|--------|----------------|-----------|
|1004        |2025-04-07T15:29:58.000Z|26.47      |74.36   |BuildingA_Floor2|TypeB      |
|1013        |2025-04-08T23:09:32.000Z|29.86      |79.28   |BuildingB_Floor1|TypeB      |
|1016        |2025-04-09T03:19:18.000Z|25.43      |77.14   |BuildingB_Floor1|TypeC      |
|1038        |2025-04-07T01:45:41.000Z|22.21      |79.22   |BuildingA_Floor2|TypeB      |
|1099        |2025-04-07T12:40:47.000Z|16.03      |34.13   |BuildingB_Floor1|TypeA      |
```
---

### **Task 2 – Filtering & Aggregation**

- Filter out temperatures <18 or >30 and count valid vs. out-of-range.
- Group by location to compute average temperature & humidity.
- Output: `task2_output.csv/`
  
## **Output**
```
|location         |avg_temperature|avg_humidity|
|-----------------|---------------|------------|
|BuildingB_Floor2 |25.3           |54.5        |
|BuildingA_Floor1 |25.1           |54.7        |
|BuildingA_Floor1 |24.9           |54.8        |
|BuildingB_Floor2 |24.7           |55.8        |


```
---

### **Task 3 – Time-Based Analysis**

- Convert string timestamps to `timestamp` type.
- Extract the hour and compute average temperature per hour.
- Identify the hottest hour.
- Output: `task3_output.csv/`

---

### **Task 4 – Window Functions**

- Compute average temperature per sensor.
- Rank sensors using `DENSE_RANK` based on descending average temperature.
- Output: `task4_output.csv/`

---

### **Task 5 – Pivot & Interpretation**

- Pivot table: rows = location, columns = hour (0–23), values = avg temperature.
- Determine which (location, hour) has highest temperature.
- Output: `task5_output.csv/`

---

## 🧠 **Skills Practiced**

- CSV ingestion and schema inference in Spark
- SQL-based filtering and aggregation
- Time-based queries using `timestamp` and `hour()`
- Window functions like `DENSE_RANK`
- Creating pivot tables with Spark SQL

---

## ✅ **Conclusion**

This project provides hands-on experience in processing structured IoT sensor data using Apache Spark SQL. It simulates real-world tasks in data engineering and analytics—ideal for working with time-series environmental data in smart buildings or industrial IoT systems.

---

Would you like this saved as a `README.md` file for direct use in your repo?
