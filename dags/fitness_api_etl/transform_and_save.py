"""The role of this jobs spark is to calculate aggregated metrics from the fitness data such as the activity streak ,
 the BMI and other aggregations"""

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, when , current_date

import os
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import to_date, lit

airflow_home = os.environ.get("AIRFLOW_HOME")
data_path = airflow_home + "/dags/fitness_api_etl/data"

def run_spark_job():

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(data_path + "/fitness_data.csv", header=True, inferSchema=True, nullValue="")

    # Add BMI column
    df = df.withColumn("BMI", when(col("height") > 0, col("weight") / col("height")**2))

    # Add an activity streak
    ## Define a window ordered by date (or any time column)
    window_spec = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)

    ## Add a binary flag: 1 if active, 0 otherwise
    df = df.withColumn("was_active", when(col("active_minutes") > 30, 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("activity_streak", sum("was_active").over(window_spec))

    # Step Objective streak
    ## Add a binary flag: 1 if active, 0 otherwise
    df = df.withColumn("reached_step_objective", when(col("step_count_delta") > 10000, 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("objective_streak", sum("reached_step_objective").over(window_spec))

    # Nutrition record streak
    ## Add a binary flag: 1 if nutrition was recorded, 0 otherwise
    df = df.withColumn("recorded_nutrition", when(col("nutrition").isNotNull(), 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("nutrition_streak", sum("recorded_nutrition").over(window_spec))

    # Nutrition Objective streak
    ## Add a binary flag: 1 if ate more than 2000 kcal, 0 otherwise
    df = df.withColumn("reached_nutrition_objective", when(col("nutrition") > 2000, 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("nutrition_objective_streak", sum("reached_nutrition_objective").over(window_spec))

    # Sleep recorded objective streak
    ## Add a binary flag: 1 if sleep recorded else 0
    df = df.withColumn("recorded_sleep", when(col("sleep_segment").isNotNull(), 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("sleep_recorded_streak", sum("recorded_sleep").over(window_spec))

    # Sleep Objective streak
    ## Add a binary flag: 1 if sleep recorded else 0
    df = df.withColumn("reached_sleep_objective", when(col("sleep_segment") > 8, 1).otherwise(0))

    ## Cumulative sum to track how many active days so far
    df = df.withColumn("sleep_objective_streak", sum("reached_sleep_objective").over(window_spec))



    # PostgreSQL connection parameters
    jdbc_url = "jdbc:postgresql://user-postgres:5432/postgres?stringtype=unspecified"
    connection_properties = {
        "user": os.environ.get("USER_POSTGRES_USER"),
        "password": os.environ.get("USER_POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    latest_date = datetime.utcnow().date() - timedelta(days=1)

    # Compare only the date part of timestamp
    latest_df = df.filter(to_date(col("timestamp")) == lit(str(latest_date)))


    columns_to_write = [
    "timestamp", "active_minutes", "step_count_delta", "calories_expended",
    "heart_rate_bpm", "weight", "blood_pressure", "blood_glucose",
    "body_temperature", "distance_delta", "height", "sleep_segment",
    "nutrition", "BMI", "activity_streak", "objective_streak",
    "nutrition_streak", "nutrition_objective_streak",
    "sleep_recorded_streak", "sleep_objective_streak"
    ]

    latest_df.select(columns_to_write).write.mode("append").jdbc(
        url=jdbc_url, table="fitness_metrics", properties=connection_properties
    )


    spark.stop()

if __name__ == "__main__":
    run_spark_job()
    print("Spark job success")
