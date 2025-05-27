from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

from fitness_api_etl.get_data import get_fitness_data
from fitness_api_etl.data_validation.ge_setup import setup_expectations
from fitness_api_etl.data_validation.run_validation import validate_fitness_data

import os
from pathlib import Path
from datetime import  timedelta
from pendulum import datetime

# Check if the expectation suite exists, if not create it
airflow_home = os.environ.get("AIRFLOW_HOME")
expectations_path = Path(airflow_home)/"dags"/"fitness_api_etl"/"data_validation"/"great_expectations"

def check_expectation_existence():
    if not expectations_path.exists():
        return 'CreateExpectationSuite'
    else:
        return 'SkipStep'

# Create the DAG
with DAG("fitness_data_monitoring_dag",
            start_date=datetime(2025, 5, 23, tz='local'),
            schedule="@daily",
            catchup=False,
            tags=["fitness","portfolio"],
            default_args={
                "owner": "airflow",
                "start_date": datetime(2025, 5, 23, tz='local'),
                "retries": 3,
                "retry_delay": timedelta(minutes=5),
            },
            ) as dag:
            # Define the tasks in the DAG
            fetch_data_task = PythonOperator(
                task_id="DataFetching",
                python_callable=get_fitness_data,
            )

            check_expectation_existence_task = BranchPythonOperator(
                task_id="CheckExpectationExistence",
                python_callable=check_expectation_existence,
            )

            create_expectation_suite = PythonOperator(
                task_id="CreateExpectationSuite",
                python_callable=setup_expectations,
            )

            skip_step = EmptyOperator(
                task_id="SkipStep",
            )

            
            validate_data_task = PythonOperator(
                task_id="DataValidation",
                python_callable=validate_fitness_data,
                trigger_rule = "none_failed_min_one_success"
            )

            create_table_task = SQLExecuteQueryOperator(
                task_id="create_fitness_table_if_not_exists",
                conn_id="postgres_default",
                sql="""
                    CREATE TABLE IF NOT EXISTS fitness_metrics (
                        timestamp TIMESTAMP,
                        active_minutes INT,
                        step_count_delta INT,
                        calories_expended FLOAT,
                        heart_rate_bpm FLOAT,
                        weight FLOAT,
                        blood_pressure FLOAT,
                        blood_glucose FLOAT,
                        body_temperature FLOAT,
                        distance_delta FLOAT,
                        height FLOAT,
                        sleep_segment FLOAT,
                        nutrition FLOAT,
                        BMI FLOAT,
                        activity_streak INT,
                        objective_streak INT,
                        nutrition_streak INT,
                        nutrition_objective_streak INT,
                        sleep_recorded_streak INT,
                        sleep_objective_streak INT
                    );
                """,
            )


            transform_data_task = SparkSubmitOperator(
                task_id="DataTransformation",
                application= airflow_home + "/dags/fitness_api_etl/transform_and_save.py",
                jars=airflow_home + "/dags/fitness_api_etl/postgresql-42.6.0.jar",
                conn_id="spark_default",
            )

            

            fetch_data_task >> check_expectation_existence_task >> [create_expectation_suite, skip_step] >> validate_data_task >> create_table_task >> transform_data_task


