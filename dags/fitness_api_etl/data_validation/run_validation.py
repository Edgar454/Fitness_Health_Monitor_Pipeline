# run_validation.py
import os
import great_expectations as gx
import pandas as pd
from pathlib import Path
from datetime import datetime

from pathlib import Path

airflow_home = os.environ.get("AIRFLOW_HOME")
expectations_path = Path(airflow_home) / "dags" / "fitness_api_etl" / "data_validation" / "great_expectations"
data_path = Path(airflow_home) / "dags" /"fitness_api_etl" / "data"

def validate_fitness_data():
    context = gx.get_context(mode="file", project_root_dir=expectations_path)

    # Load existing suite
    expectation_suite = context.suites.get(name = "fitness_data_expectations")

    batch_definition = (
    context.data_sources.get("fitness_data_source")
    .get_asset("fitness_dataframe_asset")
    .get_batch_definition("batch definition")
)

    # Run validation
    df = pd.read_csv(data_path / 'fitness_data.csv',parse_dates=["timestamp"])

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    print("Batch loaded successfully.")

    results = batch.validate(expectation_suite)
    
    if results["success"]:
        print("All expectations passed.")
    else:
        for result in results["results"]:
            if not result["success"]:
                print(f"Column: {result['expectation_config']['kwargs']['column']}")
                print(f"Expectation failed: {result['expectation_config']['type']}")
                print(f"Details: {result['result']}")
        raise ValueError("Some expectations failed.")

if __name__ == "__main__":
    validate_fitness_data()
    print("Validation passed!")
