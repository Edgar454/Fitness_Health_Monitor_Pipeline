import os
import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

airflow_home = os.environ.get("AIRFLOW_HOME")
expectations_path = Path(airflow_home) / "dags" / "fitness_api_etl" / "data_validation" / "great_expectations"
data_path = Path(airflow_home) / "dags" /"fitness_api_etl" / "data"


def setup_expectations():
    df = pd.read_csv(data_path / 'fitness_data.csv')
    context = gx.get_context(mode="file", project_root_dir= expectations_path)

    datasource = context.data_sources.add_pandas(name="fitness_data_source")


    data_asset = datasource.add_dataframe_asset(name="fitness_dataframe_asset")

    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    # Define expectations
    # Every value in our dataset needs to be positive
    expectation_suite = gx.ExpectationSuite("fitness_data_expectations")
    expectation_suite = context.suites.add(expectation_suite)

    # Table related expectations
    # Le nombre de lignes doit être égal à 7
    dt_rows_expectation = gxe.ExpectTableRowCountToEqual(value=7)
    expectation_suite.add_expectation(
        dt_rows_expectation
    )

    # Les colonnes doivent etre dans un set précis
    column_name_expectation = gxe.ExpectTableColumnsToMatchSet(
        column_set=[
            "timestamp",
            "active_minutes",
            "step_count_delta",
            "calories_expended",
            "heart_rate_bpm",
            "weight",
            "blood_pressure",
            "blood_glucose",
            "body_temperature",
            "distance_delta",
            "height",
            "sleep_segment",
            "nutrition",
        ],
        exact_match=False,
    )
    expectation_suite.add_expectation(
        column_name_expectation
    )

    # Validation des types
    timestamp_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="timestamp", type_="datetime64[ns]")
    expectation_suite.add_expectation(
        timestamp_type_expectation
    )

    active_minutes_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="active_minutes", type_="int")
    expectation_suite.add_expectation(
        active_minutes_type_expectation
    )

    step_count_delta_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="step_count_delta", type_="int")
    expectation_suite.add_expectation(
        step_count_delta_type_expectation
    )

    calories_expended_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="calories_expended", type_="float")
    expectation_suite.add_expectation(
        calories_expended_type_expectation
    )

    heart_rate_bpm_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="heart_rate_bpm", type_="float64")
    expectation_suite.add_expectation(
        heart_rate_bpm_type_expectation
    )

    weight_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="weight", type_="float")
    expectation_suite.add_expectation(
        weight_type_expectation
    )

    blood_pressure_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="blood_pressure", type_="float64")
    expectation_suite.add_expectation(
        blood_pressure_type_expectation
    )

    blood_glucose_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="blood_glucose", type_="float64")
    expectation_suite.add_expectation(
        blood_glucose_type_expectation
    )

    body_temperature_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="body_temperature", type_="float64")
    expectation_suite.add_expectation(
        body_temperature_type_expectation
    )

    distance_delta_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="distance_delta", type_="float")
    expectation_suite.add_expectation(
        distance_delta_type_expectation
    )

    height_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="height", type_="float64")
    expectation_suite.add_expectation(
        height_type_expectation
    )

    sleep_segment_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="sleep_segment", type_="float64")
    expectation_suite.add_expectation(  
        sleep_segment_type_expectation
    )

    nutrition_type_expectation = gxe.ExpectColumnValuesToBeOfType(column="nutrition", type_="float64")
    expectation_suite.add_expectation(
        nutrition_type_expectation
    )



    # Expectation 1 : Le nombre de minutes actives doit être supérieur à 0 et inférieur à 1440(24h)
    active_time_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="active_minutes",
        min_value=0,
        max_value=1440,
    )
    expectation_suite.add_expectation(
        active_time_gt_0_exp,
    )

    # Expectation 2 : Le nombre de pas doit être supérieur à 0 et inférieur à 50000
    step_count_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="step_count_delta",
        min_value=0,
        max_value=50000,
    )
    expectation_suite.add_expectation(
        step_count_gt_0_exp,
    )

    # Expectation 3 : Le nombre de calories doit être supérieur à 0 et inférieur à 5000
    calories_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="calories_expended",
        min_value=0,
        max_value=4000,
    )
    expectation_suite.add_expectation(
        calories_gt_0_exp,
    )

    # Expectation 4 : Le poids doit être supérieur à 0 et inférieur à 200
    weight_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="weight",
        min_value=20,
        max_value=200,
    )
    expectation_suite.add_expectation(
        weight_gt_0_exp,
    )
    # Expectation 5 : La fréquence cardiaque doit être supérieure à 0 et inférieure à 200
    heart_rate_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="heart_rate_bpm",
        min_value=30,
        max_value=200,
    )
    expectation_suite.add_expectation(
        heart_rate_gt_0_exp,
    )

    # Expectation 6 : La température corporelle doit être supérieure à 30 et inférieure à 45
    body_temperature_gt_30_exp = gxe.ExpectColumnValuesToBeBetween(
        column="body_temperature",
        min_value=30,
        max_value=45,
    )
    expectation_suite.add_expectation(
        body_temperature_gt_30_exp,
    )

    # Expectation 7 : La pression artérielle doit être supérieure à 0 et inférieure à 300
    blood_pressure_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="blood_pressure",
        min_value=0,
        max_value=300,
    )
    expectation_suite.add_expectation(
        blood_pressure_gt_0_exp,
    )

    # Expectation 8 : le taille doit être supérieur à 1m et inférieur à 2m
    height_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="height",
        min_value=1,
        max_value=2,
    )
    expectation_suite.add_expectation(
        height_gt_0_exp,
    )

    # Expectation 9 : la distance parcourue doit être supérieur à 0 et inférieur à 100km
    distance_delta_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="distance_delta",
        min_value=0,
        max_value=50000,
    )
    expectation_suite.add_expectation(
        distance_delta_gt_0_exp,
    )

    # Expectation 10 : Le temps de sommeil doit etre inferieur à 24h
    sleep_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="sleep_segment",
        min_value=0,
        max_value=24,
    )

    expectation_suite.add_expectation(
        sleep_gt_0_exp,
    )

    # Expectation 11 : Le taux de glucose doit être supérieur à 0 et inférieur à 100
    blood_glucose_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="blood_glucose",
        min_value=0,
        max_value=100,
    )
    expectation_suite.add_expectation(
        blood_glucose_gt_0_exp,
    )

    # Expectation 12 : La pression sanguine doit être supérieure à 0 et inférieure à 300
    blood_pressure_gt_0_exp = gxe.ExpectColumnValuesToBeBetween(
        column="blood_pressure",
        min_value=60,
        max_value=180,
    )
    expectation_suite.add_expectation(
        blood_pressure_gt_0_exp,
    )

    # Expectation 13: les dates doivent etre distinctes et uniques
    date_uniqueness_expectation = gxe.ExpectColumnValuesToBeUnique(column="timestamp")
    expectation_suite.add_expectation(
        date_uniqueness_expectation
    )

    # Expectation 14: la taille ne change pas subitement alors les valeurs de celle ci ne doivent pas beaucoup varier
    height_variation_expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
        column="height", min_value=0, max_value=3
    )
    expectation_suite.add_expectation(
        height_variation_expectation
    )



if __name__ == "__main__":
    setup_expectations()
    print("Expectation suite created successfully!")
