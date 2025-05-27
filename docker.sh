echo "export AIRFLOW_HOME=/opt/airflow/dags" >> ~/.bashrc
source ~/.bashrc

cd ${AIRFLOW_HOME}
docker compose up -d airflow-init
docker compose up -d
