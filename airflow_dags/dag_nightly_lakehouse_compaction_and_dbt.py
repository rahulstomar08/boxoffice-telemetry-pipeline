from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_nightly_lakehouse_compaction_and_dbt",
    default_args=default_args,
    description="Nightly Delta Lake compaction on MinIO followed by dbt build and tests",
    schedule_interval="45 23 * * *",  # 11:45 PM daily
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["boxoffice", "lakehouse", "dbt"],
) as dag:

    run_spark_compaction = BashOperator(
        task_id="run_spark_compaction",
        bash_command=(
            "docker exec spark_processor /opt/spark/bin/spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.12:3.1.0 "
            "/opt/spark/work-dir/processing/nightly_compaction.py"
        ),
    )

    run_dbt_build = BashOperator(
            task_id="run_dbt_build",
            bash_command=(
                "export PYTHONPATH=/home/airflow/.local/lib/python3.11/site-packages:$PYTHONPATH && "
                "cd /opt/airflow/boxoffice_warehouse && "
                "dbt build --profiles-dir /opt/airflow/dbt_profiles"
            ),
        )

    run_spark_compaction >> run_dbt_build