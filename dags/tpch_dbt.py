from airflow import DAG
from datetime import datetime
from dbt.dbt_operator import DbtOperator


dag_args = {
    'max_active_runs': 1,
    'schedule_interval': '0 7 * * *', 
    'catchup': False,
    'start_date': datetime(year=2020, month=9, day=1)
}


with DAG(
        "dbt.tpch",
        default_args=dag_args,
        catchup=False,
        max_active_runs=1
) as dag:
    database = 'demo_db'
    # TODO: create connection in your Airflow
    conn_id = 'snowflake_demo' 

    dbt_complire_task = DbtOperator(
        task_id="dbt_compile",
        snowflake_connection_id=conn_id,
        snowflake_database=database,
        models=["ods"],
        dbt_command=['compile'],
    )

    dbt_run_task = DbtOperator(
        task_id="dbt_run",
        snowflake_connection_id=conn_id,
        snowflake_database=database,
        threads=3,
        models=["ods"],
    )

    test_task = DbtOperator(
        task_id="dbt_test",
        snowflake_connection_id=conn_id,
        snowflake_database=database,
        models=["ods"],
        dbt_command=['test'],
        retries=1
    )
    dbt_complire_task >> dbt_run_task >> test_task
