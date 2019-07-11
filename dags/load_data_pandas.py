"""
"""

import pandas as pd

import airflow
import requests
from airflow.models import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator

args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id="load_data_into_pandas",
    default_args=args,
    schedule_interval= None
)


def collect_data_operator(ds, **kwargs):
    pghook = PostgresHook(postgres_conn_id="postgres_AO_Test")
    sql = "select * from batches;"
    df = pd.read_sql(sql, pghook.get_conn())
    df.to_csv("sometestcsv.csv")
    return None

collect_data = PythonOperator(
    task_id="collect_data",
    python_callable=collect_data_operator,
    provide_context=True,
    dag=dag
)


# [START save_data_operator]

# [END collect_data_operator]

# process_data >> save_data

if __name__ == "__main__":
    dag.cli()