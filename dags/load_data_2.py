"""
"""


import airflow

from airflow.models import DAG

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator


args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id="load_data_2",
    default_args=args,
    schedule_interval=None
)

data_sources = Variable.get("data_sources")



def process_data_operator(ds, **kwargs):
    pass


process_data = PythonOperator(
    task_id="process_data",
    python_callable=process_data_operator,
    provide_context=True,
    dag=dag
)


for conn_id in data_sources.split(","):
    conn_id = conn_id.strip()
    if not conn_id:
        continue
    filename = "/tmp/" + conn_id + "_data.csv"
    task = PostgresOperator(
        task_id="collect_from_" + conn_id,
        sql="COPY batches TO '" + filename + "' DELIMITER ',' CSV HEADER;",
        postgres_conn_id=conn_id,
        dag=dag
    )

    task >> process_data


if __name__ == "__main__":
    dag.cli()
