"""
"""

import csv
import pickle
from pprint import pprint

import airflow
import requests
from airflow.models import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator
from bs4 import BeautifulSoup


args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id="load_data_into_csv",
    default_args=args,
    schedule_interval="*/3 * * * *"
)


# [START collect_data_operator]
def collect_data_operator(ds, **kwargs):
    # http://mfd.gov.np/
    data = requests.get("http://mfd.gov.np/")
    # print(data, dir(data), data.text)
    soup = BeautifulSoup(data.text)
    div = soup.find("div", attrs={"class": "weather-data-table"})
    with open("collected_data.html", "w") as fp:
        fp.write(str(div))
    return None


collect_data = PythonOperator(
    task_id="collect_data",
    provide_context=True,
    python_callable=collect_data_operator,
    dag=dag
)
# [END collect_data_operator]


# [START process_data_operator]
def process_data_operator(ds, **kwargs):
    rows = []
    with open("collected_data.html", "r") as fp:
        soup = BeautifulSoup(fp.read())
        for tr in soup.find("table").find_all("tr"):
            tds = tr.find_all("td")
            if not tds or len(tds) < 4:
                continue
            _data = {
                "station": tds[0].string,
                "maximum": tds[1].string,
                "minimum": tds[2].string,
                "rainfall": tds[3].string
            }
            rows.append(_data)
    with open("collected_data.csv", "w") as fp:
        csvfile = csv.DictWriter(fp, fieldnames=[
            "station", "maximum", "minimum", "rainfall"
        ])
        csvfile.writeheader()
        csvfile.writerows(rows)
    return None


process_data = PythonOperator(
    task_id="process_data",
    provide_context=True,
    python_callable=process_data_operator,
    dag=dag
)
# [END collect_data_operator]


collect_data >> process_data


# save_data = PostgresOperator(
#     sql=""
# )


# [START save_data_operator]
def save_data_operator(ds, **kwargs):
    pghook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """INSERT INTO 
            weather(station, maximum, minimum, rainfall)
            VALUES(%s, %s, %s, %s);"""
    with open("collected_data.csv", "r") as fp:
        csvfile = csv.DictReader(fp, fieldnames=[
            "station", "maximum", "minimum", "rainfall"
        ])
        line = next(csvfile)
        for line in csvfile:
            print(line)
            pghook.run(sql, parameters=(
                line["station"],
                line["maximum"],
                line["minimum"],
                line["rainfall"]
            ))
    return None


save_data = PythonOperator(
    task_id="save_data",
    provide_context=True,
    python_callable=save_data_operator,
    dag=dag
)
# [END collect_data_operator]

process_data >> save_data

if __name__ == "__main__":
    dag.cli()