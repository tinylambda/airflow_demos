import logging
from datetime import datetime, timedelta

from airflow import DAG, macros
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


def start(ds=None, **kwargs):
    return f"start execution on {ds}"


def end(ds=None, **kwargs):
    return f"End execution on {ds}"


def copy_db(ds=None, **kwargs):
    mh_default = MySqlHook()
    mh_test = MySqlHook(mysql_conn_id="mysql_test")

    tables = mh_default.get_records("show tables")
    logging.info("tables: %s", tables)


with DAG(
    "copy_db",
    default_args={},
    description="copy one db to another db",
    start_date=datetime(2022, 7, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["db", "copy"],
) as dag:
    _start = PythonOperator(
        task_id="start",
        python_callable=start,
    )

    _end = PythonOperator(
        task_id="end",
        python_callable=end,
    )

    # t1 = PythonOperator(
    #     task_id="copy_db",
    #     python_callable=copy_db,
    # )

    # _start >> t1 >> _end
    _start >> _end
