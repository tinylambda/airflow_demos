import logging

from airflow import DAG, macros
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


def copy_db(ds=None, **kwargs):
    mh_default = MySqlHook()
    mh_test = MySqlHook(mysql_conn_id="mysql_test")

    tables = mh_default.get_records("show tables")
    logging.info("tables: %s", tables)


with DAG(
    "copy_db",
    default_args={},
    description="copy one db to another db",
    schedule_interval="@once",
    catchup=False,
    tags=["db", "copy"],
) as dag:
    t1 = PythonOperator(
        task_id="copy_db",
        python_callable=copy_db,
    )

    t1
