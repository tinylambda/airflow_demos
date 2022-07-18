import logging
from datetime import datetime, timedelta

from airflow import DAG, macros
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable


def start(ds=None, **kwargs):
    return f"start execution on {ds}"


def end(ds=None, **kwargs):
    return f"End execution on {ds}"


def get_sync_table_names():
    sync_tables: str = Variable.get("script_tables_for_sync", None)
    if sync_tables is not None and sync_tables.strip():
        sync_tables = sync_tables.strip()
        sync_table_names = [item.strip() for item in sync_tables.split()]
    else:
        mh_default = MySqlHook()
        all_tables = mh_default.get_records("show tables in eano")
        sync_table_names = [item[0] for item in all_tables]
    return sync_table_names


def copy_db(ds=None, **kwargs):
    mh_default = MySqlHook()
    mh_test = MySqlHook(mysql_conn_id="mysql_test")

    sync_table_names = get_sync_table_names()
    sync_table_names.insert(0, "xxxxx")

    for table_name in sync_table_names:
        logging.info("processing table: %s", table_name)
        try:
            columns = mh_default.get_records(f"show columns from {table_name}")
        except Exception as e:
            logging.error("catch error", exc_info=e)
            raise e


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

    t1 = PythonOperator(
        task_id="copy_db",
        python_callable=copy_db,
    )

    _start >> t1 >> _end
