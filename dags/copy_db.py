import contextlib
import logging
from datetime import datetime, timedelta

from MySQLdb import ProgrammingError
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


def try_wait_fetch(cursor):
    try:
        cursor.fetchall()
    except Exception as e:
        logging.info("IGNORE this exception", exc_info=e)


def copy_db(ds=None, **kwargs):
    mh_default = MySqlHook()
    mh_test = MySqlHook(mysql_conn_id="mysql_test")

    copy_batch = Variable.get("script_tables_sync_batch_size", "1000")
    script_tables_sync_batch_size = int(copy_batch)
    logging.info("sync tables sync batch size: %s", script_tables_sync_batch_size)
    sync_table_names = get_sync_table_names()

    for table_name in sync_table_names:
        logging.info("processing table: %s", table_name)
        try:
            create_table = mh_default.get_records(f"show create table {table_name}")
            create_table_sql = create_table[0][1]
            logging.info("create table sql: %s", create_table_sql)

            with contextlib.closing(
                mh_default.get_cursor()
            ) as from_cursor, contextlib.closing(mh_test.get_cursor()) as to_cursor:
                # drop table
                to_cursor.execute(f"drop table if exists {table_name}")
                to_cursor.execute(create_table)
                try_wait_fetch(cursor=to_cursor)

            columns = mh_default.get_records(f"show columns from {table_name}")
            logging.info("columns: %s", columns)
        except ProgrammingError as e:
            error_code = e.args[0]
            # table not exists
            if error_code == 1146:
                logging.error("table %s not exists skip it!", table_name, exc_info=e)
            else:
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
