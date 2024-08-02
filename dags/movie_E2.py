from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        PythonOperator,
        PythonVirtualenvOperator,
        BranchPythonOperator
    )

# import func
#from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df

with DAG(
    'movie_E2',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2018, 5, 1),
    end_date=datetime(2018, 8, 31),
    catchup=True,
    tags=['movie'],
) as dag:

    def get_data():
        from extract.ice_breaking import pic
        pic()

    def save_data():
        from extract.ice_breaking import pic
        pic()

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["https://github.com/de32-kca/extract.git"],
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        requirements=["https://github.com/de32-kca/extract.git"],
    )

    start >> get_data >> save_data >> end
