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
    end_date=datetime(2018, 5, 3),
    catchup=True,
    tags=['2018', 'movie', 'extract'],
) as dag:

    def get_data(ds_nodash, url_param={"":""}):
        from extract.extract import save2df
        df = save2df(ds_nodash, url_param)

    def save_data():
        from extract.ice_breaking import pic
        pic()

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule="all_done")

    get_data_origin = PythonVirtualenvOperator(
        task_id='get.data.origin',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"]
    )

    get_data_nationK = PythonVirtualenvOperator(
        task_id='get.data.nationK',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
        op_kwargs={
            "url_param" : { "repNationCd": "K" }
        }
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git@release/d1.0.0"],
    )

    start >> get_data_origin >> save_data >> end
    start >> get_data_nationK >> save_data >> end
