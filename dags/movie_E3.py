from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)


with DAG(
    'movie_E3',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=3,
    description='movie_E3',
    schedule="10 2 * * *",
    start_date=datetime(2018, 9, 1),
    end_date=datetime(2019, 12, 1),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    def get_data(ds_nodash, url_param={}):
        from extract.extract import save2df
        from extract.ice_breaking import pic

        pic()

        df=save2df(ds_nodash, url_param)
        df.to_parquet('~/code/de32-kca/data_kca', partition_cols=['load_dt', 'repNationCd'])



    get_data_origin = PythonVirtualenvOperator(
        task_id='get.data.origin',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git"]
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/code/de32-kca/data_kca/load_dt={{ ds_nodash }}',
    )

    get_data_nationK = PythonVirtualenvOperator(
        task_id='get.data.nation',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git"],
        op_kwargs={
            "url_param" : { "repNationCd": "K" }
        }
    )

    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> rm_dir >> get_data_origin >> end
    start >> rm_dir >> get_data_nationK >> end
