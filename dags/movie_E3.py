from datetime import datetime, timedelta
from textwrap import dedent

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
        df.to_parquet('~/code/de32-kca/extract_kca/', partition_cols=['load_dt', 'repNationCd'])

    def chk_exist(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        target_path=f"{home_dir}/code/de32-kca/extract_kca/load_dt={ds_nodash}"

        print(os.path.exists(target_path), target_path)

        if os.path.exists(target_path):
            return "rm.dir"
        else:
            return "get.data.origin","get.data.nation"


    get_data_origin = PythonVirtualenvOperator(
        task_id='get.data.origin',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git"],
        trigger_rule="none_failed",
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/code/de32-kca/extract_kca/load_dt={{ ds_nodash }}',
    )

    get_data_nation = PythonVirtualenvOperator(
        task_id='get.data.nation',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git"],
        trigger_rule="none_failed",
        op_kwargs={
            "url_param" : { "repNationCd": "K" }
        }
    )

    task_chk_exist = BranchPythonOperator(
        task_id="chk.exist",
        python_callable=chk_exist,
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> task_chk_exist >> rm_dir >> [get_data_nation, get_data_origin] >> end
    task_chk_exist >> [get_data_nation, get_data_origin] >> end
