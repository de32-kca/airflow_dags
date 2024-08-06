from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator

with DAG(
    'movie_T',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    description="transform movie rank 2018.01~04.",
    schedule="10 0 * * *",
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2019, 1, 1),
    catchup=True,
    tags=["movie","E", "transform","2018"],
) as dag:


    def pic():
        from transform.ice_breaking import pic
        
        pic()

    def transform_data(ds_nodash="20180101", path='~/code/de32-kca/extract_kca/'):
        from transform.transform import df_data
        
        df=df_data(int(ds_nodash),path)

        df.to_parquet("~/code/de32-kca/transform_kca/",partition_cols=["load_dt",])

    def chk_exist_E(ds_nodash="20180101"):
        import os

        home_dir = os.path.expanduser("~")
        target_path=f"{home_dir}/code/de32-kca/extract_kca/load_dt={ds_nodash}"
        
        print( os.path.exists(target_path),target_path)

        if os.path.exists(target_path):
            return "chk.exist.transform"
        else:
            return "end"
            
    def chk_exist_T(ds_nodash="20180101"):
        import os
        
        home_dir = os.path.expanduser("~")
        target_path=f"{home_dir}/code/de32-kca/transform_kca/load_dt={ds_nodash}"
        
        print( os.path.exists(target_path),target_path)

        if os.path.exists(target_path):
            return "rm.dir"
        else:
            return "transform.data"

    task_chk_exist_E=BranchPythonOperator(
                task_id="chk.exist.extract",
                python_callable=chk_exist_E,
            )

    task_chk_exist_T=BranchPythonOperator(
                task_id="chk.exist.transform",
                python_callable=chk_exist_T,
            )
    
    task_rm_dir=BashOperator(
                task_id="rm.dir",
                bash_command="""
                    rm -rf ~/code/de32-kca/transform_kca/load_dt={{ds_nodash}}
                """
                )

    task_transform_data=PythonVirtualenvOperator(
                task_id="transform.data",
                python_callable=transform_data,
                requirements=["git+https://github.com/de32-kca/transform.git@dev/d3.0.0"],
                system_site_packages=False,
                trigger_rule="none_failed"
            )


    task_start=EmptyOperator(task_id="start")
    task_end=EmptyOperator(task_id="end")

    task_start >> task_chk_exist_E >> [task_end, task_chk_exist_T]
    task_chk_exist_T >> [task_rm_dir, task_transform_data]
    task_rm_dir >> task_transform_data >> task_end
