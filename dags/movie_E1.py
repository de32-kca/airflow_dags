from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator

with DAG(
    'movie_E',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    },
    description="extract movie rank 2018.01~04.",
    schedule="10 0 * * *",
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 4, 30),
    catchup=True,
    tags=["movie","E", "extract","2018"],
) as dag:


    def pic():
        from extract.ice_breaking import pic
        
        pic()

    def get_data(ds_nodash,url_param={}):
        from extract.extract import save2df
        import pandas as pd

        df=save2df(ds_nodash,url_param)

        df.to_parquet("~/code/de32-kca/data_kca/",partition_cols=["load_dt","repNationCd"])

    def chk_exist(ds_nodash="20180101"):
        import os
        
        home_dir = os.path.expanduser("~")
        target_path=f"{home_dir}/code/de32-kca/data_kca/load_dt={ds_nodash}"
        
        print( os.path.exists(target_path),target_path)

        if os.path.exists(target_path):
            return "rm.dir"
        else:
            return "get.start"

    task_chk_exist=BranchPythonOperator(
                task_id="chk.exist",
                python_callable=chk_exist,
            )

    task_rm_dir=BashOperator(
                task_id="rm.dir",
                bash_command="""
                    rm -rf ~/code/de32-kca/data_kca/load_dt={{ds_nodash}}
                """
                )

    task_get_data=PythonVirtualenvOperator(
                task_id="get.data",
                python_callable=get_data,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )

    task_get_data_kor=PythonVirtualenvOperator(
                task_id="get.data.kor",
                python_callable=get_data,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                op_kwargs={
                    "url_param":{ "repNationCd":"K"}
                }
            )
    """
    task_save_data=PythonVirtualenvOperator(
                task_id="save.data",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )
    """
    task_start=PythonVirtualenvOperator(
                task_id="start",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )

    task_end=PythonVirtualenvOperator(
                task_id="end",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )

    task_get_start=PythonVirtualenvOperator(
                task_id="get.start",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"],
                trigger_rule="none_failed"
            )
    task_get_end=PythonVirtualenvOperator(
                task_id="get.end",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"],
                trigger_rule="none_failed"

            )
    task_start >> task_chk_exist >> [task_rm_dir, task_get_start]
    task_rm_dir >> task_get_start
    task_get_start >> [task_get_data, task_get_data_kor] >> task_get_end >> task_end
