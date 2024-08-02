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
    schedule_interval=timedelta(days=1),
    start_date=datetime(2018, 1, 1),
    end_date=datetime(2018, 1, 30),
    catchup=True,
    tags=["movie","E", "extract","2018"],
) as dag:

    def test():
        import os
        
        os.system('ice_breaking')

    task_get_data=PythonVirtualenvOperator(
                task_id="get.data",
                python_callable=test,
                requirements=["git+https://github.com/de32-kca/airflow_dags.git@d1.0.2/movie_1"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )

    task_save_data=EmptyOperator(task_id="save.data")
    task_start=EmptyOperator(task_id="start")
    task_end=EmptyOperator(task_id="end")

    task_start >> task_get_data >> task_save_data >> task_end

   
