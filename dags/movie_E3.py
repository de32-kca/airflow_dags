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
    end_date=datetime(2018, 9, 3),
    catchup=True,
    tags=['api', 'movie', 'amt'],
) as dag:

    def get_data():
        from extract.ice_breaking import pic
        pic()

    def save_data():
        from extract.ice_breaking import pic
        pic()

    # t1, t2 and t3 are examples of tasks created by instantiating operators

    get_data = PythonVirtualenvOperator(
        task_id='get.data',
        python_callable=get_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git@release/d1.0.0"],
    )

    save_data = PythonVirtualenvOperator(
        task_id='save.data',
        python_callable=save_data,
        system_site_packages=False,
        requirements=["git+https://github.com/de32-kca/extract.git@release/d1.0.0"],
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    start >> get_data >> save_data >> end
