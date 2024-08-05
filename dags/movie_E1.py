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
    end_date=datetime(2018, 1, 30),
    catchup=True,
    tags=["movie","E", "extract","2018"],
) as dag:


    def pic():
        from extract.ice_breaking import pic
        
        pic()

    def get_data(ds_nodash,url_param={}):
        from extract.extract import save2df

        df=save2df(ds_nodash,url_param)

        print(df)
    
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

    task_save_data=PythonVirtualenvOperator(
                task_id="save.data",
                python_callable=pic,
                requirements=["git+https://github.com/de32-kca/extract.git@d2.0.0/mingk"],
                system_site_packages=False,
                # op_kwargs=kwargs["op_kwargs"]
            )

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

    task_start >> [task_get_data, task_get_data_kor] >> task_save_data >> task_end
