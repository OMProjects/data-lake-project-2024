from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.data_fetcher_themoviedb import fetch_data

with DAG(
        'my_fifth_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow_installation@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A first DAG',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    dag.doc_md = """
       This is my fifth DAG in airflow_installation. It uses my custom made twitter func.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """

    t1 = PythonOperator(
        task_id='task1',
        python_callable=fetch_data,
        provide_context=True,
        op_kwargs={'task_number': 'task1'}
    )
