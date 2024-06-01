from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'my_first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
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
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """


    def task1():
        print("Hello Airflow - This is Task 1")


    def task2():
        print("Hello Airflow - This is Task 2")


    t1 = PythonOperator(
        task_id='task1',
        python_callable=task1,
    )

    t2 = PythonOperator(
        task_id='task2',
        python_callable=task2
    )

    t1 >> t2
