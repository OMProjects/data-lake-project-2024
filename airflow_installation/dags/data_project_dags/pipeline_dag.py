from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'pipeline_dag',
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

    def launch_task(**kwargs):
        print("Hello Airflow - This is Task with task_number:", kwargs['task_name'])

    source_to_raw_1 = PythonOperator(
        task_id='source_to_raw_1',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'source_to_raw_1'}
    )

    source_to_raw_2 = PythonOperator(
        task_id='source_to_raw_2',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'source_to_raw_1'}
    )

    raw_to_formated_1 = PythonOperator(
        task_id='raw_to_formated_1',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'raw_to_formated_1'}
    )

    raw_to_formated_2 = PythonOperator(
        task_id='raw_to_formated_2',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'raw_to_formated_2'}
    )

    produce_usage = PythonOperator(
        task_id='produce_usage',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'produce_usage'}
    )

    index_to_elastic = PythonOperator(
        task_id='index_to_elastic',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'index_to_elastic'}
    )

    source_to_raw_1.set_downstream(raw_to_formated_1)
    source_to_raw_2.set_downstream(raw_to_formated_2)

    produce_usage.set_upstream(raw_to_formated_1)
    produce_usage.set_upstream(raw_to_formated_2)

    produce_usage.set_downstream(index_to_elastic)

