from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.data_fetcher_theimdb import fetch_data_from_imdb
from lib.fmt_to_enriched_reddit import fmt_to_enriched_reddit
from lib.raw_to_fmt_imdb import convert_raw_to_formatted_imdb
from lib.data_fetcher_reddit import fetch_data_from_reddit_news_api
from lib.raw_to_fmt_reddit import convert_raw_to_formatted_reddit

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
        description='The dag for the data project',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['data_project_dag'],
) as dag:
    dag.doc_md = """
       A dag with the pipeline for the data project
   """


    def launch_task(**kwargs):
        print("Hello Airflow - This is Task with task_number:", kwargs['task_name'])


    source_to_raw_imdb = PythonOperator(
        task_id='source_to_raw_1',
        python_callable=fetch_data_from_imdb,
        provide_context=True,
        op_kwargs={'url': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
                   'data_entity_name': 'title.ratings.tsv.gz'}
    )

    source_to_raw_reddit = PythonOperator(
        task_id='source_to_raw_reddit',
        python_callable=fetch_data_from_reddit_news_api,
        provide_context=True,
        op_kwargs={
            'task_name': 'source_to_raw_reddit',
            'limit': 10,
            'subreddit': 'news'
        }
    )

    raw_to_formated_imdb = PythonOperator(
        task_id='raw_to_formated_1',
        python_callable=convert_raw_to_formatted_imdb,
        provide_context=True,
        op_kwargs={'file_name': 'title.ratings.tsv.gz',
                   'data_entity_name': 'MovieRating'}
    )

    raw_to_formated_reddit = PythonOperator(
        task_id='raw_to_formated_reddit',
        python_callable=convert_raw_to_formatted_reddit,
        provide_context=True,
        op_kwargs={
            'task_name': 'raw_to_formated_reddit',
            'file_name': 'reddit_news_posts.json',
            'data_entity_name': 'NewsPostsReddit'
        }
    )

    formated_to_enriched_reddit = PythonOperator(
        task_id='formated_to_enriched_reddit',
        python_callable=fmt_to_enriched_reddit,
        provide_context=True,
        op_kwargs={
            'task_name': 'formated_to_enriched_reddit',
            'file_name': 'reddit_news_posts.snappy.parquet',
            'data_entity_name': 'NewsPostsReddit'
        }
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


    def add_source_pipeline(source_task=None, transform_task=None, enrich_task=None, join_task=None):
        if enrich_task is None:
            source_task.set_downstream(transform_task)
            join_task.set_upstream(transform_task)
        else:
            source_task.set_downstream(transform_task)
            transform_task.set_downstream(enrich_task)
            join_task.set_upstream(enrich_task)


    add_source_pipeline(
        source_task=source_to_raw_reddit,
        transform_task=raw_to_formated_reddit,
        enrich_task=formated_to_enriched_reddit,
        join_task=produce_usage
    )

    add_source_pipeline(
        source_task=source_to_raw_imdb,
        transform_task=raw_to_formated_imdb,
        join_task=produce_usage
    )




    produce_usage.set_downstream(index_to_elastic)
