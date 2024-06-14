from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.produce_usage import combine_top_news_polarity
from lib.data_fetcher_thenewsapi import fetch_data_from_newsapi
from lib.fmt_to_enriched_newsapi import convert_formatted_to_enriched_newsapi
from lib.raw_to_fmt_newsapi import convert_raw_to_formatted_newsapi
from lib.fmt_to_enriched_reddit import convert_fmt_to_enriched_reddit
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


    source_to_raw_newsapi = PythonOperator(
        task_id='source_to_raw_newsapi',
        python_callable=fetch_data_from_newsapi,
        provide_context=True,
        op_kwargs={
            'url': 'https://newsapi.org/v2/top-headlines',
            'data_entity_name': 'TopHeadlinesUS',
            'country': 'us'
        }
    )

    source_to_raw_reddit = PythonOperator(
        task_id='source_to_raw_reddit',
        python_callable=fetch_data_from_reddit_news_api,
        provide_context=True,
        op_kwargs={
            'task_name': 'source_to_raw_reddit',
            'limit': 20,
            'subreddit': 'worldnews'
        }
    )

    raw_to_formated_newsapi = PythonOperator(
        task_id='raw_to_formated_newsapi',
        python_callable=convert_raw_to_formatted_newsapi,
        provide_context=True,
        op_kwargs={
            'task_name': 'raw_to_formated_newsapi',
            'file_name': 'newsapi.json',
            'data_entity_name': 'TopHeadlinesUS'
        }
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
        python_callable=convert_fmt_to_enriched_reddit,
        provide_context=True,
        op_kwargs={
            'task_name': 'formated_to_enriched_reddit',
            'file_name': 'reddit_news_posts.snappy.parquet',
            'data_entity_name': 'NewsPostsReddit'
        }
    )

    formated_to_enriched_newsapi = PythonOperator(
        task_id='formated_to_enriched_newsapi',
        python_callable=convert_formatted_to_enriched_newsapi,
        provide_context=True,
        op_kwargs={
            'task_name': 'formated_to_enriched_newsapi',
            'file_name': 'newsapi.snappy.parquet',
            'data_entity_name': 'TopHeadlinesUS'
        }
    )

    produce_usage = PythonOperator(
        task_id='produce_usage',
        python_callable=combine_top_news_polarity,
        provide_context=True,
        op_kwargs={
            'task_name': 'produce_usage'
        }
    )

    index_to_elastic = PythonOperator(
        task_id='index_to_elastic',
        python_callable=launch_task,
        provide_context=True,
        op_kwargs={'task_name': 'index_to_elastic'}
    )


    def add_source_pipeline(source_task, transform_task, enrich_task, join_task):
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
        source_task=source_to_raw_newsapi,
        transform_task=raw_to_formated_newsapi,
        enrich_task=formated_to_enriched_newsapi,
        join_task=produce_usage
    )

    produce_usage.set_downstream(index_to_elastic)
