from airflow_installation.dags.lib.fmt_to_enriched_newsapi import *


def test_fmt_to_enriched_newsapi():
    data = get_topics_newsapi('mock_data_newsapi.snappy.parquet')
    keyword_analysis_newsapi(data)
