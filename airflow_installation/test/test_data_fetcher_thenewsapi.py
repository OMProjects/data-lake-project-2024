import json

from airflow_installation.dags.lib.data_fetcher_thenewsapi import *


def test_fetch_data_from_googlenews():
    fetch_data_from_newsapi()

