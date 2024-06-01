import json

from airflow_installation.dags.lib.data_fetcher_theimdb import *


def test_fetch_data_from_imdb():
    fetch_data_from_imdb()

