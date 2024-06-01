import json

from airflow_installation.dags.lib.data_fetcher_source1 import *


def test_fetch_data():
    fetch_data()


def test_get_data_from_the_movie_db():
    get_data_from_the_movie_db()


def test_store_data():
    with open('mock_data_source1.json') as data:
        data = json.load(data)
        store_data(data)
