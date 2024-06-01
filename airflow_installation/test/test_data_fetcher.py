from airflow_installation.dags.lib.data_fetcher import fetch_data_from_twitter


def test_data_fetcher():
    fetch_data_from_twitter()
