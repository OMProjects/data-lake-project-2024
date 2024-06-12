import json

from airflow_installation.dags.lib.raw_to_fmt_newsapi import *


def test_raw_to_fmt_newsapi():
    copy_convert_raw_to_formatted_newsapi('mock_data_newsapi.json')

