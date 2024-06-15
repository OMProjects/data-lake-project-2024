import json
import os
from datetime import date, timezone, datetime

import requests

from lib.env import the_newsApi_api_key

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_newsapi(url, data_entity_name, country):
    params = {
        "country": country,
        "category": "general",
        "apiKey": the_newsApi_api_key,
    }

    response = requests.get(url, params=params)
    news = response.json()

    store_newsapi_data(news, data_entity_name)

    # Mock Data JSON Dump:
    # data = response.json()
    # with open("mock_data_newsapi.json", 'w') as file:
    #     json.dump(data, file, indent=4)
    # return data


def store_newsapi_data(news, data_entity_name):
    current = datetime(
        year=date.today().year,
        month=date.today().month,
        day=date.today().day,
        tzinfo=timezone.utc
    )
    current_str = current.strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + f"raw/newsapi/" + data_entity_name + "/" + current_str + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    f = open(TARGET_PATH + "newsapi.json", 'w+')
    f.write(json.dumps(news, indent=4))
