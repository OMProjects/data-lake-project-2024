import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_imdb_hard_coded(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
    r = requests.get(url, allow_redirects=True)
    open(TARGET_PATH + 'title.ratings.tsv.gz', 'wb').write(r.content)


def fetch_data_from_imdb(url: str, data_entity_name: str, **kwargs):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + f"raw/imdb/{data_entity_name}/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    name = url.split("/")[-1]
    r = requests.get(url, allow_redirects=True)
    open(TARGET_PATH + name, 'wb').write(r.content)
