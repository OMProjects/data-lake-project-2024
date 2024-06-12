import os
import pandas as pd
from datetime import date
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_reddit(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    POSTS_PATH = DATALAKE_ROOT_FOLDER + f"raw/reddit/{data_entity_name}/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + f"formatted/reddit/{data_entity_name}/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    df = pd.read_json(POSTS_PATH)

    parquet_file_name = file_name.replace(".json", ".snappy.parquet")

    df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
