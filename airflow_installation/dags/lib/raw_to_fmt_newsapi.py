import os
from datetime import date

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_newsapi(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    TOPHEADLINES_PATH = DATALAKE_ROOT_FOLDER + f"raw/newsapi/" + data_entity_name + "/" + current_day + "/" + file_name
    FORMATTED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "formatted/newsapi/" + data_entity_name + "/"
                                     + current_day + "/")

    if not os.path.exists(FORMATTED_TOPHEADLINES_FOLDER):
        os.makedirs(FORMATTED_TOPHEADLINES_FOLDER)

    df = pd.read_json(TOPHEADLINES_PATH)

    parquet_file_name = file_name.replace(".json", ".snappy.parquet")

    final_df = pd.DataFrame(data=df.data)
    final_df.to_parquet(FORMATTED_TOPHEADLINES_FOLDER + parquet_file_name)
