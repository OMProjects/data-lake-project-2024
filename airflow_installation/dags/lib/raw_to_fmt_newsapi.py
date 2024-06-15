import os
from datetime import date, datetime, timezone

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_newsapi(file_name, data_entity_name, opt_date=None):
    if opt_date is None:
        current = datetime(
            year=date.today().year,
            month=date.today().month,
            day=date.today().day,
            tzinfo=timezone.utc
        )
    else:
        current = datetime.strptime(opt_date, "%Y%m%d")
        current = datetime(
            year=current.year,
            month=current.month,
            day=current.day,
            tzinfo=timezone.utc
        )
    current_str = current.strftime("%Y%m%d")
    TOPHEADLINES_PATH = DATALAKE_ROOT_FOLDER + f"raw/newsapi/" + data_entity_name + "/" + current_str + "/" + file_name
    FORMATTED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "formatted/newsapi/" + data_entity_name + "/"
                                     + current_str + "/")

    if not os.path.exists(FORMATTED_TOPHEADLINES_FOLDER):
        os.makedirs(FORMATTED_TOPHEADLINES_FOLDER)

    raw_df = pd.read_json(TOPHEADLINES_PATH)

    def format_data(data):
        data["source_name"] = data["source"]["name"]
        data["source_id"] = data["source"]["id"]
        data["author"] = str(data["author"])
        data["title"] = str(data["title"])
        data["description"] = str(data["description"])
        data["url"] = str(data["url"])
        data["urlToImage"] = str(data["urlToImage"])
        data["content"] = str(data["content"])
        return data

    df = pd.DataFrame([dict(art) for art in raw_df["articles"]])
    df = df.apply(format_data, axis=1)

    parquet_file_name = file_name.replace(".json", ".snappy.parquet")

    df.to_parquet(FORMATTED_TOPHEADLINES_FOLDER + parquet_file_name)
