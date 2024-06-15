import os
import pandas as pd
from datetime import date, datetime, timezone

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_reddit(file_name, data_entity_name, opt_date=None):
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
    POSTS_PATH = DATALAKE_ROOT_FOLDER + f"raw/reddit/{data_entity_name}/" + current_str + "/" + file_name
    FORMATTED_POSTS_FOLDER = DATALAKE_ROOT_FOLDER + f"formatted/reddit/{data_entity_name}/" + current_str + "/"

    if not os.path.exists(FORMATTED_POSTS_FOLDER):
        os.makedirs(FORMATTED_POSTS_FOLDER)

    df = pd.read_json(POSTS_PATH)

    parquet_file_name = file_name.replace(".json", ".snappy.parquet")

    df.to_parquet(FORMATTED_POSTS_FOLDER + parquet_file_name)
