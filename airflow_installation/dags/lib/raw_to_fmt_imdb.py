import os
import pandas as pd
from datetime import date

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted_imdb(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    RATING_PATH = DATALAKE_ROOT_FOLDER + f"raw/imdb/{data_entity_name}/" + current_day + "/" + file_name
    FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + f"formatted/imdb/{data_entity_name}/" + current_day + "/"

    if not os.path.exists(FORMATTED_RATING_FOLDER):
        os.makedirs(FORMATTED_RATING_FOLDER)

    df = pd.read_csv(RATING_PATH, sep='\t')

    parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")

    df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
