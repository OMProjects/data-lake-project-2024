import os
from datetime import date, time, datetime, timezone
from pyspark.sql import SparkSession

from elasticsearch import Elasticsearch, helpers

from lib.env import elasticsearch_password

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def ingest_into_elastic(opt_date=None):
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
    USAGE_DEST = DATALAKE_ROOT_FOLDER + f"usage/news_topics_polarity/" + current_str + "/news_topics_polarity.snappy.parquet"

    client = Elasticsearch(
        "https://localhost:9200/",
        verify_certs=False,
        http_auth=("elastic", elasticsearch_password),
    )

    spark = SparkSession.builder.getOrCreate()
    usage_rdd = spark.read.parquet(USAGE_DEST).rdd
    usage_rdd = usage_rdd.map(lambda x: dict({"name": x[0], "polarity": x[1], "date": current})).collect()

    def generate_docs():
        for doc in usage_rdd:
            yield doc

    helpers.bulk(client, generate_docs(), index="news_topics_polarity")
