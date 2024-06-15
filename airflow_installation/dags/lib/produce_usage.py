import os
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_top_news_polarity(opt_date=None):
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
    ENRICHED_REDDIT_PATH = DATALAKE_ROOT_FOLDER + f"enriched/reddit/NewsPostsReddit/" + current_str + "/reddit_news_posts.snappy.parquet"
    ENRICHED_NEWSAPI_PATH = DATALAKE_ROOT_FOLDER + f"enriched/newsapi/TopHeadlinesUS/" + current_str + "/newsapi.snappy.parquet"
    USAGE_DEST = DATALAKE_ROOT_FOLDER + f"usage/news_topics_polarity/" + current_str + "/"

    if not os.path.exists(USAGE_DEST):
        os.makedirs(USAGE_DEST)

    spark = SparkSession.builder.getOrCreate()
    reddit_rdd = spark.read.parquet(ENRICHED_REDDIT_PATH).rdd
    newsapi_rdd = spark.read.parquet(ENRICHED_NEWSAPI_PATH).rdd

    top_news_topics = (
        newsapi_rdd
        .flatMap(lambda x: x["noun_tags"])
        .map(lambda x: (x, 0))
        .sortByKey()
    )

    topics_polarity = (
        reddit_rdd
        .map(lambda x: (x["average_comment_polarity"], x["article_nouns"]))
        .flatMapValues(lambda x: x)
        .map(lambda x: (x[1], x[0]))
        .groupByKey()
        .map(lambda x: (x[0], sum(x[1]) / len(x[1])))
        .sortByKey()
    )

    topics_polarity = topics_polarity.subtractByKey(topics_polarity.subtractByKey(top_news_topics))
    topics_polarity.toDF().toPandas().to_parquet(USAGE_DEST + "news_topics_polarity.snappy.parquet")
