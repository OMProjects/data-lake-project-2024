import os
from datetime import date

from pyspark.sql import SparkSession

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_top_news_polarity():
    current_day = date.today().strftime("%Y%m%d")
    ENRICHED_REDDIT_PATH = DATALAKE_ROOT_FOLDER + f"enriched/reddit/NewsPostsReddit/" + current_day + "/reddit_news_posts.snappy.parquet"
    ENRICHED_NEWSAPI_PATH = DATALAKE_ROOT_FOLDER + f"enriched/newsapi/TopHeadlinesUS/" + current_day + "/newsapi.snappy.parquet"
    USAGE_DEST = DATALAKE_ROOT_FOLDER + f"usage/news_topics_polarity/" + current_day + "/"

    if not os.path.exists(USAGE_DEST):
        os.makedirs(USAGE_DEST)

    spark = SparkSession.builder.getOrCreate()
    reddit_rdd = spark.read.parquet(ENRICHED_REDDIT_PATH).rdd
    newsapi_rdd = spark.read.parquet(ENRICHED_NEWSAPI_PATH).rdd

    print()
    # reddit_rdd.summary().show()
    # reddit_rdd.toPandas().info()
    # print()
    # newsapi_rdd.summary().show()
    # newsapi_rdd.toPandas().info()
    # print("Reddit")
    # reddit_rdd.toPandas().apply(lambda x: print(x["average_comment_polarity"], x["article_nouns"], x["url"]), axis=1)
    # print("News Api")
    # newsapi_rdd.toPandas().apply(lambda x: print(x["noun_tags"], x["url"]), axis=1)

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
