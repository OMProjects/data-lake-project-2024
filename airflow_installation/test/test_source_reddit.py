from lib.data_fetcher_reddit import fetch_data_from_reddit_news_api, fetch_data_from_reddit_news_bs4
from lib.fmt_to_enriched_reddit import convert_fmt_to_enriched_reddit
from lib.raw_to_fmt_reddit import convert_raw_to_formatted_reddit


def test_fetch_data_from_reddit_news_bs4():
    fetch_data_from_reddit_news_bs4()


def test_fetch_data_from_reddit_news_api():
    fetch_data_from_reddit_news_api()


def test_raw_to_fmt_reddit():
    convert_raw_to_formatted_reddit("reddit_news_posts.json", "NewsPostsReddit")


def test_fmt_to_enriched_reddit():
    convert_fmt_to_enriched_reddit("reddit_news_posts.snappy.parquet", "NewsPostsReddit")
