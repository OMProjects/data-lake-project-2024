from lib.fmt_to_enriched_reddit import convert_fmt_to_enriched_reddit


def test_fmt_to_enriched_reddit():
    convert_fmt_to_enriched_reddit("reddit_news_posts.snappy.parquet", "NewsPostsReddit")
