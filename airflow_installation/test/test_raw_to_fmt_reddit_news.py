from lib.raw_to_fmt_reddit import convert_raw_to_formatted_reddit


def test_raw_to_fmt_reddit():
    convert_raw_to_formatted_reddit("reddit_news_posts.json", "NewsPostsReddit")
