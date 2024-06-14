import json
import os
from datetime import date

import requests
from bs4 import BeautifulSoup
import praw
import lib.env as env

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_reddit_news_bs4():
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/reddit/NewsPostsReddit/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    url = "https://www.reddit.com/r/news/"
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")
    # print(soup)
    articles = soup.findAll("a", {"slot": "full-post-link"})
    for a in articles:
        print("---")
        print(a["href"])

    post = "https://www.reddit.com" + "/r/news/comments/1ddgq41/president_joe_bidens_son_hunter_biden_is/"
    post = requests.get(post)
    post = BeautifulSoup(post.content, "html.parser")

    print(post)


def fetch_data_from_reddit_news_api(subreddit="worldnews", limit=10):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/reddit/NewsPostsReddit/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    reddit = praw.Reddit(
        client_id=env.reddit_client_id,
        client_secret=env.reddit_client_secret,
        user_agent="airflow:db-project by u/ArugulaIndividual411"
    )

    news_subreddit = reddit.subreddit(subreddit).hot(limit=limit)

    news_posts = []

    for submission in news_subreddit:
        sub = dict()
        sub["url"] = submission.url
        sub["title"] = submission.title
        sub["score"] = submission.score
        sub["id"] = submission.id
        sub["created_utc"] = submission.created_utc
        comments = []
        for comment in submission.comments:
            if isinstance(comment, praw.models.Comment):
                c = dict()
                c["body"] = comment.body
                c["body_html"] = comment.body_html
                c["id"] = comment.id
                c["score"] = comment.score
                comments.append(c)
        sub["comments"] = comments

        news_posts.append(sub)

    with open(TARGET_PATH + f"reddit_news_posts.json", "w") as fp:
        json.dump(news_posts, fp)
