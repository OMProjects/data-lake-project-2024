import os
import pandas as pd
from datetime import date
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_fmt_to_enriched_reddit(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    PARQUET_PATH = DATALAKE_ROOT_FOLDER + f"formatted/reddit/{data_entity_name}/" + current_day + "/" + file_name
    ENRICHED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + f"enriched/reddit/{data_entity_name}/" + current_day + "/"

    if not os.path.exists(ENRICHED_RATING_FOLDER):
        os.makedirs(ENRICHED_RATING_FOLDER)

    df = pd.read_parquet(PARQUET_PATH)

    def post_sentiment_analysis(data):
        comment_polarity = []
        comment_subjectivity = []
        comment_nouns = []
        article_nouns = []
        comment_polarity_by_score = []

        new_comments = []

        for comment in data["comments"]:
            new_comment = comment
            blob = TextBlob(comment["body"])
            new_comment["sentiment_polarity"] = blob.sentiment.polarity
            new_comment["sentiment_subjectivity"] = blob.sentiment.subjectivity
            new_comment["nouns"] = blob.noun_phrases
            new_comments.append(new_comment)
            new_comment["polarity_by_score"] = comment["score"] * blob.sentiment.polarity

            comment_polarity_by_score.append(new_comment["polarity_by_score"])
            comment_polarity.append(blob.sentiment.polarity)
            comment_subjectivity.append(blob.sentiment.subjectivity)

            for word, pos_tag in blob.tags:
                if pos_tag.startswith('NN') and word not in comment_nouns:
                    comment_nouns.append(word)

            comment_nouns = [n.lower() for n in comment_nouns if len(n) > 1]

        data["comments"] = new_comments
        data["comment_nouns"] = comment_nouns
        data["average_comment_subjectivity"] = sum(comment_subjectivity) / len(comment_subjectivity)
        data["average_comment_polarity"] = sum(comment_polarity) / len(comment_polarity)
        data["average_comment_polarity_by_score"] = sum(comment_polarity_by_score) / len(comment_polarity_by_score)
        data["total_comment_polarity_by_score"] = sum(comment_polarity_by_score)
        data["comment_amount"] = len(data["comments"])

        blob_article = TextBlob(data["title"])
        for word, pos_tag in blob_article.tags:
            if pos_tag.startswith('NN') and word not in article_nouns:
                article_nouns.append(word)

        article_nouns = [n.lower() for n in article_nouns if len(n) > 1]

        data["article_nouns"] = article_nouns

        return data

    df = df.apply(post_sentiment_analysis, axis=1)

    df.to_parquet(ENRICHED_RATING_FOLDER + file_name)
