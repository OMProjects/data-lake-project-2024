import os
import pandas as pd
from datetime import date
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fmt_to_enriched_reddit(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    PARQUET_PATH = DATALAKE_ROOT_FOLDER + f"formatted/reddit/{data_entity_name}/" + current_day + "/" + file_name
    ENRICHED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + f"enriched/reddit/{data_entity_name}/" + current_day + "/"

    if not os.path.exists(ENRICHED_RATING_FOLDER):
        os.makedirs(ENRICHED_RATING_FOLDER)

    df = pd.read_parquet(PARQUET_PATH)

    def post_sentiment_analysis(data):

        comment_polarity = []
        comment_subjectivity = []
        comment_nouns = set()
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

            for noun in blob.noun_phrases:
                comment_nouns.add(noun)

        data["comments"] = new_comments
        data["comment_nouns"] = comment_nouns
        data["average_comment_subjectivity"] = sum(comment_subjectivity) / len(comment_subjectivity)
        data["average_comment_polarity"] = sum(comment_polarity) / len(comment_polarity)
        data["average_comment_polarity_by_score"] = sum(comment_polarity_by_score) / len(comment_polarity_by_score)
        data["total_comment_polarity_by_score"] = sum(comment_polarity_by_score)
        data["comment_amount"] = len(data["comments"])

        return data

    df = df.apply(post_sentiment_analysis, axis=1)

    df.to_parquet(ENRICHED_RATING_FOLDER + file_name)
