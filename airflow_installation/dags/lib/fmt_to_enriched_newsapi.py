import os
from datetime import date, timezone, datetime

import pandas as pd
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_formatted_to_enriched_newsapi(file_name, data_entity_name, opt_date=None):
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
    FORMATTED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "formatted/newsapi/" + data_entity_name + "/"
                                     + current_str + "/")
    enriched_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "enriched/newsapi/" + data_entity_name + "/"
                                    + current_str + "/")

    if not os.path.exists(enriched_TOPHEADLINES_FOLDER):
        os.makedirs(enriched_TOPHEADLINES_FOLDER)

    df = pd.read_parquet(FORMATTED_TOPHEADLINES_FOLDER + file_name)

    def sentiment_analysis(data):
        if data["title"] and data["description"]:
            article_string = data["title"] + ' ' + data["description"]
        elif data["title"]:
            article_string = data["title"]
        elif data["description"]:
            article_string = data["description"]
        else:
            article_string = ''

        data["key_info"] = article_string

        blob = TextBlob(article_string)
        nouns = []
        post_tags = blob.tags

        for word, pos_tag in post_tags:
            if pos_tag.startswith('NN') and word not in nouns:
                nouns.append(word)

        data['noun_tags'] = [n.lower() for n in nouns if len(n) > 1]

        data["sentiment_polarity"] = blob.sentiment.polarity
        data["sentiment_subjectivity"] = blob.sentiment.subjectivity

        return data

    df = df.apply(sentiment_analysis, axis=1)

    df.to_parquet(enriched_TOPHEADLINES_FOLDER + file_name)
