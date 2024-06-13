import json
import os
from datetime import date

import pandas as pd
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_formatted_to_enriched_newsapi(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    FORMATTED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "formatted/newsapi/" + data_entity_name + "/"
                                     + current_day + "/")
    enriched_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "enriched/newsapi/" + data_entity_name + "/"
                                    + current_day + "/")

    if not os.path.exists(enriched_TOPHEADLINES_FOLDER):
        os.makedirs(enriched_TOPHEADLINES_FOLDER)

    enriched_data = get_topics_newsapi(FORMATTED_TOPHEADLINES_FOLDER + file_name)

    final_df = pd.DataFrame(data=enriched_data)
    final_df.to_parquet(enriched_TOPHEADLINES_FOLDER + file_name)


def get_topics_newsapi(file_name):
    df = pd.read_parquet(file_name)
    json_data = df.to_json(orient='records')
    data = json.loads(json_data)

    for articles in data:
        article_data = articles['articles']
        title = article_data['title']
        description = article_data['description']
        if title and description:
            article_string = title + ' ' + description
        elif title:
            article_string = title
        elif description:
            article_string = description
        else:
            article_string = ''

        article_data['key_info'] = article_string

    enriched_data = keyword_analysis_newsapi(data)
    return enriched_data


def keyword_analysis_newsapi(enriched_data):
    for articles in enriched_data:
        article_data = articles['articles']
        article_string = article_data['key_info']

        if article_string:
            blob = TextBlob(article_string)

            # noun_tags = blob.noun_phrases
            # noun_tags_clean = [word.replace("'s", "").strip() for word in noun_tags]

            nouns = []
            post_tags = blob.tags

            for word, pos_tag in post_tags:
                if pos_tag.startswith('NN'):
                    nouns.append(word)

            article_data['noun_tags'] = nouns

    return enriched_data
