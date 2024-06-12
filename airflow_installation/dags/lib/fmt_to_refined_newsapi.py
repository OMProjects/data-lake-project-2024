import json
import os
from datetime import date

import pandas as pd
from textblob import TextBlob

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_formatted_to_refined_newsapi(file_name, data_entity_name):
    current_day = date.today().strftime("%Y%m%d")
    FORMATTED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "formatted/newsapi/" + data_entity_name + "/"
                                     + current_day + "/")
    REFINED_TOPHEADLINES_FOLDER = (DATALAKE_ROOT_FOLDER + "refined/newsapi/" + data_entity_name + "/"
                                   + current_day + "/")

    if not os.path.exists(REFINED_TOPHEADLINES_FOLDER):
        os.makedirs(REFINED_TOPHEADLINES_FOLDER)

    refined_data = get_topics_newsapi(FORMATTED_TOPHEADLINES_FOLDER)

    final_df = pd.DataFrame(data=refined_data)
    final_df.to_parquet(REFINED_TOPHEADLINES_FOLDER + file_name)


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

    refined_data = keyword_analysis_newsapi(data)
    return refined_data


def keyword_analysis_newsapi(refined_data):
    for articles in refined_data:
        article_data = articles['articles']
        article_string = article_data['key_info']

        if article_string:
            blob = TextBlob(article_string)

            # noun_tags = blob.noun_phrases
            # noun_tags_clean = [word.replace("'s", "").strip() for word in noun_tags]

            nouns = []
            pos_tags = blob.tags

            for word, pos_tag in pos_tags:
                if pos_tag.startswith('NN'):
                    nouns.append(word)

            article_data['noun_tags'] = nouns

    return refined_data









