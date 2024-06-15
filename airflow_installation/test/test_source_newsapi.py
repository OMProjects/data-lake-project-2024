from lib.data_fetcher_thenewsapi import fetch_data_from_newsapi
from lib.fmt_to_enriched_newsapi import convert_formatted_to_enriched_newsapi
from lib.raw_to_fmt_newsapi import convert_raw_to_formatted_newsapi


def test_fetch_data_from_newsapi():
    fetch_data_from_newsapi(
        **{
            'url': 'https://newsapi.org/v2/top-headlines',
            'data_entity_name': 'TopHeadlinesUS',
            'country': 'us'
        }
    )


def test_raw_to_fmt_newsapi():
    convert_raw_to_formatted_newsapi(
        **{
            'file_name': 'newsapi.json',
            'data_entity_name': 'TopHeadlinesUS'
        }
    )


def test_fmt_to_enriched_newsapi():
    convert_formatted_to_enriched_newsapi(
        **{
            'file_name': 'newsapi.snappy.parquet',
            'data_entity_name': 'TopHeadlinesUS'
        }
    )
