from lib.raw_to_fmt_imdb import convert_raw_to_formatted_imdb


def test_raw_to_fmt_imdb():
    convert_raw_to_formatted_imdb("title.ratings.tsv.gz", "MovieRating")
