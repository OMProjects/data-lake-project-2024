import os
import requests

from lib.env import the_movieDB_api_key


def fetch_data(**kwargs):
    print("")
    print(os.path.abspath(__name__))
    print("Getting twitter data...")
    print("....")
    print("DONE!")


def get_data_from_the_movie_db() -> dict:
    url = "https://api.themoviedb.org/3/trending/movie/day?language=en-US"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {the_movieDB_api_key}"
    }

    response = requests.get(url, headers=headers)

    return response.json()


def store_data(data: dict):
    print(data)
    pass
