import pandas as pd
from googleapiclient.discovery import build
import os


API_KEY = os.environ['YOUTUBE_API_KEY']


def _fetch_data(**kwargs):
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        chart="mostPopular",
        regionCode="US",
        maxResults=50
    )
    response = request.execute()
    return response
