import datetime
import pandas as pd
import logging
import boto3
import os
from io import StringIO

def _process_data(**kwargs):
    ti = kwargs['ti']
    response = ti.xcom_pull(task_ids="fetch_data")
    logging.info(response)
    items = response['items']
    data = []
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
    for i in range(len(items)):
        video_title = response['items'][i]['snippet'].get('title', None)
        published_at = response['items'][i]['snippet'].get('publishedAt', None)
        channel_title = response['items'][i]['snippet'].get(
            'channelTitle', None)
        tags = response['items'][i]['snippet'].get('tags', None)
        category_id = response['items'][i]['snippet'].get('categoryId', None)
        language = response['items'][i]['snippet'].get('defaultLanguage', None)
        view_count = response["items"][i]['statistics'].get('viewCount', None)
        like_count = response["items"][i]['statistics'].get('likeCount', None)
        comment_count = response["items"][i]['statistics'].get('commentCount', None)
        thumbnails = response["items"][i]['snippet'].get('thumbnails', None)
        video_id = response['items'][i].get("id", None)

        data.append({"video_id": video_id,
                    "title": video_title,
                    "published_at": published_at,
                     "channel_title": channel_title,
                     "thumbnails": thumbnails.get('high', {}).get('url', None),
                     "category_id": category_id,
                     "language": language,
                     "view_count": view_count,
                     "like_count": like_count,
                     "comment_count": comment_count,
                     "timestamp": timestamp
                     }
                    )
        

    df = pd.DataFrame(data)
    jsonf_buffer = StringIO()
    df.to_json(jsonf_buffer, orient="records", lines=True)
    jsonf_buffer.seek(0)
    json_data = jsonf_buffer.getvalue()
    s3_key = f"trending_videos_{timestamp}.json"

    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                      region_name=os.environ['AWS_REGION']
                    )


    bucket_name = 'youtube-trending-data'
    s3.put_object(Bucket=bucket_name,
                  Key=s3_key,
                  Body=json_data,
                  ContentType='application/json'
                )
    logging.info(f"File uploaded successfully to {bucket_name}")

