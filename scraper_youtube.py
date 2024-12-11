import os
import pandas as pd
from googleapiclient.discovery import build
from sqlalchemy import create_engine
import traceback

API_KEY = 'AIzaSyAGF6sBJNOQIhhSuRmjw_tYy4jkvXadpgA'

youtube = build('youtube', 'v3', developerKey=API_KEY)

def search_videos(query, max_results=50):
    video_ids = []
    request = youtube.search().list(
        part='id',
        q=query,
        type='video',
        maxResults=max_results,
        order='relevance'
    )

    response = request.execute()

    for item in response['items']:
        if item['id']['kind'] == 'youtube#video':
            video_ids.append(item['id']['videoId'])

    return video_ids

def get_video_comments(video_id, max_comments=500):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            textFormat='plainText',
            maxResults=100
        )
        response = request.execute()

        while response:
            for item in response['items']:
                top_comment = item['snippet']['topLevelComment']['snippet']
                comment_info = {
                    'video_id': video_id,
                    'created_at': top_comment['publishedAt'],
                    'author_name': top_comment['authorDisplayName'],
                    'full_text': top_comment['textDisplay'],
                    'like_count': top_comment['likeCount']
                }
                comments.append(comment_info)

                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        reply_info = {
                            'video_id': video_id,
                            'created_at': reply['snippet']['publishedAt'],
                            'author_name': reply['snippet']['authorDisplayName'],
                            'full_text': reply['snippet']['textDisplay'],
                            'like_count': reply['snippet']['likeCount']
                        }
                        comments.append(reply_info)

                if len(comments) >= max_comments:
                    return comments

            if 'nextPageToken' in response:
                response = youtube.commentThreads().list(
                    part='snippet,replies',
                    videoId=video_id,
                    pageToken=response['nextPageToken'],
                    textFormat='plainText',
                    maxResults=100
                ).execute()
            else:
                break
    except Exception as e:
        print(f"Error fetching comments for video {video_id}: {e}")

    return comments

def scrape_youtube_comments(search_keyword, max_results, max_comments, db_config):
    video_ids = search_videos(search_keyword, max_results)

    all_comments = []
    for video_id in video_ids:
        if len(all_comments) >= max_comments:
            break
        comments_data = get_video_comments(video_id, max_comments=max_comments - len(all_comments))
        all_comments.extend(comments_data)

    if not all_comments:
        raise ValueError("No comments scraped.")

    df = pd.DataFrame(all_comments)

    df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce', downcast='integer')
    df['created_at'] = pd.to_datetime(df['created_at'])

    print(df.head())

    df['idyoutube_datasets'] = None
    df['main_categories_idmain_categories'] = 3  

    print("Saving to database...")

    db_connection_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
    engine = create_engine(db_connection_url)

    df.to_sql('youtube_datasets', con=engine, if_exists='append', index=False)
    print(f"{len(df)} comments saved to database.")
