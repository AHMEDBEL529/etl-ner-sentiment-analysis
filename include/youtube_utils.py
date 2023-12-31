import spacy
from googleapiclient.discovery import build
from textblob import TextBlob
from include.global_variables import global_variables as gv


API_KEY = ''
VIDEO_ID = gv.MY_VIDEO_ID

youtube = build('youtube', 'v3', developerKey=API_KEY)

def get_comments_from_youtube(video_id, max_results_per_page=10, number_of_pages=10):
    comments = []
    page_token = None  # This will change as we paginate

    for _ in range(number_of_pages):
        results = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results_per_page,
            textFormat='plainText',
            pageToken=page_token  # Start with None, then fill with nextPageToken
        ).execute()

        for item in results['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)

        # Check if there's a nextPageToken. If not, break out of loop
        page_token = results.get("nextPageToken")
        if not page_token:
            break

    return comments

