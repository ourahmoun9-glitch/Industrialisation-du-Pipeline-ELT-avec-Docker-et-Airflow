import os
import requests
import json
from airflow.decorators import task
from datetime import date

maxResults = 50

@task()
def get_playlist_id():
    api_key = os.environ.get("API_KEY")
    channel_handle = os.environ.get("CHANNEL_HANDLE")
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

@task()
def get_video_ids(playlist_id):
    api_key = os.environ.get("API_KEY")
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={api_key}"
    page_token = None
    video_ids = []
    while True:
        url = base_url + (f"&pageToken={page_token}" if page_token else "")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return video_ids

@task()
def extract_video_details(video_ids):
    api_key = os.environ.get("API_KEY")
    extracted_data = []
    for i in range(0, len(video_ids), maxResults):
        batch = video_ids[i:i + maxResults]
        url = (
            f"https://youtube.googleapis.com/youtube/v3/videos?"
            f"part=snippet,contentDetails,statistics"
            f"&id={','.join(batch)}&key={api_key}"
        )
        response = requests.get(url)
        response.raise_for_status()
        for item in response.json().get("items", []):
            extracted_data.append({
                "video_id":     item["id"],
                "title":        item["snippet"]["title"],
                "publishedAt":  item["snippet"]["publishedAt"],
                "duration":     item["contentDetails"]["duration"],
                "viewCount":    item["statistics"].get("viewCount"),
                "likeCount":    item["statistics"].get("likeCount"),
                "commentCount": item["statistics"].get("commentCount"),
            })
    return extracted_data

@task()
def save_to_json(extracted_data):
    os.makedirs("/opt/airflow/data", exist_ok=True)
    file_path = f"/opt/airflow/data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)
    print(f"✅ Saved {len(extracted_data)} videos to {file_path}")