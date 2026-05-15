import os
from dotenv import load_dotenv
from googleapiclient.discovery import build

# Load .env
load_dotenv()

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")

# Create YouTube client
youtube = build("youtube", "v3", developerKey=API_KEY)

# -----------------------------
# GET CHANNEL INFO
# -----------------------------

channel_request = youtube.channels().list(
    part="contentDetails",
    forHandle=CHANNEL_HANDLE
)

channel_response = channel_request.execute()

# Get uploads playlist ID
uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

print("Uploads Playlist ID:")
print(uploads_playlist_id)

# -----------------------------
# GET VIDEOS
# -----------------------------

videos_request = youtube.playlistItems().list(
    part="snippet",
    playlistId=uploads_playlist_id,
    maxResults=5
)

videos_response = videos_request.execute()

print("\nVideos:\n")

for item in videos_response["items"]:

    video_title = item["snippet"]["title"]
    published_at = item["snippet"]["publishedAt"]

    print(f"Title: {video_title}")
    print(f"Published At: {published_at}")
    print("-" * 50)