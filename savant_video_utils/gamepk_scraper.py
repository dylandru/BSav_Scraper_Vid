from pybaseball import statcast
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
import os
from bs4 import BeautifulSoup

session = requests.Session()

def download_video(video_url, save_path):
    try:
        with session.get(video_url, stream=True) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Video downloaded to {save_path}")
    except Exception as e:
        print(f"Error downloading video {video_url}: {e}")

def get_video_url(page_url):
    try:
        response = session.get(page_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        video_container = soup.find('div', class_='video-box')
        if video_container:
            return video_container.find('video').find('source', type='video/mp4')['src']
    except Exception as e:
        print(f"Error fetching video URL from {page_url}: {e}")
    return None


def fetch_game_data(game_pk):
    """Fetch game data for a single game_pk using the global session"""
    url = f'https://baseballsavant.mlb.com/gf?game_pk={game_pk}'
    response = session.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def process_game_data(game_data, pitch_call=None):
    """Process game data and filter by pitch_call if provided."""
    team_home_data = game_data.get('team_home', [])
    df = pd.json_normalize(team_home_data)
    for entry in df:
        df['game_pk'] = df['game_pk']
    if pitch_call:
        df = df.loc[df['pitch_call'] == pitch_call]
    return df

def playids_for_date_range(start_date: str, end_date: str, team: str = None, pitch_call: str = None):
    """
    Retrieves PlayIDs for games played within a date range. Optionally filters by team or pitch call.
    """
    statcast_df = statcast(start_dt=start_date, end_dt=end_date, team=team)
    game_pks = statcast_df['game_pk'].unique()

    dfs = [process_game_data(fetch_game_data(game_pk), pitch_call=pitch_call) for game_pk in game_pks]

    play_id_df = pd.concat(dfs, ignore_index=True)
    return play_id_df
    

def get_video_for_play_id(play_id, game_pk, download_folder):
    """Process a single play ID to download the corresponding video."""
    page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
    try:
        video_url = get_video_url(page_url)
        if video_url:
            save_path = os.path.join(download_folder, f"{game_pk}_{play_id}.mp4") #Video currently named for Play ID
            download_video(video_url, save_path)
        else:
            print(f"No video found for playId {play_id}")
    except Exception as e:
        print("Unable to complete request.")