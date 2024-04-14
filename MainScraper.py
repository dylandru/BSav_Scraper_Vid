from pybaseball import statcast
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from bs4 import BeautifulSoup
from savant_video_utils import download_video, get_video_url, fetch_game_data, process_game_data, playids_for_date_range, get_video_for_play_id



def run_statcast_pull_scraper(start_date: str, end_date: str, download_folder: str, max_workers: int = 5, team: str = None, pitch_call: str = None):
    """Run scraper from Statcast Pull of Play IDs. Retrieves data and processes each row in parallel."""
    session = requests.Session()
    df = playids_for_date_range(start_date=start_date, end_date=end_date, team=team, pitch_call=pitch_call)

    if not df.empty and 'play_id' in df.columns:
        os.makedirs(download_folder, exist_ok=True)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_play_id = {executor.submit(get_video_for_play_id, row['play_id'], download_folder): row for _, row in df.iterrows()}
            for future in as_completed(future_to_play_id):
                play_id = future_to_play_id[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing Play ID {play_id['play_id']}: {str(e)}")
    else:
        print("Play ID column not in Statcast pull or DataFrame is empty")

def run_csv_pull_scraper(reference_sheet: str, download_folder: str, max_workers: int = 5):
    """
    Run scraper from CSV containing specified Play IDs to scrape video.
    """
    session = requests.Session()
    df = pd.read_csv(reference_sheet)
    if not df.empty and 'playId' in df.columns: 
        os.makedirs(download_folder, exist_ok=True)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_play_id = {executor.submit(get_video_for_play_id, row['play_id'], download_folder): row for _, row in df.iterrows()}
            for future in as_completed(future_to_play_id):
                play_id = future_to_play_id[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing Play ID {play_id['playId']}: {str(e)}")
    else:
        print("playId column not in Statcast pull or DataFrame is empty")
 

""" EXAMPLE CALL """
        
#if __name__ == "__main__":
#    download_folder = "/Users/dylandrummey/Downloads/DylanDru_GitHub/Baseball-Savant-Video-Scraper/savant_video_utils/test1/"
#    run_statcast_pull_scraper(start_date="2023-04-12", end_date="2023-04-12", download_folder=download_folder)

	
