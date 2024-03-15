import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor

#Main Function to Scrape Baseball Savant Video based on CSV containing MLB Play IDs in Column ['playID'] and save to selected folder
#Utilizes ThreadPoolExecutor to Quicken Scraping Process for Large Number of Videos - will default to 5 workers unless specified

def download_video(video_url, save_path):
	"""Chunks and saves video to folder path"""	
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
	"""Finds video within Savant Page URL"""
    try:
        response = session.get(page_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        video_container = soup.find('div', class_='video-box')
        if video_container:
            return video_container.find('video').find('source', type='video/mp4')['src']
    except Exception as e:
        print(f"Error fetching video URL from {page_url}: {e}")
    return None

def process_row(row, download_folder):
	"""Utilizes given row's Play ID to fetch and download video"""
    play_id = row['playId']
    page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
    video_url = get_video_url(page_url)
    if video_url:
        save_path = os.path.join(download_folder, f"{play_id}.mp4") #Specifies name of video as the Play ID
        download_video(video_url, save_path)
    else:
        print(f"No video found for playId {play_id}")

def run_scraper(reference_sheet: str, download_folder: str, max_workers: int = 5):
	"""Main function to run scraper"""
	session = requests.Session()
    df = pd.read_csv(reference_sheet)
    if 'playID' in df.columns:
    	os.makedirs(download_folder, exist_ok=True) #Creates directory if specified directory does not exist
    	with ThreadPoolExecutor(max_workers=max_workers) as executor:
        	for index, row in df.iterrows():
            	executor.submit(process_row, row, download_folder)
    else:
    	print("CSV does not contain column 'playID'")
    	

"""EXAMPLE CALL"""

#if __name__ == "__main__":
#    reference_sheet = "playID/path/csv.csv"
#    download_folder = "download/path/folder"
#    max_workers = 10
#    run_scraper(reference_sheet_path, download_folder, max_workers)

    
	