import requests
from bs4 import BeautifulSoup
import os

# Base URL for Common Crawl segments
base_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-30/segments/"

# Function to download WET files
def download_wet_file(file_url, output_dir="downloads"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    file_name = os.path.join(output_dir, file_url.split('/')[-1])
    print(f"Downloading {file_url} to {file_name}")
    
    response = requests.get(file_url, stream=True)
    
    if response.status_code == 200:
        with open(file_name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded: {file_name}")
    else:
        print(f"Failed to download: {file_url}")

# Function to scrape the segment pages
def scrape_segments(segment_id):
    url = f"{base_url}{segment_id}/wet/"
    print(f"Scraping URL: {url}")
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to access segment: {url}")
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all links to WET files
    links = soup.find_all('a')
    
    # Filter for links that end with .warc.wet.gz
    wet_files = [link['href'] for link in links if link['href'].endswith('.warc.wet.gz')]
    
    # Download each WET file
    for wet_file in wet_files:
        file_url = url + wet_file
        download_wet_file(file_url)

# Example usage: scrape a specific segment
segment_id = "1720763514450.42"  # Example segment ID
scrape_segments(segment_id)
