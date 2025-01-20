import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import time
import re
import os
from typing import Set, Dict, List

def load_existing_urls(filename: str) -> Set[str]:
    """Load existing URLs from a CSV file if it exists."""
    existing_urls = set()
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            existing_urls = {row['url'] for row in reader}
    return existing_urls

def get_auction_data(url: str, existing_urls: Set[str]) -> tuple[List[Dict], bool]:
    """Fetch and parse auction data from a given URL. Returns (items, should_stop)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        items = []
        existing_count = 0  # Counter for existing items
        
        for product in soup.find_all('li', class_='Product'):
            try:
                # Get title and URL
                title_elem = product.find('a', class_='Product__titleLink')
                title = title_elem.text.strip() if title_elem else "N/A"
                item_url = title_elem['href'] if title_elem else "N/A"
                
                # Skip this item if it already exists, but continue processing others
                if item_url in existing_urls:
                    existing_count += 1
                    continue
                
                # Get price information
                price_info = product.find_all('span', class_='Product__priceValue')
                successful_bid = price_info[0].text.replace('円', '').replace(',', '') if len(price_info) > 0 else "N/A"
                initial_bid = price_info[1].text.replace('円', '').replace(',', '') if len(price_info) > 1 else "N/A"
                
                # Get number of bids
                bids_elem = product.find('a', class_='Product__bid')
                num_bids = bids_elem.text if bids_elem else "0"
                
                # Get time and date
                time_elem = product.find('span', class_='Product__time')
                if time_elem:
                    date_time = time_elem.text.strip()
                    date_str, time_str = date_time.split()
                    
                    current_month = datetime.now().month
                    auction_month = int(date_str.split('/')[0])
                    
                    current_year = datetime.now().year
                    auction_year = current_year
                    
                    if auction_month > current_month:
                        auction_year -= 1
                    
                    full_date = f"{auction_year}/{date_str} {time_str}"
                    dt = datetime.strptime(full_date, "%Y/%m/%d %H:%M")
                    date = dt.strftime("%Y/%m/%d")
                    time = dt.strftime("%H:%M")
                else:
                    date = "N/A"
                    time = "N/A"
                
                items.append({
                    'title': title,
                    'successful_bid': successful_bid,
                    'initial_bid': initial_bid,
                    'num_bids': num_bids,
                    'time': time,
                    'date': date,
                    'url': item_url
                })
                
            except Exception as e:
                print(f"Error processing item: {e}")
                continue
        
        # Only stop if most items on the page are existing ones
        should_stop = existing_count > len(soup.find_all('li', class_='Product')) * 0.8
        if should_stop:
            print(f"Found {existing_count} existing auctions on this page, stopping scrape.")
        
        return items, should_stop
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return [], False

def ensure_directory_exists(filepath: str) -> None:
    """Create directory if it doesn't exist."""
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

def scrape_all_pages(base_url: str, output_file: str) -> List[Dict]:
    """Scrape all pages and save to CSV, only adding new items."""
    # Create directory if it doesn't exist
    ensure_directory_exists(output_file)
    
    # Load existing URLs
    existing_urls = load_existing_urls(output_file)
    print(f"Found {len(existing_urls)} existing entries")
    
    all_items = []
    page = 1
    increment = 100
    fieldnames = ['title', 'successful_bid', 'initial_bid', 'num_bids', 
                 'time', 'date', 'url']
    
    # If file doesn't exist, create it with headers
    file_exists = os.path.exists(output_file)
    if not file_exists:
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    
    while True:
        current_url = f"{base_url}&b={1 + (page-1)*increment}&n={increment}"
        print(f"Scraping page {page}...")
        
        items, should_stop = get_auction_data(current_url, existing_urls)
        
        if not items or should_stop:
            break
        
        # Append new items to the CSV file
        with open(output_file, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            for item in items:
                writer.writerow(item)
        
        all_items.extend(items)
        page += 1
        
        # Add delay to avoid overloading the server
        time.sleep(2)
    
    return all_items

def create_base_url(query: str) -> str:
    """Create the base URL from a search query."""
    encoded_query = requests.utils.quote(query)
    return f"https://auctions.yahoo.co.jp/closedsearch/closedsearch?p=+{encoded_query}&va={encoded_query}"

if __name__ == "__main__":
    # List of kanji names without spaces
    kanji_names = [
        "伊藤理々杏", "岩本蓮加", "久保史緒里", "中村麗乃", "佐藤楓", "梅澤美波", "与田祐希", "吉田綾乃クリスティー",
        "遠藤さくら", "林瑠奈", "賀喜遥香", "金川紗耶", "黒見明香", "松尾美佑", "佐藤璃果", "柴田柚菜",
        "田村真佑", "筒井あやめ", "矢久保美緒", "弓木奈於", "一ノ瀬美空", "池田瑛紗", "井上和", "五百城茉央",
        "川﨑桜", "小川彩", "岡本姬奈", "奥田いろは", "中西アルノ", "菅原咲月", "冨里奈央"
    ]
    
    directory = "/public"  # Change this to save in a different directory

    for name in kanji_names:
        print(f"Starting search for: {name}")
        base_url = create_base_url(name)
        output_file = os.path.join(directory, f'yahoo_auction_{name}.csv')
        items = scrape_all_pages(base_url, output_file)
        print(f"Added {len(items)} new items for {name}. Data saved to {output_file}")
