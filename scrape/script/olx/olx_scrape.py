import requests
import json
import time
import random
import pendulum
import os

# Configuration
base_url = "https://www.olx.co.id/api/relevance/v4/search"
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.2; rv:86.0) Gecko/20100101 Firefox/86.0",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 11; Pixel 4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.82 Mobile Safari/537.36"
]

# Load configuration from file
config = json.load(open('config.json'))
price_categories = config["price"]

# Timestamp for file naming
now = pendulum.now().format('YYYY-MM-DD')

# Output directory
output_dir = '/home/miracle/mobil/scrape/output/olx'
os.makedirs(output_dir, exist_ok=True)

def fetch_data_with_retries(url, params, headers, max_retries=5, backoff_factor=0.3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed with error: {e}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("Max retries exceeded. Skipping to next data.")
                return None

for price in price_categories:
    print(f"{price}")
    params = {
        "category": 198,
        "facet_limit": 100,
        "location": 1000001,
        "location_facet_limit": 20,
        "platform": "web-desktop",
        "price_max": price.get('price_max'),
        "price_min": price.get('price_min'),
        "relaxedFilters": True,
        "user": "190f4f45433x29fb0238",
        "size": 40  # Set default size per page
    }
    print(f"Fetching data for price category: {price['harga']} on date: {now}")
    print(f"{params}")

    headers = {
        "User-Agent": random.choice(user_agents)
    }

    response = fetch_data_with_retries(base_url, params, headers)
    if response:
        data = response.json()
        total_pages = data.get("metadata", {}).get("total_pages", 1)  # Get the total pages
        current_page = 1
        all_cars = []
        
        while current_page <= total_pages:
            print(f"Fetching page {current_page} of {total_pages} for price category: {price['harga']}")
            params["page"] = current_page
            headers["User-Agent"] = random.choice(user_agents)
            page_response = fetch_data_with_retries(base_url, params, headers)
            if page_response:
                page_data = page_response.json()
                all_cars.extend(page_data.get("data", []))
                print(f"Page {current_page} fetched, {len(page_data.get('data', []))} items found.")
            current_page += 1
        
        print(f"Total items fetched for price category {price['harga']}: {len(all_cars)}")
    else:
        print('Failed to fetch data or response status is not OK')
