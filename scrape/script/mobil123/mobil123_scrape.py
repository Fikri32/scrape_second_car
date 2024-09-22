import requests
import json
import os
import random
import time
import csv
import pendulum
from bs4 import BeautifulSoup
from itertools import repeat
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
    return config


def fetch_data_with_retries(url, params, headers, max_retries=5, backoff_factor=0.3):
    """Fetch data from URL with retries in case of failure."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2**attempt)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("Max retries exceeded. Skipping to next data.")
                return None


def parse_car_data(soup):
    """Parse car data from BeautifulSoup object."""
    details_list = []
    script_tag = soup.find("script", type="application/ld+json")
    if script_tag:
        try:
            json_data = json.loads(script_tag.string)
            for item in json_data:
                if "@type" in item and "ItemList" in item["@type"]:
                    item_list_elements = item.get("itemListElement", [])
                    for list_item in item_list_elements:
                        item_details = list_item.get("item", {})
                        details = {
                            "Title": item_details.get("name", "N/A"),
                            "Price": item_details.get("offers", {}).get("price", "N/A"),
                            "KM": item_details.get("mileageFromOdometer", {}).get(
                                "value", "N/A"
                            ),
                            "Fuel Type": item_details.get("fuelType", "N/A"),
                            "Transmission": item_details.get("transmission", "N/A"),
                            "Location": item_details.get("offers", {})
                            .get("seller", {})
                            .get("homeLocation", {})
                            .get("address", {})
                            .get("addressLocality", "N/A"),
                            "Seats": item_details.get("seatingCapacity", "N/A"),
                            "Engine": item_details.get("engine", "N/A"),
                            "Link": item_details.get("mainEntityOfPage", "N/A"),
                        }
                        details_list.append(details)
        except json.JSONDecodeError:
            print("Error decoding JSON from script tag.")
    return details_list


def scrape_price_category(price, base_url, headers, user_agents, dated_dir):
    """Scrape data for a specific price category."""
    price_max = price.get("price_max")
    price_min = price.get("price_min")
    price_category = price.get("category")

    headers["User-Agent"] = random.choice(user_agents)
    params = {
        "vehicle_type": "car",
        "min_price": price_min,
        "max_price": price_max,
        "badge_operator": "OR",
        "boss": "true",
        "page_size": 25,
        "facets_all": "true",
        "page_number": 1,
        "_pjax": "#classified-listings-result",
    }

    response = fetch_data_with_retries(base_url, params, headers)
    if response:
        soup = BeautifulSoup(response.text, "html.parser")
        div = soup.find("div", class_="smenu box hard fixed transition--default")
        if div:
            data_smenu_params = div.get("data-smenu-params")
            if data_smenu_params:
                params_total = json.loads(data_smenu_params)
                total = params_total.get("total", 0)
                total_page = (total // 25) + (1 if total % 25 != 0 else 0)

                all_details = []
                for page in range(1, total_page + 1):
                    print(
                        f"Fetching page {page} of {total_page} for price category: {price_category}"
                    )
                    params["page_number"] = page
                    response = fetch_data_with_retries(base_url, params, headers)
                    if response:
                        soup = BeautifulSoup(response.text, "html.parser")
                        details_list = parse_car_data(soup)
                        all_details.extend(details_list)
                    else:
                        print(
                            f"Failed to fetch page {page} for price category: {price_category}. Skipping to next category."
                        )
                        break

                    # Sleep between requests to mimic human behavior
                    sleep_time = random.uniform(1.5, 3.0)
                    print(f"Sleeping for {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)

                if all_details:
                    csv_file_name = f"mobil123_{price_category}_{pendulum.now().format('YYYY-MM-DD')}.csv"
                    csv_file_path = os.path.join(dated_dir, csv_file_name)
                    with open(
                        csv_file_path, "w", newline="", encoding="utf-8"
                    ) as csv_file:
                        writer = csv.DictWriter(
                            csv_file, fieldnames=all_details[0].keys()
                        )
                        writer.writeheader()
                        writer.writerows(all_details)
                    print(
                        f"Data for price category {price_category} written to {csv_file_path}"
                    )
                else:
                    print(
                        f"No car data found for price category {price_category} on date: {pendulum.now().format('YYYY-MM-DD')}"
                    )
            else:
                print("No data parameters found.")
        else:
            print("Div with data parameters not found.")
    else:
        print(f"Failed to fetch data for price category: {price_category}")


def mobil123_scrape():
    """Main function to start scraping process for mobil123."""
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../../scrape/script/mobil123/config.json"
        )
    )
    config = load_config(config_path)

    now = pendulum.now().format("YYYY-MM-DD")
    output_base_dir = "/home/miracle/mobil/scrape/output/mobil123"
    dated_dir = os.path.join(output_base_dir, now)
    os.makedirs(dated_dir, exist_ok=True)

    price_categories = config["price"]
    base_url = "https://www.mobil123.com/mobil-bekas-dijual/indonesia"
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, seperti Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, seperti Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, seperti Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, seperti Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, seperti Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    ]

    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Sec-Ch-Ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Pjax-Container": "#classified-listings-result",
        "X-Requested-With": "XMLHttpRequest",
    }

    # Multithreading to handle multiple price categories
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                scrape_price_category, price, base_url, headers, user_agents, dated_dir
            ): price
            for price in price_categories
        }

        # Wait for all futures to complete and handle exceptions
        for future in as_completed(futures):
            price = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred for price category {price['category']}: {e}")


if __name__ == "__main__":
    mobil123_scrape()
