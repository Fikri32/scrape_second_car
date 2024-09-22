import requests
from bs4 import BeautifulSoup
import json
import time
import csv
import random
import pendulum
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import repeat
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def load_config():
    # Load configuration from config.json
    config_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../../scrape/script/oto/config.json"
        )
    )
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
    return config


def fetch_data_with_retries(url, params, headers, max_retries=5, backoff_factor=0.3):
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


def send_kafka(car_data):
    try:
        producer.send("oto-scrape", car_data)
        producer.flush()
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")
        raise


def parse_car_data(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    cars = []
    for car in soup.find_all("li", class_="card"):
        car_data = {
            "Title": (
                car.find("a", class_="vh-name").text.strip()
                if car.find("a", class_="vh-name")
                else None
            ),
            "Price": (
                car.find("div", class_="vh-price").text.strip()
                if car.find("div", class_="vh-price")
                else None
            ),
            "KM": (
                car.find("ul", class_="list-bullet").find_all("li")[0].text.strip()
                if car.find("ul", class_="list-bullet")
                else None
            ),
            "Fuel Type": (
                car.find("ul", class_="list-bullet").find_all("li")[1].text.strip()
                if car.find("ul", class_="list-bullet")
                and len(car.find("ul", class_="list-bullet").find_all("li")) > 1
                else None
            ),
            "Transmission": (
                car.find("ul", class_="list-bullet").find_all("li")[2].text.strip()
                if car.find("ul", class_="list-bullet")
                and len(car.find("ul", class_="list-bullet").find_all("li")) > 2
                else None
            ),
            "Location": (
                car.find("ul", class_="used-car-list-card-tags")
                .find_all("li")[0]
                .text.strip()
                if car.find("ul", class_="used-car-list-card-tags")
                else None
            ),
            "Seats": (
                car.find("ul", class_="used-car-list-card-tags")
                .find_all("li")[1]
                .text.strip()
                if car.find("ul", class_="used-car-list-card-tags")
                and len(car.find("ul", class_="used-car-list-card-tags").find_all("li"))
                > 1
                else None
            ),
            "Engine": (
                car.find("ul", class_="used-car-list-card-tags")
                .find_all("li")[2]
                .text.strip()
                if car.find("ul", class_="used-car-list-card-tags")
                and len(car.find("ul", class_="used-car-list-card-tags").find_all("li"))
                > 2
                else None
            ),
            "Link": (
                "https://www.oto.com" + car.find("a", class_="vh-name").get("href")
                if car.find("a", class_="vh-name")
                else None
            ),
            "Source": ("oto"),
        }
        cars.append(car_data)
        send_kafka(car_data)
        # print(send_kafka(car_data))
    return cars


def scrape_price_category(price, base_url, headers, user_agents, dated_dir):
    safe_price = price.replace(" ", "_").replace("/", "_").replace("-", "_")
    params = {
        "filter": price,
        "order": "score",
        "orderBy": "desc",
        "business_unit": "mobil",
        "lang_code": "id",
        "langCode": "id",
        "country_code": "id",
        "countryCode": "id",
        "isServer": "0",
        "range": "{}",
    }
    all_cars = []

    headers["User-Agent"] = random.choice(user_agents)
    response = fetch_data_with_retries(base_url, params, headers)
    if response:
        data = response.json()
        total_pages = data.get("totalPage", 1)

        # Multithreading to scrape multiple pages simultaneously
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    scrape_page, page, price, params, headers, user_agents, base_url
                ): page
                for page in range(1, total_pages + 1)
            }
            for future in as_completed(futures):
                page_cars = future.result()
                if page_cars:
                    all_cars.extend(page_cars)

    if all_cars:
        csv_file_name = (
            f'oto_car_{safe_price}_{pendulum.now().format("YYYY-MM-DD")}.csv'
        )
        csv_file_path = os.path.join(dated_dir, csv_file_name)
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=all_cars[0].keys())
            writer.writeheader()
            writer.writerows(all_cars)
        print(f"Data for price category {price} written to {csv_file_path}")
    else:
        print(f"No car data found for price category {price}")


def scrape_page(page, price, params, headers, user_agents, base_url):
    print(f"Fetching page {page} for price category: {price}")
    params["page"] = page
    headers["User-Agent"] = random.choice(user_agents)
    page_response = fetch_data_with_retries(base_url, params, headers)
    if page_response:
        page_data = page_response.json()
        if "html" in page_data:
            html_content = page_data["html"]
            return parse_car_data(html_content)
    else:
        print(f"Failed to fetch page {page} for price category: {price}.")
    return []


def oto_scrape():
    config = load_config()
    price_categories = config["price"]
    base_url = "https://www.oto.com/used/get-inventory"
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

    now = pendulum.now().format("YYYY-MM-DD")
    output_base_dir = "/home/miracle/mobil/scrape/output/oto"
    dated_dir = os.path.join(output_base_dir, now)
    os.makedirs(dated_dir, exist_ok=True)

    headers = {"User-Agent": random.choice(user_agents)}

    # Multithreading to handle multiple price categories
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(
            scrape_price_category,
            price_categories,
            repeat(base_url),
            repeat(headers),
            repeat(user_agents),
            repeat(dated_dir),
        )


if __name__ == "__main__":
    oto_scrape()
