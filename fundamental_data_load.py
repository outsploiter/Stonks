import os
import sys
import time
import threading
import random

from queue import Queue

import requests
from bs4 import BeautifulSoup

from utils import proxy_scraper, proxy_checker, stockload, extract_yearly_data


INDEX_ID = 1
db_utils = stockload.DBUtils()


# Function to read proxies from the file
def load_proxies(proxies_file):
    # if file not found or if file older than a half hour
    if not os.path.isfile(proxies_file) or time.time() - os.path.getmtime(proxies_file) > 60 * 60 / 4:
        proxy_scraper.scraper(proxy='http', output=proxies_file, verbose=False)
        proxy_checker.checker(file=proxies_file, verbose=False)
    with open(proxies_file, 'r') as f:
        proxies = [line.strip() for line in f]
    duplicate_check = set()
    proxy_without_duplicate = []
    for proxy in proxies:
        if proxy.split(':')[0] not in duplicate_check:
            proxy_without_duplicate.append(proxy)
            duplicate_check.add(proxy.split(':')[0])
    print(f'Found {len(proxy_without_duplicate)} proxies to use')
    return proxy_without_duplicate


def load_user_agents(user_agents_file):
    user_agents = []
    with open(user_agents_file, "r") as f:
        for line in f:
            user_agents.append(line.replace("\n", ""))
    return user_agents


# Function to make requests and save the content
def fetch_url(queue, proxy_list, user_agent_list):
    while not queue.empty():
        proxy_str = random.choice(proxy_list)
        user_agent = random.choice(user_agent_list)
        headers = {'User-Agent': user_agent}
        stock_id, url = queue.get()
        print(f'Processing: {stock_id}->{url}')
        proxy = {'http': f'http://{proxy_str}'}
        try:
            response = requests.get(url, timeout=10, headers=headers)
            print('Response:', response.status_code)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'lxml')
                if soup == '':
                    print(f'Check this URL -> returned empty content')
                    continue
                col_headers, yearly_data, is_standalone = extract_yearly_data.extract_yearly_data_from_soup(soup)
                if is_standalone:
                    print(f"Processing Standalone: {stock_id}->{url.replace('/consolidated/', '/')}")
                    response = requests.get(url.replace('/consolidated/', '/'), timeout=10, headers=headers)
                    print('Response:', response.status_code)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'lxml')
                        if soup == '':
                            print(f'Check this URL -> returned empty content')
                            continue
                        col_headers, yearly_data, is_standalone = \
                            extract_yearly_data.extract_yearly_data_from_soup(soup)
                    if is_standalone:
                        print(f"soup is still doubtful {url.replace('/consolidated/', '/')}\n")
                        continue
                db_utils.upsert_soup(stock_id, str(soup))
                db_utils.upsert_yearly_fundamentals(stock_id, col_headers, yearly_data)
                print()
            else:
                print(f"Failed to fetch url with status code {response.status_code} with proxy : {proxy_str}")
            time.sleep(1)
        except requests.RequestException as e:
            print(f"Request failed for {url}: {e}")
            continue
        finally:
            queue.task_done()

def scrape_soup(sector, urls, stock_id_list, proxies, user_agents):
    print(f"Starting to scrape for sector: {sector} with no of urls: {len(urls)}")
    queue = Queue()
    for index, url in enumerate(urls):
        queue.put((stock_id_list[index], url))

    # Define the number of threads
    num_threads = min(len(urls), 3)

    # Create and start threads
    threads = []
    for i in range(num_threads):
        try:
            thread = threading.Thread(target=fetch_url, args=(queue, proxies, user_agents))
            thread.start()
            threads.append(thread)
        except Exception as e:
            print(f"Failed to scrape proxy: {proxies}: {e}")
            continue

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Without Threads
    # fetch_url(queue, proxies, user_agents)

    print("Scraping completed for sector: {}".format(sector))


# Main function to set up the threads and start scraping
def main():
    proxies = load_proxies('proxies.txt')
    user_agents = load_user_agents('user_agents.txt')
    urls = db_utils.get_stock_urls(index_id=INDEX_ID)
    for sector, url_list, stock_id_list in urls:
        scrape_soup(sector, url_list, stock_id_list, proxies, user_agents)

if __name__ == "__main__":
    main()