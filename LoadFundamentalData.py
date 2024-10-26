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


# Proxy and User Agent Manager
class ProxyManager:
    def __init__(self, proxies_file, user_agents_file):
        self.proxies_file = proxies_file
        self.user_agents_file = user_agents_file
        self.proxies = self.load_proxies()
        self.user_agents = self.load_user_agents()

    def load_proxies(self):
        if not os.path.isfile(self.proxies_file) or time.time() - os.path.getmtime(self.proxies_file) > 60 * 60 / 4:
            proxy_scraper.scraper(proxy='http', output=self.proxies_file, verbose=False)
            proxy_checker.checker(file=self.proxies_file, verbose=False)

        with open(self.proxies_file, 'r') as f:
            proxies = {line.strip().split(':')[0]: line.strip() for line in f}
        print(f'Found {len(proxies)} unique proxies to use')
        return list(proxies.values())

    def load_user_agents(self):
        with open(self.user_agents_file, "r") as f:
            return [line.strip() for line in f if line.strip()]

    def get_random_proxy(self):
        return random.choice(self.proxies)

    def get_random_user_agent(self):
        return random.choice(self.user_agents)


# Function to make requests and save the content
def fetch_url(queue, proxy_manager):
    while not queue.empty():
        stock_id, url = queue.get()
        print(f'Processing: {stock_id}->{url}')

        headers = {'User-Agent': proxy_manager.get_random_user_agent()}
        proxy = {'http': f"http://{proxy_manager.get_random_proxy()}"}

        try:
            response = requests.get(url, timeout=10, headers=headers, proxies=proxy)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'lxml')
                if not soup:
                    print(f'Empty content for {url}')
                    continue
                col_headers, yearly_data, is_standalone = extract_yearly_data.extract_yearly_data_from_soup(soup)

                if is_standalone:
                    new_url = url.replace('/consolidated/', '/')
                    print(f"Switching to Standalone URL: {new_url}")
                    response = requests.get(new_url, timeout=10, headers=headers, proxies=proxy)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'lxml')
                        col_headers, yearly_data, _ = extract_yearly_data.extract_yearly_data_from_soup(soup)

                db_utils.upsert_soup(stock_id, str(soup))
                db_utils.upsert_yearly_fundamentals(stock_id, col_headers, yearly_data)
            else:
                print(f"Failed to fetch URL {url} with status code {response.status_code}")
            time.sleep(1)
        except requests.RequestException as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()


def scrape_soup(sector, urls, stock_id_list, proxy_manager):
    print(f"Scraping for sector: {sector} with {len(urls)} URLs")
    queue = Queue()
    for stock_id, url in zip(stock_id_list, urls):
        queue.put((stock_id, url))

    threads = [threading.Thread(target=fetch_url, args=(queue, proxy_manager)) for _ in range(min(len(urls), 5))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print(f"Scraping completed for sector: {sector}")


# Main function to set up the threads and start scraping
def main():
    proxy_manager = ProxyManager('proxies.txt', 'user_agents.txt')
    urls_data = db_utils.get_stock_urls(index_id=INDEX_ID)
    for sector, url_list, stock_id_list in urls_data:
        scrape_soup(sector, url_list, stock_id_list, proxy_manager)


if __name__ == "__main__":
    main()
