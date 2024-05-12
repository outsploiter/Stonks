import json
import random
import re

import logging
import sys

from requests import Session, session
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

from fp.fp import FreeProxy

SEARCH_API = "https://www.screener.in/api/company/search/?q="
SCREENER_URL = "https://www.screener.in"

file_path = r'C:\Users\suwee\CodeBase\_ Personal Projects\Stonks\proxies.txt'

proxies = []
with open(file_path, 'r') as file:
    for line in file:
        line = line.strip()  # Remove extra whitespace characters
        if line:
            proxies.append(line)


class Screener:
    """
    This class acts as a Base for Screener related process
    :param ticker: symbol of stock
    """

    def __init__(self, ticker, name):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Invoked Screener Module for {ticker}")
        self.name = name
        self.ticker = ticker
        random_number = random.randint(0, len(proxies)-1)
        self.session = session()
        self.proxy = proxies[random_number]
        session().proxies['http'] = f'http://{self.proxy}'
        session().proxies['https'] = f'https://{self.proxy}'
        retries = Retry(total=3,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504, 400, 401, 402, 403])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.timeout = 30  # timeout for 30 seconds
        self.url = SCREENER_URL + self._get_url()
        self.soup = self.get_soup()

    def _get_url(self):
        """
        Encapsulated function to get url for a particular stock from the screener site
        :return:
        """
        try:
            response = self.session.get(SEARCH_API + self.ticker)
            if response.status_code == 200:
                print(f"Proxy {self.proxy} is working.")
            search_list = response.json()
            if len(search_list) == 0:
                response = self.session.get(SEARCH_API + self.name)
                search_list = response.json()
            return search_list[0]['url']
        except Exception as e:
            print('getting url', e)
            print(response)
            sys.exit()

    def get_soup(self):
        try:
            self.url = self.session.get(self.url).text
            soup = BeautifulSoup(self.url, "html.parser")
            return soup
        except Exception as e:
            print("getting soup", e)
            sys.exit()

    def stock_information(self):
        """
        Retrieves stock information from the self.soup object and
        returns a dictionary containing the following information:

        Parameters:
        - self: The instance of the class.

        Returns:
        - stock_info: A dictionary containing the following keys:
            - name: The name of the stock.
            - about: A brief description of the stock.
            - link: The link to the stock's website.
            - bse_link: The link to the stock's BSE page.
            - nse_link: The link to the stock's NSE page (empty string if not available).
            - sector: The sector the stock belongs to.
            - industry: The industry the stock belongs to.
        """
        stock_info = {}

        name = self.soup.select(
            "#top > div.flex.flex-space-between.flex-gap-8 > div > h1"
        )[0].text.strip()
        link = self.soup.select(
            "#top > div.company-links.show-from-tablet-landscape > a:nth-child(1)"
        )[0].attrs["href"]
        about = self.soup.select("#top .company-info .company-profile p")[0].text
        bse_link = self.soup.select(
            "#top > div.company-info > div.company-profile > div.company-links.hide-from-tablet-landscape.margin-top-20 > a:nth-child(2)"
        )[0].attrs["href"]
        try:
            nse_link = self.soup.select(
                "#top > div.company-info > div.company-profile > div.company-links.hide-from-tablet-landscape.margin-top-20 > a:nth-child(3)"
            )[0].attrs["href"]
        except:
            nse_link = ""

        sector = self.soup.select("#peers")[0]
        sector_industry = [
            term
            for term in re.split(
                "\s\s", sector.find_all(class_="sub")[0].text.replace("\n", "").strip()
            )
            if term != ""
        ]
        sector = sector_industry[1]
        industry = sector_industry[3]

        stock_info["name"] = name
        stock_info["about"] = about
        stock_info["link"] = link
        stock_info["bse_link"] = bse_link
        stock_info["nse_link"] = nse_link
        stock_info["sector"] = sector
        stock_info["industry"] = industry
        return stock_info


# screen = Screener('ICICIBANK')
# a = screen.stock_information()
# print(a)
