import json
import re

import logging
import sys

import requests
from bs4 import BeautifulSoup

from fp.fp import FreeProxy

SEARCH_API = "https://www.screener.in/api/company/search/?q="
SCREENER_URL = "https://www.screener.in"


class Screener:
    """
    This class acts as a Base for Screener related process
    :param ticker: symbol of stock
    """

    def __init__(self, ticker):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Invoked Screener Module for {ticker}")
        self.ticker = ticker
        self.url = SCREENER_URL + self._get_url()
        self.soup = self.get_soup()
        self.proxy = FreeProxy().get()
        self.logger.info(f'{self.ticker} : {self.proxy}')

    def _get_url(self):
        """
        Encapsulated function to get url for a particular stock from the screener site
        :return:
        """
        response = requests.get(SEARCH_API + self.ticker)
        return response.json()[0]['url']

    def get_soup(self):
        self.url = requests.get(self.url).text
        soup = BeautifulSoup(self.url, "html.parser")
        return soup

    def stock_information(self):
        """
        Retrieves stock information from the self.soup object and returns a dictionary containing the following information:

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
