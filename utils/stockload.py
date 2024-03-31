import json
import os

import dotenv
import logging

import psycopg2
from psycopg2 import Error
import pandas as pd


class DBUtils:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        dotenv.load_dotenv()
        self.logger.debug("DB Utils Initiated")

    def get_connection(self, db_params):
        try:
            connection = psycopg2.connect(**db_params)
            self.logger.debug("Connection Created!")
            return connection
        except (Exception, Error) as error:
            self.logger.error(f"Error initiating DB connection {error}\n", exc_info=True)

    def get_index_info(self, index):
        db_params = json.loads(os.getenv("STONKS_DB_CREDS").replace("'", "\""))
        conn = self.get_connection(db_params)
        cursor = conn.cursor()
        query = f"""select index_id, name, link, linktype, country, modifiedon
                    from index_base ib
                    where name = '{index.upper()}'"""
        try:
            cursor.execute(query)
            result = cursor.fetchmany()
            self.logger.debug("Fetch from Index Base Successful")
            return result
        except (Exception, Error):
            self.logger.error(f"Could not fetch from Index Base\n{query}", exc_info=True)
        finally:
            cursor.close()
            conn.close()
            self.logger.debug("Closing Stonks DB connections")

    def upsert_stocks(self, stock_df):
        db_params = json.loads(os.getenv("STONKS_DB_CREDS").replace("'", "\""))
        conn = self.get_connection(db_params)
        cursor = conn.cursor()

        query = f"""
        INSERT INTO stock_base (index_id, symbol, name, date_of_listing, isin_number)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (index_id, symbol)
        DO NOTHING;
        """

        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in stock_df.values]

        try:
            # Execute the upsert operation
            cursor.executemany(query, data)
            conn.commit()
            self.logger.info(f"Upsert into stock base successful for {stock_df.shape[0]} rows")
        except (Exception, Error) as err:
            self.logger.error(f"Could not upsert into Stock Base\n{query}", exc_info=True)
        finally:
            conn.close()
            cursor.close()
            self.logger.debug("Closing Stonks DB connections")


class IndexUtils:
    def __init__(self, index):
        self.index_id, self.name, self.link, \
            self.link_type, self.country, self.modifiedon = index
        self.logger = logging.getLogger(__name__)
        # with headers read csv might fail
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'
        }
        self.logger.info(f"Index {self.name} Initialized")

    def get_stock_details(self):
        self.logger.debug(f"Starting to get data from link {self.link}")
        if self.link_type.lower() == 'csv' and 'csv' in self.link.lower():
            csv_df = self.get_csv_data()
            formatted_df = self.handle_csv_data(csv_df)
            return formatted_df

    def get_csv_data(self):
        """
        This function Read dataframe from the index's link(csv)
        :return: dataframe from csv link
        """
        try:
            csv_df = pd.read_csv(str(self.link), storage_options=self.headers)
            self.logger.debug("Read CSV from the link Successful")
            return csv_df
        except Exception as err:
            self.logger.error(f"Cannot Read CSV from the link {self.link}\n{err}", exc_info=True)

    def handle_csv_data(self, csv_df):
        """
        This function handles nse based data and creates/renames the required the columns
        :param csv_df: dataframe read from csv
        :return: formatted_df
        """
        csv_df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
        # Columns used in Index Base
        # stock_id, index_id, symbol, "name", date_of_listing, isin_number, modifiedon
        required_cols = ["index_id", "symbol", "name", "date_of_listing", "isin_number"]
        try:
            csv_df["index_id"] = int(self.index_id)
            csv_df.rename(columns={
                "name_of_company": "name"
            }, inplace=True)
            self.logger.debug(f"Successfully formatted the csv_df from {self.name}")
            return csv_df[required_cols]
        except KeyError as e:
            self.logger.error("Could not create/rename columns from csv_df", exc_info=True)