import json
import os
import sys
from time import sleep

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
        self.db_params = json.loads(os.getenv("STONKS_DB_CREDS").replace("'", "\""))

    def get_connection(self, db_params):
        try:
            connection = psycopg2.connect(**db_params)
            self.logger.debug("Connection Created!")
            return connection
        except (Exception, Error) as error:
            self.logger.critical(f"Error initiating DB connection {error}\n", exc_info=True)

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
            self.logger.critical(f"Could not fetch from Index Base\n{query}", exc_info=True)
        finally:
            cursor.close()
            conn.close()
            self.logger.debug("Closing Stonks DB connections")

    def upsert_stocks(self, stock_df):
        conn = self.get_connection(self.db_params)
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
            self.logger.error(f"Could not upsert into Stock Base\n{query}\n{data}\n{err}", exc_info=True)
        finally:
            conn.close()
            cursor.close()
            self.logger.debug("Closing Stonks DB connections")

    def get_stock_without_sector(self, index_id):
        conn = self.get_connection(self.db_params)
        cursor = conn.cursor()
        query = f"""select stock_id, symbol, name
                            from stock_base 
                            where sector is null
                            and index_id = {index_id}
                            """
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            self.logger.debug("Fetch from Stock Base Successful")
            return result
        except (Exception, Error):
            self.logger.critical(f"Could not fetch from Stock Base\n{query}", exc_info=True)
        finally:
            cursor.close()
            conn.close()
            self.logger.debug("Closing Stonks DB connections")

    def update_stock_sector(self, data):
        conn = self.get_connection(self.db_params)
        cursor = conn.cursor()
        # ("UPDATE your_table SET column1 = %s, column2 = %s WHERE condition_column = %s")
        query = f"""
                UPDATE public.stock_base 
                set sector = %s,
                industry = %s,
                about = %s
                WHERE stock_id = %s
                """
        try:
            # Execute the upsert operation
            cursor.execute(query, data)
            conn.commit()
            self.logger.info(f"Upsert successful stock_id: {data[-1]}")
        except (Exception, Error) as err:
            self.logger.error(f"Could not upsert into Stock Base\n{query}\n{data}\n{err}", exc_info=True)
            sys.exit()
        finally:
            conn.close()
            cursor.close()
            self.logger.debug("Closing Stonks DB connections")

    def get_stock_urls(self, index_id):
        conn = self.get_connection(self.db_params)
        cursor = conn.cursor()

        query = f"""
        SELECT sector, 
           ARRAY_AGG('https://www.screener.in/company/' || symbol || '/consolidated/') AS urls,
           ARRAY_AGG(sb.stock_id) AS stock_ids
        FROM stock_base sb
        LEFT JOIN raw_soup_base rsb ON sb.stock_id = rsb.stock_id 
        WHERE sb.index_id = index_id
          AND (rsb.stock_id IS NULL OR now() - rsb.modifiedon > INTERVAL '1 month')
        GROUP BY sector
        ORDER BY COUNT(symbol) DESC;
        """
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            self.logger.debug("Fetch URLs from Stock Base Successful")
            return result
        except (Exception, Error):
            self.logger.critical(f"Could not fetch from Stock Base\n{query}", exc_info=True)
        finally:
            cursor.close()
            conn.close()
            self.logger.debug("Closing Stonks DB connections")

    def upsert_soup(self, stock_id, soup):
        print(f'Upserting {stock_id} : soup length->{len(soup)}')
        conn = self.get_connection(self.db_params)
        cursor = conn.cursor()

        query = """
        INSERT INTO raw_soup_base (stock_id, screener_soup, modifiedon)
        VALUES (%s, %s, current_date)
        ON CONFLICT (stock_id) DO UPDATE SET
            screener_soup = EXCLUDED.screener_soup,
            modifiedon = CURRENT_DATE;
        """
        try:
            cursor.execute(query, (stock_id, soup))
            conn.commit()
            self.logger.info(f"Upsert into soup base successful for {stock_id}")
        except (Exception, Error) as err:
            self.logger.error(f"Could not upsert into Stock Base\n{query}\n{soup}\n{err}", exc_info=True)
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
        self.logger.info(f"Processing Index {self.name}")

    def get_stock_details(self):
        """
        This function acts as a driver to get stock detail from a particular index
        :return: dataframe with must-have columns in index_base
        """
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
            self.logger.error(f"Could not create/rename columns from csv_df \n{e}", exc_info=True)
