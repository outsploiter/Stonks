# Common Modules
import logging

# Custom Modules
from utils import stockload
from utils.screener_utils import Screener

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s :: %(name)s :: %(levelname)s -> %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

def main():
    """
    This functions acts as a driver for loading all securities available on the index
    :return: None
    """
    logger = logging.getLogger(__name__)
    logging.info("Starting with Stock Load")
    db_utils = stockload.DBUtils()
    index_table = db_utils.get_index_info('NSE')
    for index in index_table:
        index_obj = stockload.IndexUtils(index)
        stocks_from_index = index_obj.get_stock_details()
        db_utils.upsert_stocks(stocks_from_index)
        logger.info("Completed Loading Stocks ")
        sector_less_stocks = db_utils.get_stock_without_sector(index_id=1)
        logger.info("Loading sector info")
        for i, row in enumerate(sector_less_stocks):
            try:
                sc = Screener(row[1])
                stock_info = sc.stock_information()
                db_utils.update_stock_sector([stock_info["sector"], stock_info["industry"], stock_info["about"], row[0]])
            except Exception as e:
                logger.error(e)

if __name__ == '__main__':
    main()
