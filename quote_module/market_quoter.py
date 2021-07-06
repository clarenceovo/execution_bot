from rest_client.order import ftx_client
import pandas as pd
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import threading
import time
import schedule
class market_quoter(threading.Thread):
    def __init__(self):
        super(market_quoter, self).__init__()
        """
        Function:
        1. Provide market data feed service to the order manager so that order manager can submit order with a correct limit / market price
        """
        # pass in empty string to make sure this rest client object will not get account sensitive information
        self.ftx_rest = ftx_client("","")
        self.subscript_list = []
        self.market_quote = None
        self.last_market_quote_ts = 0
        self.is_active = True


    def run(self):
        # Add the get quote function into schedule task
        schedule.every(0.2).seconds.do(self.get_quote)
        while self.is_active:
            try:
                
                schedule.run_pending()
                time.sleep(0.05)
                
            except ConnectionResetError as e:
                logger.fatal(f"Fatal Network Error:Connection Reset\n{e}")
                
            except ConnectionAbortedError as e:
                logger.fatal(f"Fatal Network Error:Connection Aborted\n{e}")
            except ConnectionRefusedError as e:
                logger.fatal(f"Fatal Network Error: Connection Refused\n{e}")
            except Exception as e:
                logger.error(e)
        logger.info("Market Quoter stopped operation.")


    def get_quote(self):
        try:
            res = self._get_response(self.ftx_rest.list_markets())
            res = res.loc[(res['restricted'] == False)]
            self.market_quote = res[['name','bid','ask','price','type','underlying','priceIncrement','sizeIncrement']]
            self.last_market_quote_ts = datetime.utcnow().timestamp()
            return res[['name','bid','ask','price','type','underlying',"sizeIncrement"]]
        except Exception as e:
            logger.error(e)
            return None


    def get_orderbook(self,contract_code):
        return self._get_response(self.ftx_rest.get_orderbook(contract_code,20))


    def _get_response(self,res):
        if res['success']:
            return pd.DataFrame(res['result'])
        else:
            logger.error("FTX API Call Error.")
            return None