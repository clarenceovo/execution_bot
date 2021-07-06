from .order_db_connector import order_db_connector
from .ftx_order import order
from util.json_util import read_json
import schedule
import time
import pandas as pd
import time
from datetime import datetime
import logging
from rest_client.order import ftx_client
import threading
from util.json_util import *
from pathlib import Path
pd.set_option('display.max_columns', None)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class order_manager(threading.Thread):
    def __init__(self,quoter):
        super(order_manager, self).__init__()
        """
        Function
        1. Create new order object when there is new unfinished order in the database,fetch the database in 1 second interval
        2. If there is new order
           2.1 get price and orderbook by API call
           2.2 Place order according to
           2.3 Check the order type. There are two types of order, 1) limit and 2) market
           2.4 Check if the order is filled in 5 second interval
              2.4.1 If the order is filled, mark complete in database
              2.4.2 If the order is not fully filled and the current price is deviate from order price a lot,
                    cancel the order and place a new order once the cancellation is confirmed
        3. keep looping step 1 and 2 
        """
        self.quoter = quoter
        self.is_active = True
        self.order_connector = order_db_connector()
        #order_list: store the unfinished order objects, if the order is finish , remove the order object from the list
        self.order_dict={} #order object list
        self.future_position_dict = {}
        self.token_position_dict = {}
        self.is_active = True
        self.ftx_client_dict = {}
        self.config = read_json(os.path.join(Path("config"), "credential_config.json"))
        self.create_ftx_authenticated_client() # Pass in credential to validate
        self.last_update_future_position_ts = None
        self.last_update_token_position_ts = None
        self.new_order_df = None
        self.executing_order_df = None
        self.historical_order_df = None
        self.get_order_lock = False

    def create_ftx_authenticated_client(self):
        if self.config['FTX']:
            for item in self.config['FTX'].keys(): #for each subaccount
                api_key = self.config['FTX'][item]['apiKey']
                api_secret = self.config['FTX'][item]['apiSecret']
                account_name = item
                try:
                    self.ftx_client_dict[item] = ftx_client(api_secret=api_secret,api_key=api_key,subaccount_name=account_name)
                except Exception as e:
                    logger.error(e)

    def update_future_position(self):
        for account in self.ftx_client_dict.keys():
            ret = self.ftx_client_dict[account].get_positions()
            if ret['success']:
                position = pd.DataFrame(self._get_response(ret))
                position = position.query("size > 0")#[position['size']>0]
                self.future_position_dict[account]=position
            else:
                logger.error(ret['error'])
        self.last_update_future_position_ts = datetime.utcnow().timestamp()


    def update_token_position(self):
        for account in self.ftx_client_dict.keys():
            ret = self.ftx_client_dict[account].get_balances()
        if ret['success']:
            tokenbook = pd.DataFrame(self._get_response(ret))
            self.token_position_dict[account] = tokenbook
        else:
            logger.error(ret['error'])
        self.last_update_token_position_ts = datetime.utcnow().timestamp()


    def get_new_order(self):
        if self.get_order_lock is False:
            self.get_order_lock = True
            try:
                conn = order_db_connector()
                ret = conn.get_new_order()
                if isinstance(ret,pd.DataFrame): #Check if the return is valid dataframe
                    new_order_ret = ret.to_dict("records")
                    logger.info(f"Number of new order:{len(new_order_ret)}")
                    for new_order_item in new_order_ret:
                        if new_order_item['id'] not in self.order_dict.keys():
                            if new_order_item['subaccount'] in self.ftx_client_dict.keys():
                                order_tmp = order(self.quoter,self.ftx_client_dict[new_order_item['subaccount']],new_order_item)
                                self.order_dict[new_order_item['id']] = order_tmp
                                order_tmp.start()

                            else:
                                logger.fatal(f"Order's subaccount {new_order_item['subaccount']} is not found in bot's configuration")
                self.get_order_lock =False

            except Exception as e:
                self.get_order_lock = False
                logger.error("Failed to get new order")
                logger.error(e)
        else:
            pass


    def order_status_check(self):
        """
        1. Clear finished order in the order dict
        """
        tmp = self.order_dict.copy()
        for order in tmp.keys():
            if tmp[order].is_finished:
                logger.info(f"Order is finished. ID:{tmp[order].id}")
                self.order_connector.finish_order(tmp[order].id)
                tmp[order].join()
                # .join() to stop the thread
                del self.order_dict[order]
                #tmp.pop(order)


    def run(self):
        """

        Create Task to update future position and token balance for every 1 second.
        Update the timestamp when the update is a success
        """
        schedule.every(0.5).seconds.do(self.update_future_position) #update future position every 0.5 secord
        schedule.every(0.5).seconds.do(self.update_token_position) #update future position every 0.5 second
        schedule.every(0.5).seconds.do(self.get_new_order) #Update new order list from DB every 0.5 second
        schedule.every(0.5).seconds.do(self.order_status_check) #Update new order list from DB every 0.5 second

        while self.is_active:
            #Call API to get position , tokenbook and the order hanging
            schedule.run_pending()
            time.sleep(0.5)

        if self.is_active is False:
            logger.info("Exiting Order Manager. Cancel all the existing order")
            #Cancel all the order in order object list
            logger.info("Cancelled all orders.")

    def cancel_all_orders(self):
        """
        1. Loop through the order object list
        2. Run order.cancel_order() function
        3. Check if the order is cancelled. If yes, mark is_cancelled is true
        4. Check all is_cancelled status
        """
        pass

    """
    def create_new_order(self):
        new_order = self.order_db_connector.get_new_order()
        if isinstance(new_order,pd.DataFrame):
            logger.info(f"New Order at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}.Number of new order:{len(new_order)}")
            tmp = new_order.to_dict("records")
            for item in tmp:
                self.order_db_connector.process_order(item['id'])
                self.order_list.append(order(item))
    """


    def process_pending_order(self):
        pass



    def order_fail_handler(self):
        """
        If there is any failed order, this function will be triggered to handle the error
        Order Failed Handling
        1. Log the error message
        2. Push the failed order to error list
        3. Fire Alert
        """
        pass

    def _get_response(self,res):
        if res['success']:
            return pd.DataFrame(res['result'])
        else:
            logger.error("FTX API Call Error.")
            return None
