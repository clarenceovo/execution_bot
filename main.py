from order_manager.order_manager import order_manager
from quote_module.market_quoter import market_quoter

import logging
from datetime import datetime
from queue import Queue
import time
import schedule
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger('schedule').propagate = False
logging.basicConfig(level = logging.INFO)

from datetime import datetime
class trading_bot:
    def __init__(self):
        """
        Initialize the bot
        1. Mark the initial timestamp
        2. Create a new market quoter object
        3. Create a new order manager object
        """
        logger.info("Order Bot Initializing")
        self.start_ts = datetime.utcnow().timestamp()
        logger.info("Initializing:Market Quoter Module")
        self.quoter = market_quoter()
        logger.info("Initializing:Order Manger Module ")
        self.order_manager = order_manager(self.quoter)
        self.last_trade_ts = None
        self.last_update_ts = None
        self.telegram_incoming_queue = Queue()
        self.telegram_outgoing_queue = Queue()

    def initialize_component(self):
        try:
            self.quoter.start()
            self.order_manager.start()
            return True
        except Exception as e:
            logger.fatal(e)
            return False

    def stop_bot(self):
        """
        1. Stop the loop of each module
        2. Join the thread accordingly
        """
        #Stopping Market Quote Feed
        logger.info("Stopping:Market Quoter Module ")
        self.quoter.is_active = False
        self.quoter.join()
        if self.quoter.is_alive() is False:
            logger.info("Stopped :Market Quoter Module ")
        ############################
        #Stopping Order Manager
        logger.info("Stopping: Order Manger Module ")
        self.order_manager.is_active = False
        self.order_manager.join()
        if self.order_manager.is_alive() is False:
            logger.info("Stopped: Order Manger Module ")
        ############################

        logger.info("All module stopped. Bye~")

    def module_health_check(self):
        """
        check the last update ts of each module to ensure all the module are running smoothly.
        """
        pass

    def run(self):
        init_is_success = self.initialize_component()
        if init_is_success:
            logger.info("Connected: Market Quoter Module")
            logger.info("Connected: Order Manger Module ")
            logger.info("Order Bot starts running")
            #time.sleep(5)
            #self.stop_bot()





if __name__ == '__main__':
    bot = trading_bot()
    bot.run() #run the
