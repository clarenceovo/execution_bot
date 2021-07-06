import threading
from threading import Lock
import schedule
import time
from .order_db_connector import order_db_connector
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import pandas as pd
import random
import math
import decimal
import numpy
numpy.seterr(divide='ignore',invalid='ignore')


class order(threading.Thread):
    def __init__(self,market_quoter,order_client,param:dict):
        logger.info(f"Order Created. ID : {param['id']}")
        super(order, self).__init__()
        self.id = int(param['id'])
        self.quoter = market_quoter
        #db_conn = order_db_connector()
        #db_conn.process_order(self.id)
        self.order_client = order_client
        self.is_active =True
        self.is_finished = False
        self.id = int(param['id'])
        #order_max_lifetime is to safeguard the order will be fully executed after the time limit
        self.order_max_lifetime = 20
        self.avg_filled_price = 0
        self.filled_quantity = 0
        self.exchange = param['exchange']
        self.side = param['side']
        self.subaccount = param['subaccount']
        self.create_order_ts = param['create_order_ts']
        self.is_post_only = param['is_post_only']
        self.finish_order_ts = None
        self.is_trading = False
        self.trading_loop_start = False
        self.price = param['price']
        self.filled_quantity_dict = {}
        self.asset_code = param['asset_code']
        self.quantity = param['quantity']
        self.execution_premium = param['execution_premium']
        self.order_type = None
        self.orderbook_snapshot = None
        #get_contract_detail is a function
        self.order_price_increment = 0
        self.order_size_increment = 0
        self.price_deviation_limit = 0.005 # 0.5%
        self.get_contract_detail()
        self.order_size_check()
        self.order_price_check()


    def get_contract_detail(self):
        contract_detail = self.quoter.market_quote.query('name == @self.asset_code')
        if isinstance(contract_detail,pd.DataFrame):
            if  all(item in list(contract_detail.columns) for item in ['priceIncrement','sizeIncrement']):
                self.order_price_increment = contract_detail['priceIncrement'].values[0]
                self.order_size_increment = contract_detail['sizeIncrement'].values[0]


    def order_size_check(self):

        if self.order_size_increment >1:
            if (self.quantity/self.order_size_increment).is_integer() is not True:
                logger.error("Failed to create order. Reason: Wrong sizing")
                raise Exception
        else:

            if abs(decimal.Decimal(self.quantity).as_tuple().exponent) > abs(
                    decimal.Decimal(self.order_size_increment).as_tuple().exponent):
                logger.error("Failed to create order. Reason: Wrong sizing")
                raise Exception
            pass


    def order_price_check(self):
        if self.order_size_increment >1:
            if (self.price/self.order_size_increment).is_integer() is not True:
                logger.error("Failed to create order. Reason: Wrong pricing")
                raise Exception
        else:
            if abs(decimal.Decimal(self.price).as_tuple().exponent) > abs(
                decimal.Decimal(self.order_size_increment).as_tuple().exponent):
                logger.error("Failed to create order. Reason: Wrong pricing")
                raise Exception


    def get_price(self):
        """
        Determine the price according to dict
        If the self.price is None -> Trailing price
        return the price according to the side of order
        If the self.price is fixed number -> place limit order
        """
        return self.price


    def submit_order(self,size):
        logger.info(f"Submitting order:{size} side:{self.side}  Client:{self.order_client.exchange}")
        target_price = self.get_price()
        ret = self.order_client.place_order(market=self.asset_code,side=self.side,price=target_price,size=size,type="limit")
        if ret:
            if ret['success']:
                order_obj = ret['result']
                return int(order_obj['id']) , float(order_obj['price'])
            else:
                return None
        else:
            return None


    def _round_nearest(self,value,fraction):
        return round(round(value/fraction)*fraction,-int(math.floor(math.log10(fraction))))


    def cancel_order(self,order_id):
        if order_id:
            ret = self.order_client.cancel_order(order_id)
            if ret:
                if ret['success']:
                    logger.info(f'Order Cancellation success.')
                else:
                    logger.info(ret['error'])


    def order_finish(self):
        """
        When the order is done, the order_finish function set the self.is_active variable to false
        """
        logger.info(f'Remaining qty:{self.quantity-self.filled_quantity}')
        self.is_active = False
        self.is_finished = True
        self.is_trading = False
        schedule.clear(tag=self.id)
        logger.info(f'Order {self.id} is finished')


    def get_orderbook(self,contract_code=None):
        if contract_code is None:
            contract_code = self.asset_code
        orderbook_ret = self._get_response(self.order_client.get_orderbook(contract_code,20))
        if isinstance(orderbook_ret,pd.DataFrame):
            orderbook_ret['bid_price'] = orderbook_ret.apply(lambda x: x['bids'][0], axis=1)
            orderbook_ret['ask_price'] = orderbook_ret.apply(lambda x: x['asks'][0], axis=1)
            orderbook_ret['bid_qty'] = orderbook_ret.apply(lambda x:x['bids'][1],axis=1)
            orderbook_ret['ask_qty'] = orderbook_ret.apply(lambda x: x['asks'][1], axis=1)
            orderbook_ret.drop(columns=['bids','asks'],inplace=True)
            self.orderbook_snapshot = orderbook_ret
        else:
            logger.error(f'Failed to get orderbook.Symbol:{contract_code}')


    def get_orderbook_depth(self,side,depth=5):

        ret = self.orderbook_snapshot.head(depth)
        if side == 'buy':
            return ret['bid_qty'].mean()
        elif side == 'sell':
            return ret['ask_qty'].mean()


    def _is_order_filled(self):
        """
        Check if the order is fully filled
        """
        if self.filled_quantity == self.quantity:
            self.order_finish()


    def check_order(self, order_id:int):
        if order_id:
            ret = self.order_client.get_order_status(order_id)
            if ret:
                if ret['success']:
                    detail = ret['result']
                    self.filled_quantity_dict[order_id] = detail['filledSize']
                    self.avg_filled_price = detail['triggerPrice']
                    return detail

                else:
                    logger.error("Wrong Order ID.Order might not exist")

            else:
                logger.error("API Gateway error")


    def get_order_price(self):
        """
        If the price variable is not null , use the variable provided
        If the price variable is null,
        1. Check the side of the order

        """
        if self.price is not None:
            return self.price #typical limit price order
        else:
            #Check the orderbook
            logger.info("floating price")
            self.get_orderbook()
            logger.info(self.orderbook_snapshot)

        pass


    def calculate_order_size_list(self,orderbook_quantity):
        order_number = math.ceil(float(self.quantity) / float(orderbook_quantity))
        if self.quantity >order_number:
            logger.info("quantity greater than order")
            remain =round(math.fmod(self.quantity,order_number),int(abs(math.log10(self.order_size_increment))))
            logger.info(f'remain:{remain}')
            logger.info(f'order number:{order_number}')
            target_quantity = self.quantity - remain
            logger.info(f'target : {target_quantity}')
            tmp = []
            for item in range(0,order_number):
                #print(decimal(target_quantity/order_number))
                tmp.append(float(target_quantity/order_number))


            tmp[-1] = tmp[-1] +remain
            #print(tmp)
            if self.order_size_increment <1:
                ret = [ self._round_nearest(num,self.order_size_increment) for num in tmp]
                print(ret)
            else:
                ret = [round(num, int(1 / self.order_size_increment)) * self.order_size_increment for num in tmp]

            if sum(ret) == self.quantity:
                logger.info('Right Quantity')
                return ret
            else:
                logger.fatal('Wrong Quantity')
                raise Exception
        else:
            print('smaller')


    def get_price_deviation(self,order_price):
        current_price = self.quoter.market_quote.query('name == @self.asset_code')['price']
        return float((current_price-order_price)/order_price)


    def trading_main(self):
        if self.trading_loop_start ==False:
            self.trading_loop_start ==True
            logger.info(f"Start looping:{self.id}") #remove when goes prod
            """
            1. Get the snapshot of orderbook depth , calculate the size of the first 10 levels of orderbook
            2. Enter order slicing logic
            3. Send limit orders ,the limit price is based on the price df in market_quota , tick up/down 10ticks
            4. When the order is sent and placed, check if the order is fully executed. If the order is not executed / partially executed,
            calculate the remaining size of order. Submit the order with updated price again.
            5. Continue Step 1-4 until the order is fully filled. Then execute order_finish function to exit the logic loop.
            """
            order_size_list = []
            self.get_orderbook(self.asset_code)
            orderbook_quantity = self.get_orderbook_depth(self.side,20)
            logger.info(f"{self.asset_code} orderbook depth average :{orderbook_quantity}")
            if orderbook_quantity > self.quantity:
                #print('One order. No need order slicing')
                order_size_list.append(self.quantity)
            else:
                #print('Need more than one order')
                order_size_list = self.calculate_order_size_list(orderbook_quantity)
            self.is_trading = True
            while self.is_trading:
                schedule.run_pending()
                for order_size in order_size_list:
                    total_order_size = order_size
                    schedule.run_pending()
                    order_is_executed = False
                    while self.is_trading is True and order_is_executed is False:
                        logger.info(f"Start executing order. Asset code: {self.asset_code} Quantity:{self.quantity}")
                        """
                        1. Send order to FTX
                        2. wait 2 seconds
                        3. Check status of order
                        3. If success , set order_is_executed to True to do next order
                        4. If failed , update the bid price redo Step 1-4 
                        """
                        order_id ,order_price = self.submit_order(total_order_size)
                        if order_id: #if order id is vaild
                            while self.get_price_deviation(order_price) < self.price_deviation_limit and order_is_executed == False:
                                check_ret = self.check_order(order_id=order_id)
                                if check_ret['remainingSize'] == 0:  # still pending open order
                                    order_is_executed = True
                            if order_is_executed == False:
                                #cancel current order and place a new one
                                cancel_ret =self.cancel_order(order_id)
                                check_ret = self.check_order(order_id)
                                if check_ret:
                                    total_order_size -= check_ret['filledSize']
                                continue
                        else: #order id not vaild
                            raise Exception
                            logger.fatal("Failed to place order")
                            continue

                    if order_is_executed:
                        logger.info(f"Order executed.Asset code: {self.asset_code} Qty:{self.quantity} Filled Qty:{self.filled_quantity}")

                self.is_trading = False
            if self.is_trading == False:
                #schedule.clear(self.id)
                self.order_finish()
                time.sleep(0.2)




    def run(self):
        """
        1. Formulate Order body
        2. Try to submit the order using the order's order client
        3. If order submission success, return success message to the order manager
        4. If order submission is fail, return failed message to the order manager for error handling
        """
        db_conn = order_db_connector()
        db_conn.process_order(self.id)
        schedule.every(self.order_max_lifetime).seconds.do(self.order_finish).tag(self.id)
        #schedule.every(5).seconds.do(self.trading_main).tag(f'{self.id}_main',self.id)
        self.trading_main()
        logger.info("ENDED trading main")
        """
        Add order status 
        """

        #Clear scheduled task to avoid task stacking in scheduler


    def _get_response(self,res):
        if res['success']:
            return pd.DataFrame(res['result'])
        else:
            logger.error("FTX API Call Error.")
            return None
