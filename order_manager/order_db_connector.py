import mysql.connector
import pandas as pd
from util.json_util import *
from pathlib import Path
import io
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import threading
import time
import schedule
class order_db_connector:

    def __init__(self):
        super(order_db_connector, self).__init__()
        """
        Read the configuration file in config folder
        """
        self.config = read_json(os.path.join(Path("config"), "db_configuration.json"))
        self.db_conn = None
        self.cursor = None
        self.is_active = True
        self.connect()

    def connect(self):
        """
        Initalize database connector 
        """
        self.db_conn = mysql.connector.connect(**self.config)
        self.cursor = self.db_conn.cursor(buffered=True,dictionary=True)

    def close_connection(self):
        try:
            self.cursor.close()
            self.db_conn.close()
        except Exception as e:
            logger.error(e)

    def get_new_order(self):
        """
        Fetch the unfinished record in the database
        :return: Return a dataframe of unfinished orders
        """
        query = "SELECT * FROM order_table WHERE is_finished = False and is_processing = False;"
        try:

            self.cursor.execute(query)
            self.db_conn.commit()
            ret = pd.DataFrame(self.cursor.fetchall())
            if self.cursor.rowcount>0:
                self.close_connection()
                return ret
        except mysql.connector.Error as e:
            logger.error(e)
            return None

    def process_order(self,order_id:int):
        """
        Set is_finish flag to true to mark finish

        :param order_id: pass in the id of finished order
        :return: return true to indicate the order has been executed. Mark finish in database

        """
        query = u"UPDATE order_table SET is_processing = True where id = %(id)s"
        try:
            self.cursor.execute(query,{"id":order_id})
            self.db_conn.commit()
            return True
        except mysql.connector.Error as e:
            logger.error(e)
            return False

    def finish_order(self,order_id:int):
        """
        Set is_finish flag to true to mark finish

        :param order_id: pass in the id of finished order
        :return: return true to indicate the order has been executed. Mark finish in database

        """
        query = u"UPDATE order_table SET is_finished = True , is_processing = False where id = %(id)s"
        try:
            self.cursor.execute(query,{"id":order_id})
            self.db_conn.commit()
            return True
        except mysql.connector.Error as e:
            logger.error(e)
            return False

    def update_error_order(self,order_id:int):
        """

        :param param_dict: storing the value that about to be modified
        :return: return success if the record is modified , return None if the update fails

        """
        query = "UPDATE order_table SET is_error = True where id = %s"
        try:
            self.cursor.execute(query,{"id":order_id})
            self.db_conn.commit()
            return True
        except mysql.connector.Error as e:
            logger.error(e)
            return None