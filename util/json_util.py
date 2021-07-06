import json
import os
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
def read_json(path):
    if os.path.lexists(path):

        with open(path) as file:
            try:
                dataObj = json.load(file)
                return dataObj
            except Exception as e:
                logger.error(e)
                return None
    else:
        return None

