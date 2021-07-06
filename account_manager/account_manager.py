from util.json_util import read_json
import schedule
import time
import pandas as pd
from rest_client.order import ftx_client
import os
from pathlib import Path

class account_manager:
    def __init__(self):
        self.self.ftx_rest = ftx_client("","")
        self.config = read_json(os.path.join(Path("config"), "db_configuration.json"))
