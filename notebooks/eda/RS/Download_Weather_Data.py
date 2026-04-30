import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, datetime, timedelta
from itertools import product
from copy import deepcopy
import pytz
from datetime import datetime

class Download_Weather_Data:
    def __init__(self, write_to_path="", system_id=0, meter_or_inverter=None, systems_cleaned=None):
        self.write_to_path = Path(write_to_path) / f"system_{system_id}_weather"
        self.system_id = system_id
        self.systems_cleaned = systems_cleaned[str(systems_cleaned['system_id'])==self.system_id]
        timezone_or_utc_offset = self.systems_cleaned['timezone_or_utc_offset'].iloc[0]
        # convert to utc_offset
        if self.looks_like_int(timezone_or_utc_offset):
            self.utc_offset = int(timezone_or_utc_offset)
        else:
            # convert timezone to utc_offset
            if timezone_or_utc_offset == 'America/New_York':
                self.utc_offset = -5

        
    def looks_like_int(x):
        try:
            return float(x).is_integer()
        except (TypeError, ValueError):
            return False

    