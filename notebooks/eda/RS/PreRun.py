from time import strftime

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, datetime, timedelta
from itertools import product
from copy import deepcopy
import requests_cache
from retry_requests import retry
import openmeteo_requests

# Order to do things in:
# 1. Create PreRun object with system_id, path, meter_or_inverter, and systems_cleaned
# 2. add_energy_features_only
# 3. add_weather_features_only
# (can swap 2 and 3)
# The dataframe as a whole with ALL selected features is stored in self.amended_data, the original data in self.data
# 4. good_end_days_naive
# 5. naive_tts_dates_only <- to get naive train/test split
# 6. data_until_ho_day <- to get all data until a given ho_day 



class PreRun:
    def __init__(self, system_id=0, path="",  meter_or_inverter=None, systems_cleaned=None):
        """creates a PreRun object that can be used to load and filter data for a given system_id and meter_or_inverter

        Args:
            path (str, optional): path to the folder containing the folders good_days, inverter, meter, other. Defaults to "".
            system_id (int, optional): system_id. Defaults to 0.
            meter_or_inverter (str, optional): 'meter', 'inverter', or 'other' (None). Defaults to None.
            systems_cleaned (pd.DataFrame, optional): the systems_cleaned dataframe. Defaults to None.

        Raises:
            ValueError: something else typed for meter_or_inverter
        """
        if meter_or_inverter is None:
            meter_or_inverter = 'other'
        elif meter_or_inverter not in ['meter', 'inverter', 'other']:
            raise ValueError("meter_or_inverter must be 'meter', 'inverter', 'other', or None")
        
        self.system_id = str(system_id)
        self.meter_or_inverter = meter_or_inverter

        #load good_days file
        path_good_days = Path(path) / 'good_days' / f'{self.system_id}_good_days_{meter_or_inverter}.csv'
        self.good_days = pd.read_csv(path_good_days) #if path_good_days.exists() else None
        self.good_days['date'] = pd.to_datetime(self.good_days['date'])

        #make path to data
        self.path = Path(path) / meter_or_inverter / str(system_id)

        #load the data
        self.data=None
        self.amended_data = None
        self.load_data()
        print(self.data)
        self.end_days = None
        self.end_days_naive = None
        

        # figure out timezone stuff
        self.systems_cleaned = systems_cleaned.loc[systems_cleaned['system_id']==int(self.system_id)] if systems_cleaned is not None else None
        timezone_or_utc_offset = self.systems_cleaned['timezone_or_utc_offset'].iloc[0] if self.systems_cleaned is not None else None
        self.is_offset = self.looks_like_int(timezone_or_utc_offset)
        # convert to utc_offset
        if self.looks_like_int(timezone_or_utc_offset):
            self.utc_offset = int(timezone_or_utc_offset)
        else:
            # convert timezone to utc_offset
            if timezone_or_utc_offset == 'America/New_York':
                self.utc_offset = -5
            else:
                raise ValueError(f"Timezone {timezone_or_utc_offset} not recognized.")
        #then fix the timezones
        self.fix_timezones()

    def looks_like_int(self, x):
        try:
            return float(x).is_integer()
        except (TypeError, ValueError):
            return False

    def load_data(self):
        """loads all data from the parquet files in the directory into a single pandas DataFrame and stores it in self.data

        Raises:
            FileNotFoundError: no parquet files found in the directory
        """
        # Load all parquet files in the directory
        folder = Path(self.path)
        requested_pq = pq.ParquetDataset(folder)
        self.data = requested_pq.read().to_pandas()
        # print(self.data.columns)
        self.data = self.data[['time', 'energy']]
        self.data['time'] = pd.to_datetime(self.data['time'])
        self.amended_data = self.data.copy()
        # print(self.amended_data)

    def fix_timezones(self):
        #want to change everything to GMT offset 
        #find original timezone using systems_cleaned
        if self.is_offset:
            return
        else:
            #convert timezone to utc_offset
            #make sure time is in localized format; this will be the actual time zone
            if self.systems_cleaned['timezone_or_utc_offset'].iloc[0] == 'America/New_York':
                self.data['time'] = self.data['time'].dt.tz_localize('America/New_York')
            #then convert
            self.data['time'] = self.data['time'].dt.tz_convert(f'Etc/GMT{self.utc_offset:+d}')
        
        self.amended_data = self.data.copy()

    def good_end_days_naive(self, streak: int) -> pd.DataFrame:
        """returns a DataFrame of the last day of each streak of good days of length >= streak.

        Args:
            streak (int): minimum length of good day streak

        Returns:
            pd.DataFrame: DataFrame of the last day of each streak of good days of length >= streak
        """
        
        self.good_days = self.good_days.sort_values('date').reset_index(drop=True)
        self.good_days['streak_id'] = (self.good_days['date'] - self.good_days['date'].shift(1) != timedelta(days=1)).cumsum()
        print(self.good_days)
        streaks = self.good_days.groupby('streak_id').filter(lambda x: len(x) >= streak)
        end_days = streaks.groupby('streak_id').last().reset_index(drop=True)
        
        self.end_days_naive = end_days

        return end_days
                
    def naive_tts_dates_only(self, train = 0.8)->tuple[pd.DataFrame, pd.DataFrame]:
        """returns a DataFrame with the train and test split of the good end days based on the date only. The train set will contain the first 80% of the good end days and the test set will contain the last 20% of the good end days.

        Args:
            train (float, optional): proportion of good end days to include in the train set. Defaults to 0.8.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the train and test dates in DataFrames
        """
        if self.end_days_naive is None:
            raise ValueError("end_days is not calculated. Please calculate good end days before splitting into train and test.")
        
        self.end_days = self.end_days.sort_values('date').reset_index(drop=True)
        split_index = int(len(self.end_days) * train)
        train_dates = self.end_days.iloc[:split_index].reset_index(drop=True)
        test_dates = self.end_days.iloc[split_index:].reset_index(drop=True)
        return train_dates, test_dates
    
    def data_until_ho_day(self, ho_day: pd.Timestamp) -> pd.DataFrame:
        """returns a DataFrame of all data (updated to include features) until the given ho_day (inclusive)

        Args:
            ho_day (pd.Timestamp): the day to filter the data until

        Returns:
            pd.DataFrame: DataFrame of all data until the given ho_day (inclusive)
        """
        if self.data is None:
            raise ValueError("data is not loaded. Please load data before filtering by ho_day.")
        
        filtered_data = self.amended_data[self.amended_data['date'] <= ho_day].reset_index(drop=True)
        return filtered_data
    
    def add_energy_features_only(self, 
                     daily_lags=0, 
                     remove_daily_lags_nans=True,
                     include_last_year=False, 
                     remove_last_year_nans=True,
                     todays_lags=0, 
                     remove_todays_lags_nans=False,
                     include_month=False,
                     include_day_of_month=False,
                     include_hour = False) -> pd.DataFrame:
        """creates the dataframe of features
        NOTE: this all has to be done in one go, and before the weather data. 
        NOTE 2: removing nans likely leaves no available streaks. Recommend not removing all nans.
        NOTE 3: if we can't deal with nans but want to use daily lags, can replace those nans with 0's (since the reading was just very low)

        Args:
            data (pd.DataFrame): first column, titled 'time', is times. Second column is 'energy' in kW
            daily_lags (int, optional): number of past days' data at the same . Defaults to 0.
            remove_daily_lags_nans (bool, optional): whether to remove rows with NaN values in the daily_lag columns. Defaults to True.
            include_last_year (bool, optional): _description_. Defaults to False.
            remove_last_year_nans (bool, optional): whether to remove rows with NaN values in the last_year and last_year_lag columns. Defaults to True.
            days_rolling_average_exact (int, optional): _description_. Defaults to 0.
            todays_lags (int, optional): _description_. Defaults to 0.
            remove_todays_lags_nans (bool, optional): whether to remove rows with NaN values in the todays_lag columns. Defaults to False.
            include_month (bool, optional): _description_. Defaults to False.
            include_hour (bool, optional): _description_. Defaults to False.
            include_day_of_month (bool, optional): _description_. Defaults to False.
            sunlight_duration (bool, optional): _description_. Defaults to False.

        Returns:
            pd.DataFrame: dataframe with all added features
        """
        df=self.data.copy()
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time').reset_index(drop=True)
        df["day"] = df["time"].dt.floor("D") # for aligning thing later. should drop.
        
        #if we want to include last year's reading
        #creates two columns: 'last_year' and 'last_year_lag'. 'last_year' is the energy reading from the same time last year. 'last_year_lag' is the difference between the energy reading from the same time last year and the energy reading from the same time last year and a day. 
        # This is to try and offset daylight savings.
        # does NOT take into account time zones/daylight savings time.
        if include_last_year:
            # Create a copy with time shifted forward by 1 year
            df_last_year = df[['time', 'energy']].copy()
            df_last_year['time'] = df_last_year['time'] + pd.DateOffset(years=1)

            # add a column containing (last year) - (last year and a day)
            df_last_year['lag'] = df_last_year['energy'] - df_last_year['energy'].shift(1)

            df_last_year = df_last_year.rename(columns={'energy': 'last_year', 'lag': 'last_year_minus_last_year_and_a_day'})

            # Merge
            df = df.merge(df_last_year, on='time', how='left')

        
        

        #do daily lags: includes previous daily_lags days at exactly the same time
        if daily_lags>0:
            for i in range(1,daily_lags+1):
                # df_temp = self.data.copy()
                # df_temp['time'] = df_temp['time'] + pd.Timedelta(days=1)
                # df_temp.rename(columns = {'energy':f'daily_lag_{i}'}, inplace=True)
                # df = df.merge(df_temp, on='time', how = 'left')
                df[f"{i}_days_ago"] = (df.groupby(df["time"].dt.time)["energy"].shift(24 * i))
        

        #todays_lags -- previous readings from the same day. 
        if todays_lags>0:
            for i in range(1,todays_lags+1):
                # df_temp = self.data.copy()
                # df_temp['energy'] = df_temp['energy'].shift(1)
                # df_temp.rename(columns = {'energy':f'{i}_hours_ago_today'}, inplace=True)
                # df = df.merge(df_temp, on='time', how = 'left')
                df[f'{i}_hours_ago_today'] = df.groupby(df['time'].dt.floor('D'))['energy'].shift(i)

        #remove nans if specified.
        if remove_last_year_nans:
            df = df.dropna(subset=['last_year', 'last_year_minus_last_year_and_a_day']).reset_index(drop=True)
        if remove_daily_lags_nans:
            df = df.dropna(subset=[f"{i}_days_ago" for i in range(1,daily_lags+1)]).reset_index(drop=True)
        if remove_todays_lags_nans: #this HAS to come after the others since we might want to keep these nans
            df = df.dropna(subset=[f'{i}_hours_ago_today' for i in range(1,todays_lags+1)]).reset_index(drop=True)

        if include_month:
            df['month'] = df['time'].dt.month

        if include_day_of_month:
            df['day_of_month'] = df['time'].dt.day
        
        if include_hour:
            df['hour'] = df['time'].dt.hour

        self.amended_data = df

        #update good_days to only include days where we have all the features 
        self.good_days = df['day'].drop_duplicates().reset_index(drop=True).to_frame().rename(columns={'day':'date'})

        return df

    def add_weather_features_only(self):
        """adds weather features (cloud cover proportion and global tilted irradiance)
        """
        #get weather data
        weather_data = self.gather_weather_data()
        #merge with energy data
        df = self.amended_data.copy()
        df = df.merge(weather_data, left_on='time', right_on='time', how='left')
        #df.dropna(inplace=True) #might drop too many things
        self.amended_data = df

    def gather_weather_data(self):
        latitude = self.systems_cleaned['latitude'].iloc[0]
        longitude = self.systems_cleaned['longitude'].iloc[0]
        tilt = self.systems_cleaned['tilt'].iloc[0]
        azimuth = self.systems_cleaned['azimuth'].iloc[0]
        start_date = self.good_days['date'].min().strftime('%Y-%m-%d')
        end_date = self.good_days['date'].max().strftime('%Y-%m-%d')
        
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        # Make sure all required weather variables are listed here
        # The order of variables in hourly or daily is important to assign them correctly below
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ["cloud_cover", "global_tilted_irradiance"],
            "timezone": "GMT",
            "tilt": tilt,
	        "azimuth": azimuth,
        }
        responses = openmeteo.weather_api(url, params = params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        # print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
        # print(f"Elevation: {response.Elevation()} m asl")
        # print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
        # print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

        # Process hourly data. The order of variables needs to be the same as requested.
        hourly = response.Hourly()
        hourly_cloud_cover = hourly.Variables(0).ValuesAsNumpy()
        hourly_global_tilted_irradiance = hourly.Variables(1).ValuesAsNumpy()

        hourly_data = {"time": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["global_tilted_irradiance"] = hourly_global_tilted_irradiance

        hourly_dataframe = pd.DataFrame(data = hourly_data)

        # make sure cloud cover is between 0 and 1
        if hourly_dataframe['cloud_cover'].max() > 1:
            hourly_dataframe['cloud_cover'] = hourly_dataframe['cloud_cover']/100

        # need to shift the time by the utc offset
        hourly_dataframe['time'] = hourly_dataframe['time'].dt.tz_convert(f'Etc/GMT{self.utc_offset:+d}')
        #will want weather data to match energy data in terms of timezone
        #remove the utc offset information and have naive timestamps. Note DST is not accounted for here, but we are using the same timestamps for energy and weather data so it should be consistent.
        hourly_dataframe["time"] = hourly_dataframe["time"].dt.tz_localize(None)
        
        return hourly_dataframe