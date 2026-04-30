import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, datetime, timedelta
from itertools import product
from copy import deepcopy

class PreRun:
    def __init__(self, path="", system_id=0, meter_or_inverter=None, systems_cleaned=None):
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
        
        self.path = Path(path) / meter_or_inverter / str(system_id)

        self.system_id = str(system_id)
        self.meter_or_inverter = meter_or_inverter
        self.data = None
        path_good_days = self.path / 'good_days' / f'{self.system_id}_good_days_{meter_or_inverter}.csv'
        self.good_days = pd.read_csv(path_good_days) if path_good_days.exists() else None

        self.end_days = None
        self.amended_data = self.data

        # figure out timezone stuff
        self.systems_cleaned = systems_cleaned[str(systems_cleaned['system_id'])==self.system_id] if systems_cleaned is not None else None
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
        

    def looks_like_int(x):
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
        all_files = list(self.path.glob("*.parquet"))
        if not all_files:
            raise FileNotFoundError(f"No parquet files found in {self.path}")
        
        data_frames = []
        for file in all_files:
            df = pq.read_table(file).to_pandas()
            data_frames.append(df)
        
        self.data = pd.concat(data_frames, ignore_index=True)
        self.data['time'] = pd.to_datetime(self.data['time'])
        self.amended_data = self.data.copy()

    def filter_good_days(self):
        """filters self.data to only include rows where the date is in self.good_days and stores the result in self.data

        Raises:
            ValueError: self.good_days is None
        """
        if self.good_days is None:
            raise ValueError("good_days is not loaded. Please load good_days before filtering.")
        
        # Ensure 'date' column is in datetime format
        self.data['date'] = pd.to_datetime(self.data['date'])
        self.good_days['date'] = pd.to_datetime(self.good_days['date'])
        
        # Filter data to only include rows where the date is in good_days
        good_dates = set(self.good_days['date'])
        self.data = self.data[self.data['date'].isin(good_dates)].reset_index(drop=True)

    def good_end_days(self, streak: int) -> pd.DataFrame:
        """returns a DataFrame of the last day of each streak of good days of length >= streak.

        Args:
            streak (int): minimum length of good day streak

        Returns:
            pd.DataFrame: DataFrame of the last day of each streak of good days of length >= streak
        """
        if self.good_days is None:
            raise ValueError("good_days is not loaded. Please load good_days before finding good end days.")
        
        self.good_days = self.good_days.sort_values('date').reset_index(drop=True)
        self.good_days['streak_id'] = (self.good_days['date'] - self.good_days['date'].shift(1) != timedelta(days=1)).cumsum()
        streaks = self.good_days.groupby('streak_id').filter(lambda x: len(x) >= streak)
        end_days = streaks.groupby('streak_id').last().reset_index(drop=True)
        
        self.end_days = end_days

        return end_days
    
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
                
    def naive_tts_dates_only(self, train = 0.8)->tuple[pd.DataFrame, pd.DataFrame]:
        """returns a DataFrame with the train and test split of the good end days based on the date only. The train set will contain the first 80% of the good end days and the test set will contain the last 20% of the good end days.

        Args:
            train (float, optional): proportion of good end days to include in the train set. Defaults to 0.8.

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the train and test dates in DataFrames
        """
        if self.end_days is None:
            raise ValueError("end_days is not calculated. Please calculate good end days before splitting into train and test.")
        
        self.end_days = self.end_days.sort_values('date').reset_index(drop=True)
        split_index = int(len(self.end_days) * train)
        train_dates = self.end_days.iloc[:split_index].reset_index(drop=True)
        test_dates = self.end_days.iloc[split_index:].reset_index(drop=True)
        return train_dates, test_dates
    
    def data_until_ho_day(self, ho_day: pd.Timestamp) -> pd.DataFrame:
        """returns a DataFrame of all data until the given ho_day (inclusive)

        Args:
            ho_day (pd.Timestamp): the day to filter the data until

        Returns:
            pd.DataFrame: DataFrame of all data until the given ho_day (inclusive)
        """
        if self.data is None:
            raise ValueError("data is not loaded. Please load data before filtering by ho_day.")
        
        filtered_data = self.data[self.data['date'] <= ho_day].reset_index(drop=True)
        return filtered_data
    
    def add_power_features_only(self, 
                     daily_lags=0, 
                     remove_daily_lags_nans=False,
                     include_last_year=False, 
                     remove_last_year_nans=False,
                     todays_lags=0, 
                     remove_todays_lags_nans=False,
                     include_month=False,
                     include_day_of_month=False,
                     include_hour = False) -> pd.DataFrame:
        """creates the dataframe of features
        NOTE: DAYLIGHT SAVINGS!!!! last_year, daily_lags

        Args:
            data (pd.DataFrame): first column, titled 'time', is times. Second column is 'power' in kW
            daily_lags (int, optional): number of past days' data at the same . Defaults to 0.
            remove_daily_lags_nans (bool, optional): whether to remove rows with NaN values in the daily_lag columns. Defaults to False.
            include_last_year (bool, optional): _description_. Defaults to False.
            remove_last_year_nans (bool, optional): whether to remove rows with NaN values in the last_year and last_year_lag columns. Defaults to False.
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

        
        #if we want to include last year's reading
        #creates two columns: 'last_year' and 'last_year_lag'. 'last_year' is the power reading from the same time last year. 'last_year_lag' is the difference between the power reading from the same time last year and the power reading from the same time last year and a day. 
        # This is to try and offset daylight savings.
        # does NOT take into account time zones/daylight savings time.
        if include_last_year:
            # Create a copy with time shifted forward by 1 year
            df_last_year = df[['time', 'power']].copy()
            df_last_year['time'] = df_last_year['time'] + pd.DateOffset(years=1)

            # add a column containing (last year) - (last year and a day)
            df_last_year['lag'] = df_last_year['power'] - df_last_year['power'].shift(1)

            df_last_year = df_last_year.rename(columns={'power': 'last_year', 'lag': 'last_year_lag'})

            # Merge
            df = df.merge(df_last_year, on='time', how='left')

        # this leaves us with lots of days with missing values for last_year and last_year_lag
        # delete these
        if remove_last_year_nans:
            df = df.dropna().reset_index(drop=True)

        #do daily lags: includes previous daily_lags days at exactly the same time
        if daily_lags>0:
            for i in range(1,daily_lags+1):
                df_temp = df.copy()
                df_temp['time'] = df_temp['time'] + pd.Timedelta(days=1)
                df_temp.rename(columns = {'power':f'daily_lag_{i}'}, inplace=True)
                df = df.merge(df_temp, on='time', how = 'left')
        if remove_daily_lags_nans:
            df = df.dropna().reset_index(drop=True)

        #todays_lags -- previous readings from the same day. 
        if todays_lags>0:
            for i in range(1,todays_lags+1):
                df_temp = df.copy()
                df_temp['power'] = df_temp['power'].shift(1)
                df_temp.rename(columns = {'power':f'todays_lag_{i}'}, inplace=True)
                df = df.merge(df_temp, on='time', how = 'left')

        if remove_todays_lags_nans:
            df = df.dropna().reset_index(drop=True)

        if include_month:
            df['month'] = df['time'].dt.month

        if include_day_of_month:
            df['day_of_month'] = df['time'].dt.day
        
        if include_hour:
            df['hour'] = df['time'].dt.hour

        self.amended_data = df

        #update good_days to only include days where we have all the features 
        if self.good_days is not None:
            self.good_days = self.good_days[self.good_days['date'].isin(df['time'].dt.date)].reset_index(drop=True)

        return df

    def add_weather_features_only(self):
        """adds weather features (cloud cover proportion and global tilted irradiance)
        """
        #get weather data
        weather_data = self.gather_weather_data()
        #merge with power data
        df = self.amended_data.copy()
        df = df.merge(weather_data, left_on='time', right_on='time', how='left')
        df.dropna(inplace=True)
        self.amended_data = df


    def gather_weather_data(self):
        latitude = self.systems_cleaned['latitude'].iloc[0]
        longitude = self.systems_cleaned['longitude'].iloc[0]
        tilt = self.systems_cleaned['tilt'].iloc[0]
        azimuth = self.systems_cleaned['azimuth'].iloc[0]
        start_date = self.good_days['date'].min()
        end_date = self.good_days['date'].max()
        
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
        print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation: {response.Elevation()} m asl")
        print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

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
        #will want weather data to match power data in terms of timezone
        #remove the utc offset information and have naive timestamps. Note DST is not accounted for here, but we are using the same timestamps for power and weather data so it should be consistent.
        hourly_dataframe["time"] = hourly_dataframe["time"].dt.tz_localize(None)
        
        return hourly_dataframe