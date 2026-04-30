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
    def __init__(self, path="", system_id=0, meter_or_inverter=None):
        """creates a PreRun object that can be used to load and filter data for a given system_id and meter_or_inverter

        Args:
            path (str, optional): path to the folder containing the folders good_days, inverter, meter, other. Defaults to "".
            system_id (int, optional): system_id. Defaults to 0.
            meter_or_inverter (str, optional): 'meter', 'inverter', or 'other' (None). Defaults to None.

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
            pd.DataFrame: _description_
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
        pass

    def add_physics_features_only(self):
        pass