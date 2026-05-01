import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date, datetime, timedelta
from itertools import product
from copy import deepcopy
#from gen_variable_standard_static import metrics_search_for_two_fragments_df
#from tqdm import tqdm

class Clean:
    def __init__(self, system_id=0, path="", systems_cleaned=pd.DataFrame(), meter_or_inverter = None, write_to_path=""):
        """initialize object

        Args:
            system_id (int or str): system id
            path (string): the path UP UNTIL the folder with system id number. DOES include the last /
            write_to_path (string): the path to where the parquet file with cleaned data should be written. DOES include the last /. No need to include system_id 
                                    Should have a subfolder for good_days lists (?)
            meter_or_inverter: if a parquet file, should say whether we're looking for meter or for inverter data.
                                'meter' or 'inverter'
        """
        self.system_id = str(system_id)
        self.path = Path(path) / str(system_id)
        self.partialpath = path
        self.write_to_path = write_to_path
        self.systems_cleaned = systems_cleaned[systems_cleaned['system_id'] == int(self.system_id)]
        #prize or parquet?
        if self.systems_cleaned.iloc[0]['is_prize_data']:
            self.prize_or_parquet = 'prize'
        elif self.systems_cleaned.iloc[0]['is_lake_parquet_data']:
            self.prize_or_parquet = 'parquet'
        else:
            self.prize_or_parquet = 'other'

        self.date_summaries = pd.DataFrame() #will be updated in good_days
        self.good_days_df = pd.DataFrame() #will be updated in good_days

        self.years = sorted([
                        int(p.name.split('=')[1])
                        for p in self.path.iterdir()
                        if p.is_dir() and p.name.startswith('year=')
                    ]) #list of years there is data for
        
        if meter_or_inverter not in ('meter', 'inverter', None):
            raise ValueError(f'meter_or_inverer, input {meter_or_inverter}, is none of "meter" or "inverter", or None.')
        self.meter_or_inverter = meter_or_inverter

            

    def standardize_dataframe(self, data: pd.DataFrame)-> pd.DataFrame:
        """returns standardized version of dataframe for input into other functions
        NOTE: need to finish neither meter nor inverter option

        Args:
            data (pd.DataFrame): input data; three or four columns

        Returns:
            pd.DataFrame: two-column dataframe: "time" and "power". 'time' entries are of type datetime
        """
        #print("\tbeginning standardize_dataframe")
        #print(data)
        df = pd.DataFrame()
        if data is None or len(data) <10:
            # print("\t\tlength too short or is empty -- returning length 0 df")
            # print(data)
            return pd.DataFrame(columns=['time', 'power'])
        #figure out column names
        #only need the column corresponding to meter, inverter, or neither meter nor inverter
        if self.meter_or_inverter == 'meter':
            col_names = data.columns[data.columns.str.contains('met', na=False)]
            #if empty, then we need to skip this whole shindig
            if len(col_names)==0:
                return pd.DataFrame(columns=['time', 'power'])
            meter_col = col_names[0]
            df = data[['time',meter_col]].copy()
            df = df.rename(columns={meter_col: 'power'})
        elif self.meter_or_inverter == 'inverter':
            # print("\t\tentered meter_or_inverter == inverter")
            col_names = data.columns[data.columns.str.contains('inv', na=False)]
            # print(f"       col_names = {col_names}")
            #if empty, then we need to skip this whole shindig
            if len(col_names)==0:
                return pd.DataFrame(columns=['time', 'power'])
            inv_col = col_names[0]
            df = data[['time',inv_col]].copy()
            # print(df)
            df = df.rename(columns={inv_col: 'power'})
        elif self.meter_or_inverter is None:
            #need column name that contains 'power' but NOT 'inv' or 'met'
            power_cols = data.columns[data.columns.str.contains('power', case=False, na=False)
                                    & ~data.columns.str.contains('inv|met', case=False, na=False)]
            if len(power_cols)==0:
                return pd.DataFrame(columns=['time', 'power'])
            col_name = power_cols[0]
            df = data[['time',col_name]].copy()
            df = df.rename(columns={col_name: 'power'})

        df['time']=pd.to_datetime(df['time'])
        df = df.dropna() #to get rid of extra rows
        # print(df)
        return df
            

    def remove_small_values(self, data: pd.DataFrame, null_or_zero = 'null', dropna=True)-> pd.DataFrame:
        """NEEDS TESTING Removes small data values and replaces them with np.nan or with 0
            First run seems to work
        Args:
            data (pd.DataFrame): two-column dataframe. Column 1: "time". column 2: "power" 
            systems_cleaned (pd.DataFrame): dataframe consisting of metadata
            null_or_zero (str): Determine what to do with data less than 1 percent
                                of maximum value
                                    If "null", replace small values by numpy.nan
                                    If "zero", replace with zero
                                    If anything else, throw a ValueError.
            dropna (bool): True for dropping na values. False does not drop na values.

        Returns:
            pd.DataFrame: cleaned dataframe
        """
        # print("\tbeginning remove_small_values")
        df = data.copy()
        if len(df)==0:
            return df

        #calculate maximum 
            #this will be related to maximum RECORDED value and maximum DC THEORETICAL value
        # grab theoretical max. value.
            # first_index = self.systems_cleaned.index[0]
            # max_dc_capacity = self.systems_cleaned.loc[first_index, 'dc_capacity_kW']
        max_dc_capacity = self.systems_cleaned.iloc[0]['dc_capacity_kW']
        # print(f"\t\t max_dc_capacity = {max_dc_capacity}")
        # maximum of power readings
        local_max = df['power'].max()
        # use the smaller of these
        smaller_max = max(min(max_dc_capacity, local_max),0)
        # print(f"       smaller_max = {smaller_max}")

        if(null_or_zero == 'zero'):
            df.loc[df['power']<0.01*smaller_max,'power'] = 0
        elif(null_or_zero == 'null'):
            # print("\t\t in elif null_or_zero == null")
            df.loc[df['power']<0.01*smaller_max,'power'] = np.nan
        else:
            raise ValueError(f'null_or_zero, input {null_or_zero}, is none of'
                             + '"null", "zero", or "raw".')
        
        if dropna:
            df.dropna(inplace = True)

        return df


    def extract_years_data_parquet(self, years : int) -> pd.DataFrame:
        """given a list of years, returns the data from that year in a pandas dataframe.

        Args:
            years (list): years you want to extract

        Returns:
            pd.DataFrame: data from the extracted years, as a dataframe
        """
        # print("\t now beginning extract_years_data_parquet")
        folder = Path(self.path)
        requested_pq = pq.ParquetDataset(
            folder,
            filters=[('year', '==', years)]
        )
        df = requested_pq.read().to_pandas()
        if len(df)<10:
            return None
        return df


    def good_days(self, data : pd.DataFrame) -> pd.DataFrame:
        """PRELIM TESTING DONE creates series of good days. A good day must have:
            - a mode time gap between readings of at most an hour
            - no large time gaps of greater than 1.02*mode
            - at least 4 hours of readings if the mode time gap is followed
            - at least 0.85*the maximum number of daylight hours recorded

        Args:
            data (pd.DataFrame): dataframe with 'time' and 'power'. 'time' is of type datetime

        Returns:
            pd.Series: a series of all good days
        """
        # # print('\tbeginning good_days')
        # NOTE: IF THERE IS AN ERROR, TRY MAKING df['date'] be df['time'].dt.floor('D') 
        # apparently this type might wok better for rolling windows

        #dataframe df that contains the data itself
        df = data.copy()
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time') #make sure in temporal order
        df['date'] = df['time'].dt.date #extract date
        df['delta_t_hours'] = (df.groupby(df['time'].dt.date)['time'].diff().dt.total_seconds() / 3600)

        #create new dataframe that will summarize the goodness of the data each day
        date_summaries = df.groupby(df['date']).agg(
            num_readings=('date', 'size'),
            delta_t_mode=('delta_t_hours', lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan)
        ).reset_index(names='date')
            #current columns: date, num_readings, delta_t_mode

        #want to mark which days are good
        #will have 4 criteria:
            # there isn't too large a gap within the day's recordings (large means > 1.02*delta_t_mode)
            # there are enough readings (i.e. at least 4 hours, so 4/delta_t_mode)
            # the mode is short (<=1 hour)
            # the number of daylight hours recorded (beginning-end) is at least 0.85*the max number of daylight houts within 3 days (before/after)
        
        #create a column in df that has the day's mode, for easy comparison
        mode_map = date_summaries.set_index('date')['delta_t_mode']
        df['delta_t_mode'] = df['date'].map(mode_map)

        #CRITERION 1: NO LARGE GAPS
        #see if there are large gaps!
            #create series of Boolean (whether is a large gap)
            #for each day, assign True (if exists large gap) or False (if not)
            #then make into a series

        # print(df['delta_t_hours'].dtype)
        # one entry each day will not have a delta_t_hours put in -- replace na with delta_t_mode
        df['delta_t_hours'] = df['delta_t_hours'].fillna(df['delta_t_mode'])
        df.dropna(inplace=True) #to ensure that there are no na values, which would mess up the comparison. This will drop days with only one reading.

        #compare delta_t_hours with delta_t_mode
        # print(f"delta_t_mode data type = {df['delta_t_mode'].dtype}")
        # print(f"delta_t_hours data type post-interpolation = {df['delta_t_hours'].dtype}")
        

        gap_flag = (df['delta_t_hours'] > 1.02 * df['delta_t_mode']).groupby(df['date']).any().astype(bool)
        
        #merge with date_summaries
        date_summaries = date_summaries.merge(
            gap_flag.rename('has_large_gap'),
            left_on='date',
            right_index=True,
            how='left'
        )
            #current columns: date, num_readings, delta_t_mode, has_large_gap

        #CRITERION 4: ENOUGH DAYLIGHT HOURS RECORDED
        date_summaries['date'] = pd.to_datetime(date_summaries['date'])
        date_summaries = date_summaries.sort_values('date')
        date_summaries = date_summaries.set_index('date')

        #figure out daily span
        # daily_span = df.groupby('date')['time'].agg(lambda x: x.max() - x.min())
        # daily_span_hours = daily_span.dt.total_seconds() / 3600 #convert to hours
        g = df.groupby('date')['time']
        daily_span_hours = (g.max() - g.min()).dt.total_seconds() / 3600
        weekly_max_span = daily_span_hours.rolling(window=7, center=True, min_periods=1).max()
        span_flag = daily_span_hours >= 0.85 * weekly_max_span #becomes boolean
        # print("\t\tspan_flag done")
        
        #going to merge along index, so make sure indices are the same
        span_flag.index = pd.to_datetime(span_flag.index)

        date_summaries = date_summaries.merge(span_flag.rename('good_span'),
                                            left_on='date',
                                            right_index=True,
                                            how='left')
            #current columns: date, num_readings, delta_t_mode, has_large_gap, good_span

        #create series for each condition 
        cond_no_large_gap = date_summaries.apply(lambda row: False if (row['has_large_gap'] is np.nan) else ~row['has_large_gap'],
                                                axis=1)

        cond_enough_readings = (
            date_summaries['num_readings'] >= 4 / date_summaries['delta_t_mode']
        )

        cond_mode_short = date_summaries['delta_t_mode'] <= 1


        #combine all conditions into a good_day column
        date_summaries['good_day'] = (
            cond_no_large_gap &
            cond_enough_readings &
            cond_mode_short &
            date_summaries['good_span']
        )

        #reset index
        date_summaries = date_summaries.reset_index()
        date_summaries['date'] = date_summaries['date'].dt.date #in case it was changed in debugging

        #restrict to the good days only
        good_days_df = date_summaries.loc[date_summaries['good_day'],['date']]

        #append to global good_days_df, in case this function is run multiple times
        self.good_days_df = pd.concat([self.good_days_df, good_days_df], ignore_index = True).drop_duplicates().sort_values(by = 'date')
        #append to global date_summaries
        self.date_summaries = pd.concat([self.date_summaries, date_summaries], ignore_index = True).drop_duplicates().sort_values(by = 'date')
        
        # print('\tending good_days')
        return good_days_df #only the good days in the data set from THIS function run


    def keep_good_days_only(self, data : pd.DataFrame) -> pd.DataFrame:
        """Given cleaned data, keeps only the good days

        Args:
            data (pd.DataFrame): cleaned data ('time' | 'power')

        Returns:
            pd.DataFrame: only the good days in the cleaned data
        """
        # print("\tbeginning keep_good_days_only")
        if len(data)==0:
            return data
        good_days_set = set(self.good_days(data)['date'])
        df_good = data[data['time'].dt.date.isin(good_days_set)].reset_index(drop=True)
        return df_good
        

    def good_days_streaks(self, streak : int ) -> pd.DataFrame:
        """NEEDS TESTING returns a dataframe containing all good days that end a streak of length at least the inputted streak.
        NOTE: Must run good_days first for ALL years!!!

        Args:
            streak (int): The number of consecutive days of good data we are seeking

        Returns:
            pd.DataFrame: all days that end a streak of streak consecutive days of good data.
        """
        df = self.good_days_df.copy()

        # ensure datetime and sorted
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # compute gaps between consecutive days
        gap = df['date'].diff().dt.days

        # identify where a new streak starts
        streak_start = (gap != 1).cumsum()

        # compute streak length within each group
        df['streak'] = df.groupby(streak_start).cumcount() + 1

        return df.loc[df['streak']>=streak, ['date']]
        pass


    def extract_data_until_day(self, end_day : str, cleaned = True) -> pd.DataFrame:
        
        day = pd.to_datetime(end_day, format = '%m/%d/%Y').date() #make it a datetime type and restrict to date
        cutoff = day.year

        #if parquet
        years_subset = [y for y in self.years if y <= cutoff]

        #if prize

        pass
    
    def convert_to_energy_LRS(self, data : pd.DataFrame) -> pd.DataFrame:
        """Turn cleaned data into hour-by-hour energy using a left Riemann sum 

        Args:
            data (pd.DataFrame): cleaned data

        Returns:
            pd.DataFrame: hourly energy (UNLESS length == 0, in which case returns original dataframe)
        """
        if len(data)==0:
            return data

        df = data.copy()
        df = df.sort_values('time')
        df['day'] = df['time'].dt.floor('D')

        # within a day, find difference between this time and the next
        df['delta_t'] = (df.groupby('day')['time']
            .shift(-1) - df['time'])
        df['delta_t'] = df['delta_t'].dt.total_seconds() / 3600   # convert to hours

        # create a map to find modes for day
        date_summaries = self.date_summaries.copy()
        mode_map = date_summaries.set_index('date')['delta_t_mode']

        # fill the last entry with the mode (likely best simple estimate we have)
        df['delta_t'] = df['delta_t'].fillna(df['day'].map(mode_map))

        df['energy'] = df['power']*df['delta_t']

        # now sort by hour and find hourly sum to get total energy in kwh
        df['hour'] = df['time'].dt.floor('h')
        hourly = df.groupby('hour', as_index=False)['energy'].sum()
        hourly = hourly.rename(columns={'hour': 'time'})

        return hourly
    
    def clean_all_and_write_to_file(self):
        """cleans data and writes it to file.
        All files become parquets.
        If the data was originally parquet, data remains partitioned by year.

        In the future, if the data was originally prize, data will not be partitioned.
        """
        
        #extract data -- done differently for parquet vs prize
        if self.prize_or_parquet == 'parquet':
            #go through year-by-year
            for year in self.years:
                print(f"Now processing system {self.system_id}, year {year}.")
                # lots of cleaning
                data = self.extract_years_data_parquet(year)
                data = self.standardize_dataframe(data)
                if len(data)==0:
                    continue
                data = self.remove_small_values(data)
                if data is None or len(data)<10:
                    continue
                data = self.keep_good_days_only(data)
                data = self.convert_to_energy_LRS(data)
                if len(data)==0:
                    continue
                # write to file
                # make sure location exists
                #inverter
                if self.meter_or_inverter == 'inverter':
                    base = Path(self.write_to_path).resolve()
                    out_dir = base / 'inverter' / str(self.system_id) / f'year={year}'
                    out_dir.mkdir(parents=True, exist_ok=True)

                #meter
                elif self.meter_or_inverter == 'meter':
                    base = Path(self.write_to_path).resolve()
                    out_dir = base / 'meter' / str(self.system_id) / f'year={year}'
                    out_dir.mkdir(parents=True, exist_ok=True)
                #other
                else:
                    base = Path(self.write_to_path).resolve()
                    out_dir = base / 'other' / str(self.system_id) / f'year={year}'
                    out_dir.mkdir(parents=True, exist_ok=True)
                # write to file
                file_path = out_dir / "data.parquet"

                data.to_parquet(file_path, engine="pyarrow", index=False)
                # data.to_parquet(out_dir,
                #                 engine="pyarrow",
                #                 index=False)
            if len(self.good_days_df) > 200: #otherwise, there's basically no point in doing this
                #make csv for good_days
                #make sure location exists
                out_dir = Path(self.write_to_path) / "good_days"
                out_dir.mkdir(parents=True, exist_ok=True)
                #make file
                if self.meter_or_inverter is None:
                    self.meter_or_inverter = 'other'
                file_path = out_dir / f"{self.system_id}_good_days_{self.meter_or_inverter}.csv"
                self.good_days_df.to_csv(file_path, index=False)

        elif self.prize_or_parquet == 'prize':
            pass
        else:
            pass

        pass
