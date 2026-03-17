import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from sklearn.linear_model import LinearRegression
from pathlib import Path


class Clean:
    def __init__(self, system_id=0, path=""):
        """initialize object

        Args:
            system_id (int): system id
            path (string): the path UP UNTIL the folder with system id number. DOES include the last /
        """
        self.system_id = system_id
        self.path = path+f'{system_id}/'
        self.partialpath = path+"/"

    def combine_and_clean(self, locations: list):
        """Combine all the data of a given inverter or meter into 1 dataframe

        Args:
            system_id (string or integer): system_id of the system being worked with. Used for file path.
            locations (list of 3-tuples): each tuple is (file name, time column title, power column title)

        Output:
            dataframe of combined information. 
            Two columns:
            'time' (datetime type)
            'power' (float, in kW)

            
        Along the way, will:
        - remove all data that has power entry < 1% of max value (industry standard is 1% of capacity, but that is unknown)
        - make time a datetime object (not string)
        - sort ascending by time

        """
        df = pd.DataFrame(columns = ['time_string', 'power'])
        if locations == []:
            return pd.DataFrame()
        #we will want to add items from each tuple! (aka file)
        dfs = [] #make a list of dataframes that will all be concatenated later
        for file in locations:
            df2 = pd.read_csv(self.path+file[0], usecols=[file[1],file[2]])
            #rename columns for consistency
            df2.columns = ['time_string', 'power']
            #mild cleaning to reduce size -- remove all 0's. 
            df2 = df2.loc[df2['power']>=0.0001]

            dfs.append(df2)

        df = pd.concat(dfs, ignore_index=True)

        #make a new column called time that is a datetime object

        df['time'] = pd.to_datetime(df['time_string'])
        df = df[['time','power']]
        df.sort_values(by = 'time', inplace= True)

        max_val = df['power'].max()
        df = df.loc[df['power']> (max_val*0.01)]
        df = df.drop_duplicates()

        return df

    def daily_average(self, data: pd.DataFrame):
        """Calculates daily average

        Args:
            data (pd.DataFrame): time series data. 2 columns: first column is dateTime, 2nd column is power

        Returns:
            dataframe: column 1: date, column 2: average power
        """
        if len(data) == 0:
            return data
        daily = data.groupby(data['time'].dt.date)['power'].mean()
        daily_df = daily.to_frame(name = 'power')
        daily_df = daily_df.reset_index()
        daily_df = daily_df.rename(columns={'index':'date'})
        return daily_df
    
    def write_to_file(self, data: pd.DataFrame, reader: str, number=0):
        """_summary_

        Args:
            data (pd.DataFrame): _description_
            reader (str): should be "inverter" or "meter"
            number (string or int): number of the inverter (no need if meter or if there's only 1 inverter)
        """
        if len(data) == 0:
            return
        number = str(number)
        if len(number)>3:
            raise ValueError("Inverter number too long: too many inverters! rewrite code.")
        elif len(number)==3:
            pass
        else:
            left = 3-len(number)
            number = left*"0"+number
        path = self.partialpath+"prize_cleaned_power/"
        file_name = str(self.system_id) + "_"+reader+"_"+number+".csv"
        data.to_csv(path+file_name, index=False)

    def system_info(self):
        pass

    def missing_days(self, data : pd.DataFrame):
        """missing days?

        Args:
            data (pd.DataFrame): time/power dataframe containing daily averages.
        
        Returns:
            list of days that are skipped over
        """
        df = data.copy() #might want to edit/add columns
        df['date_diff'] = df['time'].diff()
        return df[df["date_diff"] > pd.Timedelta(days=1)]




