import requests
import pandas as pd
from pathlib import Path
import time
from tqdm import tqdm

# choices
make_logs = True
i_start = 0
i_end = 10
my_dir_string = '../../../data_ds_project/systems/parquet_irrad_samples/'

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
parquet_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'is_lake_parquet_data']
]  # is already boolean!
all_parquet_system_ids = list(parquet_systems.system_id.unique())
all_parquet_system_ids.sort()


def point_maker(lon, lat):
    return f'POINT({lon} {lat})'


def download_irradiance_sample(
        system_id,
        api_key,
        e_mail,
        make_logs=False,
        log_path='../../logs/logs.csv',
        data_directory_description=''
):
    relevant_rows_systems = systems_cleaned[
        systems_cleaned['system_id'] == system_id
    ]
    ind = relevant_rows_systems.index[0]
    lon = systems_cleaned.loc[ind, 'longitude']
    lat = systems_cleaned.loc[ind, 'latitude']
    sample_year = int(systems_cleaned.loc[ind, 'sample_year'])
    if sample_year < 1999:
        sample_year = 1999
    url_start = 'https://developer.nlr.gov/api/nsrdb/v2/solar/'\
        + 'nsrdb-GOES-aggregated-v4-0-0-download.csv'
    payload = {'names': sample_year,
               'wkt': point_maker(lon, lat),
               'interval': 60,
               'attributes': 'dhi',
               'utc': 'false',
               'leap_day': 'true',
               'api_key': api_key,
               'email': e_mail}
    my_file = Path(
        my_dir_string + f'{system_id}_{sample_year}_irradiance.csv'
    )
    my_file.touch()
    call_time = time.time()
    r = requests.get(url=url_start, params=payload)
    results = r.text
    if results:
        with open(my_file, 'w') as writer:
            writer.writelines(r.text)
        # make logs
        if make_logs:
            log_path = Path(log_path)
            try:
                with open(log_path, mode='a') as log_adder:
                    log_adder.writelines(
                        f'GET request from {r.url}'
                        + f'at time {call_time}\n'
                    )
            except FileNotFoundError:
                log_path.touch()
                with open(log_path, mode='w') as log_adder:
                    log_adder.writelines(
                        f'GET request from {r.url}'
                        + f'at time {call_time}\n'
                    )
            except BaseException as e:
                raise e
            # append new notes to data_inventory.csv
            data_inventory_path = '../../data_inventory.csv'
            try:
                with open(data_inventory_path, 'a') as data_adder:
                    data_adder.writelines(
                        f'Filename {my_file}, {data_directory_description} \n'
                    )
            except FileNotFoundError:
                data_inventory_path.touch()
                with open(log_path, mode='w') as data_adder:
                    data_adder.writelines(
                        f'Filename {my_file}, {data_directory_description} \n'
                    )
            except BaseException as e:
                raise e
        return True
    else:
        return False


if __name__ == '__main__':
    # make target directory
    my_dir = Path(my_dir_string)
    if not my_dir.is_dir():
        my_dir.mkdir(parents=True)
    # load api and e-mail
    api_key = input('What is your NSRDB API key?')
    email = input('What is your e-mail address?')
    for i in tqdm(range(i_start, i_end + 1)):
        system_id = all_parquet_system_ids[i]
        download_irradiance_sample(
            system_id=system_id,
            api_key=api_key,
            e_mail=email,
            make_logs=make_logs,
            log_path=f'../../logs/logs_system_id={system_id}.csv',
            data_directory_description=f'Irradiance Data Sample for {system_id}'
        )
