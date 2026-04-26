import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json
from pvlib.iotools import get_nsrdb_psm4_aggregated
from pwr.gen_variable_standard_static import \
    metrics_search_for_two_fragments_df
from tqdm import tqdm
from time import sleep


# choices
make_logs = True
# default 50 locations, so index 0 - 49
i_start = 0
i_end = 49
major_dir_str = '../../../data_ds_project/nsrdb/'
existing_data_str = '../../../data_ds_project/testing_yearly_parquet/'

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
metrics_dir = Path("../../data/raw/parquet-metrics/")
metrics_pq = pq.ParquetDataset(metrics_dir)
metrics_df = metrics_pq.read().to_pandas()
metrics_id_set = set(metrics_df.system_id)
ac_power_metrics = metrics_search_for_two_fragments_df(
    metrics_df, 'ac', 'pow', 'and'
)
ac_power_systems = set(ac_power_metrics['system_id'])
two_years_days = 2 * 365
enough_data_systems = systems_cleaned[
    systems_cleaned['num_days_actual_records'] >= two_years_days
]
enough_data_ids = set(enough_data_systems.system_id)
enough_data_parquet_power_systems = enough_data_ids.intersection(
    ac_power_systems
)
enough_data_parquet_power_list = list(enough_data_parquet_power_systems)
enough_data_parquet_power_list.sort()

final_systems_selection = enough_data_systems[
    enough_data_systems['system_id'].isin(enough_data_parquet_power_list)
]
# go ahead and sort by latitude and longitude
final_systems_locs = final_systems_selection.groupby(
    ['latitude', 'longitude']
)['system_id'].unique()


def download_irradiance_data_one_year(
        latitude,
        longitude,
        api_key,
        e_mail,
        year,
        make_logs=False,
        log_path='../../logs/logs.csv',
):
    loc_weather, loc_meta = get_nsrdb_psm4_aggregated(
        latitude=latitude,
        longitude=longitude,
        api_key=api_key,
        email=e_mail,
        year=year,
        time_step=60,
        parameters=['clearsky_dhi', 'clearsky_dni',
                    'clearsky_ghi', 'dhi',
                    'dni', 'ghi',
                    'solar_zenith_angle',
                    'ssa'],
        leap_day=True
    )
    if make_logs:
        log_path = Path(log_path)
        try:
            with open(log_path, mode='a') as log_adder:
                log_adder.writelines(
                    f'NSRDB irradiance data for year {year}, '
                    + f'latitude {latitude}, longitude {longitude}.\n'
                )
        except FileNotFoundError:
            log_path.touch()
            with open(log_path, mode='w') as log_adder:
                log_adder.writelines(
                    f'NSRDB irradiance data for year {year}, '
                    + f'latitude {latitude}, longitude {longitude}.\n'
                )
        except BaseException as e:
            raise e
    return (loc_weather, loc_meta)


def copy_data_to_all_systems(
    weather_data_single_year: pd.DataFrame,
    weather_metadata_single_year: dict,
    all_ids,
    year,
    make_logs: bool,
    data_directory_description
):
    for system_id in all_ids:
        system_data_path = Path(f'{major_dir_str}{system_id}/data/')
        if not system_data_path.is_dir():
            system_data_path.mkdir(parents=True)
        weather_data_single_year.to_parquet(
            system_data_path,
            partition_cols='Year'
        )
        system_metadata_dir = Path(f'{major_dir_str}{system_id}/metadata/')
        if not system_metadata_dir.is_dir():
            system_metadata_dir.mkdir()
        system_metadata_path = Path(
            f'{major_dir_str}{system_id}/metadata/year.json'
        )
        system_metadata_path.touch(exist_ok=True)
        with open(system_metadata_path, 'w') as writer:
            json.dump(weather_metadata_single_year, writer)
        # append new notes to data_inventory.csv
        data_inventory_path = '../../data_inventory.csv'
        try:
            with open(data_inventory_path, 'a') as data_adder:
                data_adder.writelines(
                    f'Directory {system_data_path}, year {year}, {data_directory_description} \n'
                )
        except FileNotFoundError:
            data_inventory_path.touch()
            with open(data_inventory_path, mode='w') as data_adder:
                data_adder.writelines(
                    f'Filename {system_metadata_path}, {data_directory_description} \n'
                )
        except BaseException as e:
            raise e


if __name__ == '__main__':
    api_key = input('What is your NSRDB API key?')
    e_mail = input('What is your e-mail address?')
    for j in tqdm(range(i_start, i_end + 1)):
        this_lat, this_long = final_systems_locs.index[j]
        my_systems = final_systems_locs.iloc[j]
        # check each year for validity
        good_years = []
        # Meterology data starts in 1998,
        # PVDAQ data ends in 2023
        for year in range(1998, 2024):
            good_year = False
            for system_id in my_systems:
                existing_system_dir = Path(existing_data_str + f'{system_id}/')
                current_year_pq = pq.ParquetDataset(
                    existing_system_dir,
                    filters=[('year', '==', year)]
                )
                current_year_df = current_year_pq.read().to_pandas()
                if current_year_df.shape[0] > 10:  # relevant year!
                    good_year = True
                    break
            if good_year:
                # debug
                print(year)
                # downloads
                (loc_year_irrad, loc_year_meta) = download_irradiance_data_one_year(
                    latitude=this_lat,
                    longitude=this_long,
                    api_key=api_key,
                    e_mail=e_mail,
                    year=year,
                    make_logs=make_logs,
                    log_path=f'../../logs/{this_lat}_{this_long}_irrad.csv'
                )
                satellite_lat = loc_year_meta['latitude']
                satellite_long = loc_year_meta['longitude']
                copy_data_to_all_systems(
                    loc_year_irrad,
                    loc_year_meta,
                    my_systems,
                    year,
                    make_logs,
                    f'Satellite irradiance data at {satellite_lat}, {satellite_long} '
                    + f'for the year {year}'
                )
                # avoid overloading requests
                sleep(10)
