import requests
import json
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from pwr.gen_variable_standard_static import \
    metrics_search_for_two_fragments_df
from time import sleep

# full_name = input('What is your full name?')
# email = input('What is your e-mail address?')
api_key = input('What is your NSRDB api key?')

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
systems_cleaned = systems_cleaned[[
    'system_id', 'longitude', 'latitude', 'num_days_actual_records'
]]

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

main_dir_str = '../../../data_ds_project/nsrdb_info/'
main_dir = Path(main_dir_str)
if not main_dir.is_dir():
    main_dir.mkdir(parents=True)
for system_id in enough_data_parquet_power_list:
    relevant_rows_systems = systems_cleaned[
        systems_cleaned['system_id'] == system_id
    ]
    first_ind = relevant_rows_systems.index[0]
    my_long = relevant_rows_systems.at[first_ind, 'longitude']
    my_lat = relevant_rows_systems.at[first_ind, 'latitude']
    url_start = 'https://developer.nlr.gov/api/solar'\
        + '/nsrdb_data_query.json'
    payload = {'api_key': api_key, 'lon': my_long, 'lat': my_lat}
    r = requests.get(url=url_start, params=payload)
    target_file_path = main_dir_str + f'nsrdb_data_options_{system_id}.json'
    my_data = r.json()
    with open(target_file_path, 'w') as writer:
        json.dump(my_data, writer)
    # avoid clogging with too many inquiries
    sleep(2)
