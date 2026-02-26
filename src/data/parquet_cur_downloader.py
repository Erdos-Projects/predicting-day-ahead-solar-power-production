'''Download the main metadata source,
clean up the metadata a little bit,
and add more downloads with info
on electrical current.'''

import pandas as pd
import boto3
from botocore.handlers import disable_signing
import time
from systems_initializer import downloader

# choices -- choose here
i_start = 0
i_end = 77

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
parquet_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'is_lake_parquet_data']
]  # is already boolean!
# want data with current,
# but not irradiance (already downloaded)
curr_parquet_systems = parquet_systems.loc[
    parquet_systems.loc[:, 'has_current_data']
]
irrad_parquet_systems = parquet_systems.loc[
    parquet_systems.loc[:, 'has_irrad_data']
]
curr_systems_set = set(curr_parquet_systems.system_id.unique())
irrad_systems_set = set(irrad_parquet_systems.system_id.unique())
curr_not_irrad_set = curr_systems_set.difference(irrad_systems_set)
curr_not_irrad_list = list(curr_not_irrad_set)


def download_index_set(j_start, j_end):
    '''Download from the i_start position on the list
    to the i_end position on the list'''
    for j in range(j_start, j_end+1):
        print(f'j={j}')
        system_id = curr_not_irrad_list[j]
        print(f'system_id={system_id}')
        st = time.time()
        downloader(
            f'../../../data_ds_project/systems/parquet/{system_id}/',
            f'pvdaq/parquet/pvdata/system_id={system_id}/',
            warn_empty=True,
            log_path=f'../../logs/logs_system_id={system_id}.csv',
            data_directory_description=f'Parquet Data for System {system_id}'
        )
        et = time.time()
        duration = (et-st)/60
        print(f'Finished system_id {system_id} in {duration:.4f} minutes.')
        # time.sleep(90)  space out calls


if __name__ == '__main__':
    print(len(curr_not_irrad_list))
    download_index_set(i_start, i_end)
