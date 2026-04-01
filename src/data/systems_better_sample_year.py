"""Get a good sampling year for Parquet systems.
Want the second *consecutive* year, if one exists.
Otherwise, just the singleton year."""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
parquet_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'is_lake_parquet_data']
]  # is already boolean!
all_parquet_system_ids = list(parquet_systems.system_id.unique())
all_parquet_system_ids.sort()

systems_cleaned.loc[:, 'sample_year'] = pd.Series(
    data=[0] * systems_cleaned.shape[0],
    dtype='int32',
    name='sample_year'
)

for system_id in tqdm(all_parquet_system_ids):
    good_years = []
    access_system_dir = Path(f'../../../data_ds_project/systems/parquet/{system_id}/')
    for year in range(1994, 2026):
        current_year_pq = pq.ParquetDataset(
            access_system_dir,
            filters=[
                ('measured_on', '>=', datetime(year, 1, 1)),
                ('measured_on', '<', datetime(year+1, 1, 1))
            ])
        current_year_df = current_year_pq.read(columns=['measured_on',]).to_pandas()
        if (current_year_df is not None) and (current_year_df.shape[0] > 0):
            good_years.append(year)
    # determine if an appropriate sample year
    # that is to say, a consecutive year with data, if more than 1 year
    # else, just the year we have
    sample_year = pd.NA
    if len(good_years) >= 2:
        for year in good_years[:-1]:
            if (year + 1) in good_years:
                sample_year = year + 1
                break
    elif len(good_years) == 1:
        sample_year = good_years[0]
    # write to systems_cleaned
    relevant_rows = systems_cleaned[systems_cleaned['system_id'] == system_id]
    for ind in relevant_rows.index:
        systems_cleaned.loc[ind, 'sample_year'] = sample_year

systems_cleaned.to_csv('../../data/core/systems_cleaned.csv', index=None)
