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

systems_cleaned.loc[:, 'num_days_actual_records'] = pd.Series(
    data=[0] * systems_cleaned.shape[0],
    dtype='int32',
    name='num_days_actual_records'
)

num_days = {
    system_id: 0 for system_id in all_parquet_system_ids
}

for system_id in tqdm(all_parquet_system_ids):
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
            current_year_df['date'] = current_year_df['measured_on'].dt.date
            num_days[system_id] += len(current_year_df['date'].unique())
    relevant_rows = systems_cleaned[systems_cleaned['system_id'] == system_id]
    for ind in relevant_rows.index:
        systems_cleaned.loc[ind, 'num_days_actual_records'] = num_days[system_id]

systems_cleaned.to_csv('../../data/core/systems_cleaned.csv', index=None)
