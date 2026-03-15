import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from time import time
from datetime import datetime

# changes here:
# rich-data systems or all Parquet systems?
rich_only = True
# start and end index
i_start = 0
i_end = 38  # 38 if rich-data systems
# or 155 if all Parquet Systems


def metrics_search_for_fragment_df(df: pd.DataFrame, fragment: str):
    '''Search for fragments of a name in sensor_name and common_name'''
    fragment = fragment.lower()
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
    ]


def metrics_search_for_two_fragments_df(df: pd.DataFrame, fragment_1: str,
                                        fragment_2: str, and_or: str):
    '''Search for fragments of two names in sensor_name and common name.
    Use and_or to switch between "both" and "at least one" modes'''
    fragment_1 = fragment_1.lower()
    fragment_2 = fragment_2.lower()
    if and_or == 'and':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False)))
            & ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False)))
        ]
    elif and_or == 'or':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False)))
            | ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False)))
        ]


def load_relevant_data(metrics_df: pd.DataFrame,
                       system_id: int):
    relevant_rows_metrics = metrics_df[metrics_df['system_id'] == system_id]
    trimmed_relevant_rows_metrics = relevant_rows_metrics[
        ['metric_id', 'sensor_name', 'common_name', 'units']
    ]
    ac_pow_metrics = metrics_search_for_two_fragments_df(
        trimmed_relevant_rows_metrics, 'ac', 'pow', 'and'
    )
    # drop power factor, etc.
    dc_pow_metrics = metrics_search_for_two_fragments_df(
        trimmed_relevant_rows_metrics, 'dc', 'pow', 'and'
    )
    volt_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'volt'
    )
    # drop percentages for now
    volt_metrics = volt_metrics[
        volt_metrics['units'] == 'V'
    ]
    curr_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'curr'
    )
    irrad_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'irrad'
    )
    temp_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'temp'
    )
    err_metrics = metrics_search_for_two_fragments_df(
        trimmed_relevant_rows_metrics, 'err', 'fault', 'or'
    )
    return pd.concat(
        [ac_pow_metrics, dc_pow_metrics,
         volt_metrics, curr_metrics,
         irrad_metrics, temp_metrics,
         err_metrics]
    )


systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
parquet_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'is_lake_parquet_data']
]  # is already boolean!
all_parquet_system_ids = list(parquet_systems.system_id.unique())
all_parquet_system_ids.sort()

metrics_dir = Path("../../data/raw/parquet-metrics/")
metrics_pq = pq.ParquetDataset(metrics_dir)
metrics_df = metrics_pq.read().to_pandas()

all_data_systems = systems_cleaned[
    systems_cleaned['has_current_data']
    & systems_cleaned['has_irradiance_data']
    & systems_cleaned['has_power_data']
    & systems_cleaned['has_temperature_data']
    & systems_cleaned['has_voltage_data']
]
all_rich_parquet_data_ids = set(all_data_systems.system_id.unique()).intersection(
    set(all_parquet_system_ids)
)
all_rich_parquet_data_ids = list(all_rich_parquet_data_ids)
all_rich_parquet_data_ids.sort()


def filter_data(system_id: int):
    '''Extract all possible relevant data and flip it,
    for the given system_id.  No renaming possible here.'''
    selected_metrics = load_relevant_data(
        metrics_df=metrics_df, system_id=system_id
    )
    metric_ids = list(selected_metrics['metric_id'])
    metric_names = list(selected_metrics['sensor_name'])
    # potential re-namer dict
    renamer_dict = dict()
    for j in range(len(metric_ids)):
        renamer_dict[metric_ids[j]]\
            = metric_names[j]

    access_system_dir = Path(f'../../../data_ds_project/systems/parquet/{system_id}/')
    target_dir = Path(f'../../../data_ds_project/filtered/systems/parquet/{system_id}/')
    if not target_dir.is_dir():  # if the directory does exist, assumed nothing to do.
        target_dir.mkdir(parents=True)
        for year in range(1997, 2026):
            current_year_pq = pq.ParquetDataset(
                access_system_dir,
                filters=[
                    ('metric_id', 'in', metric_ids),
                    ('measured_on', '>=', datetime(year, 1, 1)),
                    ('measured_on', '<', datetime(year+1, 1, 1))
                ])
            current_year_df = current_year_pq.read().to_pandas()
            if (current_year_df is not None) and (current_year_df.shape[0] > 0):
                # standard cleaning
                current_year_df = current_year_df.drop(columns='utc_measured_on')
                current_year_df = current_year_df.drop_duplicates()
                current_year_df['mean_value'] = current_year_df.groupby(
                    ['measured_on', 'metric_id']
                )['value'].transform('mean')
                current_year_df = current_year_df.drop(columns='value')
                current_year_df = current_year_df.drop_duplicates()
                current_year_df_wide = current_year_df.pivot(
                    index='measured_on',
                    columns='metric_id',
                    values='mean_value'
                )
                current_year_df_wide.columns.name = ''
                current_year_df_wide = current_year_df_wide.reset_index()
                current_year_df_wide = current_year_df_wide.rename(columns=renamer_dict)
                current_year_df_wide['year'] = current_year_df_wide.measured_on.dt.year
                if len(set(current_year_df_wide.year)) != 1:
                    raise RuntimeError('Some coding error in extracting years!')
                else:
                    current_year_df_wide.to_parquet(
                        target_dir,
                        partition_cols=['year'],
                        index=None
                    )


if __name__ == '__main__':
    for i in range(i_start, i_end + 1):
        print(i)
        if rich_only:
            system_id = all_rich_parquet_data_ids[i]
        else:
            system_id = all_parquet_system_ids[i]
        st = time()
        filter_data(system_id=system_id)
        et = time()
        duration = (et - st)/60
        print(f'System {system_id} completed in {duration:.3f} minutes.')
