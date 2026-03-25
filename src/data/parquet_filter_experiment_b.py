'''Import one year at a time (or smaller)! to keep file-sizes down.'''
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from time import time
from itertools import product


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


def widened_search_for_fragment_df(df: pd.DataFrame, fragment: str):
    '''Search for a fragment in calc_details and source_type
    as well as in sensor_name and common_name'''
    fragment = fragment.lower()
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'calc_details'].str.contains(fragment, case=False))
        | (df.loc[:, 'source_type'].str.contains(fragment, case=False))
    ]


def widened_search_for_two_fragments_df(df: pd.DataFrame, fragment_1: str,
                                        fragment_2: str, and_or: str):
    '''Search for two fragments in calc_details and source_type
    as well as in sensor_name and common_name'''
    fragment_1 = fragment_1.lower()
    fragment_2 = fragment_2.lower()
    if and_or == 'and':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_1, case=False)))
            & ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_2, case=False)))
        ]
    elif and_or == 'or':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_1, case=False)))
            | ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_2, case=False)))
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
    curr_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'curr'
    )
    irrad_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'irrad'
    )
    temp_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'temp'
    )
    err_metrics = metrics_search_for_fragment_df(
        trimmed_relevant_rows_metrics, 'err'
    )
    return pd.concat(
        [ac_pow_metrics, dc_pow_metrics,
         volt_metrics, curr_metrics,
         irrad_metrics, temp_metrics,
         err_metrics]
    )


choice = 2
if __name__ == '__main__':
    print('Starting')
    st = time()
    metrics_dir = Path("../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(
        metrics_dir,
        filters=[
            ('system_id', '==', 4901)
        ]
    )
    metrics_df = metrics_pq.read().to_pandas()
    selection_4901 = load_relevant_data(
        metrics_df=metrics_df, system_id=4901
    )
    metric_ids = selection_4901['metric_id']
    metric_names = selection_4901['sensor_name']
    # potential re-namer dict
    renamer_dict = dict()
    for ind in selection_4901.index:
        renamer_dict[selection_4901.loc[ind, 'metric_id']]\
            = selection_4901.loc[ind, 'sensor_name']

    access_system_dir = Path('../../../data_ds_project/systems/parquet/4901/')
    if choice == 1:
        my_dir = Path('../../../data_ds_project/sorted_by_metric/4901_parquet_c/')
        my_dir.mkdir(exist_ok=True)
        for year in range(2015, 2026):
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
                        my_dir,
                        partition_cols=['year'],
                        index=None
                    )
    elif choice == 2:  # load one metric at a time
        my_dir = Path('../../../data_ds_project/sorted_by_metric/4901_parquet_d/')
        my_dir.mkdir(exist_ok=True)
        for (year, metric_id) in product(range(2015, 2026), metric_ids):
            year_metric_pq = pq.ParquetDataset(
                access_system_dir,
                filters=[
                    ('metric_id', '==', metric_id),
                    ('measured_on', '>=', datetime(year, 1, 1)),
                    ('measured_on', '<', datetime(year+1, 1, 1))
                ])
            year_metric_df = year_metric_pq.read().to_pandas()
            if (year_metric_df is not None) and (year_metric_df.shape[0] > 0):
                # standard cleaning
                year_metric_df = year_metric_df.drop(columns='utc_measured_on')
                year_metric_df = year_metric_df.drop_duplicates()
                year_metric_df['mean_value'] = year_metric_df.groupby(
                    ['measured_on', 'metric_id']
                )['value'].transform('mean')
                year_metric_df = year_metric_df.drop(columns='value')
                year_metric_df = year_metric_df.drop_duplicates()
                # no need to pivot if one metric, just rename the columns
                year_metric_df['metric_id'] = year_metric_df.apply(
                    lambda row: renamer_dict[row['metric_id']], axis=1
                )
                year_metric_df['year'] = year_metric_df.measured_on.dt.year
                if len(set(year_metric_df.year)) != 1:
                    raise RuntimeError('Some coding error in extracting years!')
                else:
                    year_metric_df.to_parquet(
                        my_dir,
                        partition_cols=['year', 'metric_id'],
                        index=None
                    )

    et = time()
    duration = (et - st)/60
    print(f'Choice {choice} completed in {duration:.3f} minutes.')
