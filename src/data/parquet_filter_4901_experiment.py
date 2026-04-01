import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from time import time


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
    err_metrics = metrics_search_for_two_fragments_df(
        trimmed_relevant_rows_metrics, 'err', 'fault', 'or'
    )
    return pd.concat(
        [ac_pow_metrics, dc_pow_metrics,
         volt_metrics, curr_metrics,
         irrad_metrics, temp_metrics,
         err_metrics]
    )


choice = 1
if __name__ == '__main__':
    print('Starting loading')
    st = time()
    metrics_dir = Path("../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    metrics_id_set = set(metrics_df.system_id)
    selection_10 = load_relevant_data(
        metrics_df=metrics_df, system_id=10
    )
    metric_ids = selection_10['metric_id']
    metric_names = selection_10['sensor_name']

    access_system_dir = Path('../../../data_ds_project/systems/parquet/10/')
    current_pq = pq.ParquetDataset(
        access_system_dir,
        filters=[
            ('metric_id', 'in', metric_ids)
        ])
    current_df = current_pq.read().to_pandas()
    # standard cleaning
    current_df = current_df.drop(columns='utc_measured_on')
    current_df = current_df.drop_duplicates()
    current_df['mean_value'] = current_df.groupby(
        ['measured_on', 'metric_id']
    )['value'].transform('mean')
    current_df = current_df.drop(columns='value')
    current_df = current_df.drop_duplicates()

    # potential re-namer dict
    renamer_dict = dict()
    for ind in selection_10.index:
        renamer_dict[selection_10.loc[ind, 'metric_id']]\
            = selection_10.loc[ind, 'sensor_name']

    et = time()
    duration = (et - st)/60
    print(f'Preprocessing completed in {duration:.3f} minutes.')

    # time start
    print('Starting divergence')
    st = time()
    # choices 1-2 = tall parquet
    if choice <= 2:
        if choice == 1:  # no renaming
            my_path = Path('../../../data_ds_project/sorted_by_metric/10_parquet_a/')
            current_df.to_parquet(
                my_path,
                partition_cols=['metric_id'],
                index=None
            )
        elif choice == 2:  # renaming
            my_path = Path('../../../data_ds_project/sorted_by_metric/10_parquet_b/')
            current_df['metric_id'] = current_df.apply(
                lambda row: renamer_dict[row['metric_id']], axis=1
            )
            current_df.to_parquet(
                my_path,
                partition_cols=['metric_id'],
                index=None
            )
    else:
        current_df_wide = current_df.pivot(
            index='measured_on',
            columns='metric_id',
            values='mean_value'
        )
        current_df_wide.columns.name = ''
        current_df_wide = current_df_wide.reset_index()
        current_df_wide = current_df_wide.rename(columns=renamer_dict)
        if choice == 3:  # parquet by year
            my_path = Path('../../../data_ds_project/sorted_by_metric/10_parquet_c/')
            current_df_wide['year'] = current_df_wide.measured_on.dt.year
            current_df_wide.to_parquet(
                my_path,
                partition_cols=['year'],
                index=None
            )
        elif choice == 4:  # csv by year
            my_dir = Path('../../../data_ds_project/sorted_by_metric/10_csv/')
            try:
                my_dir.mkdir(exist_ok=True)
            except BaseException as e:
                raise e
            current_df_wide['year'] = current_df_wide.measured_on.dt.year
            for year in current_df_wide.year.unique():
                current_df_year = current_df_wide[current_df_wide['year'] == year]
                year_path = f'../../../data_ds_project/sorted_by_metric/10_csv/{year}.csv'
                current_df_year.to_csv(
                    year_path,
                    index=False
                )
    et = time()
    duration = (et - st)/60
    print(f'Choice {choice} completed in {duration:.3f} minutes.')
