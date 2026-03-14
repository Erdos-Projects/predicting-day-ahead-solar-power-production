'''Grab relevant variables, rename them, classify by part/subpart, and save-by-metric.'''
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from dc_power_static import find_aggregate_dc_power_names, \
    find_all_dc_pow_metrics, dc_power_dataframe_generator

if __name__ == '__main__':
    # Load metadata
    systems_cleaned = pd.read_csv('../../../data/core/systems_cleaned.csv')
    metrics_dir = Path("../../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    metrics_id_set = set(metrics_df.system_id)

    # clarify dc power metadata
    (all_dc_pow_metrics, all_dc_pow_metadata) = find_all_dc_pow_metrics(
        print_messages=False,
        source_matter=False
    )
    # clarify ac power metadata
    # clarify module metadata
    # etc.

    # update systems_cleaned
    all_variables = ['dc_power', ]
    all_metadata_dataframes = [all_dc_pow_metadata,]
    # will add more later

    # update systems_cleaned, part_1
    for j in range(len(all_variables)):
        var_name = all_variables[j]
        metadata_dataframe = all_metadata_dataframes[j]
        systems_cleaned = pd.merge(
            left=systems_cleaned,
            right=metadata_dataframe,
            left_on='system_id',
            right_index=True,
            suffixes=(None, f'_{var_name}')
        )

    # make the dataframe
    for system_id in systems_cleaned.system_id:
        relevant_rows_systems = systems_cleaned[
            systems_cleaned['system_id'] == system_id
        ]
        my_dfs = []
        used_variables = []
        my_metric_ids = []
        # add dc_power
        (dc_df, dc_renamer_dict) = dc_power_dataframe_generator(
            system_id=system_id,
            tall_or_wide='wide',
            error_on_no_data=False,
            size_standard='kW'
        )
        # can definitely be omissions
        if dc_df is not None:
            my_dfs.append(dc_df)
            my_metric_ids.append(**dc_renamer_dict.keys())
            used_variables.append('dc_power')
        # add ac_power
        # add module temperature
        # etc.

        # merge dataframes for each variable
        total_df = my_dfs[0]
        for j in range(1, len(my_dfs)):
            var_name = used_variables[j]
            df = my_dfs[j]
            total_df = pd.merge(
                left=total_df,
                right=df,
                on='measured_on',
                suffixes=(None, f'_{var_name}')
            )
        # will do the saving code when running for real
        # pd.to_parquet(total_df)?
        # pd.to_csv(total_df)?
        for ind in relevant_rows_systems.index:
            systems_cleaned.loc[
                ind, 'all_metrics_used'
            ] = used_variables
        # systems_cleaned.to_csv('../../../data/core/system_cleaned.csv')
