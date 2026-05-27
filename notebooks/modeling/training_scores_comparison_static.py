#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pathlib import Path

# path to data
read_path = "../../../data_ds_project/parquet_cleaned_energy"
systems_cleaned = pd.read_csv("../../data/core/systems_cleaned.csv")
relevant_system_pairs = [(10, None), (50, None), (51, None)]

naive_folder_str = './RS/naive_errors/'
naive_file_suffix = '_naive_errors.csv'
lin_folder_str = './RS/linreg_errors/'
lin_file_suffix = '_linreg_errors.csv'
lin_fourier_suffix = '_linreg_errors_fourier.csv'
prophet_folder_str = './RS/prophet_errors/'
prophet_file_suffix = '_prophet_errors.csv'
sarimax_folder_str = './RS/sarimax_errors/'
sarimax_file_suffix = '_sarimax_errors.csv'
xgboost_folder_str = './CEB/xgboost_results/'
xgboost_folder_suffix = '.csv'
lightgbm_folder_str = './CEB/lightgbm_results/'
lightgbm_folder_suffix = '.csv'

model_tuple = ('naive', 'lin_reg', 'lin_reg_with_fourier', 'prophet', 'sarimax', 'xgboost', 'lightgbm')
model_folder = (naive_folder_str, lin_folder_str, lin_folder_str, prophet_folder_str,
                sarimax_folder_str, xgboost_folder_str, lightgbm_folder_str)
model_suffix = (naive_file_suffix, lin_file_suffix, lin_fourier_suffix, prophet_file_suffix,
                sarimax_file_suffix, xgboost_folder_suffix, lightgbm_folder_suffix)
sarimax_col_names = [
    '2,0,0', '2,0,1', '3,0,0', '3,0,1'
]
xgboost_col_names = [
    '(31, 5, 0.1, 1.0, 0.8)', '(31, 7, 0.1, 0.8, 0.8)', '(31, 10, 0.1, 0.8, 0.8)'
]

lightgbm_col_names = [
    '(31, -1, 0.1, 100, 0.8, 0.8)',
    '(31, -1, 0.1, 100, 1.0, 0.8)'
]


# ## First comparison -- whole training set

def read_results_summaries(system_id: int):
    my_cols = ['per_model_mean', 'per_model_std', 'per_model_min', 'per_model_25p', 'per_model_median', 'per_model_75p', 'per_model_max']
    per_model_results = []
    for j in range(7):
        model_name = model_tuple[j]
        model_results = pd.read_csv(f'{model_folder[j]}{system_id}_None{model_suffix[j]}')
        if model_name == 'xgboost':
            model_results = model_results[xgboost_col_names]
        elif model_name == 'lightgbm':
            model_results = model_results[lightgbm_col_names]
        elif model_name == 'sarimax':
            model_results = model_results[sarimax_col_names]
        model_results = model_results.rename(columns={
            col_name: f'{model_name}_{col_name}' for col_name in model_results.columns
        })
        model_results = model_results.transpose()
        ordinary_cols = model_results.columns
        model_results.loc[:, 'per_model_mean'] = model_results[ordinary_cols].mean(axis=1)
        model_results.loc[:, 'per_model_std'] = model_results[ordinary_cols].std(axis=1)
        model_results.loc[:, 'per_model_min'] = model_results[ordinary_cols].min(axis=1)
        model_results.loc[:, 'per_model_25p'] = model_results[ordinary_cols].quantile(q=0.25, axis=1)
        model_results.loc[:, 'per_model_median'] = model_results[ordinary_cols].quantile(q=0.5, axis=1)
        model_results.loc[:, 'per_model_75p'] = model_results[ordinary_cols].quantile(q=0.75, axis=1)
        model_results.loc[:, 'per_model_max'] = model_results[ordinary_cols].max(axis=1)
        per_model_results.append(model_results[my_cols])
    total_results = pd.concat(per_model_results)
    return total_results


result_summaries_folder = Path('../../results/final/training_summaries/')
if not result_summaries_folder.is_dir():
    result_summaries_folder.mkdir()
for system_id in [10, 50, 51]:
    results_id = read_results_summaries(system_id=system_id)
    results_id.to_csv(result_summaries_folder / f'full_results_{system_id}.csv', index=False)

# Quick commentary
# System 10
# Sarimax best, prophet second-best
# System 50
# Sarimax best, XGBoost 2nd-best
# System 51
# OK, Sarimax appears to be the best model-set in training!

# 2nd Comparison: 2nd half training data only
# (on the assumption that later, more-time-data datasets will perform better)


def read_results_second_half(system_id: int):
    my_cols = ['per_model_mean', 'per_model_std', 'per_model_min',
               'per_model_25p', 'per_model_median', 'per_model_75p',
               'per_model_max']
    per_model_results = []
    for j in range(7):
        model_name = model_tuple[j]
        model_results = pd.read_csv(f'{model_folder[j]}{system_id}_None{model_suffix[j]}')
        if model_name == 'xgboost':
            model_results = model_results[xgboost_col_names]
        elif model_name == 'lightgbm':
            model_results = model_results[lightgbm_col_names]
        elif model_name == 'sarimax':
            model_results = model_results[sarimax_col_names]
        model_results = model_results.rename(columns={
            col_name: f'{model_name}_{col_name}' for col_name in model_results.columns
        })
        # 2nd half of data
        num_entries = model_results.shape[0]
        model_results = model_results.iloc[int(num_entries/2):]
        model_results = model_results.transpose()
        ordinary_cols = model_results.columns
        model_results.loc[:, 'per_model_mean'] = model_results[ordinary_cols].mean(axis=1)
        model_results.loc[:, 'per_model_std'] = model_results[ordinary_cols].std(axis=1)
        model_results.loc[:, 'per_model_min'] = model_results[ordinary_cols].min(axis=1)
        model_results.loc[:, 'per_model_25p'] = model_results[ordinary_cols].quantile(q=0.25, axis=1)
        model_results.loc[:, 'per_model_median'] = model_results[ordinary_cols].quantile(q=0.5, axis=1)
        model_results.loc[:, 'per_model_75p'] = model_results[ordinary_cols].quantile(q=0.75, axis=1)
        model_results.loc[:, 'per_model_max'] = model_results[ordinary_cols].max(axis=1)
        per_model_results.append(model_results[my_cols])
    total_results = pd.concat(per_model_results)
    return total_results


for system_id in [10, 50, 51]:
    second_results_id = read_results_second_half(system_id=system_id)
    second_results_id.to_csv(result_summaries_folder / f'second_half_results_{system_id}.csv', index=False)

# System 10
# The baseline model and linear regression are almost as good as SARIMAX; gradient boosting has no such consistent effect.

# System 50
# Still the same rankings as the full set of data.

# System 51
# Ditto
