from PreRun import PreRun, PostRun
import pandas as pd
import numpy as np
from pathlib import Path
import lightgbm as lgb
from itertools import product
from by_dates_Kfold import k_fold_split_option_a
from tqdm import tqdm

results_folder = Path('./lightbgm_results/')
if not results_folder.is_dir():
    results_folder.mkdir()

systems_cleaned = pd.read_csv('../../../data/core/systems_cleaned.csv')


# Wrappers on custom objective function
def custom_obj(preds: np.ndarray, eval_data: lgb.Dataset):
    y_true = eval_data.get_label()
    # first derivative is (2x) * [0.5 signum(x) + 1.5]
    multiplier = 1.5 + 0.5 * np.sign(preds - y_true)
    grad = 2 * (preds - y_true) * multiplier
    hess = 2 * multiplier * np.ones_like(preds)
    return (grad, hess)


def custom_eval(preds: np.ndarray, eval_data: lgb.Dataset):
    y_true = eval_data.get_label()
    metric_name = 'weight_over'
    value = PostRun.custom_error(y_true, preds, a=1, b=2)
    is_higher_better = False
    return (metric_name, value, is_higher_better)


def custom_lgbm_regressor_obj(y_true: np.ndarray, y_pred: np.ndarray):
    # first derivative is (2x) * [0.5 signum(x) + 1.5]
    multiplier = 1.5 + 0.5 * np.sign(y_pred - y_true)
    grad = 2 * (y_pred - y_true) * multiplier
    hess = 2 * multiplier * np.ones_like(y_pred)
    return (grad, hess)


def custom_lgbm_regressor_eval(y_true: np.ndarray, y_pred: np.ndarray):
    metric_name = 'weight_over'
    value = PostRun.custom_error(y_true, y_pred, a=1, b=2)
    maximize = False
    return (metric_name, value, maximize)


def custom_pre_scorer(y_true, y_pred):
    return PostRun.custom_error(y_true, y_pred, a=1, b=2)


# The general runner
def lightgbm_one_layer_d(system_id: int, read_path: str, met_or_inv, systems_cleaned: pd.DataFrame, streak_len: int,
                         n_splits_outer: int, sample_spacing: int):
    prerun_system = PreRun(system_id, read_path, met_or_inv, systems_cleaned)
    prerun_system.add_energy_features_only(daily_lags=2, include_last_year=True, include_hour_cyclic=True, include_day_of_year_cyclic=True)
    prerun_system.add_weather_features_only()
    prerun_system.good_end_days_naive(streak=streak_len)
    good_ends = prerun_system.end_days_naive.copy(deep=True)
    df = prerun_system.amended_data.copy(deep=True)
    df['year'] = df['time'].dt.year
    my_cols = ['year', 'hour_sin', 'hour_cos', 'day_of_year_sin', 'day_of_year_cos',
               'last_year', '2_days_ago', 'cloud_cover', 'global_tilted_irradiance', 'proportion_daytime']
    df_train = df.iloc[0:int(len(df)*0.8)]
    outer_cv = k_fold_split_option_a(
        df_train=df_train,
        good_ends=good_ends,
        n_splits=n_splits_outer,
        window_size=None,
        front_or_back='back',
        gap_day=True,
        return_type='index',
        sample_spacing=sample_spacing)
    param_grid = {
        'num_leaves': (7, 15, 31),
        'max_depth': (-1, 5, 7),
        'learning_rate': (0.1, 0.2),
        'n_estimators': (100,),  # wanted more, but there is literally no time!
        'subsample': (0.8, 1.0),
        'colsample_bytree': (0.8, 1.0),
    }
    col_names = [str(param_settings) for param_settings in product(
        param_grid['num_leaves'],
        param_grid['max_depth'],
        param_grid['learning_rate'],
        param_grid['n_estimators'],
        param_grid['subsample'],
        param_grid['colsample_bytree'])]
    outer_test_results = pd.DataFrame(
        np.zeros((len(outer_cv), len(col_names))),
        columns=col_names
    )
    for i, (train_ind, test_ind) in enumerate(tqdm(outer_cv)):
        df_tt = df_train.loc[train_ind]
        df_ho = df_train.loc[test_ind]
        X_tt = df_tt[my_cols]
        y_tt = df_tt['energy']
        lgb_tt = lgb.Dataset(data=X_tt, label=y_tt)
        X_ho = df_ho[my_cols]
        y_ho = df_ho['energy']
        lgb_ho = lgb.Dataset(data=X_ho, label=y_ho, reference=lgb_tt)
        for j, param_settings in enumerate(product(
            param_grid['num_leaves'],
            param_grid['max_depth'],
            param_grid['learning_rate'],
            param_grid['n_estimators'],
            param_grid['subsample'],
            param_grid['colsample_bytree']
        )):
            temp_params = {
                'objective': custom_obj,
                'task': 'train',
                'num_leaves': param_settings[0],
                'max_depth': param_settings[1],
                'learning_rate': param_settings[2],
                'num_iterations': param_settings[3],
                'subsample': param_settings[4],
                'colsample_bytree': param_settings[5],
                'metric': 'custom',
                'verbose': -1
            }
            bst = lgb.train(
                params=temp_params,
                train_set=lgb_tt,
                feval=custom_eval,
                valid_sets=[lgb_ho,]
            )
            y_pred = bst.predict(X_ho)
            val_error = PostRun.custom_error(y_pred, y_ho, 1, 2)
            outer_test_results.at[i, col_names[j]] = val_error
    outer_test_results.to_csv(f'./testing_many_folds/{system_id}_{met_or_inv}.csv', index=False)
    return outer_test_results


params = [(10, None, 2), (50, None, 5), (51, None, 5)]
for param in params:
    system_id = params[0]
    met_or_inv = params[1]
    num_skips = params[2]
    lightgbm_one_layer_d(
        system_id, '../../../../data_ds_project/parquet_cleaned_energy/', met_or_inv,
        systems_cleaned, 7, -1, num_skips
    )
