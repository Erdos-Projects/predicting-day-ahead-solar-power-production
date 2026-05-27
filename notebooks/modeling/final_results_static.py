#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import timedelta
from itertools import product
from RS.PreRun import PreRun, PostRun
from statsmodels.tsa.statespace.sarimax import SARIMAX

#  path to data
read_path = "../../../data_ds_project/parquet_cleaned_energy/"
systems_cleaned = pd.read_csv("../../data/core/systems_cleaned.csv")
relevant_system_pairs = [(10, None), (50, None), (51, None)]

#  results folder config
results_folder_str = './final_sarimax_errors/'
results_folder = Path(results_folder_str)
if not results_folder.is_dir():
    results_folder.mkdir()


# Helper function
def use_sarimax(df_train, df_ho, p=0, d=0, q=0, P=0, D=0, Q=0, s=0):

    # separate out the exogenous variables
    df_train = df_train.set_index('time')
    df_ho = df_ho.set_index('time')

    #  Remove duplicate indices (keep first occurrence)
    df_train = df_train[~df_train.index.duplicated(keep='first')]
    df_ho = df_ho[~df_ho.index.duplicated(keep='first')]

    df_train = df_train.asfreq('h')
    df_ho = df_ho.asfreq('h')

    energy_train = df_train['energy']
    # energy_ho = df_ho['energy']

    exog_train = df_train.drop(columns=['energy'])
    exog_ho = df_ho.drop(columns=['energy'])

    # fit the model
    model_sarimax = SARIMAX(
        energy_train, exog=exog_train, order=(p, d, q),
        seasonal_order=(P, D, Q, s)).fit(maxiter=600, disp=False)
    # predict
    y_pred = model_sarimax.forecast(len(df_ho), exog=exog_ho) 

    # error = PostRun.custom_error(energy_ho, y_pred, 1,2)

    return y_pred  # , error


# Training data
p_choices = [2, 3]
# d_choices = [0, 1]
d_choices = [0]
q_choices = [0, 1]

for pair in relevant_system_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'starting system {system_id}, {reader_type}')

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type,
                    path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()

    prerun.fill_missing_hours()
    prerun.add_energy_features_only(
        include_last_year=True, remove_last_year_nans=True, highest_fourier_term_hour=2
    )
    # print(prerun.amended_data)

    prerun.add_weather_features_only()

    all_data = prerun.amended_data

    prerun.good_end_days_naive(10)
    prerun.tts_of_data_using_end_days(remove_first_year=False)

    system_recorded_max = prerun.data['energy'].max()

    # get all train/ho data sets ONCE so it's not redone for each (p,d,q)
    splits = []

    for pred_date in prerun.train_dates['date']:
        train_mask = (
            (all_data['time'] < pred_date - timedelta(days=1)) &
            (all_data['time'] >= pred_date - timedelta(days=10))
        )
        ho_mask = (
            (all_data['time'] >= pred_date - timedelta(days=1)) &
            (all_data['time'] < pred_date + timedelta(days=1))
        )

        train_data = all_data.loc[train_mask].reset_index(drop=True)
        ho_data = all_data.loc[ho_mask].reset_index(drop=True)

        splits.append((pred_date, train_data, ho_data))

    results_dict = {}
    #  print(system_recorded_max)
    #  print(ho_data)
    #  print(ho_data.columns)

    for p, d, q in product(p_choices, d_choices, q_choices):
        print(f'     starting p={p}, d={d}, q={q}')
        errors = []

        for pred_date, train_data, ho_data in splits:
            try: 
                y_pred = use_sarimax(train_data, ho_data, p=p, d=d, q=q)
                #  print(f"original y_pred: {y_pred}")

                # make sure value between 0 and highest observed max
                y_pred = np.clip(y_pred, 0, system_recorded_max)
                # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
                darkness_mask = (~((ho_data['proportion_daytime'] == 0) & (ho_data['global_tilted_irradiance'] == 0))).astype(int)
                #  = 0 when both sublight prop and irr are 0
                y_pred = y_pred*np.array(darkness_mask)
                #  print(f"y_pred with insurance: {y_pred}")

                energy_ho = ho_data['energy']
                #  print(f"energy_ho: {energy_ho}")
                #  grab error data for *after* the skip-day.
                error = PostRun.custom_error(energy_ho.iloc[24:], y_pred.iloc[24:], 1, 2)
                #  print(f'error = {error}')
            except BaseException as e:
                print(f'FAILED: (p,d,q) = {p, d, q} --> {type(e).__name__}: {e}')
                raise e
            errors.append(error)

        results_dict[f"{p},{d},{q}"] = errors
    all_errors_df = pd.DataFrame(results_dict)

    all_errors_df.to_csv(f'{results_folder_str}{system_id}_{reader_type}_sarimax_errors.csv', index=False)


# Print out errors
for pair in relevant_system_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'System {system_id}, {reader_type}, SARIMAX with streak = 10')
    errors = pd.read_csv(f'{results_folder_str}/{system_id}_{reader_type}_sarimax_errors.csv')
    hyperparams = errors.columns

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
    system_recorded_max = prerun.data['energy'].max()
    print(f'recorded system max: {system_recorded_max}')
    for hp in hyperparams:
        err = errors[hp]
        print(f'  Hyperparameters: {hp}')
        print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
        print('\n')

# Now compute on test set
all_errors = []
for pair in relevant_system_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'starting system {system_id}, {reader_type}')

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type,
                    path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()

    prerun.fill_missing_hours()
    prerun.add_energy_features_only(
        include_last_year=True, remove_last_year_nans=True, highest_fourier_term_hour=2
    )
    # print(prerun.amended_data)

    prerun.add_weather_features_only()

    all_data = prerun.amended_data

    prerun.good_end_days_naive(10)
    prerun.tts_of_data_using_end_days(remove_first_year=False)

    system_recorded_max = prerun.data['energy'].max()

    splits = []

    for pred_date in prerun.test_dates['date']:
        train_mask = (
            (all_data['time'] < pred_date - timedelta(days=1)) &
            (all_data['time'] >= pred_date - timedelta(days=10))
        )
        ho_mask = (
            (all_data['time'] >= pred_date - timedelta(days=1)) &
            (all_data['time'] < pred_date + timedelta(days=1))
        )

        train_data = all_data.loc[train_mask].reset_index(drop=True)
        ho_data = all_data.loc[ho_mask].reset_index(drop=True)

        splits.append((pred_date, train_data, ho_data))

    #  print(system_recorded_max)
    #  print(ho_data)
    #  print(ho_data.columns)

    # choose p,d,q based on 'winning' models from before
    d = 0
    q = 0
    if system_id == 10:
        p = 2
    elif (system_id == 50) | (system_id == 51):
        p = 3

    system_errors = []
    for pred_date, train_data, ho_data in splits:
        try:
            y_pred = use_sarimax(train_data, ho_data, p=p, d=d, q=q)
            #  print(f"original y_pred: {y_pred}")

            # make sure value between 0 and highest observed max
            y_pred = np.clip(y_pred, 0, system_recorded_max)
            # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
            darkness_mask = (~((ho_data['proportion_daytime'] == 0) & (ho_data['global_tilted_irradiance'] == 0))).astype(int)
            #  = 0 when both sublight prop and irr are 0
            y_pred = y_pred*np.array(darkness_mask)
            #  print(f"y_pred with insurance: {y_pred}")

            energy_ho = ho_data['energy']
            #  print(f"energy_ho: {energy_ho}")

            error = PostRun.custom_error(energy_ho.iloc[24:], y_pred.iloc[24:], 1, 2)
            #  print(f'error = {error}')
        except Exception as e:
            print(f'FAILED: (p,d,q) = {p, d, q} --> {type(e).__name__}: {e}')
            error = -1
        system_errors.append(error)
    system_errors_df = pd.DataFrame({'error': system_errors})
    system_errors_df.to_csv(f'{results_folder_str}{system_id}_test_set_sarimax_errors.csv', index=False)


# Test errors histograms
systems = [10, 50, 51]
for system in systems:
    print('System', system)
    df = pd.read_csv(f'{results_folder_str}{system}_test_set_sarimax_errors.csv')
    fig = plt.figure()
    sns.histplot(
        x=df['error'],
    )
    plt.title(f'System {system_id} test errors histogram')
    fig.savefig(f'{results_folder_str}{system}_test_errors_hist.png', format='png')
    print(f'mean = {df['error'].mean():.5f}, median = {df['error'].median():.5f}, '
          + f'min = {df['error'].min():.5f}, max = {df['error'].max():.5f}, std = {df['error'].std():.5f}')
    print('\n\n')


# Let's try graphing the actual performance on the last test-day.
def last_test_date_data(system_id: int, met_or_inv: str | None):
    prerun = PreRun(system_id=system_id, meter_or_inverter=met_or_inv,
                    path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()

    prerun.fill_missing_hours()
    prerun.add_energy_features_only(
        include_last_year=True, remove_last_year_nans=True,
        highest_fourier_term_hour=2
    )
    # print(prerun.amended_data)

    prerun.add_weather_features_only()

    all_data = prerun.amended_data

    prerun.good_end_days_naive(10)
    prerun.tts_of_data_using_end_days(remove_first_year=False)

    system_recorded_max = prerun.data['energy'].max()

    splits = []
    data_to_return = []

    last_pred_date = prerun.test_dates['date'].max()
    train_mask = (
        (all_data['time'] < last_pred_date - timedelta(days=1)) &
        (all_data['time'] >= last_pred_date - timedelta(days=10))
    )
    ho_mask = (
        (all_data['time'] >= last_pred_date - timedelta(days=1)) &
        (all_data['time'] < last_pred_date + timedelta(days=1))
    )

    train_data = all_data.loc[train_mask].reset_index(drop=True)
    ho_data = all_data.loc[ho_mask].reset_index(drop=True)

    splits.append((last_pred_date, train_data, ho_data))
    data_to_return.append(last_pred_date)
    data_to_return.append(ho_data)

    #  print(system_recorded_max)
    #  print(ho_data)
    #  print(ho_data.columns)

    # choose p,d,q
    d = 0
    q = 0
    if system_id == 10:
        p = 2
    elif (system_id == 50) | (system_id == 51):
        p = 3

    for last_pred_date, train_data, ho_data in splits:
        try:
            y_pred = use_sarimax(train_data, ho_data, p=p, d=d, q=q)
            #  print(f"original y_pred: {y_pred}")

            # make sure value between 0 and highest observed max
            y_pred = np.clip(y_pred, 0, system_recorded_max)
            # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
            darkness_mask = (~((ho_data['proportion_daytime'] == 0) & (ho_data['global_tilted_irradiance'] == 0))).astype(int)
            #  = 0 when both sublight prop and irr are 0
            y_pred = y_pred*np.array(darkness_mask)
            data_to_return.append(y_pred)
        except BaseException as e:
            print(f'FAILED: (p,d,q) = {p, d, q} --> {type(e).__name__}: {e}')
            raise e
    return data_to_return


last_date_10, last_data_10, last_preds_10 = last_test_date_data(10, None)
last_date_50, last_data_50, last_preds_50 = last_test_date_data(50, None)
last_date_51, last_data_51, last_preds_51 = last_test_date_data(51, None)


def save_graph_last_test_date_data(system_id: int, met_or_inv: str | None):
    test_date, test_data, y_pred = last_test_date_data(system_id, met_or_inv)
    #  show that we have 2 days of data, for Sarimax continuity reasons
    assert (test_data.shape == (48, 10))
    assert (y_pred.shape == (48,))
    #  trim to the test date
    x_true = test_data.loc[24:, 'time'].values
    y_true = test_data.loc[24:, 'energy'].values
    y_pred = y_pred[24:].values
    #  graph prediction
    fig, ax = plt.subplots()
    sns.lineplot(x=x_true, y=y_pred, label='Predicted hourly energy')
    sns.lineplot(x=x_true, y=y_true, label='True energy')
    plt.xlabel('Time')
    ax.tick_params(axis='x', labelrotation=30)
    plt.ylabel('Energy (kWh)')
    plt.title('Last test-day -- compare predicted and actual energy.')
    plt.savefig(f'{results_folder_str}{system_id}_last_day_graph.png', format='png')


save_graph_last_test_date_data(10, None)
save_graph_last_test_date_data(50, None)
save_graph_last_test_date_data(51, None)

# OK, so performance on any particular test-day is a tossup,
# since the model wants to predict a 'typical' day.
# Averaged test results?


def averaged_test_performance_for_graphing(system_id: int, met_or_inv: str | None):
    prerun = PreRun(system_id=system_id, meter_or_inverter=met_or_inv,
                    path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
    prerun.fill_missing_hours()
    prerun.add_energy_features_only(include_last_year=True, remove_last_year_nans=True, highest_fourier_term_hour=2)
    # print(prerun.amended_data)
    prerun.add_weather_features_only()
    all_data = prerun.amended_data
    prerun.good_end_days_naive(10)
    prerun.tts_of_data_using_end_days(remove_first_year=False)
    system_recorded_max = prerun.data['energy'].max()
    my_test_dates = prerun.test_dates['date']
    my_test_dates_short = prerun.test_dates['date'].dt.date
    num_dates = len(my_test_dates)
    splits = []
    for j in range(num_dates):
        pred_date = my_test_dates.iloc[j]
        pred_date_short = my_test_dates_short.iloc[j]
        train_mask = (
            (all_data['time'] < pred_date - timedelta(days=1)) &
            (all_data['time'] >= pred_date - timedelta(days=10))
        )
        ho_mask = (
            (all_data['time'] >= pred_date - timedelta(days=1)) &
            (all_data['time'] < pred_date + timedelta(days=1))
        )

        train_data = all_data.loc[train_mask].reset_index(drop=True)
        ho_data = all_data.loc[ho_mask].reset_index(drop=True)
        splits.append((pred_date_short, train_data, ho_data))
    #  print(system_recorded_max)
    #  print(ho_data)
    #  print(ho_data.columns)

    # choose p,d,q
    d = 0
    q = 0
    if system_id == 10:
        p = 2
    elif (system_id == 50) | (system_id == 51):
        p = 3

    num_dates = len(my_test_dates)
    true_data_lists = []
    pred_data_lists = []
    for pred_date, train_data, ho_data in splits:
        try: 
            y_pred = use_sarimax(train_data, ho_data, p=p, d=d, q=q)
            # print(f"original y_pred: {y_pred}")

            # make sure value between 0 and highest observed max
            y_pred = np.clip(y_pred, 0, system_recorded_max)
            # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
            darkness_mask = (~((ho_data['proportion_daytime'] == 0) & (ho_data['global_tilted_irradiance'] == 0))).astype(int)
            #  = 0 when both sublight prop and irr are 0
            y_pred = y_pred*np.array(darkness_mask)
        except BaseException as e:
            print(f'FAILED: (p,d,q) = {p, d, q} --> {type(e).__name__}: {e}')
            raise e
        assert (ho_data.shape == (48, 10))
        assert (y_pred.shape == (48, ))
        #  get data for test-date only
        ho_data_latter = ho_data.loc[24:].copy(deep=True)
        ho_data_latter.loc[:, 'hour_of_day'] = ho_data_latter['time'].dt.hour
        ho_data_latter = ho_data_latter.rename(columns={'energy': f'{pred_date}_energy'})
        true_data_lists.append(ho_data_latter[['hour_of_day', f'{pred_date}_energy']])
        y_pred_latter = y_pred.iloc[24:].copy(deep=True)
        y_pred_latter.index.name = 'time'
        y_pred_latter = y_pred_latter.reset_index()
        y_pred_latter.loc[:, 'hour_of_day'] = y_pred_latter['time'].dt.hour
        y_pred_latter = y_pred_latter.rename(columns={'predicted_mean': f'{pred_date}_pred'})
        pred_data_lists.append(y_pred_latter[['hour_of_day', f'{pred_date}_pred']])
    # combine
    if len(true_data_lists) >= 2:
        true_data_df = pd.merge(left=true_data_lists[0], right=true_data_lists[1],
                                left_on='hour_of_day', right_on='hour_of_day',
                                how='outer')
        pred_data_df = pd.merge(left=pred_data_lists[0], right=pred_data_lists[1],
                                left_on='hour_of_day', right_on='hour_of_day',
                                how='outer')
        if len(true_data_lists) > 2:
            for j in range(2, len(true_data_lists)):
                true_data_df = pd.merge(
                    left=true_data_df, right=true_data_lists[j],
                    left_on='hour_of_day', right_on='hour_of_day',
                    how='outer'
                )
                pred_data_df = pd.merge(
                    left=pred_data_df, right=pred_data_lists[j],
                    left_on='hour_of_day', right_on='hour_of_day',
                    how='outer'
                )
    else:
        raise RuntimeError('Not enough test data!')
    ord_cols_true = [col_name for col_name in true_data_df.columns
                     if 'hour' not in col_name]
    ord_cols_pred = [col_name for col_name in pred_data_df.columns
                     if 'hour' not in col_name]
    true_data_df.loc[:, 'mean_true'] = true_data_df[ord_cols_true].mean(axis=1)
    pred_data_df.loc[:, 'mean_pred'] = pred_data_df[ord_cols_pred].mean(axis=1)
    return true_data_df, pred_data_df


true_10, pred_10 = averaged_test_performance_for_graphing(10, None)
true_50, pred_50 = averaged_test_performance_for_graphing(50, None)
true_51, pred_51 = averaged_test_performance_for_graphing(51, None)


def save_graph_avg_test_data(system_id: int, met_or_inv: str | None,
                             true_df: pd.DataFrame, pred_df: pd.DataFrame):
    #  graph prediction
    fig, ax = plt.subplots()
    sns.lineplot(data=pred_df, x='hour_of_day',
                 y='mean_pred', label='Predicted hourly energy (avg. of test_days)')
    sns.lineplot(data=true_df, x='hour_of_day',
                 y='mean_true', label='Actual hourly energy (avg. of test_days)')
    plt.xlabel('Hour of Day')
    ax.tick_params(axis='x', labelrotation=30)
    plt.ylabel('Energy (kWh)')
    plt.title(f'Averaged Test performance for SARIMAX model, System {system_id}')
    plt.savefig(f'{results_folder_str}{system_id}_averaged_test_error.png', format='png')


save_graph_avg_test_data(10, None, true_10, pred_10)
save_graph_avg_test_data(50, None, true_50, pred_50)
save_graph_avg_test_data(51, None, true_51, pred_51)
