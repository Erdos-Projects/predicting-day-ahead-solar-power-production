'''Run SARIMAX Model

Note: using "previous day" would NOT mean that today is used to predict tomorrow.
It means the prediction for today is used to predict tomorrow.'''
# import everything!
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta
from itertools import product
from PreRun import PreRun, PostRun
from statsmodels.tsa.statespace.sarimax import SARIMAX


# path to data
read_path = "../../../../data_ds_project/parquet_cleaned_energy"
# systems
good_systems_list = [4, 10, 33, 36, 50, 51, 1199, 1204, 1283, 1284, 1289, 1332, 4902, 4903]
reader_types = ["meter", "inverter", None]
# systems_cleaned
systems_cleaned = pd.read_csv("../../../data/core/systems_cleaned.csv")

results_folder = Path('/.sarimax_errors')
if not results_folder.is_dir():
    results_folder.mkdir()


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
        seasonal_order=(P, D, Q, s)
    ).fit(maxiter=600, disp=False)
    # predict
    y_pred = model_sarimax.forecast(len(df_ho), exog=exog_ho)

    # error = PostRun.custom_error(energy_ho, y_pred, 1,2)

    return y_pred  # , error


system_reader_pairs = [(10, None), (50, None), (51, None)]

p_choices = [2, 3]
# d_choices = [0,1]
d_choices = [0]
q_choices = [0, 1]

for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'starting system {system_id}, {reader_type}')

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()

    prerun.fill_missing_hours()
    prerun.add_energy_features_only(include_last_year=True, remove_last_year_nans=True, highest_fourier_term_hour=2)
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

                error = PostRun.custom_error(energy_ho.iloc[24:], y_pred.iloc[24:], 1, 2)
                #  print(f'error = {error}')
            except Exception as e:
                print(f'FAILED: (p,d,q) = {p, d, q} --> {type(e).__name__}: {e}')
                error = -1
            errors.append(error)

        results_dict[f"{p},{d},{q}"] = errors
    all_errors_df = pd.DataFrame(results_dict)
    all_errors_df.to_csv(f'sarimax_errors/{system_id}_{reader_type}_sarimax_errors.csv', index=False)

# compare hyperparameters
# System 10
print('System 10, None, SARIMAX')
errors = pd.read_csv('sarimax_errors/10_None_sarimax_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=10, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

# al very close
# 2,0,0; 3,0,0; 2,0,1; 3,0,1
# NOTE: ran with d=1 before and was significantly worse. Narrowed down to d=0.

# compare hyperparameters
# System 50
print('System 50, None, SARIMAX')
errors = pd.read_csv('sarimax_errors/50_None_sarimax_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=50, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

# 3,0,0; 3,0,1; 2,0,0; 2,0,1
# prefer 3,0,0 due to fewer errors

# compare hyperparameters
# System 51
print('System 51, None, SARIMAX')
errors = pd.read_csv('sarimax_errors/51_None_sarimax_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=51, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

# much less clear. There are some clear "lowest mean", but some others seem to have a much lower spread.
#  Preferred because of mean: 3,0,0; 3,0,1; followed by 2,0,0 and 2,0,1

# #  run winning errors only for test set!
system_reader_pairs = [(10, None), (50, None), (51, None)]


all_errors = []
for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'starting system {system_id}, {reader_type}')

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()

    prerun.fill_missing_hours()
    prerun.add_energy_features_only(include_last_year=True, remove_last_year_nans=True, highest_fourier_term_hour=2)
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

    # choose p,d,q
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
    system_errors_df.to_csv(f'sarimax_errors/{system_id}_test_set_sarimax_errors.csv', index=False)

systems = [10, 50, 51]

for system in systems:
    df = pd.read_csv(f'sarimax_errors/{system}_test_set_sarimax_errors.csv')
    print('System', system)
    print(f'mean = {df.mean()}, median = {df.median()}, min = {df.min()}, max = {df.max()}, std = {df.std()}')
