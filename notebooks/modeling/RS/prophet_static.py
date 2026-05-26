'''Run prophet models.
hyperparameters to tune:
- seasonality_prior_scale (technically for reducing overfitting)
- fourier_order (should likely be done separately for daily and yearly)
- additive vs multiplicative???
- Trend-related (changepoints)
    - n_changepoints (default = 25)
    - changepoint_prior_scale (default = 0.05)
    - changepoint_range (0.8) (this means "changepoints can occur in the first 80% of data")
'''
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta
from itertools import product
from PreRun import PreRun, PostRun
from prophet import Prophet
from joblib import Parallel, delayed
import logging
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

# path to data
read_path = "../../../../data_ds_project/parquet_cleaned_energy"
# systems_cleaned
systems_cleaned = pd.read_csv("../../../data/core/systems_cleaned.csv")
# systems and readers
system_reader_pairs = [(10, None), (50, None), (51, None)]

out_dir_additive_holidays = Path('./prophet_errors/')
if not out_dir_additive_holidays.is_dir():
    out_dir_additive_holidays.mkdir()
out_dir_multiplicative_holidays = Path('./prophet_errors_mult/')
if not out_dir_multiplicative_holidays.is_dir():
    out_dir_multiplicative_holidays.mkdir()


#  variant with additive errors
#  System 10
def _run_one_hyp_c(cps, nc, sps, hps, splits, system_recorded_max):

    errors = []
    for _, train_data, ho_data in splits:
        X_ho = ho_data.drop(columns=['y'])
        proph = Prophet(
            weekly_seasonality=False,
            yearly_seasonality=True,
            changepoint_prior_scale=cps,
            n_changepoints=nc,
            seasonality_prior_scale=sps,
            holidays_prior_scale=hps
        )
        proph.fit(train_data)
        y_pred = proph.predict(X_ho)['yhat']
        y_pred = np.clip(y_pred, 0, system_recorded_max)
        darkness_mask = (~((X_ho['proportion_daytime'] == 0) & (X_ho['global_tilted_irradiance'] == 0))).astype(int)
        y_pred = y_pred * np.array(darkness_mask)
        errors.append(PostRun.custom_error(ho_data['y'], y_pred))

    return (cps, nc, sps, hps), errors


changepoint_prior_scale = [0.1, 0.5]
n_changepoints = [10]
seasonality_prior_scale = [20]
holidays_prior_scale = [2]

first_group = [(10, None), ]
for pair in first_group:
    system_id = pair[0]
    reader_type = pair[1]
    print(f"System {system_id}, {reader_type}")

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
    prerun.fill_missing_hours()
    prerun.add_weather_features_only()
    prerun.good_end_days_naive(7)
    prerun.tts_of_data_using_end_days()
    all_data = prerun.amended_data.copy()
    #  float() so workers receive a plain Python float, not a numpy scalar
    system_recorded_max = float(prerun.data['energy'].max())

    all_data.rename(columns={'time': 'ds', 'energy': 'y'}, inplace=True)

    first_day = prerun.good_days.loc[0, 'date']
    remove_until = first_day + timedelta(days=730)
    train_dates = prerun.train_dates.loc[prerun.train_dates['date'] >= remove_until].reset_index(drop=True)
    n = int(len(train_dates) / 200)
    some_train_dates = train_dates[::n]

    #  .copy() ensures each slice is a fully independent DataFrame before pickling
    splits = []
    for pred_date in some_train_dates['date']:
        train_mask = (all_data['ds'] < pred_date - timedelta(days=1))
        ho_mask = ((all_data['ds'] >= pred_date) & (all_data['ds'] < pred_date + timedelta(days=1)))
        splits.append((
            pred_date,
            all_data.loc[train_mask].reset_index(drop=True).copy(),
            all_data.loc[ho_mask].reset_index(drop=True).copy()
        ))

    hyperparams = list(product(changepoint_prior_scale, n_changepoints, seasonality_prior_scale, holidays_prior_scale))

    results = Parallel(n_jobs=4, backend='loky')(
        delayed(_run_one_hyp_c)(cps, nc, sps, hps, splits, system_recorded_max)
        for cps, nc, sps, hps in hyperparams
    )

    #  key comes from what the worker actually used, not what was passed in
    results_dict = {f"{hyp}": errors for hyp, errors in results}
    all_errors_df = pd.DataFrame(results_dict)
    all_errors_df.to_csv(f'prophet_errors/{system_id}_{reader_type}_prophet_errors.csv', index=False)
    print("  Saved.")

print("Done!")


#  now second group
def _run_one_hyp_d(hyp, splits, system_recorded_max):

    cps, nc, sps, hps = hyp
    errors = []

    for pred_date, train_data, ho_data in splits:
        X_ho = ho_data.drop(columns=['y'])

        proph = Prophet(
            weekly_seasonality=False,
            yearly_seasonality=True,
            changepoint_prior_scale=cps,
            n_changepoints=nc,
            seasonality_prior_scale=sps,
            holidays_prior_scale=hps
        )
        proph.fit(train_data)
        y_pred = proph.predict(X_ho)['yhat']

        y_pred = np.clip(y_pred, 0, system_recorded_max)
        darkness_mask = (~((X_ho['proportion_daytime'] == 0) & (X_ho['global_tilted_irradiance'] == 0))).astype(int)
        y_pred = y_pred * np.array(darkness_mask)

        error = PostRun.custom_error(ho_data['y'], y_pred)
        errors.append(error)

    return f"{hyp}", errors


changepoint_prior_scale = [0.1]
n_changepoints = [10]
seasonality_prior_scale = [20]
holidays_prior_scale = [2]

second_group = [(50, None), (51, None)]

for pair in second_group:
    system_id = pair[0]
    reader_type = pair[1]
    print(f"System {system_id}, {reader_type}")

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
    prerun.fill_missing_hours()
    prerun.add_weather_features_only()
    prerun.good_end_days_naive(7)
    prerun.tts_of_data_using_end_days()
    all_data = prerun.amended_data.copy()
    system_recorded_max = prerun.data['energy'].max()

    all_data.rename(columns={'time': 'ds', 'energy': 'y'}, inplace=True)

    first_day = prerun.good_days.loc[0, 'date']
    remove_until = first_day + timedelta(days=730)
    train_dates = prerun.train_dates.loc[prerun.train_dates['date'] >= remove_until].reset_index(drop=True)
    n = int(len(train_dates) / 200)
    some_train_dates = train_dates[::n]

    splits = []
    for pred_date in some_train_dates['date']:
        train_mask = (all_data['ds'] < pred_date - timedelta(days=1))
        ho_mask = ((all_data['ds'] >= pred_date) & (all_data['ds'] < pred_date + timedelta(days=1)))
        train_data = all_data.loc[train_mask].reset_index(drop=True)
        ho_data = all_data.loc[ho_mask].reset_index(drop=True)
        splits.append((pred_date, train_data, ho_data))

    hyperparams = list(product(changepoint_prior_scale, n_changepoints, seasonality_prior_scale, holidays_prior_scale))

    results = Parallel(n_jobs=4, backend='loky')(
        delayed(_run_one_hyp_d)(hyp, splits, system_recorded_max)
        for hyp in hyperparams
    )

    results_dict = {key: errors for key, errors in results}
    all_errors_df = pd.DataFrame(results_dict)
    all_errors_df.to_csv(f'prophet_errors/{system_id}_{reader_type}_prophet_errors.csv', index=False)
    print("  Saved.")

print("Done!")

#  Print results
# System 10
print('System 10, None, Prophet')
errors = pd.read_csv('prophet_errors/10_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=10, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

print()
# System 50
print('System 50, None, Prophet')
errors = pd.read_csv('prophet_errors/50_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=50, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

print()
# System 51
print('System 51, None, Prophet')
errors = pd.read_csv('prophet_errors/51_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=51, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

# #  now make the errors multiplicative instead of additive.
system_reader_pairs = [(10, None), (50, None), (51, None)]


def _run_one_hyp_m(hyp, splits, system_recorded_max):

    cps, nc, sps, hps = hyp
    errors = []

    for pred_date, train_data, ho_data in splits:
        X_ho = ho_data.drop(columns=['y'])

        proph = Prophet(
            weekly_seasonality=False,
            yearly_seasonality=True,
            changepoint_prior_scale=cps,
            n_changepoints=nc,
            seasonality_prior_scale=sps,
            holidays_prior_scale=hps,
            holidays_mode='multiplicative'
        )
        proph.fit(train_data)
        y_pred = proph.predict(X_ho)['yhat']

        y_pred = np.clip(y_pred, 0, system_recorded_max)
        darkness_mask = (~((X_ho['proportion_daytime'] == 0) & (X_ho['global_tilted_irradiance'] == 0))).astype(int)
        y_pred = y_pred * np.array(darkness_mask)

        error = PostRun.custom_error(ho_data['y'], y_pred)
        errors.append(error)

    return f"{hyp}", errors


changepoint_prior_scale = [0.1]
n_changepoints = [10]
seasonality_prior_scale = [20]
holidays_prior_scale = [2]

for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f"System {system_id}, {reader_type}")

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
    prerun.fill_missing_hours()
    prerun.add_weather_features_only()
    prerun.good_end_days_naive(7)
    prerun.tts_of_data_using_end_days()
    all_data = prerun.amended_data.copy()
    system_recorded_max = prerun.data['energy'].max()

    all_data.rename(columns={'time': 'ds', 'energy': 'y'}, inplace=True)

    first_day = prerun.good_days.loc[0, 'date']
    remove_until = first_day + timedelta(days=730)
    train_dates = prerun.train_dates.loc[prerun.train_dates['date'] >= remove_until].reset_index(drop=True)
    n = int(len(train_dates) / 200)
    some_train_dates = train_dates[::n]

    splits = []
    for pred_date in some_train_dates['date']:
        train_mask = (all_data['ds'] < pred_date - timedelta(days=1))
        ho_mask = ((all_data['ds'] >= pred_date) & (all_data['ds'] < pred_date + timedelta(days=1)))
        train_data = all_data.loc[train_mask].reset_index(drop=True)
        ho_data = all_data.loc[ho_mask].reset_index(drop=True)
        splits.append((pred_date, train_data, ho_data))

    hyperparams = list(product(changepoint_prior_scale, n_changepoints, seasonality_prior_scale, holidays_prior_scale))

    results = Parallel(n_jobs=4, backend='loky')(
        delayed(_run_one_hyp_m)(hyp, splits, system_recorded_max)
        for hyp in hyperparams
    )

    results_dict = {key: errors for key, errors in results}
    all_errors_df = pd.DataFrame(results_dict)
    all_errors_df.to_csv(f'prophet_errors_mult/{system_id}_{reader_type}_prophet_errors.csv', index=False)
    print("  Saved.")

print("Done!")

#  Multiplicative errors printout
# System 10
print('System 10, None, Prophet, mult')
errors = pd.read_csv('prophet_errors_mult/10_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=10, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

print()
# System 50
print('System 50, None, Prophet, mult')
errors = pd.read_csv('prophet_errors_mult/50_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=50, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

print()
# System 51
print('System 51, None, Prophet, mult')
errors = pd.read_csv('prophet_errors_mult/51_None_prophet_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=51, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
