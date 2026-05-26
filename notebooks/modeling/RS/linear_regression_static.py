'''Run linear regression models.'''

# import statements
import numpy as np
import pandas as pd
from pathlib import Path
from PreRun import PreRun, PostRun
# from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.linear_model import LinearRegression


# path to data
read_path = "../../../../data_ds_project/parquet_cleaned_energy"
# systems
good_systems_list = [4, 10, 33, 36, 50, 51, 1199, 1204, 1283, 1284, 1289, 1332, 4902, 4903]
reader_types = ["meter", "inverter", None]
# systems_cleaned
systems_cleaned = pd.read_csv("../../../data/core/systems_cleaned.csv")

results_folder = Path('./linreg_errors/')
if not results_folder.is_dir():
    results_folder.mkdir()

# #  basic linear regression run
system_reader_pairs = [(4, None), (10, None), (33, None), (50, None),
                       (51, None), (1283, 'inverter'), (1283, 'meter')]
# system_reader_pairs = [(1283,'inverter'),(1283,'meter')]


for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
        
    prerun.fill_missing_hours()
    prerun.add_energy_features_only(daily_lags=2, remove_daily_lags_nans=True, include_last_year=True,
                                    remove_last_year_nans=True, include_day_of_year_cyclic=True, include_hour_cyclic=True)

    prerun.add_weather_features_only()

    prerun.amended_data = prerun.amended_data.dropna()

    prerun.good_end_days_naive(1)
    prerun.tts_of_data_using_end_days()

    all_errors = []

    linReg = LinearRegression()
    system_recorded_max = prerun.data['energy'].max()
    first_debug = True
    for end_date in prerun.train_dates['date']:
        # print(type(date))
        data = prerun.data_until_ho_day(end_date)
        #  end-date is the pd.Timestamp for midnight on the target day
        data_train = data[data['time'] < end_date - pd.Timedelta(days=1)]
        X_train = data_train.drop(columns=['time', 'energy'])
        y_train = data_train['energy']
        data_ho = data[(data['time'] >= end_date)
                       & (data['time'] < end_date + pd.Timedelta(days=1))]
        X_ho = data_ho.drop(columns=['time', 'energy'])
        y_ho = data_ho['energy']
        linReg.fit(X_train, y_train)

        y_pred = linReg.predict(X_ho)
        # make sure value between 0 and highest observed max
        y_pred = np.clip(y_pred, 0, system_recorded_max)
        # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
        darkness_mask = (~((X_ho['proportion_daytime'] == 0) & (X_ho['global_tilted_irradiance'] == 0))).astype(int)
        y_pred = y_pred*np.array(darkness_mask)
        error = PostRun.custom_error(y_ho, y_pred)
        all_errors.append(error)

    # save errors
    errors = pd.DataFrame(all_errors, columns=['error'])
    errors.to_csv(f'linreg_errors/{system_id}_{reader_type}_linreg_errors.csv', index=False)

# compare hyperparameters
# System 4
print('System 4, None, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/4_None_linreg_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=4, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
    
print()

print('System 10, None, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/10_None_linreg_errors.csv')
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

print('System 33, None, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/33_None_linreg_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=33, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
    
print()

print('System 50, None, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/50_None_linreg_errors.csv')
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


print('System 51, None, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/51_None_linreg_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=51, meter_or_inverter=None, path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
    
print()


print('System 1283, Inverter, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/1283_inverter_linreg_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=1283, meter_or_inverter='inverter', path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
    
print()

print('System 1283, Meter, LinReg with streak = 1')
errors = pd.read_csv('linreg_errors/1283_meter_linreg_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=1283, meter_or_inverter='meter', path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')

#  Variant with more cycle-Fourier terms (3)
system_reader_pairs = [(4, None), (10, None), (33, None), (50, None),
                       (51, None), (1283, 'inverter'), (1283, 'meter')]

for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]

    prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    prerun.load_data()
        
    prerun.fill_missing_hours()
    prerun.add_energy_features_only(
        daily_lags=2, remove_daily_lags_nans=True, include_last_year=True,
        remove_last_year_nans=True, include_day_of_year_cyclic=True,
        highest_fourier_term_hour=3
    )

    prerun.add_weather_features_only()

    prerun.amended_data = prerun.amended_data.dropna()

    prerun.good_end_days_naive(1)
    prerun.tts_of_data_using_end_days()

    all_errors = []

    linReg = LinearRegression()
    system_recorded_max = prerun.data['energy'].max()
    first_debug = True
    for end_date in prerun.train_dates['date']:
        # print(type(date))
        data = prerun.data_until_ho_day(end_date)
        #  end-date is the pd.Timestamp for midnight on the target day
        data_train = data[data['time'] < end_date - pd.Timedelta(days=1)]
        X_train = data_train.drop(columns=['time', 'energy'])
        y_train = data_train['energy']
        data_ho = data[(data['time'] >= end_date)
                       & (data['time'] < end_date + pd.Timedelta(days=1))]
        X_ho = data_ho.drop(columns=['time', 'energy'])
        y_ho = data_ho['energy']
        linReg.fit(X_train, y_train)

        y_pred = linReg.predict(X_ho)
        # make sure value between 0 and highest observed max
        y_pred = np.clip(y_pred, 0, system_recorded_max)
        # make sure that if it's not sunlight hours and irradiance = 0, then energy = 0
        darkness_mask = (~((X_ho['proportion_daytime'] == 0) & (X_ho['global_tilted_irradiance'] == 0))).astype(int)
        #  = 0 when both sublight prop and irr are 0
        y_pred = y_pred*np.array(darkness_mask)
        error = PostRun.custom_error(y_ho, y_pred)
        all_errors.append(error)

    # save errors
    errors = pd.DataFrame(all_errors, columns=['error'])
    errors.to_csv(f'linreg_errors/{system_id}_{reader_type}_linreg_errors_fourier.csv', index=False)

# compare hyperparameters
for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]
    print(f'System {system_id}, {reader_type}, LinReg (Fourier) with streak = 1')
    errors = pd.read_csv(f'linreg_errors/{system_id}_{reader_type}_linreg_errors_fourier.csv')
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
