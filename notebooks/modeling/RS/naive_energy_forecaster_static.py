'''Run the naive baseline model on selected systems.'''
# import everything!
import numpy as np
import pandas as pd
from pathlib import Path
from PreRun import PreRun

# path to data
read_path = "../../../../data_ds_project/parquet_cleaned_energy"
# systems
good_systems_list = [4, 10, 33, 36, 50, 51, 1199, 1204, 1283, 1284, 1289, 1332, 4902, 4903]
reader_types = ["meter", "inverter", None]
# systems_cleaned
systems_cleaned = pd.read_csv("../../../data/core/systems_cleaned.csv")

results_folder = Path('./naive_errors/')
if not results_folder.is_dir():
    results_folder.mkdir()

# #  Run the naive predictions
system_reader_pairs = [(4, None), (10, None), (33, None), (50, None),
                       (51, None), (1283, 'inverter'), (1283, 'meter')]
#  system_reader_pairs = [(1283,'inverter'),(1283,'meter')]
for pair in system_reader_pairs:
    system_id = pair[0]
    reader_type = pair[1]

    check_prerun = PreRun(system_id=system_id, meter_or_inverter=reader_type, path=read_path, systems_cleaned=systems_cleaned)
    check_prerun.fill_missing_hours()
    system_recorded_max = check_prerun.data['energy'].max()
    check_prerun.good_end_days_naive(1)
    check_prerun.tts_of_data_using_end_days()
    all_data = check_prerun.amended_data.copy()

    pred_days = (check_prerun.end_days_naive['date']).dt.date
    pred_days_set = set(pred_days)

    # make the predictions! will be made as a new column of all_data
    all_data['key'] = list(zip(all_data['time'].dt.month, all_data['time'].dt.day, all_data['time'].dt.hour))
    all_data['naive_pred'] = (
        all_data.groupby('key')['energy']  # group by same month/day/time
        .transform(lambda x: x.expanding().mean().shift(1))
        # expanding average, then shift by one to not include this year in calculation
    )

    # make sure value between 0 and highest observed max
    all_data['naive_pred'] = np.clip(all_data['naive_pred'], 0, system_recorded_max)

    all_data['naive_pred'] = all_data['naive_pred'].ffill().bfill()

    predicted_times = all_data.loc[all_data['time'].dt.date.isin(pred_days_set)]

    diff = predicted_times['energy'] - predicted_times['naive_pred']
    predicted_times['error'] = np.where(
        diff > 0,
        1 * diff**2,
        2 * diff**2
    )
    predicted_times['date'] = predicted_times['time'].dt.date
    daily_error = predicted_times.groupby('date')['error'].mean()
    #  y_true = all_data.loc[all_data['time'].dt.date.isin(pred_days_set)][['time','energy']]
    #  y_pred = all_data.loc[all_data['time'].dt.date.isin(pred_days_set)][['time','naive_pred']]
    #  diff = y_true-y_pred
    #  errors = pd.Series(
    #      np.where(diff > 0, 1 * diff**2, 2 * diff**2),
    #       index=diff.index
    #  )
    daily_error.to_csv(f'naive_errors/{system_id}_{reader_type}_naive_errors.csv', index=False)

# compare outputs

# System 4
print('System 4, None, Naive')
errors = pd.read_csv('naive_errors/4_None_naive_errors.csv')
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

print('System 10, None, Naive with streak = 1')
errors = pd.read_csv('naive_errors/10_None_naive_errors.csv')
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

print('System 33, None, Naive with streak = 1')
errors = pd.read_csv('naive_errors/33_None_naive_errors.csv')
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

print('System 50, None, Naive with streak = 1')
errors = pd.read_csv('naive_errors/50_None_naive_errors.csv')
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


print('System 51, None, Naive with streak = 1')
errors = pd.read_csv('naive_errors/51_None_naive_errors.csv')
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

print('System 1283, Inverter, Naive with streak = 1')
errors = pd.read_csv('naive_errors/1283_inverter_naive_errors.csv')
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

print('System 1283, Meter, Naive with streak = 1')
errors = pd.read_csv('naive_errors/1283_meter_naive_errors.csv')
hyperparams = errors.columns

prerun = PreRun(system_id=1283, meter_or_inverter='meter', path=read_path, systems_cleaned=systems_cleaned)
prerun.load_data()
system_recorded_max = prerun.data['energy'].max()
print(f'recorded system max: {system_recorded_max}')

for hp in hyperparams:
    err = errors[hp]
    print(f'  Hyperparameters: {hp}')
    print(f'     mean: {err.mean()}, median: {err.median()}, min: {err.min()}, max: {err.max()}, std: {err.std()}')
