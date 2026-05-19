import pandas as pd
import pyarrow.parquet as pq
from clean_and_collect_power_data import Clean

systems_cleaned = pd.read_csv('../../../data/core/systems_cleaned.csv')
access_base = '../../../../data_ds_project/testing_yearly_parquet/'
write_base = './clean_again/'
met_or_inv_choices = ('inverter', 'meter', None)

good_timezone_systems = [4, 10, 33, 36, 50, 51, 1199, 1204, 1283,
                         1284, 1289, 1332, 4902, 4903]

for system_id in good_timezone_systems:
    blank_year_pq = pq.ParquetDataset(
        f'{access_base}{system_id}/',
        filters=[('year', '==', 1990)]
    )
    blank_year_df = blank_year_pq.read().to_pandas()
    pow_cols = [col_name
                for col_name in blank_year_df.columns
                if ('pow' in col_name)]
    is_inv = any([('inv' in col_name) for col_name in pow_cols])
    is_met = any([('met' in col_name) for col_name in pow_cols])
    if is_inv and is_met:
        met_inv_inputs = ['inverter', 'meter']
        met_inv_names = met_inv_inputs
    elif is_inv:
        met_inv_inputs = ['inverter',]
        met_inv_names = met_inv_inputs
    elif is_met:
        met_inv_inputs = ['meter',]
        met_inv_names = met_inv_inputs
    else:
        met_inv_inputs = [None,]
        met_inv_names = ['unknown',]
    for j in range(len(met_inv_inputs)):
        met_or_inv = met_inv_inputs[j]
        met_or_inv_name = met_inv_names[j]
        system_cleaning_module = Clean(system_id, access_base, systems_cleaned,
                                       met_or_inv, 
                                       write_base)
        system_cleaning_module.clean_all_and_write_to_file()