import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from gen_variable_standard_static import metrics_search_for_two_fragments_df, \
    find_aggregate_variable_names_gen_mod, find_all_variable_names_gen_mod, \
    metadata_part_name


# Step 2: Find aggregate dc names
def find_aggregate_dc_power_names(
    print_messages: bool,
    sources_matter: bool = False,
    known_sources=('inverter',),  # meter not possible, that's AC
    known_sources_short=('inv',)
):
    dc_power_metrics = metrics_search_for_two_fragments_df(
        metrics_df, 'pow', 'dc', 'and'
    )
    var_name = 'dc_power'
    dc_agg_sensor_names = ['dc_power', 'dc_power_hW',
                           'dc_power_kW', 'dc_power_1_6',
                           'InvPDC_kW_Avg', 'dc_power_calc']
    return find_aggregate_variable_names_gen_mod(
        systems_cleaned=systems_cleaned,
        filtered_metrics_df=dc_power_metrics,
        var_name=var_name,
        agg_var_sensor_names=dc_agg_sensor_names,
        print_messages=print_messages,
        sources_matter=sources_matter,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )


# Step 3: Find all dc power names
def find_all_dc_pow_metrics(
    print_messages: bool,
    sources_matter: bool = True,
    known_sources=('inverter',),
    known_sources_short=('inv',)
):
    dc_pow_agg_metrics, dc_power_agg_metadata = find_aggregate_dc_power_names(
        print_messages=print_messages,
        sources_matter=sources_matter,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )
    dc_power_metrics = metrics_search_for_two_fragments_df(
        metrics_df, 'pow', 'dc', 'and'
    )
    # special filter -- dc_power_positive and dc_power_negative
    # not really subunits in the same way
    dc_power_cleaned_metrics = dc_power_metrics[
        ~dc_power_metrics['sensor_name'].str.contains('positive')
        & ~dc_power_metrics['sensor_name'].str.contains('negative')
    ]
    var_name = 'dc_power'
    dc_agg_sensor_names = ['dc_power', 'dc_power_hW',
                           'dc_power_kW', 'dc_power_1_6',
                           'InvPDC_kW_Avg', 'dc_power_calc']
    dc_pow_all_metrics, dc_pow_all_metadata = find_all_variable_names_gen_mod(
        var_aggs_dict=dc_pow_agg_metrics,
        var_aggs_metadata=dc_power_agg_metadata,
        filtered_metrics_df=dc_power_cleaned_metrics,
        var_name=var_name,
        agg_var_sensor_names=dc_agg_sensor_names,
        sources_matter=sources_matter,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )
    # problem with system 1207 -- bad units
    for system_id in dc_pow_agg_metrics.keys():
        for metric_dict in dc_pow_agg_metrics[system_id]:
            if metric_dict['units'] == '-':
                assert (system_id == 1207)
                metric_dict['units'] = 'W'  # from aggregate
    return (dc_pow_all_metrics, dc_pow_all_metadata)


# Step 4: make the DataFrame
def dc_power_total_name(has_subparts: bool, unit: str = 'W'):
    '''Make the standardized variable name.'''
    total_name = 'dc_power'
    if has_subparts:
        total_name = total_name + '_total'
    total_name = total_name + '_' + unit
    return total_name


def dc_power_partial_name(ind: int, unit: str = 'W'):
    '''Make the standardized part-name.'''
    subpart_name = 'dc_power'
    subpart_name = subpart_name + f'_{ind}_{unit}'
    return subpart_name


def dc_power_dataframe_generator(
    system_id: int,
    tall_or_wide: str,
    error_on_no_data: bool,
    size_standard: str,
):
    '''Make the (tall or wide) pandas DataFrame with all dc power data.'''
    dc_power_names, dc_power_metadata = find_all_dc_pow_metrics(
        print_messages=False,
        sources_matter=False
    )
    try:
        my_dc_power_names = dc_power_names[system_id]
        my_metadata = dc_power_metadata.loc[system_id, :]
    except KeyError:
        if error_on_no_data:
            raise ValueError(f'System {system_id} has no DC power data!')
        else:
            return None
    except BaseException as e:
        raise e
    metric_ids = []
    whole_metric_ids = []
    # grab all metric ids, putting the 'whole' category first
    for metric_data_dict in my_dc_power_names:
        if metric_data_dict['whole_or_part'] == 'whole':
            metric_ids.insert(0, metric_data_dict['metric_id'])
            whole_metric_ids.append(metric_data_dict['metric_id'])
        elif metric_data_dict['whole_or_part'] == 'part':
            metric_ids.append(metric_data_dict['metric_id'])
        else:
            raise ValueError('The "whole_or_part" result of find_all_dc_power_names()\n'
                             f'is not correct for system {system_id}.')
    # Load only these metrics from the system
    my_system_parquet_data_path = Path(f'../../../../data_ds_project/systems/parquet/{system_id}/')
    my_system_parquet_selection = pq.ParquetDataset(
        my_system_parquet_data_path, filters=[
            ('metric_id', 'in', metric_ids)
        ]
    )
    system_df = my_system_parquet_selection.read().to_pandas()
    # for reference, 4 columns (see
    # https://github.com/openEDI/documentation/blob/main/pvdaq.md#pvdaq_pvdata)
    # measured_on, utc_measured_on, metric_id, value)
    # standard cleaning
    system_df = system_df.drop_duplicates()
    # See if multiple values at a given time
    # if so, forced to replace value by mean value
    if any(system_df.duplicated(subset=['measured_on', 'metric_id'])):
        system_df.loc[:, 'mean_value'] = system_df.groupby(
            ['measured_on', 'metric_id']
        )['value'].transform('mean')
        system_df = system_df.drop(columns='value')
        system_df = system_df.rename(columns={'mean_value': 'value'})
        system_df.drop_duplicates()
    # if still duplicates, forced to drop utc_measured_on,
    # a frequent source of off-by-one-hour errors
    # (and points with the same 'measured_on' but different 'utc_measured_on'
    # have the same value, so it is likely that utc_measured_on is the problem)
    if any(system_df.duplicated(subset=['measured_on', 'metric_id', 'value'])):
        system_df = system_df.drop(columns='utc_measured_on')
        system_df = system_df.drop_duplicates()
    # ready to widen the columns
    wide_df = system_df.pivot(
        index='measured_on',
        columns='metric_id',
        values='value'
    )
    # reset the metric_id name of the index of columns
    wide_df.columns.name = ''
    # reset the index
    wide_df = wide_df.reset_index()
    # before continuing, standardize the capitalization of the size term
    if size_standard.lower() == 'w':
        size_standard = 'W'
    elif size_standard.lower() == 'kw':
        size_standard = 'kW'
    else:
        raise ValueError('Only supports watts and kilowatts for now.')
    # standardize units -- probably only necessary for power and temperature
    # Irradiance is pretty clearly in W/m^2, current in A, voltage in V
    if size_standard == 'W':
        for metric_data_dict in my_dc_power_names:
            if metric_data_dict['units'].lower() == 'kw':
                wide_df.loc[:, metric_data_dict['metric_id']] = wide_df[metric_data_dict['metric_id']] * 1000
    elif size_standard == 'kW':
        for metric_data_dict in my_dc_power_names:
            if metric_data_dict['units'].lower() == 'w':
                wide_df.loc[:, metric_data_dict['metric_id']] = wide_df[metric_data_dict['metric_id']] / 1000
    else:
        raise ValueError('Only supports watts and kilowatts for now.')
    # push the 'whole' columns to the beginning of the pack
    # despite re-ordering earlier, can still be loaded in the wrong order.
    reordered_columns = ['measured_on'] + whole_metric_ids + (wide_df.columns.drop(
        ['measured_on'] + whole_metric_ids
    ).tolist())
    wide_df = wide_df[reordered_columns]
    # rename columns
    renamer_dict = dict()
    for metric_data_dict in my_dc_power_names:
        if metric_data_dict['whole_or_part'] == 'whole':
            renamer_dict[metric_data_dict['metric_id']] = dc_power_total_name(
                has_subparts=my_metadata[metadata_part_name('dc_power')],
                unit=size_standard)
        elif metric_data_dict['whole_or_part'] == 'part':
            renamer_dict[metric_data_dict['metric_id']] = dc_power_partial_name(
                size_standard, metric_data_dict['index']
            )
        else:
            raise ValueError('The "whole_or_part" result of find_all_dc_power_metrics()\n'
                             f'is not correct for system {system_id}.')
    wide_df = wide_df.rename(columns=renamer_dict)
    # convert back to tall format if that is what we wanted
    if tall_or_wide == 'wide':
        our_df = wide_df
    elif tall_or_wide == 'tall':
        our_df = wide_df.melt(
            id_vars='measured_on',
            value_vars=list(renamer_dict.values()),
            var_name='metric_name',
            value_name='value'
        )
    else:
        raise ValueError('The term "tall_or_wide" must be "tall" or "wide.\n'
                         + f'Recieved {tall_or_wide}')
    return (our_df, renamer_dict)


if __name__ == '__main__':
    systems_cleaned = pd.read_csv('../../../data/core/systems_cleaned.csv')
    metrics_dir = Path("../../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    metrics_id_set = set(metrics_df.system_id)

    (all_dc_pow_metrics, all_dc_pow_metadata) = find_all_dc_pow_metrics(
        print_messages=False,
        source_matter=False
    )
    # append to metadata

    for system_id in all_dc_pow_metrics.keys():
        (my_df, my_renamer_dict) = dc_power_dataframe_generator(
            system_id=system_id,
            tall_or_wide='wide',
            error_on_no_data=False,
            size_standard='kW'
        )
