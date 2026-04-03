import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, datetime, timedelta
from itertools import product
from copy import deepcopy
from gen_variable_standard_static import sources_checker, \
    find_aggregate_variable_names_gen_mod, \
    find_all_variable_names_gen_mod, \
    check_variable_data_exists_single_system, \
    metrics_search_for_two_fragments_df
from tqdm import tqdm

# choices
# start and end index
# 82 (0-81) in general
i_start = 0
i_end = 9
# save as csv or a parquet group?
save_mode = 'parquet'
save_folder_csv = '../../../../data_ds_project/testing_yearly_csv/'
save_folder_parquet = '../../../../data_ds_project/testing_yearly_parquet/'

# Load system data
systems_cleaned = pd.read_csv('../../../data/core/systems_cleaned.csv')
# Load system metadata
metrics_dir = Path("../../../data/raw/parquet-metrics/")
metrics_pq = pq.ParquetDataset(metrics_dir)
metrics_df = metrics_pq.read().to_pandas()
metrics_id_set = set(metrics_df.system_id)
# determine good systems
two_years_of_days = 2*365
enough_data_systems = systems_cleaned[
    systems_cleaned['num_days_actual_records'] >= two_years_of_days
]
enough_data_ids = set(enough_data_systems.system_id)
ac_pow_systems = metrics_search_for_two_fragments_df(
    metrics_df, 'ac', 'pow', 'and'
)
ac_pow_data_ids = set(ac_pow_systems.system_id)
enough_ac_pow_data_ids = list(enough_data_ids.intersection(ac_pow_data_ids))
enough_ac_pow_data_ids.sort()


# find all aggregate power names
def ac_pow_metadata_name(source_type: str):
    '''Give the name to put in the metadata table.'''
    if source_type is not None:
        return f'has_ac_power_{source_type}_aggregate'
    else:
        return 'has_ac_power_aggregate'


def find_aggregate_ac_power_names(
    systems_cleaned: pd.DataFrame,
    metrics_df: pd.DataFrame,
    print_messages: bool,
    known_sources=('inverter', 'meter'),
    known_sources_short=('inv', 'met')
):
    var_name = 'ac_power'
    filtered_pow_metrics = metrics_search_for_two_fragments_df(
        metrics_df, 'ac', 'pow', 'and'
    )

    # after a manual search, these are the aggregate power names!
    ac_agg_sensor_names = ('ac_power', 'ac_power_hW', 'ac_power_kW', 'ac_power_1_6',
                           'InvPAC_kW_Avg', 'ac_power_calc', 'W_avg',
                           'ac_power_metered_kW', 'RTW',
                           'inv_total_ac_power', 'metered_ac_power',
                           'real_power_kW', 'ac_power_KwAC',
                           'ac_power_metered_1_2',
                           'ac_inverter_power',
                           'PwrMtrP_kW_Avg')

    return find_aggregate_variable_names_gen_mod(
        systems_cleaned=systems_cleaned,
        filtered_metrics_df=filtered_pow_metrics,
        var_name=var_name,
        agg_var_sensor_names=ac_agg_sensor_names,
        print_messages=print_messages,
        sources_matter=True,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )


# helper functions to find all ac power names
def ac_pow_metadata_part_name(source_type: str):
    '''Give the name to put in the metadata table.'''
    if source_type is not None:
        return f'has_ac_power_{source_type}_subsystems'
    else:
        return 'has_ac_power_subsystems'


def common_prefix_and_suffix(names_collection, first_name):
    '''Find the common prefix and suffix of a collection of the strings,
    with the first name in the collection set aside for ease of coding.'''
    common_prefix = ''
    j = 0
    good_prefix = True
    max_len = len(first_name)
    while good_prefix:
        if all(
            [name.startswith(common_prefix) for name in names_collection]
        ):
            j += 1
            common_prefix = first_name[0:j]
            if j >= max_len + 1:
                print('Common prefix is whole thing!')
                good_prefix = False
                common_prefix = first_name
        else:  # bad prefix, back it up one
            good_prefix = False
            common_prefix = common_prefix[0:-1]
    common_suffix = ''
    j = 0
    good_suffix = True
    while good_suffix:
        if all(
            [name.endswith(common_suffix) for name in names_collection]
        ):
            j += 1
            common_suffix = first_name[-j:]
            if j >= max_len + 1:
                print('Common suffix is whole thing!')
                good_suffix = False
                common_suffix = first_name
        else:  # take the last amendment off
            good_suffix = False
            common_suffix = common_suffix[1:]
    return (common_prefix, common_suffix)


def find_all_ac_power_names(
    systems_cleaned: pd.DataFrame,
    metrics_df: pd.DataFrame,
    print_messages: bool = False,
    known_sources=('inverter', 'meter'),
    known_sources_short=('inv', 'met'),
    drop_competing_meter_data: bool = True
):
    '''Add subsystem names to aggregation names for each ac-power system.

    Parameters
    -----------
    systems_cleaned: pandas.DataFrame
        The cleaned-system data
    metrics_df: pd.DataFrame
        The data-frame of all relevangt metrics
    print_messages: bool
        Whether or not to reprint previous-phase messages here.
        Should probably be False.
    known_sources: iterable of strings
        An iterable of known sources.
    known_sources_short: iterable of strings
        An iterable of shorthands.  Must be the same length as known_sources.
    drop_competing_meter_data:
        Drop the known overlpas in Systems 1200, 1208, 1283
        True for run-through
        False for testing

    Returns
    -----------
    var_total_dict: dict[list[dict]]
        A dictionary, indexed by relevant system_id's.
        The value of var_total_dict[system_id] is a list of dictionaries,
        one for each variable-related metric for the systems_id.
            "metric_id" -- the metric_id number
            "sensor_name" -- the sensor_name term
            "common_name" -- the common-name term
            "units" -- the units for each term
            "whole_or_part" -- determining whether each term is aggregate or a sub-part
        If sources_matter = True, then add
            "source_type": the source type if known, or "unknown" if unknown
        If a sub-part, add in
            "index" -- the identifying string of the sub-part
    var_total_metadata_df: pandas.DataFrame
        If sources_matter = True, then a DataFrame indicating both
        which systems have aggregate variable data,
        which systems have subpart variable data,
        and the breakdowns per subtype
        If sources_matter = False, a DataFrame indicating which systems have
        aggregate and subpart variable data only.
    '''
    var_name = 'ac_power'
    sources_matter = True
    # after a manual search, these are the aggregate power names!
    ac_agg_sensor_names = ('ac_power', 'ac_power_hW', 'ac_power_kW',
                           'ac_power_1_6',
                           'InvPAC_kW_Avg', 'ac_power_calc', 'W_avg',
                           'ac_power_metered_kW', 'RTW',
                           'inv_total_ac_power', 'metered_ac_power',
                           'real_power_kW', 'ac_power_KwAC',
                           'ac_power_metered_1_2',
                           'ac_inverter_power',
                           'PwrMtrP_kW_Avg')

    agg_power_metrics, agg_power_metadata = find_aggregate_ac_power_names(
        systems_cleaned=systems_cleaned,
        metrics_df=metrics_df,
        print_messages=print_messages,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )
    ac_power_metrics = metrics_search_for_two_fragments_df(
        metrics_df, 'ac', 'pow', 'and'
    )
    # to filter to non-power-factor terms, it happens to work
    # to filter each term to units 'W', 'kW'
    # (don't try this with dc_power without cleaning, though!)
    non_power_factor_metrics = ac_power_metrics[
        ac_power_metrics['units'].isin(['W', 'kW'])
    ]
    all_ac_pow_metrics, all_ac_pow_metadata = find_all_variable_names_gen_mod(
        var_aggs_dict=agg_power_metrics,
        var_aggs_metadata=agg_power_metadata,
        filtered_metrics_df=non_power_factor_metrics,
        var_name=var_name,
        agg_var_sensor_names=ac_agg_sensor_names,
        sources_matter=sources_matter,
        known_sources=known_sources,
        known_sources_short=known_sources_short
    )
    if drop_competing_meter_data:
        # make the override -- drop the duplicates aggregates from the previous step.
        # had to wait till now, because otherwise they would be added again.
        known_sources, known_sources_short = sources_checker(
            known_sources, known_sources_short
        )
        for system_id in all_ac_pow_metrics.keys():
            num_aggs_by_type = {
                source_type: 0 for source_type in known_sources
            }
            for metric_dict in all_ac_pow_metrics[system_id]:
                if metric_dict['whole_or_part'] == 'whole':
                    num_aggs_by_type[metric_dict['source_type']] += 1
            for source_type in known_sources:
                if num_aggs_by_type[source_type] > 1:
                    assert (system_id in [1200, 1208, 1283])
                    for (system_id, duplicate_to_remove) in (
                        (1200, 'ac_power'), (1208, 'ac_power_metered_1_2'),
                        (1283, 'ac_power')
                    ):
                        for e, metric_dict in enumerate(all_ac_pow_metrics[system_id]):
                            if metric_dict['sensor_name'] == duplicate_to_remove:
                                all_ac_pow_metrics[system_id].pop(e)
                                break

    return (all_ac_pow_metrics, all_ac_pow_metadata)


# load power data
def temp_agg_name(source_type):
    '''Provide the temporary manual-aggregate name just for reference later.'''
    return f'ac_power_{source_type}_artificial_sum'


def ac_power_gather_data_single_year(
    all_ac_pow_metrics,
    all_ac_pow_metadata: pd.DataFrame,
    system_id: int,
    year: int,
    error_on_no_data: bool,
    add_aggs: bool,
    known_sources=('inverter', 'meter'),
    known_sources_short=('inv', 'met'),
):
    '''Gather all ac power-data per-system,
    given a list of aggregate sensor names.
    
    Parameters
    ----------
    all_ac_pow_metrics
        first result of find_all_ac_power_names(*args)
    all_ac_pow_metadata
        second result of find_all_ac_power_names(*args)
    system_id: int
        Index of system in systems_cleaned and metric_df
    year: int
        The year in question.  Should be in the range 1994 to 2023 for PVDAQ data sets.
    tall_or_wide: str
        If 'wide', return wide DataFrame
        if 'tall', convert back to a 3-column array.
    error_in_no_data: bool
        If True, return an error if the system_id has no ac-power data.
        If False, return None if the system-system_id has no ac-power data.
    add_aggs: bool
        If True, and there are parts without a corresponding aggregate,
            add the aggregate, according to agg_type.
        If False, do nothing.
    print_warnings: bool
        Print warnings about too few or too many aggregators.
    known_sources: iterable of strings
        Full names of the known source types.
    known_sources_short: iterable of strings
        fragments of the known source names suitable for searching

    Returns
    --------
    A pandas DataFrame object with the desired data.
    '''
    known_sources, known_sources_short = sources_checker(
        known_sources, known_sources_short
    )
    # specialize to current ID number
    try:
        my_ac_power_names = deepcopy(all_ac_pow_metrics[system_id])
        my_ac_power_metadata = all_ac_pow_metadata.loc[system_id].copy(deep=True)
    except KeyError:
        if error_on_no_data:
            raise ValueError(f'System {system_id} has no AC Power data!')
        else:
            return None
    except BaseException as e:
        raise e
    # for known problem-cases, clean the data of spurious variables
    if system_id in [1422, 1423, 1429]:
        (my_ac_power_names, my_ac_power_metadata) = check_variable_data_exists_single_system(
            var_total_dict=all_ac_pow_metrics,
            var_total_metadata_df=all_ac_pow_metadata,
            path_to_raw_data_dir='../../../../data_ds_project/systems/parquet/',
            system_id=system_id,
            var_name='ac_power',
            sources_matter=True,
            known_sources=known_sources,
            known_sources_short=known_sources_short
        )
    # grab some metadata, quickly
    metric_ids = []
    whole_metric_ids = []
    source_type_metric_ids = {
        source_type: [] for source_type in known_sources
    }
    # grab all metric ids, putting the 'whole' category first
    for metric_data_dict in my_ac_power_names:
        if metric_data_dict['whole_or_part'] == 'whole':
            metric_ids.insert(0, metric_data_dict['metric_id'])
            whole_metric_ids.append(metric_data_dict['metric_id'])
        elif metric_data_dict['whole_or_part'] == 'part':
            metric_ids.append(metric_data_dict['metric_id'])
        else:
            raise ValueError('The "whole_or_part" result of find_all_ac_power_names()\n'
                             f'is not correct for system {system_id}.')
        source_type_metric_ids[metric_data_dict['source_type']].append(
            metric_data_dict['metric_id']
        )
    # Load only these metrics from the system
    my_system_parquet_data_path = Path(f'../../../../data_ds_project/systems/parquet/{system_id}/')
    my_system_parquet_selection = pq.ParquetDataset(
        my_system_parquet_data_path, filters=[
            ('metric_id', 'in', metric_ids),
            ('measured_on', '>=', datetime(year, 1, 1)),
            ('measured_on', '<', datetime(year + 1, 1, 1))
        ]
    )
    system_year_df = my_system_parquet_selection.read().to_pandas()
    # for reference, 4 columns (see
    # https://github.com/openEDI/documentation/blob/main/pvdaq.md#pvdaq_pvdata)
    # measured_on, utc_measured_on, metric_id, value)
    # standard cleaning
    # first, see if any data.  If not, just return Nones and be done with it
    if system_year_df.shape[0] == 0:
        # no data for this year
        if error_on_no_data:
            raise ValueError(f'System {system_id} has no AC Power data\n'
                             + f'for the year {year}!')
        else:
            return None
    # if missing variables, grab variable names to include later
    missing_ids = set(metric_ids).difference(set(system_year_df['metric_id']))
    if (len(missing_ids) > 0) and error_on_no_data:
        raise ValueError(f'System {system_id} has missing AC Power data\n'
                         + f'for the year {year}!\n'
                         + f'{missing_ids}')

    system_year_df = system_year_df.drop_duplicates()
    # See if multiple values at a given time
    # if so, forced to replace value by mean value
    if any(system_year_df.duplicated(subset=['measured_on', 'metric_id'])):
        system_year_df.loc[:, 'mean_value'] = system_year_df.groupby(
            ['measured_on', 'metric_id']
        )['value'].transform('mean')
        system_year_df = system_year_df.drop(columns='value')
        system_year_df = system_year_df.rename(columns={'mean_value': 'value'})
        system_year_df.drop_duplicates()
    # if still duplicates, forced to drop utc_measured_on,
    # a frequent source of off-by-one-hour errors
    # (and points with the same 'measured_on' but different 'utc_measured_on'
    # have the same value, so it is likely that utc_measured_on is the problem)
    if any(system_year_df.duplicated(subset=['measured_on', 'metric_id', 'value'])):
        system_year_df = system_year_df.drop(columns='utc_measured_on')
        system_year_df = system_year_df.drop_duplicates()
    # ready to widen the columns
    wide_df = system_year_df.pivot(
        index='measured_on',
        columns='metric_id',
        values='value'
    )
    # reset the metric_id name of the index of columns
    wide_df.columns.name = ''
    # reset the index
    wide_df = wide_df.reset_index()
    # if missing metric_id's, add them at this point.
    if len(missing_ids) > 0:
        for metric_id in missing_ids:
            wide_df.loc[:, metric_id] = np.full(wide_df.shape[0], np.nan)
    # Some systems have part-data and not aggregate data;  
    # amend this mistake.
    if add_aggs:
        for source_type in known_sources:
            if (my_ac_power_metadata[ac_pow_metadata_part_name(source_type)])\
              and (not my_ac_power_metadata[ac_pow_metadata_name(source_type)]):
                source_type_total_name = temp_agg_name(source_type=source_type)
                # as agg_type == 'sum' in gen_variable_standardizer
                wide_df[source_type_total_name] = wide_df.apply(
                    lambda row: np.sum(
                        [row[j] for j in source_type_metric_ids[source_type]]
                    ), axis=1
                )
                sensor_names_summed = []
                units_group = []
                for metric_dict in my_ac_power_names:
                    if metric_dict['metric_id'] in source_type_metric_ids[source_type]:
                        sensor_names_summed.append(metric_dict['sensor_name'])
                        units_group.append(metric_dict['units'])
                calc_type = sensor_names_summed[0]
                for j in range(1, len(sensor_names_summed)):
                    calc_type = f'{calc_type} + {sensor_names_summed[j]}'
                units_group = set(units_group)
                if len(units_group) == 1:
                    # grab the singleton as our unit
                    # see https://stackoverflow.com/questions/1619514/how-to-extract-the-member-from-single-member-set-in-python
                    # for more info on this
                    (my_unit, ) = units_group
                else:
                    raise RuntimeError('Multiple units in the subparts summed!  No good!')
                whole_metric_ids.append(source_type_total_name)
                source_type_metric_ids[source_type].append(
                    source_type_total_name
                )
                # adjoin new variables to our metadata lists as well
                my_ac_power_metadata.at[ac_pow_metadata_name(source_type)] = True
                my_ac_power_names.append(
                    {
                        'sensor_name': source_type_total_name,
                        'units': my_unit,
                        'calc_type': calc_type,
                        'common_name': 'AC power',
                        'metric_id': 'N/A',
                        'whole_or_part': 'whole',
                        'source_type': source_type
                    }
                )

    # preserve 'whole' columns
    whole_columns = ['measured_on',] + whole_metric_ids 
    wide_df = wide_df[whole_columns]
    renamer_dict = dict()
    for metric_data_dict in my_ac_power_names:
        renamer_dict[metric_data_dict['metric_id']] = metric_data_dict['sensor_name']
    wide_df = wide_df.rename(columns=renamer_dict)

    return (my_ac_power_names, my_ac_power_metadata, wide_df)


# manually recorded data of systems that switch units midway
switch_unit_systems = [34, 35, 1200, 1201, 1202, 1208,
                       1239, 1276, 1277, 1283, 1332, 1420]
switch_in_2018_systems = deepcopy(switch_unit_systems)
switch_in_2018_systems.pop(-1)
my_unit_shift_days = {
    system_id: date(2014, 8, 4) for system_id in switch_in_2018_systems
}
my_unit_shift_days[1420] = date(2017, 3, 29)
my_unit_shift_days


def starting_ending_units(system_id: int, var_name: str):
    '''For systems that change their units midstream,
    what are the units?
    Manually checked against data for the 12 flagged systems.'''
    if 'hw' in var_name.lower():
        return ('hW', 'W')
    elif 'kw' in var_name.lower():
        return ('kW', 'W')
    elif (
        (system_id == 1208 and 'inv' in var_name.lower())
        or (system_id == 1332 and 'meter' in var_name.lower())
        or (system_id == 1283 and 'inv' in var_name.lower())
    ):
        return ('kW', 'W')
    elif (
        (system_id == 1200 and 'inv' in var_name.lower())
        or (system_id == 1202 and var_name == 'ac_power')
        or (system_id == 1332 and 'inv' in var_name.lower())
    ):
        return ('W', None)
    else:
        raise ValueError('Case not accounted for.')


# for systems non-switching, this function and the next
# determine accuracy of the units
def find_good_date(system_id: int,
                   systems_cleaned: pd.DataFrame,
                   error_out: bool = True):
    '''Find a good date to check for ac power production,
    based on satellite irradiance figures (in the Google Drive).'''
    relevant_rows_systems = systems_cleaned[systems_cleaned['system_id'] == system_id]
    ind = relevant_rows_systems.index[0]
    target_year = int(relevant_rows_systems.loc[ind, 'sample_year'])
    if target_year < 1999:
        target_year = 1999
    my_irrad_address = '../../../../data_ds_project/systems/parquet_irrad_samples/'\
        + f'{system_id}_{target_year}_irradiance.csv'
    # skip header of two rows at the start
    try: 
        irrad_data_target_year = pd.read_csv(my_irrad_address, skiprows=2)
    except pd.errors.EmptyDataError as e:
        print(system_id)
        if error_out:
            raise e
        else:
            return None
    except BaseException as e:
        raise e
    
    irrad_data_nonzero = irrad_data_target_year[irrad_data_target_year['DHI'] >= 1]
    # find a good day
    irrad_75 = np.quantile(irrad_data_nonzero['DHI'].values, 0.75)
    irrad_good_hours = irrad_data_nonzero[irrad_data_nonzero['DHI'] >= irrad_75]
    irrad_good_hours_counts = irrad_good_hours.groupby(['Year', 'Month', 'Day'])['DHI'].count()
    max_count = irrad_good_hours_counts.max()
    # prefer spring/fall to summer/winter -- good irradiance without temperature extremes
    preference_months = [4, 9, 3, 10, 5, 8, 2, 11, 1, 12, 6, 7]
    for (month, day) in product(preference_months, range(1, 32)):
        try:
            this_day_figure = irrad_good_hours_counts[target_year][month][day]
            if this_day_figure == max_count:
                good_date = date(target_year, month, day)
                break
        except KeyError:
            continue
        except BaseException as e:
            raise e
    # load the data for that good day
    return good_date


def ac_power_units_tester_single_year(
    ac_pow_names,
    ac_pow_meta,
    system_id: int,
    systems_cleaned: pd.DataFrame,
    print_warnings: bool = True,
):
    # grab initial attempt at good day
    my_good_date = find_good_date(
        system_id=system_id,
        systems_cleaned=systems_cleaned,
        error_out=True
    )
    # grab the data on the appropriate year.
    my_good_year = my_good_date.year
    (my_metrics, my_metadata, my_data) = ac_power_gather_data_single_year(
        all_ac_pow_metrics=ac_pow_names, all_ac_pow_metadata=ac_pow_meta,
        system_id=system_id, year=my_good_year,
        # have to refuse errors on no data, else will break if 
        # any meters down on the good year.
        error_on_no_data=False, add_aggs=True
    )
    whole_metrics_name_unit_pairs = dict()
    for metric_dict in my_metrics:
        if metric_dict['whole_or_part'] == 'whole':
            whole_metrics_name_unit_pairs[metric_dict['sensor_name']]\
                = metric_dict['units']
    # check for goodness of the guessed day
    is_good_date = False
    test_count = 1
    my_data['date'] = my_data['measured_on'].dt.date
    my_data_on_date = my_data[my_data['date'] == my_good_date]
    while not is_good_date:
        my_data['date'] = my_data['measured_on'].dt.date
        my_data_on_date = my_data[my_data['date'] == my_good_date]
        if my_data_on_date.shape[0] > 0:
            is_good_date = True
        else:
            test_count += 1
            my_good_date += timedelta(days=1)
            # note -- in theory, we could iterate into the next year
            # fortunately, given our testing, we don't for the systems
            # we care about
            my_data_on_date = my_data[my_data['date'] == my_good_date]
            if test_count > 150:  # call it quits
                if print_warnings:
                    print(f'System {system_id} does not have any data on 150 test-days.')
                return None
    # find the kW incentive
    relevant_rows_systems = systems_cleaned[systems_cleaned['system_id'] == system_id]
    ind = relevant_rows_systems.index[0]
    my_dc_power_kW = relevant_rows_systems.loc[ind, 'dc_capacity_kW']
    # test each aggregator
    results = {
        name: True for name in whole_metrics_name_unit_pairs.keys()
    }
    for (name, unit) in whole_metrics_name_unit_pairs.items():
        max_data = my_data_on_date[name].max()
        if unit == 'W':
            max_data = max_data / 1000
        if my_dc_power_kW / max_data >= 10:
            if print_warnings:
                print(f'For System {system_id}, variable {name},\n',
                      'units should be kW')
            results[name] = False
        elif (max_data / my_dc_power_kW >= 10):
            raise ValueError(
                f'For System {system_id}, variable {name},\n',
                f'max val of {max_data} is far above {my_dc_power_kW}'
            )
        else:
            results[name] = True
    return results


def units_normalizer_single_year(
    ac_pow_names,
    ac_pow_metadata: pd.DataFrame,
    system_id: int,
    year: int,
    systems_cleaned: pd.DataFrame,
    unit_shift_days,
):
    # grab the data
    data_returned = ac_power_gather_data_single_year(
        ac_pow_names,
        ac_pow_metadata,
        system_id,
        year=year,
        error_on_no_data=False,  # since single-year, can easily be no data!
        add_aggs=True
    )
    if data_returned is None:
        return None
    else:
        (my_rev_pow_names, my_rev_pow_metadata, my_data) = data_returned
    # normalize units to kW, starting with the switching systems
    if system_id in unit_shift_days.keys():
        right_date = unit_shift_days[system_id]
        end_old_time = datetime(right_date.year, right_date.month, right_date.day, 0, 0)
        start_new_time = end_old_time + timedelta(days=1)
        my_data_early = my_data[my_data['measured_on'] < end_old_time]
        my_data_late = my_data[my_data['measured_on'] >= start_new_time]
        # this already drops the questionable day.
        # But must convert units before and after
        for metric_dict in my_rev_pow_names:
            # only have the 'whole' columns to work with
            if metric_dict['whole_or_part'] == 'whole':
                sensor_name = metric_dict['sensor_name']
                # because sensors come in and out,
                # cannot assume that data in every year
                if sensor_name in my_data.columns:
                    (early_unit, late_unit) = starting_ending_units(system_id, sensor_name)
                    if early_unit == 'hW':
                        my_data_early.loc[:, sensor_name] = my_data_early[sensor_name] / 10
                    elif early_unit == 'W':
                        my_data_early.loc[:, sensor_name] = my_data_early[sensor_name] / 1000
                    # if early_unit = kW, nothing to do
                    if late_unit is None:
                        my_data_late.loc[:, sensor_name] = pd.Series(
                            np.full(my_data_late[sensor_name].shape, np.nan),
                            dtype='float64'
                        )
                    elif late_unit == 'W':
                        my_data_late.loc[:, sensor_name] = my_data_late[sensor_name] / 1000
                    # if late_unit = kW, nothing to do.
                    metric_dict['units'] = 'kW'
            my_data = pd.concat([my_data_early, my_data_late])
    else:
        # run the unit-accuracy subroutine
        unit_accuracy = ac_power_units_tester_single_year(
            ac_pow_names, ac_pow_metadata, system_id, systems_cleaned, False
        )
        if unit_accuracy is None:
            raise ValueError(f'For System {system_id}, not a good units reading!')
        for metric_dict in my_rev_pow_names:
            if metric_dict['whole_or_part'] == 'whole':
                sensor_name = metric_dict['sensor_name']
                unit = metric_dict['units']
                # again, units can come in and out.
                if unit == 'W':
                    if unit_accuracy[sensor_name]:
                        # True units are indeed W, but should be kW
                        my_data.loc[:, sensor_name] = my_data[sensor_name] / 1000
                    # if False, true value kW, nothing to do.
                    metric_dict['units'] = 'kW'
                elif unit == 'kW':
                    if not unit_accuracy[sensor_name]:
                        # truly W, must convert to kW
                        my_data.loc[:, sensor_name] = my_data[sensor_name] / 1000
                    # if kW accurate, nothing to do.
    # return the revised data
    return (my_rev_pow_names, my_rev_pow_metadata, my_data)


# Manually collected outlier days
outlier_days = {
    4: [date(2010, 1, 21), date(2012, 10, 18), date(2014, 10, 6)],
    10: [date(2011, 10, 4)],
    33: [date(2011, 9, 15)],
    36: [date(2012, 9, 28)],
    50: [date(2004, 10, 11)],
    1208: [date(2012, 5, 15)],
    1289: [date(2014, 10, 7)],
    1431: [date(2016, 9, 2)],
    4903: [date(2016, 10, 6)]
}
# compile outlier years as well for convenience later
outlier_years = []
for system_id in outlier_days.keys():
    for out_date in outlier_days[system_id]:
        outlier_years.append(out_date.year)
outlier_years = set(outlier_years)


def outliers_removal(
    units_cleaned_data: pd.DataFrame,
    system_id: int,
    outlier_days_list
):
    '''Remove data on known outlier days.'''
    if system_id in outlier_days_list.keys():
        for bad_day in outlier_days_list[system_id]:
            end_old_datetime = datetime(
                bad_day.year, bad_day.month, bad_day.day, 0, 0
            )
            start_new_datetime = end_old_datetime + timedelta(days=1)
            excluded_data = units_cleaned_data[
                (units_cleaned_data.measured_on >= end_old_datetime)
                & (units_cleaned_data.measured_on < start_new_datetime)
            ]
            units_cleaned_data = units_cleaned_data.drop(
                index=excluded_data.index
            )
    return units_cleaned_data


# towards the final daily averaging
def ac_power_total_name_alt(source_type: str):
    '''Make the standardized variable name.'''
    total_name = 'ac_power_kW'
    if (source_type is not None) and (source_type != 'unknown'):
        total_name = total_name + '_' + source_type
    return total_name


def renaming_only(
    my_metrics,
    my_data: pd.DataFrame,
    system_id: int,
    all_sources=('inverter', 'meter', 'unknown')
):
    renamer_dict = {
        col_name: '' for col_name in my_data.columns[1:]
    }
    renamer_dict['measured_on'] = 'time'
    # clean too-small data
    for metric_dict in my_metrics:
        if metric_dict['whole_or_part'] == 'whole':
            sensor_name = metric_dict['sensor_name']
            source_type = metric_dict['source_type']
            renamer_dict[sensor_name] = ac_power_total_name_alt(source_type)
    my_data = my_data.rename(columns=renamer_dict)

    # reorder to match like to like
    columns_reordered = ['time']
    for source_type in all_sources:
        if ac_power_total_name_alt(source_type) in my_data.columns:
            columns_reordered.append(ac_power_total_name_alt(source_type))
    my_data = my_data[columns_reordered]
    return my_data


# run the code
if __name__ == '__main__':
    (ac_power_names_slim, ac_power_metadata_slim) = find_all_ac_power_names(
        systems_cleaned,
        metrics_df,
        print_messages=False,
        drop_competing_meter_data=True
    )
    output_dir_str = '../../../../data_ds_project/testing/'
    output_dir = Path(output_dir_str)
    if not output_dir.is_dir():
        output_dir.mkdir(parents=True)
    for system_id in tqdm(enough_ac_pow_data_ids[i_start:i_end+1]):
        # all parquet-stored data 1994-2003
        for year in range(1994, 2024):
            outputs = units_normalizer_single_year(
                ac_power_names_slim, ac_power_metadata_slim,
                system_id, year,
                systems_cleaned, my_unit_shift_days
            )
            if outputs is not None:
                (my_metrics, _, my_data) = outputs
                my_data = outliers_removal(
                    my_data, system_id, outlier_days
                )
                my_renamed_data = renaming_only(
                    my_metrics,
                    my_data,
                    system_id=system_id
                )
                if save_mode == 'csv':
                    output_dir_str = save_folder_csv + f'{system_id}/'
                    output_dir = Path(output_dir_str)
                    if not output_dir.is_dir():
                        output_dir.mkdir(parents=True)
                    output_file = Path(
                        output_dir_str + f'all_data_{system_id}_{year}.csv'
                    )
                    output_file.touch()
                    my_renamed_data.to_csv(output_file, index=False)
                elif save_mode == 'parquet':
                    output_dir_alt_str = save_folder_parquet + f'{system_id}/'
                    output_dir_alt = Path(output_dir_alt_str)
                    if not output_dir_alt.is_dir():
                        output_dir_alt.mkdir(parents=True)
                    my_renamed_data.loc[:, 'year'] = my_renamed_data['time'].dt.year
                    my_renamed_data.to_parquet(
                        output_dir_alt,
                        partition_cols=['year',]
                    )
                else:
                    raise ValueError('Invalid save_mode command!')
