# flake8 noqa E501
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path


def metrics_search_for_fragment_df(df: pd.DataFrame, fragment: str):
    '''Search for fragments of a name in sensor_name and common_name'''
    fragment = fragment.lower()
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
    ]


def metrics_search_for_two_fragments_df(df: pd.DataFrame, fragment_1: str,
                                        fragment_2: str, and_or: str):
    '''Search for fragments of two names in sensor_name and common name.
    Use and_or to switch between "both" and "at least one" modes'''
    fragment_1 = fragment_1.lower()
    fragment_2 = fragment_2.lower()
    if and_or == 'and':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False)))
            & ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False)))
        ]
    elif and_or == 'or':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False)))
            | ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False)))
        ]


def widened_search_for_fragment_df(df: pd.DataFrame, fragment: str):
    '''Search for a fragment in calc_details and source_type
    as well as in sensor_name and common_name'''
    fragment = fragment.lower()
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'calc_details'].str.contains(fragment, case=False))
        | (df.loc[:, 'source_type'].str.contains(fragment, case=False))
    ]


def widened_search_for_two_fragments_df(df: pd.DataFrame, fragment_1: str,
                                        fragment_2: str, and_or: str):
    '''Search for two fragments in calc_details and source_type
    as well as in sensor_name and common_name'''
    fragment_1 = fragment_1.lower()
    fragment_2 = fragment_2.lower()
    if and_or == 'and':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_1, case=False)))
            & ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_2, case=False)))
        ]
    elif and_or == 'or':
        return df[
            ((df.loc[:, 'sensor_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_1, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_1, case=False)))
            | ((df.loc[:, 'sensor_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'common_name'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'calc_details'].str.contains(fragment_2, case=False))
                | (df.loc[:, 'source_type'].str.contains(fragment_2, case=False)))
        ]


# Feel free to adjust these naming conventions for your variable.
def metadata_agg_name(var_name: str):
    '''Give the name to put in the metadata table.'''
    return f'has_{var_name}_aggregate'


def metadata_agg_subtype_name(var_name: str, source_type: str):
    '''Give the name to put in the metadata table.'''
    return f'has_{var_name}_{source_type}_aggregate'


def sources_checker(known_sources, known_sources_short):
    '''Do some standard checking with the lists of known sources'''
    # if lists not of the same length, can't correspond
    if len(known_sources) != len(known_sources_short):
        raise ValueError('Incorrect match between names and fragments\n'
                         + 'of known_sources and known_sources_short:\n'
                         + f'{len(known_sources)} vs. {len(known_sources_short)}')
    elif 'unknown' not in known_sources:
        # add 'unknown' category to catch all remaining terms
        # and lowercase everything
        known_sources = [
            source_type.lower() for source_type in known_sources
        ]
        known_sources.append('unknown')
        known_sources_short = [
            source_fragment.lower()
            for source_fragment in known_sources_short
        ]
        known_sources_short.append('')  # unknown is a catch-all
    return (known_sources, known_sources_short)


def find_aggregate_variable_names_gen_mod(
    systems_cleaned: pd.DataFrame,
    filtered_metrics_df: pd.DataFrame,
    var_name: str,
    agg_var_sensor_names,
    print_messages: bool,
    sources_matter: bool,
    known_sources=('inverter', 'meter'),
    known_sources_short=('inv', 'met')
):
    '''Find all aggregrate variable names for each Parquet system.

    Parameters
    -----------
    systems_cleaned: pd.DataFrame
        The pre-existing systems_cleaned.csv holder.
        Only an argument to enable portability
    filtered_metrics_df: pd.DataFrame
        The relevant metrics for your variable.
        Designed to be a filter of the metrics_df coming from
        data/raw/parquet/
        through the functions
        metrics_search_for_fragment_df(*args)
        and
        metrics_search_for_two_fragments_df(*args)
    var_name: str
        The variable name you are searching for
    agg_var_sensor_names: iterable of strings
        The collection of sensor_name entries (exact) for aggregates,
        manually accrued
    var_short_1: str
        The first short string to search for
        For example, 'power' abbreviates to 'pow' in some places, so we search for 'pow'
    is_var_short_2: bool
        if True, look at var_short_2 and and_or
        If False, ignore those values  (but due to Python rules, must put some placeholder there.)
    var_short_2: str
        The second short string to search for.  (Again, must include some placeholder
    and_or: str, "and" or "or"
        If and_or == "and", require both var_short_1 and var_short_2 to be found
        If and_or == "or", require at least one of var_short_1 and var_short_2 to be found.
    print_messages: bool
        If True, prints some message at the end if certain subsystems have no aggregate data,
        or they have too many aggregates.
        If False, does not print these error messages.
        Designed to be set to True while testing,
        and False when called as a subroutine in later functions.
    sources_matter: bool
        If True, collect data about where the terms are located.
        If False, do not collect such data
    known_sources: iterable of strings
        An iterable of known sources.
    known_sources_short: iterable of strings
        An iterable of shorthands.  Must be the same length as known_sources.

    Returns
    -----------
    var_aggs_dict: dict[list[dict]]
        A dictionary, indexed by relevant system_id's.
        The value of var_aggs_dict[system_id] is a list of dictionaries,
        one for each aggregator metric for the systems_id.  Keys for each metric are:
            "metric_id" -- the metric_id number
            "sensor_name" -- the sensor_name term
            "common_name" -- the common-name term
            "units" -- the units for each term
            "whole_or_part" -- determining whether each term is aggregate or a sub-part
            always "whole" for now, but we will add sub-parts in the next function
        If sources_matter = True, then add
            "source_type": the source type if known, or "unknown" if unknown
    var_aggs_metadata: pandas.DataFrame
        If sources_matter = True, then a DataFrame indicating both
        which systems have aggregate variable data, and the aggregate data
        per subtype
        If sources_matter = False, a DataFrame indicating which systems have
        aggregate variable data only.
    '''
    # sanitize known_sources and known_sources_short input
    if sources_matter:
        (known_sources, known_sources_short) = sources_checker(
            known_sources=known_sources,
            known_sources_short=known_sources_short
        )
    var_system_ids = set(systems_cleaned.system_id).intersection(
        set(filtered_metrics_df.system_id)
    )
    # create receptacles for outer variables
    num_ids = len(var_system_ids)
    var_aggs_dict = {
        system_id: [] for system_id in var_system_ids
    }
    col_names = [metadata_agg_name(var_name)]
    if sources_matter:
        for source_type in known_sources:
            col_names.append(metadata_agg_subtype_name(var_name, source_type))
    num_cols = len(col_names)
    var_aggs_metadata = pd.DataFrame(
        np.full(shape=(num_ids, num_cols), fill_value=False, dtype='bool'),
        index=var_aggs_dict.keys(),
        columns=col_names
    )
    var_aggs_metadata = var_aggs_metadata.sort_index()
    # run through agg_var_sensor_names and see which systems
    # have that exact sensor name
    for sensor_name in agg_var_sensor_names:
        exact_name_metrics = filtered_metrics_df[
                filtered_metrics_df['sensor_name'] == sensor_name
        ]
        # again clean against systems_cleaned
        for system_id in set(exact_name_metrics['system_id']).intersection(
            set(systems_cleaned.system_id)
        ):
            relevant_rows_metrics = exact_name_metrics[
                exact_name_metrics['system_id'] == system_id
            ]
            if len(relevant_rows_metrics.index) > 1:
                raise RuntimeError(f'System {system_id} has multiple sensors named {sensor_name}!')
            else:
                var_aggs_metadata.loc[system_id, metadata_agg_name(var_name)] = True
                ind = relevant_rows_metrics.index[0]
                metric_id = relevant_rows_metrics.loc[ind, 'metric_id']
                common_name = relevant_rows_metrics.loc[ind, 'common_name']
                given_unit = relevant_rows_metrics.loc[ind, 'units']
                calc_type = relevant_rows_metrics.loc[ind, 'calc_details']
                raw_source_type = relevant_rows_metrics.loc[ind, 'source_type']
                # clean up data -- frequently empty item
                if raw_source_type is np.nan or raw_source_type is None:
                    raw_source_type = 'unknown'
                if sources_matter:
                    for j in range(len(known_sources_short)):
                        source_type = known_sources[j]
                        source_fragment = known_sources_short[j]
                        if (
                            (source_fragment in sensor_name.lower())
                            or (source_fragment in common_name.lower())
                            or (source_fragment in calc_type.lower())
                            or (source_fragment in raw_source_type.lower())
                        ):
                            var_aggs_dict[system_id].append({
                                'metric_id': metric_id,
                                'sensor_name': sensor_name,
                                'common_name': common_name,
                                'units': given_unit,
                                'calc_details': calc_type,
                                'whole_or_part': 'whole',
                                'source_type': source_type,
                            })
                            # because the last source_fragment is now '',
                            # we get the unknown type for free.
                            var_aggs_metadata.loc[
                                system_id, metadata_agg_subtype_name(var_name, source_type)
                            ] = True
                            # for each particular system_id/sensor_name pair,
                            # only one type, so as soon as a match,
                            break
                else:
                    # record non-source-type data
                    var_aggs_dict[system_id].append({
                        'metric_id': metric_id,
                        'sensor_name': sensor_name,
                        'common_name': common_name,
                        'units': given_unit,
                        'calc_details': calc_type,
                        'whole_or_part': 'whole',
                    })
    # quick checks for 0 or multiple aggregate values
    if print_messages:
        for system_id in var_system_ids:
            # check for missing entries
            if len(var_aggs_dict[system_id]) == 0:
                print(f'System {system_id} appears to have no obvious {var_name} aggregator name.')
            # check for duplicates
            elif len(var_aggs_dict[system_id]) != 1:
                if sources_matter:
                    # only worry about multiples with the same source_type
                    source_type_counts = {
                        source_type: 0 for source_type in known_sources
                    }
                    for metric_dict in var_aggs_dict[system_id]:
                        source_type_counts[metric_dict['source_type']] += 1
                    for source_type in source_type_counts:
                        if source_type_counts[source_type] > 1:
                            print(f'System {system_id} has multiple {var_name} '
                                  + f'aggregators from the {source_type} source:')
                            for metric_dict in var_aggs_dict[system_id]:
                                print(metric_dict)

                else:  # presumed potentially worrisome
                    print(f'System {system_id} has multiple {var_name} aggregators!')
                    for metric_dict in var_aggs_dict[system_id]:
                        print(metric_dict)

    return (var_aggs_dict, var_aggs_metadata)


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
            # avoid infinite loop
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
            # avoid infinite loop
            if j >= max_len + 1:
                print('Common suffix is whole thing!')
                good_suffix = False
                common_suffix = first_name
        else:  # take the last amendment off
            good_suffix = False
            common_suffix = common_suffix[1:]
    return (common_prefix, common_suffix)


# Feel free to adjust these naming conventions for your variable.
def metadata_part_name(var_name: str):
    '''Give the name to put in the metadata table.'''
    return f'has_{var_name}_subsystems'


def metadata_part_subtype_name(var_name: str, source_type: str):
    '''Give the name to put in the metadata table.'''
    return f'has_{var_name}_{source_type}_subsystems'


def find_all_variable_names_gen_mod(var_aggs_dict,
                                    var_aggs_metadata,
                                    filtered_metrics_df: pd.DataFrame,
                                    var_name: str,
                                    agg_var_sensor_names,
                                    sources_matter: bool,
                                    known_sources=('inverter', 'meter'),
                                    known_sources_short=('inv', 'met'),
                                    systems_cleaned=None):
    '''Add subsystem names to aggregation names for each Parquet system.

    Parameters
    -----------
    var_aggs_dict: dict[list[dict]] or None
        The (modified) first output from find_aggregate_variable_names_gen_mod()
        If None, will run the first part again.
    var_aggs_metadata: pandas.DataFrame or None
        The (modified) second output from find_aggregate_variable_names_gen_mod()
        If None, will run the first part again
    filtered_metrics_df: pandas.DataFrame
        The selection from metrics_df of potentially useful variables.
        Pre-filter for best effect.
    var_name: str
        The variable name you are searching for
    agg_var_sensor_names: iterable of strings
        The collection of sensor_name entries (exact) for aggregates,
        manually accrued
    sources_matter: bool
        If True, collect data about where the terms are located.
        If False, do not collect such data
    known_sources: iterable of strings
        An iterable of known sources.
    known_sources_short: iterable of strings
        An iterable of shorthands.  Must be the same length as known_sources.
    systems_cleaned: pd.DataFrame or None
        Only needed if var_aggs_dict or var_aggs_metadata is None
        systems_cleaned file to check for valid system_id's in first part

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
    # sanitize inputs
    if var_aggs_metadata is None or var_aggs_dict is None:
        (var_aggs_dict, var_aggs_metadata) = find_aggregate_variable_names_gen_mod(
            systems_cleaned=systems_cleaned,
            filtered_metrics_df=filtered_metrics_df,
            var_name=var_name,
            agg_var_sensor_names=agg_var_sensor_names,
            print_messages=False,
            sources_matter=sources_matter,
            known_sources=known_sources,
            known_sources_short=known_sources_short
        )
    if sources_matter:
        (known_sources, known_sources_short) = sources_checker(
            known_sources, known_sources_short
        )
    # prep new terms
    var_total_dict = var_aggs_dict
    num_ids = len(var_aggs_dict.keys())
    col_names = [metadata_part_name(var_name)]
    if sources_matter:
        for source_type in known_sources:
            col_names.append(metadata_part_subtype_name(var_name, source_type))
    num_cols = len(col_names)
    var_parts_metadata = pd.DataFrame(
        np.full(shape=(num_ids, num_cols), fill_value=False, dtype='bool'),
        index=var_aggs_dict.keys(),
        columns=col_names
    )
    # for the last one, we went by sensor_name, and then by system_id
    # here, we work by system_id, and then by name
    for system_id in var_aggs_dict.keys():
        # grab the metrics with the correct system name
        relevant_rows_non_agg_metrics = filtered_metrics_df[
            (filtered_metrics_df['system_id'] == system_id)
            & (~filtered_metrics_df['sensor_name'].isin(agg_var_sensor_names))
        ]
        # see if any terms remaining
        num_subparts = relevant_rows_non_agg_metrics.shape[0]
        if num_subparts > 1:  # at least two subparts, keep going!
            var_parts_metadata.loc[system_id, metadata_part_name(var_name)] = True
            if sources_matter:
                for j in range(len(known_sources)):
                    source_type = known_sources[j]
                    source_fragment = known_sources_short[j]
                    var_subparts_known_type = widened_search_for_fragment_df(
                        relevant_rows_non_agg_metrics, source_fragment
                    )
                    num_known_type = var_subparts_known_type.shape[0]
                    if num_known_type > 1:
                        var_parts_metadata.loc[
                            system_id, metadata_part_subtype_name(var_name, source_type)
                        ] = True
                        var_subparts_known_type = var_subparts_known_type.sort_values('sensor_name')
                        var_subparts_known_type_names = var_subparts_known_type['sensor_name'].values
                        first_known_name = var_subparts_known_type_names[0]
                        common_prefix, common_suffix = common_prefix_and_suffix(
                            var_subparts_known_type_names, first_known_name
                        )
                        # add the partial names on there
                        for k in range(0, num_known_type):
                            kth_metric = var_subparts_known_type.iloc[k, :]
                            kth_sensor_name = kth_metric['sensor_name']
                            if (
                                kth_sensor_name.startswith(common_prefix)
                                and kth_sensor_name.endswith(common_suffix)
                            ):
                                kth_interior = kth_sensor_name.removeprefix(
                                    common_prefix
                                ).removesuffix(common_suffix)
                            else:
                                raise ValueError('Bad prefix or suffix!')
                            var_total_dict[system_id].append({
                                'metric_id': kth_metric['metric_id'],
                                'sensor_name': kth_sensor_name,
                                'common_name': kth_metric['common_name'],
                                'units': kth_metric['units'],
                                'calc_details': kth_metric['calc_details'],
                                'whole_or_part': 'part',
                                'source_type': source_type,
                                'index': kth_interior
                            })
                        # avoid repeats by dropping the used terms
                        relevant_rows_non_agg_metrics = relevant_rows_non_agg_metrics.drop(
                            index=var_subparts_known_type.index
                        )
                    elif num_known_type == 1:  # only one sub-part?  Something's wrong.
                        print(f'System {system_id} has only one {source_type}-type '
                              + f'subpart for {var_name}!')
                        print(var_subparts_known_type.iloc[0, :])
                        raise ValueError('Incorrect subpart description, presumably.')
                    # if 0 of known type, just pass on!
            else:  # no source_type, just start making prefixes and suffixes
                relevant_rows_non_agg_metrics = relevant_rows_non_agg_metrics.sort_values('sensor_name')
                relevant_rows_non_agg_names = relevant_rows_non_agg_metrics['sensor_name'].values
                first_name = relevant_rows_non_agg_names[0]
                (common_prefix, common_suffix) = common_prefix_and_suffix(
                    relevant_rows_non_agg_names, first_name
                )
                for k in range(0, num_subparts):
                    kth_metric = relevant_rows_non_agg_metrics.iloc[k, :]
                    kth_sensor_name = kth_metric['sensor_name']
                    if (
                        kth_sensor_name.startswith(common_prefix)
                        and kth_sensor_name.endswith(common_suffix)
                    ):
                        kth_interior = kth_sensor_name.removeprefix(common_prefix).removesuffix(common_suffix)
                    else:
                        raise ValueError('Bad prefix or suffix!')
                    var_total_dict[system_id].append({
                        'metric_id': kth_metric['metric_id'],
                        'sensor_name': kth_sensor_name,
                        'common_name': kth_metric['common_name'],
                        'units': kth_metric['units'],
                        'calc_details': kth_metric['calc_details'],
                        'whole_or_part': 'part',
                        'index': kth_interior
                        })
        elif num_subparts == 1:  # only one subpart?  Presumably a missing aggregate name
            print(f'System {system_id} has only one subpart for {var_name}!')
            print(relevant_rows_non_agg_metrics.iloc[0, :])
            raise ValueError('Incorrect subpart description, presumably.')
        # if 0 sub-parts, move to the next system_id
    var_total_metadata_df = pd.merge(left=var_aggs_metadata,
                                     right=var_parts_metadata,
                                     how='inner',
                                     left_index=True,
                                     right_index=True)
    return (var_total_dict, var_total_metadata_df)


# can and should adjust these naming functions
# in particular, if there are a lot of unknown-source-type systems,
# may want to omit source_type if unknown
def var_agg_name(var_name: str, source_type: str, has_subparts: bool, agg_type: str):
    '''Make the standardized variable name for the aggregate value.

    Parameters
    -----------
    var_name: str
        The name of the variable we are studying
    source_type: str or None
        If not None, the source_type of the variable
    has_subparts: bool
        Whether or not there are any subparts.
    agg_type: str, 'sum' or 'mean'
        How the sub-parts are combined.
        Only used if has_subparts = True
        For most variables, agg_type is 'sum',
        but for temperature, it is almost certainly averaged.
    '''
    agg_name = var_name
    if source_type is not None:  # and source_type != 'unknown' ??
        agg_name = agg_name + '_' + source_type
    if has_subparts:
        agg_name = agg_name + '_' + agg_type
    return agg_name


def var_part_name(var_name: str, source_type: str, ind):
    '''Make the standardized variable name for the subpart.

    Parameters
    -----------
    var_name: str
        The name of the variable we are studying
    source_type: str or None
        If not None, the source_type of the variable
    ind: int or str
        The index of the term.  Can be an int or a string.
    '''
    if source_type is not None:
        return f'{var_name}_{source_type}_{ind}'
    else:
        return f'{var_name}_{ind}'


def var_dataframe_generator_mod(
    all_var_metrics,
    all_var_metadata,
    system_id: int,
    tall_or_wide: str,
    error_on_no_data: bool,
    order_priority: str,
    agg_type: str,
    add_aggs: bool,
    var_name: str,
    sources_matter: bool,
    known_sources=('inverter', 'meter'),
    known_sources_short=('inv', 'met'),
    filtered_metrics_df=None,
    agg_var_sensor_names=None,
    systems_cleaned=None
):
    '''Make the (tall or wide) pandas DataFrame with all variable data
    from a Parquet system.

    Parameters
    ----------
    all_var_metrics: dict[list[dict]] or None
        The first output from find_all_variable_names_gen(*args)
        or find_all_variable_names_gen_mod(*args)
        If None, builds up the automated Step 3.
    all_var_metadata: pandas.DataFrame or None
        The second output from find_all_variable_names_gen(*args)
        or find_all_variable_names_gen_mod(*args)
        If None, builds up the automated Step 3.
    system_id: int
        Index of system in systems_cleaned and metric_df
    tall_or_wide: str
        If 'wide', return wide Dataframe
        if 'tall', convert back to a 3-column array.
    error_in_no_data: bool
        If True, return an error if the system_id has no power-factor data.
        If False, return None if the system-system_id has no power factor data.
    order_priority: str, "whole_before_part" or "connect_like_terms"
        If "whole_before_part", puts all aggregate figures before all subdata_figures
        If "connect_like_terms", lists inverter aggregate, then inverter parts,
            then meter aggregate, then meter parts, then unknown together, then unknown parts.
            Has no effect if sources_matter = False
    agg_type: str, 'sum' or 'mean'
        How are the sub-parts being combined?
        For most variables, you just sum across sub-units,
        But temperature, and possibly power_factor, should be averaged.
    add_aggs: bool
        If True, and there are parts without a corresponding aggregate,
            add the aggregate, according to agg_type.
        If False, do nothing.
    var_name: str
        The variable name you are searching for.
    sources_matter: bool
        If True, collect data about where the data is collected (meter, inverter, etc.),
            and also stratify data by source.
        If False, do not collect such data.
    known_sources: iterable of strings
        An iterable of known sources.
    known_sources_short: iterable of strings
        An iterable of shorthands.  Must be the same length as known_sources.
    filtered_metrics_df: None or pandas.DataFrame
        Not needed unless all_var_metrics or all_var_metadata is None
        The valid, filtered set of potentially relevant metrics
        to pass to find_all_variable_names_gen_mod()
    agg_var_sensor_names: None or pandas.DataFrame
        Not needed unless all_var_metrics or all_var_metadata is None
        The list of aggregate variable sensor names
    systems_cleaned: None or pandas.DataFrame
        Not needed unless all_var_metrics or all_var_metadata is None.
        Get the systems_cleaned passed in to check for valid IDs.


    Returns
    ---------
    our_df: pd.DataFrame
        A pandas DataFrame object with the desired data.
    renamer_dict: dict
        The re-naming dictionary, just to be able to error-trace if there is some problem.
    '''
    # sanitize inputs
    if all_var_metrics is None or all_var_metadata is None:
        (all_var_metrics, all_var_metadata) = find_all_variable_names_gen_mod(
            var_aggs_dict=None,
            var_aggs_metadata=None,
            filtered_metrics_df=filtered_metrics_df,
            agg_var_sensor_names=agg_var_sensor_names,
            sources_matter=sources_matter,
            known_sources=known_sources,
            known_sources_short=known_sources_short,
            systems_cleaned=systems_cleaned
        )
    if sources_matter:
        (known_sources, known_sources_short) = sources_checker(
            known_sources=known_sources,
            known_sources_short=known_sources_short
        )
    # grab current system_id
    try:
        my_var_metrics = all_var_metrics[system_id]
        my_metadata = all_var_metadata.loc[system_id, :]
    except KeyError:
        if error_on_no_data:
            raise ValueError(f'System {system_id} has no {var_name} data!')
        else:
            return None
    except BaseException as e:
        raise e
    # some quick reads of the data
    metric_ids = []
    whole_metric_ids = []
    if sources_matter:
        source_type_metric_ids = {
            source_type: [] for source_type in known_sources
        }
    # grab all metric ids, putting the 'whole' category first
    for metric_data_dict in my_var_metrics:
        # whole-part distribution
        if metric_data_dict['whole_or_part'] == 'whole':
            metric_ids.insert(0, metric_data_dict['metric_id'])
            whole_metric_ids.append(metric_data_dict['metric_id'])
        elif metric_data_dict['whole_or_part'] == 'part':
            metric_ids.append(metric_data_dict['metric_id'])
        else:
            raise ValueError('The "whole_or_part" result of find_all_variable_names_gen()\n'
                             f'is not correct for system {system_id}.')
        # get source-type metric updated.
        if sources_matter:
            source_type_metric_ids[metric_data_dict['source_type']].append(
                metric_data_dict['metric_id']
            )
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
    # measured_on, utc_measured_on, metric_id, value
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
    # Some systems have part-data and not aggregate data;
    # amend this mistake.
    if add_aggs:
        if sources_matter:
            for source_type in known_sources:
                if (my_metadata[metadata_part_subtype_name(var_name, source_type)])\
                  and (not my_metadata[metadata_agg_subtype_name(var_name, source_type)]):
                    source_type_total_name = var_agg_name(
                        var_name, source_type, True, agg_type
                    )
                    if agg_type == 'sum':
                        wide_df.loc[:, source_type_total_name] = wide_df.apply(
                            lambda row: np.sum(
                                [row[j] for j in source_type_metric_ids[source_type]]
                            ), axis=1
                        )
                    elif agg_type == 'mean':
                        wide_df.loc[:, source_type_total_name] = wide_df.apply(
                            lambda row: np.mean(
                                [row[j] for j in source_type_metric_ids[source_type]]
                            ), axis=1
                        )
                    else:
                        raise ValueError(f'Bad agg_type = {agg_type}.'
                                         + ' Should be "sum" or "mean".')
                    whole_metric_ids.append(source_type_total_name)
                    source_type_metric_ids[source_type].append(
                        source_type_total_name
                    )
        elif (
            my_metadata[metadata_part_name(var_name)]
            and (not my_metadata[metadata_agg_name(var_name)])
        ):
            total_name = var_agg_name(var_name, None, True, agg_type)
            if agg_type == 'sum':
                wide_df.loc[:, total_name] = wide_df.apply(
                    lambda row: np.sum(
                        [row[j] for j in wide_df.columns[1:]]
                    ), axis=1
                )
            elif agg_type == 'mean':
                wide_df.loc[:, total_name] = wide_df.apply(
                    lambda row: np.mean(
                        [row[j] for j in wide_df.columns[1:]]
                    ), axis=1
                )
            else:
                raise ValueError(f'Bad agg_type = {agg_type}.'
                                 + ' Should be sum or mean.')
            whole_metric_ids.append(total_name)
    # reorder columns according to order_priority
    if order_priority == 'whole_before_part':
        # push the 'whole' columns to the beginning of the pack
        # despite re-ordering earlier, can still be loaded in the wrong order.
        reordered_columns = ['measured_on'] + whole_metric_ids + (wide_df.columns.drop(
            ['measured_on'] + whole_metric_ids
        ).tolist())
        wide_df = wide_df[reordered_columns]
    elif (order_priority == 'connect_like_terms') and sources_matter:
        reordered_columns = ['measured_on',]
        for source_type in known_sources:
            cols_known_type = []
            for j in source_type_metric_ids[source_type]:
                if j in whole_metric_ids:
                    cols_known_type.insert(0, j)
                else:
                    cols_known_type.append(j)
            reordered_columns = reordered_columns + cols_known_type
        wide_df = wide_df[reordered_columns]
    # rename columns
    renamer_dict = dict()
    if sources_matter:
        for metric_data_dict in my_var_metrics:
            source_type = metric_data_dict['source_type']
            if metric_data_dict['whole_or_part'] == 'whole':
                renamer_dict[metric_data_dict['metric_id']] = var_agg_name(
                    var_name,
                    source_type,
                    my_metadata[
                        metadata_agg_subtype_name(var_name, source_type)
                    ],
                    agg_type
                )
            elif metric_data_dict['whole_or_part'] == 'part':
                renamer_dict[metric_data_dict['metric_id']] = var_part_name(
                    var_name,
                    source_type,
                    metric_data_dict['index']
                )
            else:
                raise ValueError('The "whole_or_part" result of '
                                 + 'find_all_variable_names_gen()\n'
                                 f'is not correct for system {system_id}.')
    else:
        for metric_data_dict in my_var_metrics:
            if metric_data_dict['whole_or_part'] == 'whole':
                renamer_dict[metric_data_dict['metric_id']] = var_agg_name(
                    var_name,
                    None,
                    my_metadata[metadata_part_name(var_name)],
                    agg_type
                )
            elif metric_data_dict['whole_or_part'] == 'part':
                renamer_dict[metric_data_dict['metric_id']] = var_part_name(
                    var_name,
                    None,
                    metric_data_dict['index']
                )
            else:
                raise ValueError('The "whole_or_part" result of '
                                 + 'find_all_variable_names_gen()\n'
                                 f'is not correct for system {system_id}.')
    wide_df = wide_df.rename(columns=renamer_dict)
    # convert back to tall format if that is what we wanted
    if tall_or_wide == 'wide':
        our_df = wide_df
    elif tall_or_wide == 'tall':
        our_df = wide_df.melt(
            id_vars='measured_on',
            value_vars=wide_df.columns[1:],
            var_name='metric_name',
            value_name='value'
        )
    else:
        raise ValueError('The term "tall_or_wide" must be "tall" or "wide.\n'
                         + f'Recieved {tall_or_wide}')
    return (our_df, renamer_dict)
