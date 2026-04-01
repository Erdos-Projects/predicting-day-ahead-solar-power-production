'''Clean up the metadata a little bit,
and do some preliminary checks as to which data
has the most information.'''

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import boto3
from botocore.handlers import disable_signing
import datetime
import json

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")

metric_names_and_fragments = {
    'irradiance': 'irrad',
    'ambient_temperature': 'mbient',
    'temperature': 'temp',
    'power': 'pow',
    'current': 'curr',
    'voltage': 'volt',
    'ac': 'ac',
    'dc': 'dc'
}


def metric_col_name(metric_name: str):
    return f'has_{metric_name}_data'


def metrics_search_for_fragment_df(df: pd.DataFrame, fragment: str):
    fragment = fragment.lower()
    return df[
        (df.loc[:, 'sensor_name'].str.contains(fragment, case=False))
        | (df.loc[:, 'common_name'].str.contains(fragment, case=False))
    ]


def metrics_search_for_fragment_dict(the_dict, key, fragment: str):
    fragment = fragment.lower()
    if fragment in key.lower():
        return True
    # go to the next level
    this_metric = the_dict[key]
    this_metric_sensor = this_metric['sensor_name'].lower()
    this_metric_common = this_metric['common_name'].lower()
    if fragment in this_metric_sensor or fragment in this_metric_common:
        return True
    else:
        return False


if __name__ == '__main__':
    # load sources
    systems_cleaned = pd.read_csv('../../data/raw/systems_20250729.csv')
    # drop some empty unnamed columns [coming from extra commas in the csv]
    unnamed_columns = [
        col_name for col_name in systems_cleaned.columns.array
        if "Unnamed:" in col_name
    ]
    systems_cleaned = systems_cleaned.drop(
        columns=unnamed_columns
    )
    # put starting/ending dates as datetime type
    systems_cleaned['first_timestamp'] = pd.to_datetime(
        systems_cleaned['first_timestamp'], format='%m/%d/%Y %H:%M'
    ).astype('datetime64[s]')
    systems_cleaned['last_timestamp'] = pd.to_datetime(
        systems_cleaned['last_timestamp'], format='%m/%d/%Y %H:%M'
    ).astype('datetime64[s]')
    systems_cleaned.loc[:, 'first_year']\
        = systems_cleaned['first_timestamp'].dt.year
    num_sources = systems_cleaned.shape[0]
    systems_cleaned.loc[:, 'is_prize_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_parquet_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    systems_cleaned.loc[:, 'is_lake_csv_data']\
        = pd.Series([False]*num_sources, dtype='boolean')
    for metric in metric_names_and_fragments.keys():
        col_name = metric_col_name(metric)
        systems_cleaned[col_name]\
            = pd.Series([False]*num_sources, dtype='boolean')
    systems_id_set = set(systems_cleaned['system_id'].unique())
    print("Proceeding to load metadata from prize data.")
    # by manual inspection, there are 5 sites in the prize data,
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        # load the metadata
        metadata_filepath = Path(
                '../../data/raw/prize-metadata/'
                + f'{system_id}_system_metadata.json'
            )
        with open(metadata_filepath) as json_reader:
            local_metadata = json.load(json_reader)
            system_metrics = local_metadata['Metrics']
            # for reasons to get into later, we override the 'first_year'
            first_timestamp = local_metadata['System']['first_timestamp']
            first_year = datetime.datetime.strptime(
                first_timestamp, "%Y-%m-%d %H:%M:%S"
            ).year

            # assign the data to the chart.
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_prize_data'] = True
                systems_cleaned.loc[ind, 'first_year'] = first_year
                # check for metrics
                for metric, fragment in metric_names_and_fragments.items():
                    for key in system_metrics.keys():
                        if metrics_search_for_fragment_dict(
                            system_metrics, key, fragment
                        ):
                            systems_cleaned.loc[
                                ind, f'has_{metric}_data'
                            ] = True
                            break

    print("Proceeding to load metadata from parquet data.")
    metrics_dir = Path("../../data/raw/parquet-metrics/")
    metrics_pq = pq.ParquetDataset(metrics_dir)
    metrics_df = metrics_pq.read().to_pandas()
    parquet_metrics_set = set(metrics_df['system_id'].unique())
    systems_dir = Path('../../data/raw/parquet-systems/')
    systems_pq = pq.ParquetDataset(systems_dir)
    systems_df = systems_pq.read().to_pandas()
    parquet_systems_set = set(systems_df['system_id'].unique())
    # At first, I was worried, because there were 4 items in
    # parquet_metrics_set that were not in systems_id_set.
    # We now demonstrate that it is pointless to worry.
    # First, we only allow metrics data
    # that also has system data, or else it is just too
    # incomplete.
    parquet_full_data_set = parquet_metrics_set.intersection(
        parquet_systems_set
    )
    parquet_not_original_list = list(
        parquet_full_data_set.difference(systems_id_set)
    )
    if (len(parquet_not_original_list) != 1) or (
        int(parquet_not_original_list[0]) != 2045
    ):
        raise RuntimeError('Additional terms in set difference!')
    # Assuming no trouble, the only system_id remaining is 2045.
    # From actually reading the parquet-systems file, system 2045
    # is an irradiance-measuring tool disconnected from any solar cells.
    # Also, it is in Golden, CO where many other solar facilities are.
    # So, it can be ignored!

    for system_id in parquet_metrics_set.intersection(systems_id_set):
        # first, can definitely flag the 'is_lake_parquet_data' flag
        relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in relevant_rows.index:
            systems_cleaned.loc[ind, 'is_lake_parquet_data'] = True
            # Correcting first year: it was an unpleasant surprise to learn
            # that the first year from systems_20250729.csv
            # was calculated incorrectly for some parquet-data systems
            # (see systems 1283, 1284, 1289)
            # Our simple strategy is to start with the hinted year
            # and increment until we actually have a good starting year.
            first_ind = relevant_rows.index[0]
            first_year = int(relevant_rows.loc[first_ind, 'first_year'])
            good_first_year = False
            while not good_first_year:
                prefix = "pvdaq/parquet/pvdata/"\
                    + f"system_id={system_id}/year={first_year}"
                # recall the s3 Bucket object, bucket
                # our access point to the data set.
                objects = bucket.objects.filter(
                    Prefix=prefix
                )
                if (objects is None) or (len(list(objects)) == 0):
                    first_year += 1
                    if first_year >= 2024:
                        print(system_id)
                        print('Breaking to avoid infinite loop, '
                              + 'but not enough data.')
                        good_first_year = True
                else:
                    good_first_year = True
            # correct the first year
            relevant_rows.loc[first_ind, 'first_year'] = first_year

    # now fill in metrics search
    for metric, fragment in metric_names_and_fragments.items():
        col_name = metric_col_name(metric)
        collect_my_metric = metrics_search_for_fragment_df(
            metrics_df, fragment
        )
        good_metric_ids = set(collect_my_metric['system_id'].unique())
        for system_id in good_metric_ids.intersection(systems_id_set):
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, col_name] = True

    print("Proceeding to load metadata from csv data.")
    csv_metadata_dir = Path('../../data/raw/csv-metadata/')
    # now grab the json files, infer the system_id, and
    # check for metadata
    jsons = csv_metadata_dir.glob("*_system_metadata.json")
    for file_path in jsons:
        system_id = int(
            file_path.parts[-1].replace('_system_metadata.json', '')
        )
        with open(file_path) as reader:
            local_metadata = json.load(reader)
            has_metrics = True
            try:
                system_metrics = local_metadata['Metrics']
            except KeyError:
                has_metrics = False
            except BaseException as e:
                raise e
            # we again override the 'first_year' data
            first_timestamp = local_metadata['System']['started_on']
            first_year = datetime.datetime.strptime(
                first_timestamp, "%Y-%m-%d %H:%M:%S"
            ).year
            # assign the data to the chart.
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'is_lake_csv_data'] = True
                systems_cleaned.loc[ind, 'first_year'] = first_year
                if has_metrics:
                    for metric, fragment in metric_names_and_fragments.items():
                        for key in system_metrics.keys():
                            if metrics_search_for_fragment_dict(
                                system_metrics, key, fragment
                            ):
                                col_name = metric_col_name(metric)
                                systems_cleaned.loc[ind, col_name] = True
                                break
                else:
                    # by observation, standard outputs have ac_power
                    # and ac_energy as daily averages,
                    # and nothing else
                    # but parquet systems are echoed in csv,
                    # and we do not wish to invent data there.
                    if not systems_cleaned.loc[ind, 'is_lake_parquet_data']:
                        systems_cleaned.loc[ind, metric_col_name('ac')] = True
                        systems_cleaned.loc[
                            ind, metric_col_name('power')
                        ] = True
    # by prior exploration, there are 3 sites with no data.
    # let us go ahead and remove them
    systems_cleaned_no_data = systems_cleaned[
        (~systems_cleaned['is_prize_data'])
        & (~systems_cleaned['is_lake_parquet_data'])
        & (~systems_cleaned['is_lake_csv_data'])
    ]
    no_data_systems = set(systems_cleaned_no_data['system_id'])
    assert (no_data_systems == {7334, 12423, 12494})
    # Systems 12423 and 12494 are acknowledged by the metadata
    # to have no first_timestamp or last_timestamp
    # System 7334 may be a duplicate of System 7333 with a
    # different sampling time.
    for system_id in no_data_systems:
        relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        systems_cleaned = systems_cleaned.drop(index=relevant_rows.index)
    # finally, save the data!
    permanent_systems_cleaned_path = Path(
        '../../data/core/systems_cleaned.csv'
    )
    systems_cleaned.to_csv(permanent_systems_cleaned_path,
                           index=False)
