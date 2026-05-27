#!/usr/bin/env python
# coding: utf-8
'''Grabbing data from '''
# Getting Power codes
# This is very specific and not uniform across entries,
# so we will download specific files.
import boto3
from botocore.handlers import disable_signing
from all_parquet_downloader import downloader_mk_2

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")


def online_prefix_starter(system_name):
    return "pvdaq/2023-solar-data-prize/"\
        + f"{system_name}_OEDI/data/"\
        + f"{system_name}_"


def local_file_dir(system_id):
    return f'../../../data_ds_project/systems/prize/{system_id}/'


def log_path_starter(system_id):
    return f'../../logs/logs_system_id={system_id}.csv'


def docs_description(system_id):
    return f'Solar-Prize Data for System {system_id}'


systems_shortlist = [2105, 2107, 7333, 9068, 9069]
systems_namelist = ['2105', '2107', '7333_5_min', '9068', '9069']
add_logs = False  # or True!  Adjust as needed.

# System 2105

downloader_mk_2(
    local_file_dir(2105),
    online_prefix_starter('2105') + 'inv',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(2105),
    data_directory_description=docs_description(2105)
)

downloader_mk_2(
    local_file_dir(2105),
    online_prefix_starter('2105') + 'meter',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(2105),
    data_directory_description=docs_description(2105)
)

# System 2107

downloader_mk_2(
    local_file_dir(2107),
    online_prefix_starter('2107') + 'electrical_data_v1',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(2107),
    data_directory_description=docs_description(2107)
)


downloader_mk_2(
    local_file_dir(2107),
    online_prefix_starter('2107') + 'meter_15m_data.csv',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(2107),
    data_directory_description=docs_description(2107)
)

# system 7333

downloader_mk_2(
    local_file_dir(7333),
    online_prefix_starter('7333_5_min') + 'ac',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(7333),
    data_directory_description=docs_description(7333)
)

downloader_mk_2(
    local_file_dir(7333),
    online_prefix_starter('7333_5_min') + 'powerfactor',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(7333),
    data_directory_description=docs_description(7333)
)

# System 9068

downloader_mk_2(
    local_file_dir(9068),
    online_prefix_starter('9068') + 'ac',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(9068),
    data_directory_description=docs_description(9068)
)

downloader_mk_2(
    local_file_dir(9068),
    online_prefix_starter('9068') + 'meter',
    warn_empty=True,
    make_logs=add_logs,
    log_path=log_path_starter(9068),
    data_directory_description=docs_description(9068)
)

# System 9069 Struck for incomprehensibility.
