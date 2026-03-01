'''Download the main metadata source.'''

from pathlib import Path
import os
import boto3
from botocore.handlers import disable_signing
import time

# single choice
add_logs = False

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")


def downloader_mk_2(
    path_to_dir_local: str, path_to_dir_online: str,
    warn_empty=False, is_specific_file_type=False,
    specific_file_type='',
    make_logs=False,
    log_path='../../logs/logs.csv',
    data_directory_description=''
):
    '''Download a file or collection of files from the
    OEDI PVDAQ Data Lake.
    More granular control than the pvdaq_access package,
    and included some logging.

    Parameters
    ------------
    path_to_dir_local: str
        A string representing the desired path to the file storage
        on the local system.  Must be a valid directory [end in / or \\]
    path_to_dir_online: str
        A string directing the prefix of the filenames of the files
        to access online.
        Despite the name, this does not need to be a directory.
    warn_empty: bool
        Print if there are no items to download.
    is_specific_file_type: bool
        Print if you want to restrict to a particular file_type
    specific_file_type: str
        The specific file type you want.
    make_logs: bool
        Deciding whether or not to make logs.
    log_path: str
        The path to the log file you want.
    data_directory_description: str
        The describing text you want in data_inventory.csv
    '''
    global bucket
    downloads_list = []
    if path_to_dir_local[-1] != '/' and path_to_dir_local[-1] != '\\':
        raise ValueError('Local path does not end in "/" or "\\",'
                         + ' and hence is not a possible directory!')
    my_local_dir = Path(path_to_dir_local)
    if not my_local_dir.is_dir():
        my_local_dir.mkdir()
    objects = bucket.objects.filter(
        Prefix=path_to_dir_online
    )
    # check if no objects
    if len(list(objects)) == 0:
        if warn_empty:
            print('No such files!')
        return False
    else:
        for obj in objects:
            # horrible mix of os.path and pathlib.Path, but it works
            file_path = Path(
                os.path.join(
                    my_local_dir, os.path.basename(obj.key)
                )
            )
            # Sometimes filter messes up and just gives us a directory back.
            # if it's the same as the starting directory, ok,
            # harmless error, skip it
            # if it's not the same as the starting directory, flag it.
            if file_path.is_dir():
                if file_path != my_local_dir:
                    print(file_path)
                    raise ValueError('Somehow we got a new directory back!')
            elif not file_path.is_file():  # time to download!
                # check for file type if asked
                suffix_len = len(specific_file_type)
                type_valid = False
                if not is_specific_file_type:
                    type_valid = True
                elif f'{obj.key}'[-suffix_len:] == specific_file_type:
                    type_valid = True
                if type_valid:
                    download_time = time.time()
                    bucket.download_file(
                        obj.key, file_path
                    )
                    sources_download = {
                        "Filename": str(file_path),
                        "Source": str(obj.key),
                        "Access Time": download_time
                    }
                    downloads_list.append(sources_download)
        if (len(downloads_list) > 0) and make_logs:
            # save download logs
            log_path = Path(log_path)
            try:
                with open(log_path, mode='a') as log_adder:
                    log_adder.writelines(
                        [f'{inst["Filename"]},'
                         + f'{inst["Source"]},'
                         + f'{inst["Access Time"]}\n'
                         for inst in downloads_list]
                    )
            except FileNotFoundError:
                log_path.touch()
                with open(log_path, mode='w') as log_adder:
                    log_adder.writelines(
                        [f'{inst["Filename"]},'
                         + f'{inst["Source"]},'
                         + f'{inst["Access Time"]}\n'
                         for inst in downloads_list]
                    )
            except BaseException as e:
                raise e

            # append new notes to data_inventory.csv
            data_inventory_path = '../../data_inventory.csv'
            try:
                with open(data_inventory_path, 'a') as data_adder:
                    data_adder.writelines(
                        [f'{inst["Filename"]},'
                         + data_directory_description + '\n'
                         for inst in downloads_list]
                    )
            except FileNotFoundError:
                data_inventory_path.touch()
                with open(log_path, mode='w') as data_adder:
                    data_adder.writelines(
                        [f'Filename: {inst["Filename"]},'
                         + data_directory_description + '\n'
                         for inst in downloads_list]
                    )
            except BaseException as e:
                raise e
        return True


if __name__ == '__main__':
    # download the sources_file
    downloader_mk_2(
        '../../data/raw/',
        'pvdaq/csv/systems_20250729.csv',
        make_logs=add_logs,
        log_path='../../logs/log_systems_metadata.csv',
        data_directory_description="Metadata for all systems."
    )
    print("Proceeding to download data from prize data.")
    # by manual inspection, there are 5 sites in the prize data,
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        # download the metadata
        downloader_mk_2(
            '../../data/raw/prize-metadata/',
            f'pvdaq/2023-solar-data-prize/{system_id}_OEDI/metadata/',
            warn_empty=True,
            make_logs=add_logs,
            log_path=f'../../logs/{system_id}_prize_metadata.csv',
            data_directory_description="Metadata for "
            + f'site {system_id}, prize data'
        )
    # Note that 7333 is a really-fast-reporting location,
    # and has downsampled its data in a different folder.
    # We grab the metadata for reference
    downloader_mk_2(
        '../../data/raw/prize-metadata/',
        'pvdaq/2023-solar-data-prize/7333_5_min_OEDI/metadata/',
        warn_empty=True,
        make_logs=add_logs,
        log_path='../../logs/7333_5_min_metadata.csv',
        data_directory_description="Metadata for "
        + 'site 7333, downsampled, prize data'
    )

    print("Proceeding to download data from parquet data.")
    # We begin by downloading metadata.
    # late change -- need modules metadata for solar panel type.
    downloader_mk_2(
        "../../data/raw/parquet-modules/",
        "pvdaq/parquet/modules/",
        warn_empty=True,
        make_logs=add_logs,
        log_path='../../logs/parquet_modules_metadata.csv',
        data_directory_description="Metadata for "
        + 'parquet data systems -- solar panel module composition'
    )
    downloader_mk_2(
        "../../data/raw/parquet-metrics/",
        "pvdaq/parquet/metrics/",
        make_logs=add_logs,
        log_path='../../logs/parquet_metrics_metadata.csv',
        data_directory_description="Metadata for "
        + 'parquet data systems -- metrics list.'
    )
    downloader_mk_2(
        "../../data/raw/parquet-sites/",
        "pvdaq/parquet/site/",
        make_logs=add_logs,
        log_path='../../logs/parquet_sites_metadata.csv',
        data_directory_description="Metadata for "
        + 'parquet data systems -- site information'
    )
    downloader_mk_2(
        "../../data/raw/parquet-systems/",
        "pvdaq/parquet/system/",
        make_logs=add_logs,
        log_path='../../logs/parquet_systems_metadata.csv',
        data_directory_description="Metadata for "
        + 'parquet data -- system information'
    )

    print("Proceeding to download metadata from csv data.")
    downloader_mk_2(
        "../../data/raw/csv-metadata/",
        "pvdaq/csv/system_metadata/",
        warn_empty=False,
        is_specific_file_type=True,
        specific_file_type='.json',
        make_logs=add_logs,
        log_path='../../logs/csv_metadata.csv',
        data_directory_description="Metadata for "
        + 'csv-reported data'
    )
