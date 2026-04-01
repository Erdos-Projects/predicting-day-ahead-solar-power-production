'''Download all data from all
parquet-stored systems.'''
import pandas as pd
import boto3
from botocore.handlers import disable_signing
import time
import os
from pathlib import Path
import concurrent.futures

# choices -- choose here
# add logs?
add_logs = True
# 156 parquet systems in general [temporarily indexed 0-155]
# either run it for a very long time,
# or download in batches to fit your schedule/availability
# Will not download a file twice,
# so can re-run with full range to double-check
i_start = 0
i_end = 155

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
parquet_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'is_lake_parquet_data']
]  # is already boolean!
all_parquet_system_ids = list(parquet_systems.system_id.unique())
all_parquet_system_ids.sort()


def downloader_mk_2(
    path_to_dir_local: str, path_to_dir_online: str,
    warn_empty=False, is_specific_file_type=False,
    specific_file_type='',
    make_logs=False,
    log_path='../../logs/logs.csv',
    data_directory_description='',
    s3_bucket=None
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
    downloads_list = []
    # allow using a provided s3 Bucket (useful for per-thread buckets)
    bucket_to_use = s3_bucket if s3_bucket is not None else bucket
    if path_to_dir_local[-1] != '/' and path_to_dir_local[-1] != '\\':
        raise ValueError('Local path does not end in "/" or "\\",'
                         + ' and hence is not a possible directory!')
    my_local_dir = Path(path_to_dir_local)
    if not my_local_dir.is_dir():
        my_local_dir.mkdir(parents=True)
    objects = bucket_to_use.objects.filter(
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
                    bucket_to_use.download_file(
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


def download_index_set(j_start, j_end):
    '''Download from the j_start position on the list
    to the j_end position on the list'''
    def _download_system(j):
        print(f'[Worker {j}] Starting index j={j}')
        system_id = all_parquet_system_ids[j]
        print(f'[Worker {j}] system_id={system_id}')
        st = time.time()
        # create a per-thread s3 Bucket/resource to avoid any shared-state issues
        s3_local = boto3.resource("s3")
        s3_local.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
        bucket_local = s3_local.Bucket("oedi-data-lake")
        try:
            downloader_mk_2(
                f'../../../data_ds_project/systems/parquet/{system_id}/',
                f'pvdaq/parquet/pvdata/system_id={system_id}/',
                warn_empty=True,
                make_logs=add_logs,
                log_path=f'../../logs/logs_system_id={system_id}.csv',
                data_directory_description=f'Parquet Data for System {system_id}',
                s3_bucket=bucket_local
            )
        except Exception:
            raise
        et = time.time()
        duration = (et-st)/60
        print(f'[Worker {j}] Finished system_id {system_id} in {duration:.4f} minutes.')
        # if significant duration, space out download calls (per-thread sleep)
        if duration > 1.5:
            time.sleep(10)
        return j, system_id, duration

    total = j_end - j_start + 1
    workers = min(max_workers, total)
    print(f'[Main] Starting parallel downloads: {total} systems with {workers} workers')
    completed_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_download_system, j): j for j in range(j_start, j_end+1)}
        for fut in concurrent.futures.as_completed(futures):
            j = futures[fut]
            try:
                j_val, system_id, duration = fut.result()
                completed_count += 1
                print(f'[Main] Completed {completed_count}/{total}: index {j_val}, system_id {system_id}')
            except Exception as e:
                print(f'[Main] Error downloading index {j}: {e}')
    print(f'[Main] All downloads complete. Total: {completed_count}/{total} systems')


if __name__ == '__main__':
    print(len(all_parquet_system_ids))
    download_index_set(i_start, i_end)
