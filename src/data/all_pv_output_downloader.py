'''Download all data from all
csv-stored systems from PV Output
(Minimal data for maximal success processing).'''
import pandas as pd
import boto3
from botocore.handlers import disable_signing
from tqdm import tqdm
import time
import concurrent.futures
from all_parquet_downloader import downloader_mk_2

# choices -- choose here
# add logs?
add_logs = True
# 1454 parquet systems in general [temporarily indexed 0-1453]
# either run it for a very long time,
# or download in batches to fit your schedule/availability
# Will not download a file twice,
# so can re-run with full range to double-check
i_start = 100
i_end = 1453
# Multitasking?
is_multitasking = False
max_workers = 20

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")

systems_cleaned = pd.read_csv('../../data/core/systems_cleaned.csv')
pv_output_systems = systems_cleaned.loc[
    systems_cleaned.loc[:, 'system_source'] == 'PV Output'
]
pv_output_system_ids = list(pv_output_systems.system_id.unique())


def download_index_set(j_start, j_end):
    '''Download from the j_start position on the list
    to the j_end position on the list'''
    def _download_system(j):
        print(f'[Worker {j}] Starting index j={j}')
        system_id = pv_output_system_ids[j]
        print(f'[Worker {j}] system_id={system_id}')
        st = time.time()
        # create a per-thread s3 Bucket/resource to avoid any shared-state issues
        s3_local = boto3.resource("s3")
        s3_local.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
        bucket_local = s3_local.Bucket("oedi-data-lake")
        try:
            downloader_mk_2(
                f'../../../data_ds_project/systems/csv/{system_id}/',
                f'pvdaq/csv/pvdata/system_id={system_id}/',
                warn_empty=True,
                make_logs=add_logs,
                log_path=f'../../logs/logs_system_id={system_id}.csv',
                data_directory_description=f'CSV Data for System {system_id}',
                s3_bucket=bucket_local
            )
        except BaseException:
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
    print(len(pv_output_system_ids))
    if is_multitasking:
        download_index_set(i_start, i_end)
    else:
        for i in tqdm(range(i_start, i_end + 1)):
            system_id = pv_output_system_ids[i]
            try:
                downloader_mk_2(
                    f'../../../data_ds_project/systems/csv/{system_id}/',
                    f'pvdaq/csv/pvdata/system_id={system_id}/',
                    warn_empty=True,
                    make_logs=add_logs,
                    log_path=f'../../logs/logs_system_id={system_id}.csv',
                    data_directory_description=f'CSV Data for System {system_id}',
                )
            except BaseException as e:
                raise e
            # some pauses
            if i % 100 == 99:
                time.sleep(30)
