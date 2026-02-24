'''Download selections from the PVDAQ Data Lake,
2023-Solar-Data-Prize collection.'''

import boto3
from botocore.handlers import disable_signing
from systems_initializer import downloader

s3 = boto3.resource("s3")
s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
bucket = s3.Bucket("oedi-data-lake")


# choices -- configure per run

# found by direct inspection on website
systems_shortlist = [2105, 2107, 7333, 9068, 9069]
systems_namelist = ['2105', '2107', '7333_5_min', '9068', '9069']

for j in range(5):
    system_id = systems_shortlist[j]
    system_name = systems_namelist[j]
    local_file_dir = f'../../../data_ds_project/systems/prize/{system_id}/'
    file_prefix_e = "pvdaq/2023-solar-data-prize/"\
        + f"{system_name}_OEDI/data/"\
        + f"{system_name}_environment"
    file_prefix_i = "pvdaq/2023-solar-data-prize/"\
        + f"{system_name}_OEDI/data/"\
        + f"{system_name}_irradiance"
    downloader(
        local_file_dir,
        file_prefix_e,
        warn_empty=True,
        log_path=f'../../logs/logs_system_id={system_id}.csv',
        data_directory_description=f'Prize Data for System {system_id}'
    )
    downloader(
        local_file_dir,
        file_prefix_i,
        warn_empty=True,
        log_path=f'../../logs/logs_system_id={system_id}.csv',
        data_directory_description=f'Prize Data for System {system_id}'
    )
    # other data groups much more space-intensive, will adjust as necessary.
