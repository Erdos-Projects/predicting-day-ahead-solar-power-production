'''Add information about solar cell types
to our matrix.
Assumes that systems_initializer.py
has already been run.'''

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True


def simplified_classifier(row):
    cell_type = row['module_type']
    cell_type = cell_type.lower()
    if ('mono' in cell_type):
        return 'monocrystalline_Si'
    elif ('poly' in cell_type or 'multi' in cell_type):
        return 'multicrystalline_Si'
    elif ('perc' in cell_type or 'pert' in cell_type):
        return 'some_crystalline_Si_with_passivation'
    elif ('cigs' in cell_type or 'cdte' in cell_type
          or 'thin' in cell_type or 'amorphous' in cell_type):
        return 'thin_film'
    elif ('hit' in cell_type or 'sjt' in cell_type
          or 'shj' in cell_type or 'topcon' in cell_type):
        return 'heterojunction'
    elif ('unknown' in cell_type):
        return 'unknown'
    # note: 3 systems in systems_cleaned.csv have no data,
    # so I cannot just error out with no data.
    else:
        return 'No data available'


if __name__ == '__main__':
    permanent_systems_cleaned_path = Path(
        '../../data/core/systems_cleaned.csv'
    )
    systems_cleaned = pd.read_csv(permanent_systems_cleaned_path)
    numrows = systems_cleaned.shape[0]
    systems_cleaned['module_type'] = pd.Series(
        ['']*numrows, name='module_type', dtype='str'
    )
    print('Adding module type for prize systems')
    prize_system_ids = [2105, 2107, 7333, 9068, 9069]
    for system_id in prize_system_ids:
        # load the metadata
        metadata_filepath = Path(
                '../../data/raw/prize-metadata/'
                + f'{system_id}_system_metadata.json'
            )
        with open(metadata_filepath) as json_reader:
            local_metadata = json.load(json_reader)
            system_modules = local_metadata['Modules']
            # by testing in modules_explorer.ipynb,
            # each system has only one solar-panel type attached.
            for e, key in enumerate(system_modules.keys()):
                if e == 0:
                    module_type = system_modules[key]['type']
            # write the result to each row
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'module_type'] = module_type
    print('Reading modules data for parquet sites.')
    modules_dir = Path('../../data/raw/parquet-modules/')
    modules_pq = pq.ParquetDataset(modules_dir)
    modules_df = modules_pq.read().to_pandas()
    modules_df.head()
    for system_id in modules_df['system_id'].unique():
        # find the type for each system.
        # Again, by exploration in modules_explorer.ipynb,
        # while some systems have multiple rows,
        # no system has multiple types
        local_view = modules_df[modules_df['system_id'] == system_id]
        for ind in local_view.index[0:1]:
            module_type = local_view.loc[ind, 'type']
        # write the result to each row
        relevant_rows = systems_cleaned.loc[
            systems_cleaned.loc[:, 'system_id'] == system_id
        ]
        for ind in relevant_rows.index:
            systems_cleaned.loc[ind, 'module_type'] = module_type
    print('Reading modules data for csv sites.')
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
            system_modules = local_metadata['Modules']
            # again, one module type per system.
            for e, key in enumerate(system_modules.keys()):
                if e == 0:
                    module_type = system_modules[key]['type']
            # write the result
            relevant_rows = systems_cleaned.loc[
                systems_cleaned.loc[:, 'system_id'] == system_id
            ]
            for ind in relevant_rows.index:
                systems_cleaned.loc[ind, 'module_type'] = module_type
    # add the simpified type
    systems_cleaned['simplified_type'] = systems_cleaned.apply(
        simplified_classifier, axis=1
    )
    # save and quit!
    systems_cleaned.to_csv(permanent_systems_cleaned_path,
                           index=False)
