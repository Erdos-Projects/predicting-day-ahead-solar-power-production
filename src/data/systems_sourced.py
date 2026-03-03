import pandas as pd
from pathlib import Path

# prepare for future pandas 3.0 usage
pd.options.mode.copy_on_write = True


def system_source(row):
    pub_name = row['system_public_name']
    prize_stat = row['is_prize_data']
    if prize_stat:
        return 'Prize'
    elif pub_name[0:12] == 'Pvoutput.org':
        return 'PV Output'
    elif pub_name[0:4] == 'PVDB':
        return 'PVDB'
    else:
        return 'PVDAQ General'


if __name__ == '__main__':
    # load data
    permanent_systems_cleaned_path = Path(
        '../../data/core/systems_cleaned.csv'
    )
    systems_cleaned = pd.read_csv(permanent_systems_cleaned_path)
    # add the new row
    systems_cleaned.loc[:, 'system_source'] = systems_cleaned.apply(
        system_source, axis=1
    ).astype('str')
    # save and quit!
    systems_cleaned.to_csv(permanent_systems_cleaned_path,
                           index=False)
