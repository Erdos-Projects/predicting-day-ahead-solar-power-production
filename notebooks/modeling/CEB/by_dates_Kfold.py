import numpy as np
import pandas as pd
from datetime import date, timedelta


def k_fold_split(df: pd.DataFrame, good_ends, no_overlap: bool, n_splits: int):
    '''Make k-fold split of days, ensuring decent chunk in good_ends.'''
    if 'date' not in df.columns:
        date_before = False
        df.loc[:, 'date'] = df['time'].dt.date
    else:
        date_before = True
    # make sure good_days and good_ends are localized to the current dataset
    # (possibly training/testing mismatch, etc.)
    df_last_date = df['date'].max()
    if not isinstance(good_ends, pd.Series):
        good_ends = pd.Series(good_ends, name='good_ends')
    good_ends = good_ends[good_ends  <= df_last_date]
    if good_ends is None or len(good_ends) < n_splits:
        raise ValueError('Data has fewer good ends than the number of splits.')
    # draft values
    ideal_len_per_fold = len(good_ends) // n_splits
    remainder = len(good_ends) % n_splits
    splits_list = []
    current_start = 0
    for split_count in range(n_splits):
        len_split = ideal_len_per_fold + int(split_count < remainder)
        testing_units = []
        for j in range(len_split):
            current_end = good_ends.at[current_start + j]
            if no_overlap:
                if current_start + j > 0:  # not very first one
                    prior_end = good_ends.at[current_start + j - 1]
                    df_from_one_day_before = df[
                        (df['date'] >= prior_end)
                        & (df['date'] < (current_end - timedelta(days=1)))
                    ]
                else:
                    df_from_one_day_before = df[df['date'] < (current_end - timedelta(days=1))]
            else:
                df_from_one_day_before = df[df['date'] < (current_end - timedelta(days=1))]
            df_date = df[df['date'] == current_end]
            if not date_before:
                df_date.drop(columns=['date',])
                df_from_one_day_before.drop(columns=['date',])
            testing_units.append((df_from_one_day_before, df_date))
        splits_list.append(testing_units)
        current_start += len_split
    return splits_list
        