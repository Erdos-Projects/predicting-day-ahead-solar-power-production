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


def k_fold_split_option_a(df_train: pd.DataFrame, good_ends, n_splits: int, window_size, return_type):
    '''Make k-fold split of days.
    If padded by 0's, and not too much missingness,
    comparable to TimeSeriesSplit(n_splits=n_splits, test_length=24, gap=24).
    
    Parameters
    ------------
    df_train: pd.Dataframe
        training data
    good_ends: pd.DataFrame or pd.Series (with 'date' column)
        Series or DataFrame with good ending-days of streaks of desired length.
    n_splits: int
        Number of splits you want.  Defaults to the last few valid splits.
    window_size: int or None
        If int, only want window_size days of data (assuming it exist)
        Warning: if you set streak = 7 in PreRun, window_size is only guaranteed to be 5. (because of the skipped day).
        If None, take all earlier data.
    return_type: str
        If return_type == 'index', return indices for the DataFrames test and split
        (If data padded with 0's, this is more-or-less equivalent to TimeSeriesSplit(gap=24),)
        If return_type == 'DataFrame', return the DataFrame itself
        *Warning*: return_type == 'index' is required to plug into GridSearchCV and similar!
    
    Returns:
    splits_list:  list[tuple[list, list]] | list[tuple[pandas.DataFrame, pandas.DataFrame]]
        If return_type == 'index', just a list, where each list is a tuple (list of training indices, list of validation indices)
        If return_type == 'DataFrame', each list is a DataFrame (list of training indices, list of validation indices)
    '''
    if 'date' not in df_train.columns:
        date_before = False
        df_train.loc[:, 'date'] = df_train['time'].dt.date
    else:
        date_before = True
    # make sure good_ends is localized to the current dataset
    # (possibly training/testing mismatch, etc.)
    df_first_date = df_train['date'].min()
    df_last_date = df_train['date'].max()
    if not isinstance(good_ends, pd.Series):
        good_ends = pd.Series(good_ends, name='good_ends')
    good_ends = good_ends[good_ends  <= df_last_date]
    # if window_size is None, make sure we have a decent chunk of days to work with
    if window_size is None:
        good_ends = good_ends[good_ends >= df_first_date + timedelta(days=50)]
    # if not enough splits, give a warning.
    if good_ends is None or len(good_ends) < n_splits:
        raise ValueError('Data has fewer good ends than the number of splits.')
    splits_list = []
    current_start = 0
    for _ in range(n_splits):
        current_end = good_ends.at[current_start]
        df_date = df_train[df_train['date'] == current_end]
        if window_size is not None:
            df_from_one_day_before = df_train[
                (df_train['date'] >= current_end - timedelta(days = 1 + window_size))
                & (df_train['date'] < (current_end - timedelta(days=1)))
            ]
        else:
            df_from_one_day_before = df_train[df_train['date'] < (current_end - timedelta(days=1))]
        if not date_before:
            df_date.drop(columns=['date',])
            df_from_one_day_before.drop(columns=['date',])
        if return_type == 'index':
            splits_list.append((df_from_one_day_before.index, df_date.index))
        elif return_type == 'DataFrame':
            splits_list.append((df_from_one_day_before.index, df_date.index))
    return splits_list
    
