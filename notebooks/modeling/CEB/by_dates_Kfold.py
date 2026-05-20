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
    if isinstance(good_ends, pd.DataFrame):
        good_ends = good_ends['date']
    elif isinstance(good_ends, list):
        good_ends = pd.Series(good_ends, name='date')
    # make sure that entries are datetime.date objects
    # rather than pd.Timestamp objects
    # to match the output of df_train['date'] above
    if isinstance(good_ends.iloc[0], pd.Timestamp):
        good_ends = good_ends.dt.date
    good_ends = good_ends[good_ends <= df_last_date]
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
                    df_from_one_day_before = df[
                        df['date'] < (current_end - timedelta(days=1))
                    ]
            else:
                df_from_one_day_before = df[
                    df['date'] < (current_end - timedelta(days=1))
                ]
            df_date = df[df['date'] == current_end]
            if not date_before:
                df_date.drop(columns=['date',])
                df_from_one_day_before.drop(columns=['date',])
            testing_units.append((df_from_one_day_before, df_date))
        splits_list.append(testing_units)
        current_start += len_split
    return splits_list


def k_fold_split_option_a(df_train: pd.DataFrame, good_ends,
                          n_splits, window_size,
                          front_or_back: str, gap_day: bool,
                          return_type: str,
                          sample_spacing: int = 1):
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
        If positive integer, n_splits is the number of splits you want.
        If n_splits = -1, take all (valid) end days as validation splits.
    window_size: int | None
        If int, only want window_size days of data (assuming it exist)
        Warning: if you set streak = 7 in PreRun,
        window_size is only guaranteed to be 5
        (because of the possibly-skipped day).
        If None, take all earlier data.
    gap_day: bool
        If gap_day, strict day inbetween training and validation
        If not gap_day, include data (for time-series when need to project 48 hours ahead.)
    front_or_back: str
        If 'front', give the first n splits.
        If 'back', give the last n splits
        If 'back_offset_one', give the splits from the back, but forward by one
        (so that the last n splits can be the test indices).
        If n_splits = -1, auto-corrects to 'front'.
        If n_splits > 0 and window_size is None,
        choose 'back' or 'back_but_one' for best results.
    return_type: str
        If return_type == 'index', return indices for the
        DataFrames train and validation
        (If data padded with 0's, this is more-or-less equivalent
        to sklearn.model_selection.TimeSeriesSplit(test_length=24, gap=24),)
        If return_type == 'DataFrame', return the DataFrame itself
        *Warning*: return_type == 'index' is required
        to plug into GridSearchCV and similar!
    sample_spacing: int, default 1
        If sample_spacing = n > 1, returns every nth sample group (just to cut down the size a little)

    Returns:
    splits_list:  list[tuple[list, list]] | list[tuple[pandas.DataFrame, pandas.DataFrame]]
        If return_type == 'index', just a list,
        where each element is a tuple
        (list of training indices, list of validation indices)
        If return_type == 'DataFrame',
        each element of the outer list is a tuple
        (training data, vailidation-day data)
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
    # convert good_ends to pandas.Series object
    if isinstance(good_ends, pd.DataFrame):
        good_ends = good_ends['date']
    elif isinstance(good_ends, list):
        good_ends = pd.Series(good_ends, name='date')
    # make sure that entries are datetime.date objects
    # rather than pd.Timestamp objects
    # to match the entries of df_train['date'] above
    if isinstance(good_ends.iloc[0], pd.Timestamp):
        good_ends = good_ends.dt.date
    good_ends = good_ends[good_ends <= df_last_date]
    # Make sure valid end days have good yearly data
    good_ends = good_ends[good_ends >= df_first_date + timedelta(days=365)]
    # Now that have trimmed good_ends, can give the number of splits
    if n_splits == -1:
        n_splits = len(good_ends)
        front_or_back = 'front'  # pre-empt problems
    # if not enough splits, give a warning.
    if good_ends is None or len(good_ends) < n_splits:
        raise ValueError('Data has fewer good ends than the number of splits.')
    start_ind = good_ends.index[0]
    end_ind = good_ends.index[-1]
    splits_list = []
    for j in range(n_splits):
        if front_or_back == 'front':
            current_end = good_ends.at[start_ind + j]
        elif front_or_back == 'back':
            current_end = good_ends.at[end_ind - n_splits + 1 + j]
        elif front_or_back == 'back_offset_one':
            current_end = good_ends.at[end_ind - n_splits + j]
        else:
            raise ValueError(
                'front_or_back should be "front", "back", '
                + 'or "back_offset".\n'
                + f'Recieved {front_or_back}.'
            )
        if gap_day:
            df_val = df_train[df_train['date'] == current_end]
        else:
            df_val = df_train[(df_train['date'] >= current_end - timedelta(days=1))
                            & (df_train['date'] <= current_end)]
        if window_size is not None:
            df_from_one_day_before = df_train[
                (df_train['date'] >= current_end - timedelta(days=1 + window_size))
                & (df_train['date'] < (current_end - timedelta(days=1)))
            ]
        else:
            df_from_one_day_before = df_train[
                df_train['date'] < (current_end - timedelta(days=1))
            ]
        if not date_before:
            df_val.drop(columns=['date',])
            df_from_one_day_before.drop(columns=['date',])
        if return_type == 'index':
            splits_list.append((df_from_one_day_before.index,
                                df_val.index))
        elif return_type == 'DataFrame':
            splits_list.append((df_from_one_day_before, df_val))
        else:
            raise ValueError(
                'return_type should be "index" or "DataFrame", '
                + f'recieved {return_type}.'
            )
    if sample_spacing == 1:
        return splits_list
    else:
        return splits_list[::sample_spacing]
