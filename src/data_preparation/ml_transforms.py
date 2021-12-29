import numpy as np
import pandas as pd
import logging
from common.utils import pickle_me, unpickle_me, initialize_logging_for_modules

log = logging.getLogger(__file__)
initialize_logging_for_modules(log)

def solve_mismatching_dimensions(df, dimensions, train_users, valid_users, test_users):
    train_user_removals = set()
    valid_user_removals = set()
    test_user_removals = set()
    
    for dimension in dimensions:
        train_user_removals = train_user_removals | get_user_removals(df, train_users, dimension)
        valid_user_removals = valid_user_removals | get_user_removals(df, valid_users, dimension)
        test_user_removals = test_user_removals | get_user_removals(df, test_users, dimension)
    
    train_users_subset = list(set(train_users) - train_user_removals)
    valid_users_subset = list(set(valid_users) - valid_user_removals)
    test_users_subset = list(set(test_users) - test_user_removals)

    log.debug('train users reduced from {} to {} due to mismatching dimensions.'.format(
        len(df[df['user'].isin(train_users)]['user'].unique()),
        len(df[df['user'].isin(train_users_subset)]['user'].unique())
        ))
    log.debug('valid users reduced from {} to {} due to mismatching dimensions.'.format(
        len(df[df['user'].isin(valid_users)]['user'].unique()),
        len(df[df['user'].isin(valid_users_subset)]['user'].unique())
        ))
    log.debug('test users reduced from {} to {} due to mismatching dimensions.'.format(
        len(df[df['user'].isin(test_users)]['user'].unique()),
        len(df[df['user'].isin(test_users_subset)]['user'].unique())
        ))

    return train_users_subset, valid_users_subset, test_users_subset

def get_user_removals(df, user_list, dimension):
    df = df[(df['user'].isin(user_list)) & (df['insight'].str.contains('_{}'.format(dimension)))]
    current_user_set = set(df['user'].unique())
    user_set = set(user_list)
    removals = user_set - current_user_set
    return removals

def build_dimension_input_arrays(df, user_list, dimension, live_suppress=False):
    df = df[df['user'].isin(user_list)].copy()

    # Build dimension dataframe
    dim_df = df[df['insight'].str.contains('_{}'.format(dimension))]
    dim_df = dim_df.sort_values(['user', 'insight', 'bin'])
    num_users = dim_df['user'].nunique()
    num_insights = dim_df['insight'].nunique()
    dim_nb_array = dim_df['nb'].values
    if len(dim_nb_array) == 0:
        if not live_suppress:
            log.debug('Warning: {} dimension had no data'.format(dimension))
        return np.empty((0,0,0,0))
    dim_nb_array = np.reshape(
        dim_nb_array, 
        (num_users, num_insights, 1, -1)
    )
    dim_value_array = dim_df['value'].values
    dim_value_array = np.reshape(
        dim_value_array, 
        (num_users, num_insights, 1, -1)
    )
    dim_array = np.concatenate((dim_nb_array, dim_value_array), axis=2)
    return dim_array

def build_labels(df, cheat_users, legit_users):
    users = pd.DataFrame({'user':df['user'].unique()})
    users['cheat'] = np.where(users['user'].isin(cheat_users), 1, 
                            np.where(users['user'].isin(legit_users), 0, np.nan))
    users = users.dropna()
    users = users.sort_values('user').reset_index(drop=True)
    return list(users['user'].values), users['cheat'].values

def handle_missing_values(df, live, use_eval):
    """
    Replaces missing insights data with the mean value of that insight, by user
    """
    # Create NaNs for missing data
    user_keys = df[['user', 'insight']].drop_duplicates()
    if live:
        bin_keys = unpickle_me('input_data/use_eval_{}/insight_bin_keys.pkl'.format(use_eval))
        insight_averages = unpickle_me('input_data/use_eval_{}/insight_averages.pkl'.format(use_eval))
    else:
        bin_keys = df[['insight', 'bin']].drop_duplicates()
        pickle_me(bin_keys, 'input_data/use_eval_{}/insight_bin_keys.pkl'.format(use_eval))
        insight_averages = df[['insight', 'value']].groupby(
            'insight', as_index=False).mean().rename(columns={'value':'avg_value'})
        pickle_me(insight_averages, 'input_data/use_eval_{}/insight_averages.pkl'.format(use_eval))

    keys = user_keys.merge(bin_keys)
    df = keys.merge(df[['user', 'insight', 'bin', 'nb', 'value']], how='left')
    df = df.sort_values(['user', 'insight', 'bin'])

    # Turn infinite values into NaNs
    df = df.replace([np.inf, -np.inf], np.nan)

    # Fill NaNs
    df['nb'] = df['nb'].fillna(1)
    df['value'] = df['value'].fillna(
        df[['user','insight', 'value']].groupby(['user', 'insight']).transform('mean')['value'])

    # In some rare cases, an entire insight might be missing for a player.
    # In that case we can fill with average values across all players
    df_list = []
    bin_keys['dummy'] = 1
    user_keys = pd.DataFrame({'user':df['user'].unique(), 'dummy':1})
    for dimension in list(set([insight.split('_')[1] for insight in df['insight'].unique()])):
        df_subset = df[df['insight'].str.contains('_'+dimension)]
        df_subset = user_keys.merge(
            bin_keys[bin_keys['insight'].str.contains('_'+dimension)]).merge(
            df_subset, how='left').drop('dummy', 1)
        filled_rows = len(df_subset[df_subset.isnull().any(axis=1)])
        if filled_rows:
            log.debug('Filling {} insight-user combos with overall averages for {} dimension'.format(
            filled_rows, dimension))
        
        # Fill sample size
        df_subset['nb'] = df_subset['nb'].fillna(1)

        # Fill values
        df_subset = df_subset.merge(insight_averages, how='left')
        df_subset['value'] = df_subset['value'].fillna(df_subset['avg_value'])
        df_list.append(df_subset)
        
    df = pd.concat(df_list, axis=0)
    df = df.drop('avg_value', 1)
    df = df.sort_values(['user', 'insight', 'bin']).reset_index(drop=True)
    return df

def convert_dtypes(df):
    df['value'] = pd.to_numeric(df['value'], errors='raise')
    df['nb'] = pd.to_numeric(df['nb'], errors='raise')
    df['tc'] = pd.to_numeric(df['tc'], errors='raise')
    df['days'] = pd.to_numeric(df['days'], errors='raise')
    return df

def get_quantile_values(df, train_users, lower_quantile, upper_quantile):
    """
    Get min or max values by insight, tc, days for nb and value.
    direction must be either 'min' or 'max'
    """
    # Only use train set to determine min and max values
    df = df[df['user'].isin(train_users)]

    # Get min and max values
    lower_quantile_df = df[['insight', 'tc', 'days', 'nb', 'value']].groupby(
        ['insight', 'tc', 'days'], as_index=False).quantile(lower_quantile)
    lower_quantile_df = lower_quantile_df.rename(columns={'nb':'nb_min', 'value':'value_min'})

    upper_quantile_df = df[['insight', 'tc', 'days', 'nb', 'value']].groupby(
        ['insight', 'tc', 'days'], as_index=False).quantile(upper_quantile)
    upper_quantile_df = upper_quantile_df.rename(columns={'nb':'nb_max', 'value':'value_max'})

    return lower_quantile_df.merge(upper_quantile_df)

def clip_values(df, quantile_df):
    df = df.merge(quantile_df, on=['insight', 'tc', 'days'])
    df['nb'] = df['nb'].clip(lower=df['nb_min'], upper=df['nb_max'])
    df['value'] = df['value'].clip(lower=df['value_min'], upper=df['value_max'])
    return df

def clip_and_scale(df, train_users, live, use_eval):
    # Convert datatypes
    df = convert_dtypes(df)

    # Deal with negative values
    df['value'] = np.where(df['insight']=='ratinggain_date', df['value'] + 10000, df['value'])

    # log transform
    df = log_transforms(df)

    if live:
        # Read in quantile data
        quantile_df = unpickle_me('input_data/use_eval_{}/minmax_values_for_clipping.pkl'.format(use_eval))

    else:
        # Find quantile values
        quantile_df = get_quantile_values(df, train_users, lower_quantile=0.01, upper_quantile=0.98)

        # Remember boundaries for prediction
        pickle_me(quantile_df, 'input_data/use_eval_{}/minmax_values_for_clipping.pkl'.format(use_eval))
    
    # Clip values
    df = clip_values(df, quantile_df)

    # Scale values between -1 and +1
    df['value'] = 2 * (df['value'] - df['value_min']) / (df['value_max'] - df['value_min']) - 1
    df['nb'] = 2 * (df['nb'] - df['nb_min']) / (df['nb_max'] - df['nb_min']) - 1

    # Drop minmax columns
    df = df.drop(['nb_min', 'nb_max', 'value_min', 'value_max'], 1)

    return df

def log_transforms(df):
    df['nb'] = np.log(df['nb'])
    df['value'] = np.log(df['value']+1)
    return df