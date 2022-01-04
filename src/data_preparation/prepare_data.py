from pymongo import MongoClient
import datetime
from sklearn.model_selection import train_test_split
import glob
import pandas as pd
import logging
from data_preparation.pipelines import acpl_by_date_pipeline, movetime_by_date_pipeline
from data_preparation.insights_generation import define_user_lists, first_insight, \
    build_eligible_player_dct, generate_insights, build_additional_insights
from data_preparation.user_level_data import  create_marked_dt_dct, build_dense_layer_data
from data_preparation.ml_transforms import clip_and_scale, handle_missing_values, \
    solve_mismatching_dimensions, build_labels, build_dimension_input_arrays
from common.utils import pickle_me, unpickle_me, configure_logging, initialize_logging_for_modules

log = logging.getLogger(__file__)
initialize_logging_for_modules(log)

def build_data(insights, user_collection, live, live_user_list=None):
    # Parameters
    datagen_date = datetime.datetime(2021, 11, 4)
    min_moves = 1000
    max_moves = 5000
    max_games = 250
    num_date_buckets={180:8,}
    tc_list = [2, 6]
    use_eval = 0
    overwrite_data = False  # Should be True when using new mondodb collection, False otherwise
    overwrite_traintest_users = False   # Must be True for new mongodb training collections, otherwise False for speedup
    overwrite_user_eligiblity_dct = False # Must be True for new mongodb training collections, other False for speedup

   
    if live:
        # Set date to now
        datagen_date = datetime.datetime.utcnow()
    else:
        # Index fields
        insights.create_index([('u', 1)])
        insights.create_index([('d', 1)])
        insights.create_index([('p', 1)])


    # Build dictionary of marked users
    marked_dt_dct = create_marked_dt_dct(user_collection, live=live)

    # Define user lists
    legit_users, cheat_users = define_user_lists(insights, user_collection, live=live, live_user_list=live_user_list)

    # First insight
    # This also determines which players meet minimum move thresholds for entry into the dataset
    if use_eval:
        first_metric = 'acpl_date'
        first_pipeline = acpl_by_date_pipeline
    else:
        first_metric = 'movetime_date'
        first_pipeline = movetime_by_date_pipeline

    log.debug(first_metric+' pipeline')
    first_insight_legit_df = first_insight(
        insights=insights, 
        live=live,
        fdir='insights_df_chunks/use_eval_{}/'.format(use_eval),
        user_list=legit_users, 
        tc_list=tc_list,   
        days_list=list(num_date_buckets.keys()), 
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        min_moves=min_moves, 
        pipeline=first_pipeline,
        metric=first_metric,
        num_date_buckets=num_date_buckets,
        user_list_marked=False, 
        marked_dt_dct=None,
        overwrite_data=overwrite_data or live
    )
    
    first_insight_cheat_df = first_insight(
        insights=insights,
        live=live,
        fdir='insights_df_chunks/use_eval_{}/'.format(use_eval),
        user_list=cheat_users, 
        tc_list=tc_list,   
        days_list=list(num_date_buckets.keys()), 
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        min_moves=min_moves, 
        pipeline=first_pipeline,
        metric=first_metric,
        num_date_buckets=num_date_buckets,
        user_list_marked=True, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data or live
    )

    # Create list of eligible players for each dataset
    # Player list is based on first insight min_moves meeting requirements
    # This is to ensure that sample sizes are large enough to return relevant insights
    eligible_player_dct = build_eligible_player_dct(first_insight_cheat_df, first_insight_legit_df, tc_list, num_date_buckets)

    # Get user lists
    if live:
        train_users, valid_users, test_users = [], [], legit_users+cheat_users
    
    elif overwrite_traintest_users:
        # Split users 70/20/10 train/valid/test
        train_users, test_users = train_test_split(legit_users+cheat_users, test_size=0.1)
        train_users, valid_users = train_test_split(train_users, test_size=0.22) 
        
        # Write metadata to disk
        log.debug('writing metadata to disk')
        pickle_me({
            'cheat_users':cheat_users,
            'legit_users':legit_users,
            'train_users':train_users,
            'valid_users':valid_users,
            'test_users':test_users,
            'eligible_player_dct':eligible_player_dct
        }, 'input_data/use_eval_{}/metadata_dct.pkl'.format(use_eval))
    
    else:
        metadata_dct = unpickle_me('input_data/use_eval_{}/metadata_dct.pkl'.format(use_eval))
        train_users = metadata_dct['train_users']
        valid_users = metadata_dct['valid_users']
        test_users = metadata_dct['test_users']

    ## Generate Insights ##
    log.debug('Generating insights for legit users')
    insights_df_chunks = [first_insight_cheat_df, first_insight_legit_df] 
    insights_df_chunks = generate_insights(
        insights, 
        user_list=legit_users, 
        eligible_player_dct=eligible_player_dct,
        datagen_date=datagen_date, 
        num_date_buckets=num_date_buckets,
        live=live,
        insights_df_chunks=insights_df_chunks,
        use_eval=use_eval,
        user_list_marked=False, 
        marked_dt_dct=None,
        max_games=max_games, 
        max_moves=max_moves,
        overwrite_data=overwrite_data,
        overwrite_user_eligiblity_dct=overwrite_user_eligiblity_dct
    )
    
    if not live:
        log.debug('Generating insights for cheat users')  
        generate_insights(
            insights, 
            user_list=cheat_users, 
            eligible_player_dct=eligible_player_dct,
            datagen_date=datagen_date, 
            num_date_buckets=num_date_buckets,
            live=live,
            insights_df_chunks=insights_df_chunks,
            use_eval=use_eval,
            user_list_marked=True, 
            marked_dt_dct=marked_dt_dct,
            max_games=max_games, 
            max_moves=max_moves,
            overwrite_data=overwrite_data,
            overwrite_user_eligiblity_dct=overwrite_user_eligiblity_dct
        )

    if live:
        insights_df = pd.concat(insights_df_chunks)
    else:
        # Read insights from disk and combine insights into single DataFrame
        log.debug('reading insights from disk')
        insights_df_chunks = glob.glob("insights_df_chunks/use_eval_{}/*.pkl".format(use_eval))
        insights_df = pd.concat([unpickle_me(chunk) for chunk in insights_df_chunks], axis=0)

    if use_eval:
        # Create dense layer data
        log.debug('building dense layer data')
        insights_df = build_dense_layer_data(insights_df, collections=[user_collection,])

        # Build composite insights
        log.debug('building composite insights')
        insights_df = build_additional_insights(insights_df)

    # Handle outliers by clipping, log-transforming, and scaling values
    log.debug('clipping and scaling data')
    insights_df = clip_and_scale(insights_df, train_users, live=live, use_eval=use_eval)

    # Write full data to disk for testing
    if not live:
        pickle_me(insights_df, 'input_data/use_eval_{}/insights_df.pkl'.format(use_eval))

    live_data = {}
    for tc in insights_df['tc'].unique():
        for days in insights_df['days'].unique():
            log.debug('Building data for tc={} and days={}'.format(tc, days))
            # Create data subset
            df = insights_df[
                (insights_df['tc']==tc) & 
                (insights_df['days']==days)].copy()
            
            # Fill in missing values
            df = handle_missing_values(df, live, use_eval=use_eval)

            # Solve mismatching dimensions bug
            dimensions = sorted(list(set([i.split('_')[1] for i in df['insight'].unique()])))
            train_users_subset, valid_users_subset, test_users_subset = solve_mismatching_dimensions(
                df, dimensions, train_users, valid_users, test_users)

            # Build labels
            train_user_list, train_labels = build_labels(df[df['user'].isin(train_users_subset)], cheat_users, legit_users)
            valid_user_list, valid_labels = build_labels(df[df['user'].isin(valid_users_subset)], cheat_users, legit_users)
            test_user_list, test_labels = build_labels(df[df['user'].isin(test_users_subset)], cheat_users, legit_users)

            # Build data for model training
            data_dct = {
                'train':{}, 
                'valid':{}, 
                'test':{},
                'train_labels':train_labels,
                'valid_labels':valid_labels,
                'test_labels':test_labels,
                'train_user_list':train_user_list,
                'valid_user_list':valid_user_list,
                'test_user_list':test_user_list,
                'dimensions':dimensions,
                'df':df
                }

            # Build input arrays
            for dimension in dimensions:
                data_dct['train'][dimension] = build_dimension_input_arrays(df, train_users_subset, dimension, live_suppress=live)
                data_dct['valid'][dimension] = build_dimension_input_arrays(df, valid_users_subset, dimension, live_suppress=live)
                data_dct['test'][dimension] = build_dimension_input_arrays(df, test_users_subset, dimension)

            # Write to disk
            if live:
                live_data[(tc, days)] = data_dct
            else:
                pickle_me(data_dct, 'input_data/use_eval_{}/data_dct_tc{}_days{}.pkl'.format(use_eval, tc, days))
    if live:
        return live_data

if __name__ == '__main__':
    log = logging.getLogger(__file__)
    log.setLevel('DEBUG')
    configure_logging(log)

    # Connect to mongodb
    client = MongoClient()
    db = client['lichess']
    build_data(db.insight_202111, db.user_202111, live=0)
    log.debug('run completed successfully')