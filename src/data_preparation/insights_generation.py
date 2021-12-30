import pandas as pd
import numpy as np
import datetime
import os
from data_preparation.pipelines import *
from common.utils import pickle_me, unpickle_me, initialize_logging_for_modules
import logging

log = logging.getLogger(__file__)
initialize_logging_for_modules(log)

def iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct):
    if u % 500 == 0:
        log.debug("Processed {} users.".format(u))

    if user_list_marked:
        try:
            latest_date = marked_dt_dct[user]
        except KeyError:
            return None
    else:
        latest_date = datagen_date
    
    return latest_date

def build_eligible_player_dct(acpl_date_cheat_df, acpl_date_legit_df, tc_list, num_date_buckets):
    eligible_player_dct = {tc:{days:[] for days in num_date_buckets.keys()} for tc in tc_list}
    for tc in acpl_date_legit_df['tc'].unique():
        for days in acpl_date_legit_df['days'].unique():
            eligible_player_dct[tc][days]=list(
                acpl_date_cheat_df[
                    (acpl_date_cheat_df['tc']==tc) & 
                    (acpl_date_cheat_df['days']==days)]['user'].unique()
                    )+list(
                        acpl_date_legit_df[
                            (acpl_date_legit_df['tc']==tc) & 
                            (acpl_date_legit_df['days']==days)]['user'].unique()
                    )
    return eligible_player_dct


def create_user_eligibility_dct(tc_list, days_list, eligible_player_dct, user_list, 
overwrite_user_eligiblity_dct, marked_users, live, use_eval):
    fname = 'input_data/use_eval_{}/user_eligibility_dct_{}.pkl'.format(use_eval, marked_users)
    
    if overwrite_user_eligiblity_dct or live:
        dct = {
            key:val for key,val in
            {
                user:[
                    (tc, days) for tc in tc_list for days in days_list if user in eligible_player_dct[tc][days]
                    ] 
                for user in user_list
            }.items() if bool(val)
        }
        if not live:
            pickle_me(dct, fname)
        return dct
    else:
        return unpickle_me(fname)

def define_user_lists(insights, user_collection, live=0, live_user_list=None):
    if live:
        legit_users = live_user_list
        cheat_users = []
    else:
        all_users_set = set(insights.distinct('u'))
        cheat_users = [user['_id'] for user in list(user_collection.find({'marks':'engine'}, {'_id'}))]
        cheat_users = list(set(cheat_users) & all_users_set)
        legit_users = [user['_id'] for user in list(user_collection.find({'marks': {'$ne':'engine'}}, {'_id'}))]
        legit_users = list(set(legit_users) & all_users_set)
        
    return legit_users, cheat_users


def first_insight(insights, live, fdir, user_list, tc_list, days_list, datagen_date, max_games, max_moves, 
min_moves, pipeline, metric, num_date_buckets, user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, metric, bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        df = unpickle_me(fname)
        return df

    df_list = []
    pipeline[1]['$limit'] = max_games
    pipeline[4]['$limit'] = max_moves
    for u, user in enumerate(user_list):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        pipeline[0]['$match']['u'] = user
        if latest_date is None:
            continue
        for tc in tc_list:
            for days in days_list:
                earliest_date = latest_date - datetime.timedelta(days=days)
                pipeline[0]['$match']['p'] = tc
                pipeline[0]['$match']['d'] = {'$lte':latest_date, '$gte':earliest_date}
                pipeline[5]['$bucketAuto']['buckets'] = num_date_buckets[days]
                q = list(insights.aggregate(pipeline))
                if sum([row['nb'] for row in q]) < min_moves:
                    continue
                else:
                    for i, row in enumerate(q):
                        df_list.append([
                            metric, 
                            user,
                            tc, 
                            days,
                            i, 
                            row['nb'], 
                            row['v']
                        ])

    df = pd.DataFrame(df_list, columns=['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value'])
    if live:
        return df
    else:
        pickle_me(df, fname)
        return df

def metric_dimension(insights, live, fdir, datagen_date, max_games, max_moves, max_moves_ix,
user_eligibility_dct, pipeline, metric, user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, metric, bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        return 0

    df_list = []
    pipeline[1]['$limit'] = max_games
    pipeline[max_moves_ix]['$limit'] = max_moves

    for u, user in enumerate(list(user_eligibility_dct.keys())):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        if latest_date is None:
            continue
        pipeline[0]['$match']['u'] = user
        for tc, days in user_eligibility_dct[user]:
            earliest_date = latest_date - datetime.timedelta(days=days)
            pipeline[0]['$match']['p'] = tc
            pipeline[0]['$match']['d'] = {'$lte':latest_date, '$gte':earliest_date}
            q = list(insights.aggregate(pipeline))
            for row in q:
                df_list.append([
                    metric, 
                    user,
                    tc, 
                    days,
                    row['_id'], 
                    row['nb'], 
                    row['v']
                ])
                
    df = pd.DataFrame(df_list, columns=['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value'])
    if '_blur' in metric:
        df['bin'] = df['bin'].replace(np.nan, 0).replace(True, 1)
    
    if live:
        return df
    else:
        pickle_me(df, fname)

def move_metric_date(insights, live, fdir, datagen_date, max_games, max_moves, max_moves_ix,
user_eligibility_dct, pipeline, metric, num_date_buckets,
user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, metric, bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        return 0

    df_list = []
    pipeline[1]['$limit'] = max_games
    pipeline[max_moves_ix]['$limit'] = max_moves
    for u, user in enumerate(list(user_eligibility_dct.keys())):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        if latest_date is None:
            continue
        pipeline[0]['$match']['u'] = user
        for tc, days in user_eligibility_dct[user]:
            earliest_date = latest_date - datetime.timedelta(days=days)
            pipeline[0]['$match']['p'] = tc
            pipeline[0]['$match']['d'] = {'$lte':latest_date, '$gte':earliest_date}
            pipeline[max_moves_ix+1]['$bucketAuto']['buckets'] = num_date_buckets[days]
            q = list(insights.aggregate(pipeline))
            for i, row in enumerate(q):
                df_list.append([
                    metric, 
                    user,
                    tc, 
                    days,
                    i, 
                    row['nb'], 
                    row['v']
                ])

    df = pd.DataFrame(df_list, columns=['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value'])
    if live:
        return df
    else:
        pickle_me(df, fname)

def game_metric_date(insights, live, fdir, datagen_date, max_games,
user_eligibility_dct, pipeline, metric, num_date_buckets, 
user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, metric, bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        return 0

    df_list = []
    pipeline[1]['$limit'] = max_games
    for u, user in enumerate(list(user_eligibility_dct.keys())):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        if latest_date is None:
            continue
        pipeline[0]['$match']['u'] = user
        for tc, days in user_eligibility_dct[user]:
            earliest_date = latest_date - datetime.timedelta(days=days)
            pipeline[0]['$match']['p'] = tc
            pipeline[0]['$match']['d'] = {'$lte':latest_date, '$gte':earliest_date}
            pipeline[2]['$bucketAuto']['buckets'] = num_date_buckets[days]
            q = list(insights.aggregate(pipeline))
            for i, row in enumerate(q):
                df_list.append([
                    metric, 
                    user,
                    tc, 
                    days,
                    i, 
                    row['nb'], 
                    row['v']
                ])

    df = pd.DataFrame(df_list, columns=['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value'])
    if live:
        return df
    else:
        pickle_me(df, fname)

def game_metric_dimension(insights, live, fdir, datagen_date, max_games,
user_eligibility_dct, pipeline, metric, user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, metric, bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        return 0

    df_list = []
    pipeline[1]['$limit'] = max_games
    for u, user in enumerate(list(user_eligibility_dct.keys())):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        if latest_date is None:
            continue
        pipeline[0]['$match']['u'] = user
        for tc, days in user_eligibility_dct[user]:
            earliest_date = latest_date - datetime.timedelta(days=days)
            pipeline[0]['$match']['p'] = tc
            pipeline[0]['$match']['d'] = {'$lte':latest_date, '$gte':earliest_date}
            q = list(insights.aggregate(pipeline))
            for i, row in enumerate(q):
                df_list.append([
                    metric, 
                    user,
                    tc, 
                    days,
                    row['_id'], 
                    row['nb'], 
                    row['v']
                ])

    df = pd.DataFrame(df_list, columns=['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value'])
    if live:
        return df
    else:
        pickle_me(df, fname)

def movetime_firstthreemoves(insights, live, fdir, datagen_date,
user_eligibility_dct, user_list_marked=False, marked_dt_dct=None, overwrite_data=False):
    fname = '{}/{}_{}.pkl'.format(fdir, 'movetime_firstthreemoves', bool(user_list_marked))
    if os.path.isfile(fname) and not overwrite_data:
        return 0

    df_list = []
    for u, user in enumerate(list(user_eligibility_dct.keys())):
        latest_date = iterate_users(u, user, datagen_date, user_list_marked, marked_dt_dct)
        if latest_date is None:
            continue
        for tc, days in user_eligibility_dct[user]:
            earliest_date = latest_date - datetime.timedelta(days=days)
            q = list(insights.find(
                {'u':user, 'p':tc, 'd':{'$lte':latest_date, '$gte':earliest_date}},
                {'m.t':{'$slice': ['$m.t', 1, 3]}}
            ))
            for game in q:
                try:
                    df_list.append([
                        user,
                        tc,
                        days,
                        game['_id'], 
                        game['m'][0]['t'][0], 
                        game['m'][0]['t'][1], 
                        game['m'][0]['t'][2]])
                except IndexError:
                    pass
            
    df = pd.DataFrame(data = df_list, columns = ['user', 'tc', 'days', 'id', 'm1', 'm2', 'm3'])
    df = df[['user', 'tc', 'days', 'm1', 'm2', 'm3']].groupby(['user', 'tc', 'days']).agg(
        mean_m1 = ('m1', 'mean'),
        mean_m2 = ('m2', 'mean'),
        mean_m3 = ('m3', 'mean'),
        median_m1 = ('m1', 'median'),
        median_m2 = ('m2', 'median'),
        median_m3 = ('m3', 'median'),
        std_m1 = ('m1', 'std'),
        std_m2 = ('m2', 'std'),
        std_m3 = ('m3', 'std'),
        nb = ('m1', 'count')
    ).reset_index()

    col_dct = {col:c for c, col in enumerate(list(df.columns)) if col not in ['user', 'tc', 'days', 'nb']}

    df = df.melt(id_vars=['user', 'tc', 'days', 'nb'], var_name='bin', value_name='value')
    df['bin'] = df['bin'].map(col_dct)
    df['insight'] = 'movetime_firstthreemoves'
    df = df[['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value']]

    if live:
        return df
    else:
        pickle_me(df, fname)

def generate_insights(insights, user_list, eligible_player_dct, datagen_date, 
num_date_buckets, live, insights_df_chunks, use_eval, user_list_marked=False, marked_dt_dct=None,
max_games=1000, max_moves=20000, overwrite_data=False, overwrite_user_eligiblity_dct=True):
    """
    Returns a list of pandas DataFrames with the following columns:
    ['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value']
    """
    overwrite_data = overwrite_data or live

    days_list = list(num_date_buckets.keys())
    user_eligibility_dct = create_user_eligibility_dct(
        tc_list=[2,6], 
        days_list=days_list, 
        eligible_player_dct=eligible_player_dct, 
        user_list=user_list,
        overwrite_user_eligiblity_dct=overwrite_user_eligiblity_dct,
        marked_users = bool(user_list_marked),
        live=live,
        use_eval=use_eval
    )

    fdir = 'insights_df_chunks/use_eval_{}/'.format(use_eval)

    # Time variance/Date
    log.debug("Time variance by date pipeline")
    insights_df_chunks.append(move_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix = 5,
        user_eligibility_dct=user_eligibility_dct,
        pipeline=timevariance_by_date_pipeline,
        metric='timevariance_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # blur/date
    log.debug("blur by date pipeline")
    insights_df_chunks.append(move_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_date_pipeline,
        metric='blur_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # opponentrating/Date
    log.debug("opponentrating by date pipeline")
    insights_df_chunks.append(game_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=opponentrating_by_date_pipeline,
        metric='opponentrating_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # ratinggain/Date
    log.debug("ratinggain by date pipeline")
    insights_df_chunks.append(game_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=ratinggain_by_date_pipeline,
        metric='ratinggain_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("timevariance/movetime pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_movetime_pipeline,
        metric='timevariance_movetime', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("blur/movetime pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_movetime_pipeline,
        metric='blur_movetime', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # Movetime material
    log.debug("Movetime by material pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_material_pipeline,
        metric='movetime_material', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # timevariance material
    log.debug("timevariance by material pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_material_pipeline,
        metric='timevariance_material',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # blur material
    log.debug("blur by material pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_material_pipeline,
        metric='blur_material',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("timevariance/phase pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_phase_pipeline,
        metric='timevariance_phase', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("blur/phase pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_phase_pipeline,
        metric='blur_phase', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("movetime/phase pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_phase_pipeline,
        metric='movetime_phase', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("timevariance/blur pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_blur_pipeline,
        metric='timevariance_blur', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("movetime/blur pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_blur_pipeline,
        metric='movetime_blur', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("blur/timevariance pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_timevariance_pipeline,
        metric='blur_timevariance', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("movetime/timevariance pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_timevariance_pipeline,
        metric='movetime_timevariance', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("timevariance/result pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_result_pipeline,
        metric='timevariance_result', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("blur/result pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_result_pipeline,
        metric='blur_result', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("blurfiltered/result pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blurfiltered_by_result_pipeline,
        metric='blurfiltered_result', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("movetime/result pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_result_pipeline,
        metric='movetime_result', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # opponentrating/result
    log.debug("opponentrating by result pipeline")
    insights_df_chunks.append(game_metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=opponentrating_by_result_pipeline,
        metric='opponentrating_result',
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # Movetime piecemoved
    log.debug("Movetime by piecemoved pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_piecemoved_pipeline,
        metric='movetime_piecemoved', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # timevariance piecemoved
    log.debug("timevariance by piecemoved pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_piecemoved_pipeline,
        metric='timevariance_piecemoved',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # blur piecemoved
    log.debug("blur by piecemoved pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_piecemoved_pipeline,
        metric='blur_piecemoved',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # movetime firsthreemoves
    log.debug("first three moves pipeline")
    insights_df_chunks.append(movetime_firstthreemoves(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        user_eligibility_dct=user_eligibility_dct,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct, 
        overwrite_data=overwrite_data
    ))

    if not use_eval:
        return insights_df_chunks

    # movetime/date
    log.debug("movetime by date pipeline")
    insights_df_chunks.append(move_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_date_pipeline,
        metric='movetime_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # acplfiltered by date
    log.debug("acplfiltered by date pipeline")
    insights_df_chunks.append(move_metric_date(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acplfiltered_by_date_pipeline,
        metric='acplfiltered_date',
        num_date_buckets=num_date_buckets,
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # acpl/movetime
    log.debug("acpl/movetime pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_movetime_pipeline,
        metric='acpl_movetime', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("acpl/phase pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_phase_pipeline,
        metric='acpl_phase', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # ACPL material
    log.debug("ACPL by material pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_material_pipeline,
        metric='acpl_material',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("acpl/blur pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_blur_pipeline,
        metric='acpl_blur', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("acplfiltered/blur pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acplfiltered_by_blur_pipeline,
        metric='acplfiltered_blur', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("acpl/timevariance pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_timevariance_pipeline,
        metric='acpl_timevariance', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    log.debug("acpl/result pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_result_pipeline,
        metric='acpl_result', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # ACPL piecemoved
    log.debug("ACPL by piecemoved pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=4,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_piecemoved_pipeline,
        metric='acpl_piecemoved',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # Movetime evaluation
    log.debug("Movetime by evaluation pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_evaluation_pipeline,
        metric='movetime_evaluation', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # ACPL evaluation
    log.debug("ACPL by evaluation pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=acpl_by_evaluation_pipeline,
        metric='acpl_evaluation',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # timevariance evaluation
    log.debug("timevariance by evaluation pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_evaluation_pipeline,
        metric='timevariance_evaluation',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # blur evaluation
    log.debug("blur by evaluation pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_evaluation_pipeline,
        metric='blur_evaluation',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # timevariance centipawnloss
    log.debug("timevariance by centipawnloss pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=timevariance_by_centipawnloss_pipeline,
        metric='timevariance_centipawnloss',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # blur centipawnloss
    log.debug("blur by centipawnloss pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=blur_by_centipawnloss_pipeline,
        metric='blur_centipawnloss',  
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    # Movetime centipawnloss
    log.debug("Movetime by centipawnloss pipeline")
    insights_df_chunks.append(metric_dimension(
        insights=insights,
        live=live,
        fdir=fdir,
        datagen_date=datagen_date,
        max_games=max_games, 
        max_moves=max_moves,
        max_moves_ix=5,
        user_eligibility_dct=user_eligibility_dct, 
        pipeline=movetime_by_centipawnloss_pipeline,
        metric='movetime_centipawnloss', 
        user_list_marked=user_list_marked, 
        marked_dt_dct=marked_dt_dct,
        overwrite_data=overwrite_data
    ))

    return insights_df_chunks

def build_additional_insights(insights_df):
    insights_df = insights_df.append(build_composite_insights(insights_df, 'movetime', 'acpl'))
    insights_df = insights_df.append(build_composite_insights(insights_df, 'opponentrating', 'acpl'))
    return insights_df

def build_composite_insights(df, a, b):
    df = df[df['insight']=='{}_date'.format(a)][['user', 'tc', 'days', 'bin', 'nb', 'value']].rename(
        columns={'nb':'nb_{}_date'.format(a), 'value':'value_{}_date'.format(a)}).merge(
    df[df['insight']=='{}_date'.format(b)][['user', 'tc', 'days', 'bin', 'nb', 'value']].rename(
        columns={'nb':'nb_{}_date'.format(b), 'value':'value_{}_date'.format(b)}))
    df['insight'] = '{}over{}_date'.format(a, b)
    df['nb'] = df['nb_{}_date'.format(a)]/df['nb_{}_date'.format(b)]
    df['value'] = df['value_{}_date'.format(a)]/df['value_{}_date'.format(b)]
    return df[['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value']]