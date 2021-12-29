import pandas as pd


def normalize_df(df, list_of_dct_col):
    exploded = df.explode(list_of_dct_col)
    df = pd.concat([
            df.drop(list_of_dct_col, 1), 
            exploded[list_of_dct_col].apply(pd.Series).drop(0, 1)],
        axis=1)
    return df

def create_cheat_df(user):
    cheat_user_data = user.find({'marks':'engine'})
    cheat_df = pd.json_normalize(cheat_user_data)
    return cheat_df

def create_marks_df(cheat_df):
    marks = cheat_df[['_id', 'modlog']].copy()

    # Normalize marks data
    marks = normalize_df(marks, 'modlog')

    # Remove rows where the mod is not known, or the date is not known
    marks = marks[~marks[[col for col in marks.columns if col!='_id']].isnull().all(axis=1)]

    # Reduce to a single mark per user
    marks = marks.sort_values(['_id', 'date'])
    marks = marks.groupby('_id', as_index=False).last()
    return marks

def create_marked_dt_dct(user_collection, live):
    if live:
        return {}

    cheat_df = create_cheat_df(user_collection)
    marks = create_marks_df(cheat_df)
    return dict(marks[['_id', 'date']].values)

def get_rating_df(collections):
    tc_map = {2:'blitz', 6:'rapid'}
    dfs = []

    for collection in collections:
        all_data = collection.find({})
        raw_df = pd.json_normalize(list(all_data))
    
        for tc in tc_map.keys():
            tc_string = tc_map[tc]
            df = raw_df.rename(columns={
                '_id':'user', 
                'perfs.{}.gl.r'.format(tc_string):'rating', 
                'perfs.{}.gl.d'.format(tc_string):'rd'})
            df['tc'] = tc
            dfs.append(df.copy())
    
    output_df = pd.concat(dfs, axis=0)
    output_df = output_df[['user', 'tc', 'rating', 'rd']].dropna()
    return output_df

def build_dense_layer_data(insights_df, collections):
    """
    when we go live with eval, we can run this to get user data

    user_coll.find( { "_id": { "$in": ['name1', 'name2'] } }

    this will return rating and rd data
    """
    # Build input features
    df = insights_df[['user', 'tc', 'days']].drop_duplicates().merge(

        insights_df[(insights_df['insight']=='acpl_result') & (insights_df['bin']==1)][
        ['user', 'tc', 'days', 'nb', 'value']].rename(
        columns={'nb':'nb1', 'value':'value1'}), how='left').merge(

        insights_df[(insights_df['insight']=='acpl_result') & (insights_df['bin']==3)][
        ['user', 'tc', 'days', 'nb', 'value']].rename(
        columns={'nb':'nb3', 'value':'value3'}), how='left').merge(

        insights_df[(insights_df['insight']=='acpl_result')][
        ['user', 'tc', 'days', 'nb']].groupby(['user', 'tc', 'days'],
        as_index=False).sum().rename(columns={'nb':'eval_ss'}), how='left').merge(

        insights_df[(insights_df['insight']=='movetime_result') & (insights_df['bin']==1)][
        ['user', 'tc', 'days', 'nb']].groupby(['user', 'tc', 'days'], 
        as_index=False).sum().rename(columns={'nb':'all_nb1'}), how='left').merge(

        insights_df[(insights_df['insight']=='movetime_result') & (insights_df['bin']==3)][
        ['user', 'tc', 'days', 'nb']].groupby(['user', 'tc', 'days'], 
        as_index=False).sum().rename(columns={'nb':'all_nb3'}), how='left').merge(

        insights_df[(insights_df['insight']=='movetime_result')][
        ['user', 'tc', 'days', 'nb']].groupby(['user', 'tc', 'days'], 
        as_index=False).sum().rename(columns={'nb':'all_ss'}), how='left')

    # Calculate fields
    df['eval_win_loss_ratio'] = (df['nb1']/df['nb3']).fillna(50)
    df['all_win_loss_ratio'] = (df['all_nb1']/df['all_nb3']).fillna(50)
    df['eval_all_win_loss_ratio'] = df['eval_win_loss_ratio']/df['all_win_loss_ratio']
    df['eval_sample_ratio'] = df['eval_ss']/df['all_ss']

    # Drop unnecessary intermediate fields
    df = df.drop(['nb1', 'value1', 'nb3', 'value3', 'all_nb1', 'all_nb3'], 1)

    # Get user rating data
    rating_df = get_rating_df(collections)
    df = df.merge(rating_df)

    # Reshape the data so it'll play nice with other functions
    df = pd.melt(df, id_vars=['user', 'tc', 'days'], var_name='bin')
    df['nb'] = 1
    dense_fields = sorted(list(df['bin'].unique()))
    df['num_map'] = df['bin'].map(dict(zip(dense_fields, range(len(dense_fields)))))
    df['insight'] = 'dense'+df['num_map'].astype(str)+'_layer'
    df['bin'] = 1
    df = df[['insight', 'user', 'tc', 'days', 'bin', 'nb', 'value']].sort_values(
        ['insight', 'user', 'tc', 'days', 'bin'])
    
    return insights_df.append(df)