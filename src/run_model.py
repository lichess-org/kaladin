from pymongo import MongoClient
from data_preparation.prepare_data import build_data
from model.predict import predict_all
from common.utils import mongo_retry, initialize_logging_for_modules
import logging

log = logging.getLogger(__file__)
initialize_logging_for_modules(log)


@mongo_retry(log=log)
def run(insight_collection, user_collection, use_eval=0, explainers=None, live_user_list=None):
    """
    insights : pymongo.collection.Collection
    user_collection: pymongo.collection.Collection

    Get predictions from the model live.
    """
    if live_user_list and len(live_user_list) == 1:
        try:
            data_dct = build_data(
                insight_collection, 
                user_collection, 
                live=1,  # don't change this when connected to production servers!
                live_user_list=live_user_list)

        except Exception as e:
            log.warn(f'run returned Exception: {e}')
            return None

    else:
        data_dct = build_data(
            insight_collection, 
            user_collection, 
            live=1,  # don't change this when connected to production servers!
            live_user_list=live_user_list)

    df = predict_all(
        tc_list=(2, 6),
        days_list=(180,),
        use_eval=use_eval,
        explainers=explainers,
        data_dct=data_dct
    )

    return df


if __name__ == '__main__':
    client = MongoClient()
    db = client['lichess']
    run(db.insight_202111, db.user_202111, explainers=None)
