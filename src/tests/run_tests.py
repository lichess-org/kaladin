from pymongo import MongoClient
from run_model import run
from common.utils import pickle_me, configure_logging
import random
import time
import logging

def main(insights, user_collection):
    """
    insights : pymongo.collection.Collection
    user_collection: pymongo.collection.Collection
    """

    # Get list of un-marked users
    legit = list(user_collection.find({'marks': {'$ne':'engine'}}, {'_id'}))
    legit_users = [user['_id'] for user in legit]

    test_results = []

    for x in [1, 10, 100]:
        users = random.sample(legit_users, x)
        
        start_time = time.time()
        output = run(insights, user_collection, use_eval=0, explainers=None, live_user_list=users)
        end_time = time.time()

        test_results = {
                'runtime':end_time - start_time,
                'output':output
            }
        log.debug(end_time - start_time)
        pickle_me(test_results, 'tests/test_case_{}_result.pkl'.format(x))
        
if __name__ == '__main__':
    log = logging.getLogger(__file__)
    log.setLevel('DEBUG')
    configure_logging(log)
    client = MongoClient()
    db = client['lichess']
    main(db.insight_202111, db.user_202111)