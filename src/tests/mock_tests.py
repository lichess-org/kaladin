import pymongo
import pickle
import time
from common.utils import pickle_me
from run_model import run


def main(insights):
    """
    insights : pymongo.collection.Collection
    user_collection: pymongo.collection.Collection
    """

    users = list(insights.distinct('u'))
    user_collection = None
    start_time = time.time()
    output = run(insights, user_collection, use_eval=0, explainers=None, live_user_list=users)
    end_time = time.time()

    test_results = {
        'runtime': end_time - start_time,
        'output': output
    }
    print(end_time - start_time)
    pickle_me(test_results, 'tests/test_case_anon_result.pkl')


if __name__ == '__main__':
    with open('tests/anon_insights.pkl', 'rb') as f:
        documents = pickle.load(f)
    client = pymongo.MongoClient()
    db = client['lichess']
    collection = db.collection
    try:
        collection.insert_many(documents)
    except pymongo.errors.BulkWriteError as e:
        pass
    main(collection)
