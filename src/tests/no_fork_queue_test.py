import pymongo
import pickle
import time
import os

from queue_manager import QueueManager 

if __name__ == '__main__':
    qm = QueueManager() # just to access collection, TODO isolate Db in it's own class
    ids = qm.insights_coll.distinct("u")
    print(ids)
    results = {x:[] for x in ids} # ids -> [activation]
    for i in range(10):
        qm.kaladin_queue_coll.drop()
        minimal_insert_many = [{'_id': x, 'priority': 100} for x in ids]
        print(minimal_insert_many)
        qm.kaladin_queue_coll.insert_many(minimal_insert_many)
        for _id in ids:
            resp = None
            while resp is None:
                bdoc = qm.kaladin_queue_coll.find_one({'_id': _id})
                resp = bdoc.get('response')
                if resp is not None:
                    activation = resp['pred']['activation']
                    results[_id].append(activation)
                else:
                    time.sleep(2)
            print(results)
    for _id in ids:
        print(f"{_id}, min: {min(results[_id])}, max: {max(results[_id])}")
