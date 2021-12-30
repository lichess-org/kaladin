from pymongo import MongoClient
from datetime import datetime
from common.utils import configure_logging

import logging
import random
import time
import string

log = logging.getLogger(__file__)
log.setLevel('DEBUG')
configure_logging(log)

# create or assign the queue mongo collection
client = MongoClient('mongodb://localhost:27017/')
db = client['kaladin']
kaladin_queue = db['kaladin_queue']

log.debug(list(db.kaladin_queue.find({})))
# remove old documents
kaladin_queue.delete_many({})

# simulate Lichess adding requests and reading/removing results
while True:
    priority = random.randint(1, 100)
    user = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))
    db.kaladin_queue.insert_one(
        {'_id': user, 'priority': priority, 'queuedAt': datetime.utcnow()})
    log.debug(user, "added to queue.")
    time.sleep(2)
    log.debug(list(db.kaladin_queue.find({})))
