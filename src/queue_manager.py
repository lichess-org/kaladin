import enum
import logging
import logging.handlers
import time

from copy import deepcopy
from datetime import datetime, timedelta
from pymongo import MongoClient
from typing import Any, Dict, Union, List

from run_model import run
from model.shap_values import train_shap_explainer
from common.utils import configure_logging, load_config, mongo_retry

#############
# Constants #
#############

BDoc = Dict[str, Any] # BSONDocument. Mypy does not support recursive type definition

config = load_config()

INSIGHT_URI = config['INSIGHT_URI']
INSIGHT_DB = config['INSIGHT_DB']
INSIGHT_COLL = config['INSIGHT_COLL']
INSIGHT_QUEUE_COLL = config['INSIGHT_QUEUE_COLL']

LOGGING_LEVEL = config['LOGGING_LEVEL'].upper()

# time in seconds to wait before proceeding with analysis on non-full batch
BATCH_TIMEOUT = int(config['BATCH_TIMEOUT'])
BATCH_SIZE = int(config['BATCH_SIZE'])  # users to process per analysis run
BATCH_REFRESH_WAITING_TIME = int(config['BATCH_REFRESH_WAITING_TIME'])

LICHESS_DB = config['LICHESS_DB']
LICHESS_USER_COLL = config['LICHESS_USER_COLL']

###########
# Logging #
###########

log = logging.getLogger(__file__)
log.setLevel(LOGGING_LEVEL)
configure_logging(log)


###########
# Classes #
###########

# Kinds of error returned to lila.
# Avoid changing error name because it will break monitoring visualisation.
class Error(enum.Enum):
    BATCH_ERROR = enum.auto() # The whole batch was corrupted
    USER_ERROR = enum.auto() # For now only know one case where it happens, that is when the user has not enough moves to be processed

class QueueManager:

    def __init__(self) -> None:
        #######################
        # Database connection #
        #######################

        # connect to the prod insights collection
        insights_client = MongoClient(INSIGHT_URI)
        insights_db = insights_client[INSIGHT_DB]
        insights_coll = insights_db[INSIGHT_COLL]

        # collection for queue
        kaladin_queue_coll = insights_db[INSIGHT_QUEUE_COLL]
        log.info('Successfully connected to insight db')

        # initialize SHAP explainer
        model_cfgs = [
            (0, 2, 180), # (use_eval, tc, days)
            (0, 6, 180)
        ]
        explainers = {
            model_cfg: train_shap_explainer('model/eval{}_tc{}_days{}/'.format(*model_cfg)) 
            for model_cfg in model_cfgs
        }

        self.insights_coll = insights_coll
        self.kaladin_queue_coll = kaladin_queue_coll
        self.main_user_coll = None # placeholder, user coll currently not needed
        self.explainers = explainers

    def run(self) -> None:
        while True:
            batch_start_time = datetime.utcnow()
            # take requests. Unfortunately PyMongo is not yet statically typed: https://github.com/mongodb/mongo-python-driver/pull/829
            users_to_analyse: List[BDoc] = []
            while len(users_to_analyse) < BATCH_SIZE:
                self.getNextFromQueue(users_to_analyse)

                # if no batch analysed for time_to_wait seconds, proceed with the analysis run with non-full batch
                wait_time = (datetime.utcnow() - batch_start_time).total_seconds()
                if users_to_analyse and wait_time > BATCH_TIMEOUT:
                    break

            log.debug(f'To analyse: {users_to_analyse}')

            try:
                analysed_users = self.analysisFunction(users_to_analyse)
                log.debug(f'Analysis: {analysed_users}')
                self.updateQueue(analysed_users)
            except Exception as e:
                # If analysis fails for batch, try one user at a time
                # TODO binary chop
                log.warn(e)
                for user_to_analyse in users_to_analyse:
                    self.analyseOneUser(user_to_analyse)

    def analysisFunction(self, users_to_analyse: List[BDoc]) -> List[BDoc]:
        # parameter format [{'_id': 'somethingpretentious'}]
        analysed_users = deepcopy(users_to_analyse)
        user_names_only = [user['_id'] for user in users_to_analyse]

        # Run Kaladin
        response = run(
            self.insights_coll,
            self.main_user_coll,
            use_eval=0, 
            explainers=self.explainers, 
            live_user_list=user_names_only)

        # Check for Kaladin response and re-format
        log.debug(f"Kaladin response: {response}")
        if not (response is None or response.empty):
            response = response.set_index('user').to_dict('index')

        # For each user dictionary, update with kaladin response
        # available data ['user', 'tc', 'days', 'label', 'pred', 
        # 'insight_1', 'shap_score_1', 'insight_2', 'shap_score_2', 'insight_3', 'shap_score_3']
        for user_dict in analysed_users:
            user_response: Dict[str, Union[datetime, str, BDoc]] = {}
            user_response['at'] = datetime.utcnow()
            if response is None:
                user_response['err'] = Error.BATCH_ERROR.name
            elif user_dict['_id'] not in response:
                user_response['err'] = Error.USER_ERROR.name
            else:
                user_data = response[user_dict['_id']]
                user_inner_response = {
                    'activation': user_data['pred'], 
                    'insights': [
                        user_data['insight_1'], 
                        user_data['insight_2'], 
                        user_data['insight_3']], 
                    'tc': user_data['tc']}
                user_response['pred'] = user_inner_response
            user_dict['response'] = user_response
        return analysed_users

    @mongo_retry(log=log)
    def getNextFromQueue(self, users_to_analyse: List[BDoc]) -> None:
        """Add one pending user to the `users_to_analyse` if any."""
        user_document = self.kaladin_queue_coll.find_one_and_update(
            {
                # Using '$not' '$gt' instead of '$lte' allows to pick users without `startAt` field set.
                # Must not pick twice the same user. There is at most `BATCH_TIMEOUT`s between the first, and the last user picked, before processing.
                # Add a safety margin by multiplying by some constant.
                'startedAt': {'$not': {'$gt': datetime.utcnow() - timedelta(seconds = 10 * BATCH_TIMEOUT)}},
                # Hits the "response.at_1_priority_-1" mongodb index
                'response.at': {'$exists': False}
            },
            {'$set': {'startedAt': datetime.utcnow()}},
            {'sort': {'priority': -1}}
        )
        if user_document:
            users_to_analyse.append(user_document)
        else:
            time.sleep(BATCH_REFRESH_WAITING_TIME)  # wait to poll again if there are no users in queue to be analysed

    @mongo_retry(log=log)
    def updateQueue(self, analysed_users: List[BDoc]) -> None:
        # update queue, bulk write should be much faster for huge batch
        operations = [pymongo.UpdateOne({'_id': user['_id']}, {'$set': {'response': user['response']}}) for user in analysed_users]
        self.kaladin_queue_coll.bulk_write(operations)


    def analyseOneUser(self, user_to_analyse: BDoc) -> None:
        analysed_user = self.analysisFunction([user_to_analyse])
        log.debug(f'Analysis: {analysed_user}')
        self.updateQueue(analysed_user)

if __name__ == '__main__':
    QueueManager().run()
