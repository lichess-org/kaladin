import bson
import pickle

import random
import string


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


with open('insight_test_case_10.bson', 'rb') as f:
    data = bson.decode_all(f.read())

users = set()

for game in data:
    users.add(game['u'])

rename = {user: randomword(10) for user in users}

for game in data:
    game['_id'] = randomword(8)
    game['u'] = rename[game['u']]

with open('anon_insights.pkl', 'wb') as f:
    pickle.dump(data, f)
