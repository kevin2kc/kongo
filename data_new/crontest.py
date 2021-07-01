import random

import dataconnection
import random
import datetime
import json

db = dataconnection.mongodbconnection()

for i in range(1, 5):
    data = {'id': random.randint(1, 100), 'time': datetime.datetime.now()}

    try:

        db.crontest.insert_one(data)
    except Exception as exp:
        print(exp)