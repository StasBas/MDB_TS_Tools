import os
import json
import pymongo
import pprint
import certifi
from pprint import pprint
from datetime import datetime, timezone
import bson

# DB_USER = os.environ.get("dbuser")
# DB_PASS = os.environ.get("dbpass")
CONN_STR = "mongodb://localhost:6070,localhost:6071,localhost:6072"
TARGET_DB = "test"
TARGET_COLL = "test"


def main():
    client = pymongo.MongoClient(CONN_STR)
    database = client[TARGET_DB]
    collection = database[TARGET_COLL]

    qfilter = {}
    cursor = collection.find(qfilter, batch_size=1)

    for item in cursor:
        print(item)

    # pipeline = [
    #     {
    #         "$match": {
    #             "receiptNumber": {"$in": [str(i) for i in range(1, 200)]},
    #             "status": {"$in": ["created", "claimed"]}
    #         }
    #     },
    #     {
    #         "$sort": {"date": 1}
    #     }
    # ]

    # cursor = collection.aggregate(pipeline)
    # for item in cursor:
    #     print(item)

    # exp = database.command('aggregate', TARGET_COLL, pipeline=pipeline, explain=True)
    # pprint(exp)

    # print(target_timestamp, timestamped_id)


if __name__ == "__main__":
    main()
