import os
import json
import pymongo
import certifi
from pprint import pprint
from datetime import datetime, timezone
import bson

DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CLUSTER = f"cluster0.qsg3m.mongodb.net/pmdb"
TARGET_DB = "test"
TARGET_COLL = "test"


def main():
    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority",
                                 tlsCAFile=certifi.where())
    database = client[TARGET_DB]
    collection = database[TARGET_COLL]

    # qfilter = (json.loads(open("find.txt").read()))
    # cursor = collection.find(qfilter)
    # a = cursor.next()
    # print(a)

    pipeline = [
        {
            "$match": {
                "receiptNumber": {"$in": [str(i) for i in range(1, 200)]},
                "status": {"$in": ["created", "claimed"]}
            }
        },
        {
            "$sort": {"time": 1}
        }
    ]

    # cursor = collection.aggregate(pipeline)
    # for item in cursor:
    #     print(item)

    exp = database.command('aggregate', TARGET_COLL, pipeline=pipeline, explain=True)
    pprint(exp)

    # print(target_timestamp, timestamped_id)


if __name__ == "__main__":
    main()
