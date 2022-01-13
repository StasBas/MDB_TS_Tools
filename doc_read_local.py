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
CONN_STR = "mongodb://localhost:5070"  # "mongodb://localhost:6070,localhost:6071,localhost:6072"
TARGET_DB = "test"
TARGET_COLL = "test"


def main():
    client = pymongo.MongoClient(CONN_STR)
    database = client[TARGET_DB]
    collection = database[TARGET_COLL]

    # QUERY #
    # qfilter = {}
    # cursor = collection.find(qfilter, batch_size=1)
    #
    # for item in cursor:
    #     print(item)

    # AGGREGATION #
    pipeline = [
        {
            "$match": {
                "receiptNumber": {"$in": [str(i) for i in range(1, 200)]},
                "status": {"$in": ["created", "claimed"]}
            }
        },
        {
            "$sort": {"date": 1}
        }
    ]
    cursor = collection.aggregate(pipeline)
    for item in cursor:
        print(item)

    print("\n\n")
    print(client.admin.command(({'setParameter': 1, 'transactionLifetimeLimitSeconds': 100})))

    # exp = database.command('aggregate', TARGET_COLL, pipeline=pipeline, explain=True)
    # pprint(exp)

    # print(target_timestamp, timestamped_id)

    # index_name = find_index(client, {"scor": 1, "source": 1})
    # print(index_name)


def find_index(client: pymongo.MongoClient, index_keys: dict, db: str = "test", coll: str = "test"):
    collection = client[db][coll]
    pipeline = [
        {"$indexStats": {}},
        {"$match": {"spec.key": index_keys}}
    ]
    cursor = collection.aggregate(pipeline)
    try:
        return [item for item in cursor][0].get('name')
    except IndexError:
        print("no such index")


if __name__ == "__main__":
    main()
