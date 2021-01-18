import os
import pymongo
import certifi
import pprint
from datetime import datetime, timezone
import bson

DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CLUSTER = f"cluster0.qsg3m.mongodb.net/pmdb"
TARGET_DB = "sample_supplies"
TARGET_COLL = "sales"


def main():
    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority",
                                 ssl_ca_certs=certifi.where())
    database = client[TARGET_DB]
    collection = database[TARGET_COLL]

    # my_agg = [{'$addFields': {'timestamp': {'$toDate': '$_id'}, '_id': 0}},
    #           {'$match': {'timestamp': {'$gte': datetime(2017, 1, 1, 0, 0, 0, tzinfo=timezone.utc)}}}]
    # cursor = collection.aggregate(pipeline=my_agg)
    # a = cursor.next()
    # print(a)

    target_timestamp = datetime(2020, 1, 1, 10, 30, 00)
    timestamped_id = bson.ObjectId.from_datetime(target_timestamp)
    # result = collection.find({"_id": {"$lt": dummy_id}})

    print(target_timestamp, timestamped_id)


if __name__ == "__main__":
    main()
