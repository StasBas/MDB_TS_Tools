import os
import pymongo
import certifi
from pprint import pprint
from datetime import datetime, timezone
import bson

DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CONN_STR = None  # "mongodb+srv://<username>:<password>@cluster0.qsg3m.mongodb.net/?retryWrites=true&w=majority"
TARGET_DB = "test"
TARGET_COLL = "test"


def main():
    client = get_client_util()
    agg_explain(client, TARGET_DB, TARGET_COLL)


def agg_explain(client, db_name, coll_name):
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

    db = client[db_name]
    exp = db.command('aggregate', coll_name, pipeline=pipeline, explain=True)
    pprint(exp)

    cursor = db[coll_name].aggregate(pipeline)
    for i in range(3):
        cursor.next()

    print("\n\n")
    print(client.admin.command(({'setParameter': 1, 'transactionLifetimeLimitSeconds': 100})))


def find_index_name(client: pymongo.MongoClient, index_keys: dict, db: str, coll: str):
    collection = client[db][coll]
    pipeline = [
        {"$indexStats": {}},
        {"$match": {"spec.key": index_keys}}
    ]
    cursor = collection.aggregate(pipeline)
    try:
        return [item for item in cursor][0].get('name')
    except IndexError:
        print("index Not Found")
        return None


def batch_size_query(client, db_name, coll_name, batch_size=1):
    qfilter = {}
    collection = client[db_name][coll_name]
    cursor = collection.find(qfilter, batch_size=batch_size)
    for item in cursor:
        print(item)


def get_client_util():
    global CONN_STR
    global DB_USER
    global DB_PASS
    if not CONN_STR:
        CONN_STR = input("Paste driver connection string:\n")
    connection_string = CONN_STR.replace("<username>", str(DB_USER)).replace("<password>", str(DB_PASS))
    client = pymongo.MongoClient(connection_string, tlsAllowInvalidCertificates=True)
    return client


if __name__ == "__main__":
    main()
