import os
import bson
import pymongo
import certifi
from faker import Faker
from datetime import datetime
import multiprocessing
from multiprocessing import Queue
from random import choice, randint

REQUESTS = 20
CONCURRENCY = 20
DOCS_PER_REQUEST = 100
DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CLUSTER = f"cluster0.qsg3m.mongodb.net/stasGreatDB"
TARGET_DB = "stasGreatDB"
TARGET_COLL = "test"
FAKE = Faker()


def main():
    # Drop collection if it exists
    drop_collection_if_has_docs()

    # Insert documents
    # insert(i=1)  # debug single run

    multiprocessing.Pool(CONCURRENCY).map(insert, range(REQUESTS))


def drop_collection_if_has_docs(db_name=TARGET_DB, collection_name=TARGET_COLL, docs_threshold=0):
    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority",
                                 ssl_ca_certs=certifi.where())
    db = client[db_name]
    collection = db[collection_name]

    if collection.estimated_document_count() > docs_threshold:
        collection.drop()


def insert(i):
    print(multiprocessing.current_process())
    id_1 = id_factory(value=(i*DOCS_PER_REQUEST))
    id_2 = id_factory()

    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority",
                                 ssl_ca_certs=certifi.where())
    db = client[TARGET_DB]
    collection = db[TARGET_COLL]
    docs = list()
    Faker.seed(randint(1, 9999))
    for j in range(DOCS_PER_REQUEST):
        d_id = next(id_2)
        id_val = next(id_1)
        dd = datetime(randint(2019, 2021), randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59),
                      randint(0, 59))

        docs.append(
            {
                ################################################################################################
                # THE DOCUMENT                                                                                 #
                ################################################################################################
                "id": id_val,
                "object": bson.ObjectId(),
                "date": dd,
                "sts": datetime.timestamp(dd),
                "msts": (datetime.timestamp(dd) * 1000) + 500,
                "description": FAKE.text(),
                "active": choice([True, False]),
                "public": choice([True, False]),
                "location": [randint(0, 90), randint(0, 90)],
                "person": {
                    "name": FAKE.first_name(),
                    "lastname": FAKE.last_name(),
                    "address": FAKE.address(),
                },
                "receiptNumber": str(randint(0, 3000)),
                "status": choice(["created", "claimed", "other"]),
                "score": randint(1, 100),
                "source": f"source_{randint(1, 3)}",

                ################################################################################################
                # THE DOCUMENT                                                                                 #
                ################################################################################################
            }
        )
    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(f"Iteration {i}: Got result '{resp.acknowledged}'")
    else:
        print(f"Iteration {i} Done.")


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


if __name__ == "__main__":
    main()
