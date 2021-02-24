import pymongo
import bson
from pymongo.errors import DocumentTooLarge, WriteError
import multiprocessing
from multiprocessing import Pool, Queue
from datetime import datetime
from faker import Faker
from random import randint, choice
from uuid import uuid4


REQUESTS = 100
CONCURRENCY = 20
DOCS_PER_REQUEST = 1000

CONN_STR = "mongodb://localhost:6070,localhost:6071,localhost:6072"

TARGET_DB = "test"
TARGET_COLL = "test"
DROP = True

FAKE = Faker()


def main():

    if DROP:
        drop_collection_if_has_docs()

    multiprocessing.Pool(CONCURRENCY).map(insert, range(REQUESTS))


def insert(i):
    print(multiprocessing.current_process())

    client = pymongo.MongoClient(CONN_STR)
    db = client[TARGET_DB]
    collection = db[TARGET_COLL]

    docs = list()
    Faker.seed(randint(1, 9999))
    id_1 = id_factory(value=(i*DOCS_PER_REQUEST))

    for j in range(DOCS_PER_REQUEST):
        docs.append(
            {
                ################################################################################################
                # THE DOCUMENT                                                                                 #
                ################################################################################################
                "id": next(id_1),
                "object": bson.ObjectId(),
                "date": datetime(randint(2019, 2021), randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59),
                                 randint(0, 59)),
                "description": FAKE.text(),
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


def drop_collection_if_has_docs(db_name=TARGET_DB, collection_name=TARGET_COLL, docs_threshold=0):
    client = pymongo.MongoClient(CONN_STR)
    db = client[db_name]
    collection = db[collection_name]

    if collection.estimated_document_count() > docs_threshold:
        collection.drop()


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


if __name__ == "__main__":
    main()
