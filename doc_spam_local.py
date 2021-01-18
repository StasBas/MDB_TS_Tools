import pymongo
import bson
from pymongo.errors import DocumentTooLarge, WriteError
import multiprocessing
from multiprocessing import Pool, Queue
from datetime import datetime
from faker import Faker
from random import randint, choice


REQUESTS = 2
CONCURRENCY = 2
DOCS_PER_REQUEST = 100

PORT = 5070
HOST = "localhost"
TARGET_DB = "pmdb"
TARGET_COLL = "pmcl"

QIN = Queue()
FAKE = Faker()


def main():

    # client = pymongo.MongoClient(host=HOST, port=PORT)
    # db = client[TARGET_DB]
    # collection = db[TARGET_COLL]

    drop_collection_if_has_docs()

    ###############################################################################################################
    # Patch - Find smarter way later - generate queue of ids for unique ids in multiple processes * multiple docs #
    ###############################################################################################################
    global QIN
    rid = id_factory()
    for i in range(1, REQUESTS * DOCS_PER_REQUEST+1):
        QIN.put(i)
    ###############################################################################################################

    multiprocessing.Pool(CONCURRENCY).map(insert, range(REQUESTS))


def insert(i):
    # global ID_1
    id_1 = id_factory()

    client = pymongo.MongoClient(host=HOST, port=PORT)
    db = client[TARGET_DB]
    collection = db[TARGET_COLL]
    docs = list()
    for j in range(DOCS_PER_REQUEST):
        docs.append(
            {
                ################################################################################################
                # THE DOCUMENT                                                                                 #
                ################################################################################################
                "id": QIN.get(),
                "date": datetime(randint(2019, 2021), randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59),
                                 randint(0, 59)),
                "description": FAKE.text(),
                "person": {
                    "name": FAKE.first_name(),
                    "lastname": FAKE.last_name(),
                    "address": FAKE.address(),
                }
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
    client = pymongo.MongoClient(host=HOST, port=PORT)
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
