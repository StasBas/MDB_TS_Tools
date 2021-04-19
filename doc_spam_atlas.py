import os
import pymongo
import certifi
from faker import Faker
from datetime import datetime
import multiprocessing
import bson
from multiprocessing import Queue
from random import choice, randint

DB_USER = os.environ.get("dbuser")  # USERNAME (add to env "export <username>")
DB_PASS = os.environ.get("dbpass")  # PASSWORD (add to env "export <password>")
CLUSTER = "cluster1.efy5d.mongodb.net/test"  # CLUSTER ADDRESS
CONN_STR = f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority"

REQUESTS = 10  # TOTAL NUMBER OF REQUESTS SENT TO DB (will affect duration)
CONCURRENCY = 10  # MAX CONCURRENT REQUESTS (macs die past 30)
DOCS_PER_REQUEST = 1000  # DOCS TO INSERT PER REQUEST
TARGET_DB = "test"  # DB TO SPAM
TARGET_COLL = "test"  # COLLECTION TO SPAM
DROP = True  # DROP COLLECTION BEFORE SPAM
FAKE = Faker()
CLIENT = None
LOG_COLL = True
LOG_COLL_NAME = None
FORK_CLIENT = False
SPLIT_COLLECTIONS = 0

RAMP_UP_SECONDS = 60  # TODO: not utilized
MAX_DURATION = None  # TODO: not utilized


def main():
    if DROP:
        drop_collection_if_has_docs()

    if FORK_CLIENT:
        client = get_client()
        client["test"].command("ping")

    a = multiprocessing.current_process()
    print(a)

    multiprocessing.Pool(CONCURRENCY).map(insert, range(REQUESTS))


def drop_collection_if_has_docs(db_name=TARGET_DB, collection_name=TARGET_COLL, docs_threshold=0):
    client = pymongo.MongoClient(CONN_STR, ssl_ca_certs=certifi.where())
    db = client[db_name]
    collection = db[collection_name]
    if collection.estimated_document_count() > docs_threshold:
        collection.drop()


def insert(i):
    wrkr = multiprocessing.current_process().name

    if SPLIT_COLLECTIONS:
        collection = get_collection(coll_name=choice([TARGET_COLL+f"_{i}" for i in range(SPLIT_COLLECTIONS)]))
    else:
        collection = get_collection()

    if LOG_COLL:
        global LOG_COLL_NAME
        if not LOG_COLL_NAME:
            LOG_COLL_NAME = datetime.now().strftime(f"spam_{TARGET_COLL}_%Y%m%d%H%M%S")
        log_coll = get_collection(coll_name=LOG_COLL_NAME)
        log_coll.insert_one({"iteration": i, "start": datetime.now(), "worker": wrkr, "collection": collection.name})
    print(f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i} Start")

    docs = list()
    Faker.seed(randint(1, 9999))
    id_1 = id_factory(value=(i * DOCS_PER_REQUEST))
    id_2 = id_factory()

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
                "date_rolled": dd,
                "ts_ms": (datetime.timestamp(dd) * 1000) + randint(100, 999),
                "description": FAKE.text(),
                "keyword": str(FAKE.text()).split(' ')[0],
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
                "internal": {
                    "iteration": i,
                    "internal_id": d_id,
                    "date_created": datetime.now(),
                },

                ################################################################################################
                # THE DOCUMENT                                                                                 #
                ################################################################################################
            }
        )

    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(
            f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i}: Got result '{resp.acknowledged}'")
    else:
        if LOG_COLL:
            log_coll.update_one(filter={"iteration": i}, update={"$set": {"end": datetime.now()}})
        print(
            f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} ({TARGET_DB}.{collection.name}) {wrkr}: Iteration {i} Done. "
            f"Documents committed: {collection.count_documents({})}"  # TODO: Replace with count to reduce stress
        )


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


def get_client():
    global CLIENT
    if not CLIENT:
        CLIENT = pymongo.MongoClient(CONN_STR, ssl_ca_certs=certifi.where())
    return CLIENT


def get_collection(db_name=TARGET_DB, coll_name=TARGET_COLL):
    client = get_client()
    db = client[db_name]
    collection = db[coll_name]
    return collection


if __name__ == "__main__":
    main()
