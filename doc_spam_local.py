import pymongo
import bson
from pymongo.errors import DocumentTooLarge, WriteError
import multiprocessing
from multiprocessing import Pool, Queue
from datetime import datetime
from faker import Faker
from random import randint, choice
from uuid import uuid4

CONN_STR = "mongodb://localhost:6070,localhost:6071,localhost:6072"
REQUESTS = 100  # REQUESTS (will affect duration)
CONCURRENCY = 10  # MAX CONCURRENCY (macs die past 30)
DOCS_PER_REQUEST = 1000  # DOCS TO INSERT PER REQUEST
TARGET_DB = "test"  # DB TO SPAM
TARGET_COLL = "test"  # COLLECTION TO SPAM
DROP = True  # DROP COLLECTION BEFORE SPAM
FAKE = Faker()
CLIENT = None
LOG_COLL = True
LOG_COLL_NAME = None
FORK_CLIENT = False


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
    client = pymongo.MongoClient(CONN_STR)
    db = client[db_name]
    collection = db[collection_name]
    if collection.estimated_document_count() > docs_threshold:
        collection.drop()


def insert(i):
    wrkr = multiprocessing.current_process().name
    if LOG_COLL:
        global LOG_COLL_NAME
        if not LOG_COLL_NAME:
            LOG_COLL_NAME = datetime.now().strftime("spam_log_%Y%m%d%H%M%S")
        log_coll = get_collection(coll_name=LOG_COLL_NAME)
        log_coll.insert_one({"iteration": i, "start": datetime.now(), "worker": wrkr})
    print(f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i} Start")

    collection = get_collection()

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
            f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i} Done. "
            f"Documents existing now: {collection.count_documents({})}"
        )


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


def get_client():
    global CLIENT
    if not CLIENT:
        CLIENT = pymongo.MongoClient(CONN_STR)
    return CLIENT


def get_collection(db_name=TARGET_DB, coll_name=TARGET_COLL):
    client = get_client()
    db = client[db_name]
    collection = db[coll_name]
    return collection


if __name__ == "__main__":
    main()
