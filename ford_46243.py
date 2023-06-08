import pymongo
import certifi
# import bson
# from pymongo.errors import DocumentTooLarge, WriteError
import multiprocessing
# from multiprocessing import Pool, Queue
from datetime import datetime
from faker import Faker
from random import randint, choice
from uuid import uuid4

CONN_STR = "mongodb://localhost:5070"
# CONN_STR = os.environ.get("CONNSTR")
REQUESTS = 10000  # REQUESTS (will affect duration)
CONCURRENCY = 20  # MAX CONCURRENCY (macs die past 30)
DOCS_PER_REQUEST = 1000  # DOCS TO INSERT PER REQUEST
TARGET_DB = "test"  # DB TO SPAM
TARGET_COLL = "test"  # COLLECTION TO SPAM
DROP = False  # DROP COLLECTION BEFORE SPAM
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
    # client = pymongo.MongoClient(CONN_STR, ssl_ca_certs=certifi.where())
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
    id_1 = id_factory(value=((i + 5000000) * DOCS_PER_REQUEST))
    id_2 = id_factory()

    for j in range(DOCS_PER_REQUEST):
        d_id = next(id_2)
        id_val = next(id_1)
        dd = datetime(randint(2019, 2022), randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59),
                      randint(0, 59))

        doc_arr = list()
        for n in range(randint(0, 1)):
            t = randint(65, 88)
            doc_arr.append({
                # "in_id": n + 1,
                "in_id": randint(1, 5),
                "str_id": f"identifier{n + 1}",
                "type": f"{chr(t)}{chr(t + 1)}{chr(t + 2)}"
            })

        doc = {
            ################################################################################################
            # THE DOCUMENT                                                                                 #
            ################################################################################################
            "id": id_val,
            "object": uuid4().hex,
            "date_rolled": dd,
            "ts_ms": (datetime.timestamp(dd) * 1000) + randint(100, 999),
            "description": FAKE.text(),
            "keyword": str(FAKE.text()).split(' ')[0],
            "active": choice([True, False]),
            "public": choice([True, False]),
            "location": [randint(0, 90), randint(0, 90)],
            "words_array": FAKE.text().replace(".", "").replace("\n", "").split(" "),
            "person": {
                "name": choice([FAKE.first_name(), None]),
                "lastname": FAKE.last_name(),
                "eye_color": choice(["blue", "green", "crimson", "brown", "black"]),
                "hair_color": choice(["bold", "blond", "black", "white", "grey"]),
                "address": FAKE.address(),
            },
            "receiptNumber": str(randint(0, 3000)),
            "status": choice(["created", "claimed", "other"]),
            "score": randint(1, 10000),
            "source": f"source_{randint(1, 3)}",
            "obj_array": [
                {
                    "name": f"name{randint(0, 1000)}",
                    "type": randint(1, 10)
                },
                {
                    "name": f"name{randint(0, 1000)}",
                    "type": randint(1, 10)
                },
                {
                    "name": f"name{randint(0, 1000)}",
                    "type": randint(1, 10)
                },
            ],
            "nest_obj_arr": {
                "ex_type": choice(["t1", "t2", "t3"]),
                "obj_arr": doc_arr
            },
            "nest_obj_obj": {
                "type": choice(["t1", "t2", "t3"]),
                "properties": {
                    "prop1": choice(["p1", "p2", "p3"]),
                    "prop2": choice(["p1", "p2", "p3"]),
                    "prop3": choice(["p1", "p2", "p3"]),
                    "prop4": choice(["p1", "p2", "p3"])
                }
            },
            "internal": {
                "iteration": i,
                "internal_id": d_id,
                "date_created": datetime.now(),
            },

            ################################################################################################
            # THE DOCUMENT                                                                                 #
            ################################################################################################
        }
        if (randint(0, 1)):
            doc["ts_partially_existing"] = (datetime.timestamp(dd) * 1000) + randint(100, 999)
        docs.append(doc)

    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(
            f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i}: Got result '{resp.acknowledged}'")
    else:
        if LOG_COLL:
            log_coll.update_one(filter={"iteration": i}, update={"$set": {"end": datetime.now()}})
        # print(
        #     f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {wrkr}: Iteration {i} Done. "
        #     f"Documents existing now: {collection.count_documents({})}"
        # )


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


def get_client():
    global CLIENT
    if not CLIENT:
        CLIENT = pymongo.MongoClient(CONN_STR)
        # CLIENT = pymongo.MongoClient(CONN_STR, ssl_ca_certs=certifi.where())
    return CLIENT


def get_collection(db_name=TARGET_DB, coll_name=TARGET_COLL):
    client = get_client()
    db = client[db_name]
    collection = db[coll_name]
    return collection


if __name__ == "__main__":
    main()
