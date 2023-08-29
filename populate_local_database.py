import pymongo
import logging
# import certifi
# import bson
# from pymongo.errors import DocumentTooLarge, WriteError
import multiprocessing
from datetime import datetime
from faker import Faker
from random import randint, choice
from uuid import uuid4
import argparse
import tkinter as tk

import queue
import threading
import _thread
import time

CONN_STR = "mongodb://localhost:6070,localhost:6071,localhost:6072"
ITERATIONS = 30  # REQUESTS (will affect duration)
CONCURRENCY = 10  # MAX CONCURRENCY (macs die past 30)
DOCS_COUNT = 1000  # DOCS TO INSERT PER REQUEST
TARGET_DB = "test"  # DB TO SPAM
TARGET_COLL = "test"  # COLLECTION TO SPAM
DROP = True  # DROP COLLECTION BEFORE SPAM

LOG_COLL = False
LOG_COLL_NAME = None
FORK_CLIENT = False

FAKE = Faker()
CLIENT = None
DONE = False


def main():
    print(f"Usage: <populate db script> [-h] [-c CONNECTION_STRING] [-db DATABASE] "
          f"[-coll COLLECTION] [-i ITERATIONS]"
          f"[-dc DOCS_COUNT] [-w CONCURRENCY] [-drop DROP]\n")

    if not CONN_STR:
        print(f"To run without UI provide \"CONNECTION_STRING\" parameter in CLI.\n")
        ui_main().mainloop()
    else:
        populate_db()


def populate_db(ui_ran: bool = False):
    # Read Params
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', "--connection_string", help=f"Log file path. ex: \"mongodb://localhost:5000\"",
                        required=False,
                        default=CONN_STR.get() if isinstance(CONN_STR, tk.StringVar) else CONN_STR)
    parser.add_argument('-db', "--database", help=f"DB Name. ex: \"myDb\"",
                        default=TARGET_DB.get() if isinstance(TARGET_DB, tk.StringVar) else TARGET_DB)
    parser.add_argument('-coll', "--collection", help=f"Collection Name. ex: \"myCollection\"",
                        default=TARGET_COLL.get() if isinstance(TARGET_COLL, tk.StringVar) else TARGET_COLL)
    parser.add_argument('-i', "--iterations", help=f"Number of iterations. ex: 10", type=int,
                        default=ITERATIONS.get() if isinstance(ITERATIONS, tk.IntVar) else ITERATIONS)
    parser.add_argument('-dc', "--docs_count", help=f"Number of documents per iteration. ex: 100", type=int,
                        default=DOCS_COUNT.get() if isinstance(DOCS_COUNT, tk.IntVar) else DOCS_COUNT)
    parser.add_argument('-w', "--concurrency", help=f"Concurrency to run keep under 30 on macs. ex: 100", type=int,
                        default=CONCURRENCY.get() if isinstance(CONCURRENCY, tk.IntVar) else CONCURRENCY)
    parser.add_argument('-drop', "--drop", help=f"Drop target database. (1/0)", type=bool,
                        default=DROP.get() if isinstance(DROP, tk.BooleanVar) else DROP)
    args = parser.parse_args()

    # Print Params
    print(f"\n---\n")

    if ui_ran:
        print("Entered Params:")
    else:
        print("Running with params:")
    for k, v in vars(args).items():
        print(f"\t{k}:{' ' * (18 - len(k))}{v}")
    print("\n")

    if args.drop:
        drop_collection_if_has_docs(args.database, args.collection, args.connection_string)

    if FORK_CLIENT:
        client = get_client()
        client["test"].command("ping")

    global DONE
    DONE = False

    _thread.start_new_thread(side_thread_work, (args.collection, args.database, args.connection_string))

    print(f"Starting {multiprocessing.current_process().name}\n")

    ops = [(i, args.connection_string, args.database, args.collection, args.docs_count) for i in range(args.iterations)]
    time.sleep(10)  # prs = multiprocessing.Pool(args.concurrency).map(insert, ops)

    DONE = True
    time.sleep(1)

    print("\n\nDone.")


def get_value(x):
    return x.get() if isinstance(x, tk.StringVar) or isinstance(x, tk.IntVar) or isinstance(x, tk.BooleanVar) else x


def side_thread_work(c, d, cn):
    time.sleep(1)
    while not DONE:
        time.sleep(0.3)
        print(end=f"\rCollection \"{c}\" Estimated Documents: "
                  f"{count_collection_documents(c, d, cn):,d}")


def drop_collection_if_has_docs(db_name, collection_name, connection_string, docs_threshold=0):
    client = pymongo.MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]
    if collection.estimated_document_count() > docs_threshold:
        collection.drop()
    client.close()


def count_collection_documents(collection, database, conn_str):
    client = pymongo.MongoClient(conn_str)
    collection = client[database][collection]
    # count = collection.count_documents({})
    count = collection.estimated_document_count()
    client.close()
    return count


def insert(params):
    iteration, connection_string, database, collection, docs_count = params

    worker_name = multiprocessing.current_process().name

    if LOG_COLL:
        global LOG_COLL_NAME
        if not LOG_COLL_NAME:
            LOG_COLL_NAME = datetime.now().strftime("spam_log_%Y%m%d%H%M%S")
        log_coll = get_collection(coll_name=LOG_COLL_NAME)
        log_coll.insert_one({"iteration": iteration, "start": datetime.now(), "worker": worker_name})
    # print(f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: Iteration {iteration} Start")

    collection = get_collection(database, collection, connection_string)

    docs = list()
    Faker.seed(randint(1, 9999))
    id_1 = id_factory(value=((iteration + 5000000) * docs_count))
    id_2 = id_factory()

    for j in range(docs_count):
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
                "iteration": iteration,
                "internal_id": d_id,
                "date_created": datetime.now(),
            },

            ################################################################################################
            # THE DOCUMENT                                                                                 #
            ################################################################################################
        }

        # Add partial field to documents
        if randint(0, 1):
            doc["timeStamp_partially_existing"] = (datetime.timestamp(dd) * 1000) + randint(100, 999)
        docs.append(doc)

    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(
            f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: "
            f"Iteration {iteration}: Got result '{resp.acknowledged}'")
    else:
        if LOG_COLL:
            log_coll.update_one(filter={"iteration": iteration}, update={"$set": {"end": datetime.now()}})
        # print(
        #     f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: Iteration {iteration} Done. "
        #     f"Documents existing now: {collection.count_documents({})}"
        # )


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


def get_client(conn_str=None):
    global CLIENT
    if not CLIENT:
        conn_str = conn_str if conn_str is not None else get_value(CONN_STR)
        CLIENT = pymongo.MongoClient(get_value(conn_str))
        logging.debug(f"{multiprocessing.current_process().name} Opening new client on \"{conn_str}\"")
        print(f"{multiprocessing.current_process().name} opening new client")
    return CLIENT


def get_collection(db_name=None, coll_name=None, connection_string=None):
    db_name = db_name if db_name is not None else get_value(TARGET_DB)
    coll_name = coll_name if coll_name is not None else get_value(TARGET_COLL)
    connection_string = connection_string if connection_string is not None else get_value(CONN_STR)
    client = get_client(connection_string)
    db = client[db_name]
    collection = db[coll_name]
    return collection


def ui_main():
    root = tk.Tk()
    root.resizable(False, False)
    root.title("Populate MongoDB")

    tk.Label(root, text="MongoDB Populate Database Script").pack(pady=5)

    # Define Canvas and Form
    size = (600, 400)
    main_canvas = tk.Canvas(root, width=size[0], height=size[1])
    main_frame = tk.Frame(root)
    buttons_frame = tk.Frame(root)

    # Make scrollbars bound to root and scrolling form canvas
    h_bar = tk.Scrollbar(root, orient=tk.HORIZONTAL)
    h_bar.pack(side=tk.BOTTOM, fill=tk.X)
    h_bar.config(command=main_canvas.xview)
    v_bar = tk.Scrollbar(root, orient=tk.VERTICAL)
    v_bar.pack(side=tk.RIGHT, fill=tk.Y)
    v_bar.config(command=main_canvas.yview)
    main_canvas.configure(xscrollcommand=h_bar.set, yscrollcommand=v_bar.set)

    # Position Canvas and Form frame
    main_canvas.pack(anchor=tk.NW)
    main_canvas.create_window(0.015 * size[0], 0.1 * size[1], window=main_frame, anchor=tk.NW)
    main_canvas.bind("<Configure>", lambda event: main_canvas.configure(scrollregion=main_canvas.bbox("all")))

    structure_params_form(main_frame)

    # Place buttons frame
    buttons_frame.pack(anchor=tk.SE, expand=0)  # , fill=tk.Y, expand=True)

    # Buttons Frame Buttons
    tk.Button(buttons_frame, text="Execute", command=lambda: populate_db(True)).pack(side="right")
    tk.Button(buttons_frame, text="Exit", command=lambda: close_window(root)).pack(side='left')

    # Enable use of enter and escape.
    root.bind('<Return>', lambda event: populate_db(True))
    root.bind('<Escape>', lambda event: close_window(root))

    return root


def close_window(window):
    window.destroy()


def check_params(*args, **kwargs):
    for arg in args:
        print(arg)

    for k, v in kwargs.items():
        print(f"{k}: {v}")


def structure_params_form(my_frame: tk.Frame):
    tk.Label(my_frame, text="Connection String: ").grid(row=10, column=10, sticky=tk.W)
    global CONN_STR
    CONN_STR = tk.StringVar()
    # CONN_STR.set("mongodb://localhost:5000")
    CONN_STR.set("mongodb://localhost:6070,localhost:6071,localhost:6072")
    tk.Entry(my_frame, textvariable=CONN_STR).grid(row=10, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Database Name: ").grid(row=20, column=10, sticky=tk.W)
    global TARGET_DB
    TARGET_DB = tk.StringVar()
    TARGET_DB.set("PopulatedDataBase")
    tk.Entry(my_frame, textvariable=TARGET_DB).grid(row=20, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Collection Base Name: ").grid(row=30, column=10, sticky=tk.W)
    global TARGET_COLL
    TARGET_COLL = tk.StringVar()
    TARGET_COLL.set("PopulatedCollection")
    tk.Entry(my_frame, textvariable=TARGET_COLL).grid(row=30, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Iterations count: ").grid(row=40, column=10, sticky=tk.W)
    global ITERATIONS
    ITERATIONS = tk.IntVar()
    ITERATIONS.set(20)
    tk.Entry(my_frame, textvariable=ITERATIONS).grid(row=40, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Documents per iteration: ").grid(row=50, column=10, sticky=tk.W)
    global DOCS_COUNT
    DOCS_COUNT = tk.IntVar()
    DOCS_COUNT.set(1000)
    tk.Entry(my_frame, textvariable=DOCS_COUNT).grid(row=50, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Concurrency: ").grid(row=60, column=10, sticky=tk.W)
    global CONCURRENCY
    CONCURRENCY = tk.IntVar()
    CONCURRENCY.set(10)
    tk.Entry(my_frame, textvariable=CONCURRENCY).grid(row=60, column=20, sticky=tk.W)

    tk.Label(my_frame, text="Drop Collection: ").grid(row=70, column=10, sticky=tk.W)
    global DROP
    DROP = tk.BooleanVar()
    DROP.set(True)
    tk.Checkbutton(my_frame, text='', variable=DROP, onvalue=1, offvalue=0).grid(row=70, column=20)

    tk.Label(my_frame, text="Log collection: ").grid(row=80, column=10, sticky=tk.W)
    _ = tk.IntVar()
    _.set(0)
    tk.Checkbutton(my_frame, text='', variable=_, onvalue=1, offvalue=0).grid(row=80, column=20)
    tk.Label(my_frame, text="NOT IMPLEMENTED: Create a collection with log of inserts").grid(
        row=80, column=30, sticky=tk.W)

    tk.Label(my_frame, text="Log collection name: ").grid(row=90, column=10, sticky=tk.W)
    _ = tk.StringVar()
    # _.set("log_collection")
    tk.Entry(my_frame, textvariable=_).grid(row=90, column=20, sticky=tk.W)
    tk.Label(my_frame, text="NOT IMPLEMENTED: Name of the log collection (will generate default if empty)").grid(
        row=90, column=30, sticky=tk.W)

    tk.Label(my_frame, text="Fork Client: ").grid(row=100, column=10, sticky=tk.W)
    _ = tk.IntVar()
    _.set(0)
    tk.Checkbutton(my_frame, text='', variable=_, onvalue=1, offvalue=0).grid(row=100, column=20)
    tk.Label(my_frame, text="NOT IMPLEMENTED: One client to rule them all. Pymongo don't like that but it works").grid(
        row=100, column=30, sticky=tk.W)


if __name__ == "__main__":
    main()
