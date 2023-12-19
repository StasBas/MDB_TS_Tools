import multiprocessing
import os
import random
import time
import json
import pymongo
import logging
import certifi

from datetime import datetime
from faker import Faker
from random import randint, choice
from uuid import uuid4

import argparse
import tkinter as tk
from tkinter import ttk

from threading import Thread
import _thread
from multiprocessing import Process, Queue, freeze_support, Event

CONN_STR = "mongodb://localhost:27017"

# EXECUTION
ITERATIONS = 100  # REQUESTS (will affect duration)
CONCURRENCY = os.cpu_count()  # MAX WORKERS
DOCS_COUNT = 1000  # DOCS TO INSERT PER REQUEST
TARGET_DB = "test"  # DB TO SPAM
TARGET_COLL = "test"  # COLLECTION TO SPAM
DROP = True  # DROP COLLECTION BEFORE SPAM
SAMPLE_DOC_PATH = None  # "~/Documents/multiDoc.json"  # PATH TO SAMPLE DOCUMENT
BULK = False  # Use bulk insertOne instead of insertMany

# AUTH
USERNAME = "stas"
PASSWORD = ''
TLS = False

# INTERNAL
LOG_COLL = False
LOG_COLL_NAME = None
FORK_CLIENT = False

# UTIL
FAKE = Faker()
CLIENT = None


def main():
    # Read Params
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', "--connection_string", help=f"Connection String. ex: \"mongodb://localhost:5000\"",
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
    parser.add_argument('-s', "--sample", help=f"Sample Document Path. Target must be JSON formatted. "
                                               f"ex ~/Documents/myDod.json", type=str,
                        default=get_value(SAMPLE_DOC_PATH))
    parser.add_argument('-drop', "--drop", help=f"Drop target database. (1/0)", type=bool,
                        default=DROP.get() if isinstance(DROP, tk.BooleanVar) else DROP)
    parser.add_argument('-b', "--bulk", help=f"Use Bulk insertOne instead of insertMany (1/0)", type=bool,
                        default=BULK)

    parser.add_argument('-u', "--username", help=f"Atlas username",
                        default=get_value(USERNAME))
    parser.add_argument('-p', "--password", help=f"Atlas password",
                        default=get_value(PASSWORD))
    parser.add_argument('-tls', "--tls", help=f"TLS Enabled", type=bool,
                        default=get_value(TLS))
    args = parser.parse_args()

    if not args.connection_string:
        print(f"To run without UI provide \"CONNECTION_STRING\" parameter in CLI.\n")
        form().mainloop()
    else:
        populate_db(**vars(args))


def populate_db(*args, **kwargs):
    # Print Params
    print(f"\n---\n")

    if kwargs.get('formConfig'):
        print("Entered Params:")
    else:
        print("Running with params:")
    for k, v in kwargs.items():
        print(f"\t{k}: {v}")
    if args:
        print(args)
    print("\n")

    if kwargs['sample']:
        translation = """If using document sample, use the following field values to generate random data.
            "setText()",
            "setNumber(<MIN>,<MAX>)",
            "setBool()",
            "setTextArray(<SIZE>)",
            "setNumArray(<SIZE>)",
            "setDoc(<SIZE>)",
            "setDate()"
            """
        print(f"\r{translation}\n")

    # Params Post Process
    kwargs['connection_string'] = kwargs['connection_string']. \
        replace("<username>", kwargs['username']). \
        replace("<password>", kwargs['password'])

    if kwargs['tls']:
        client_params = dict(host=kwargs['connection_string'], tlsCAFile=certifi.where())
    else:
        client_params = dict(host=kwargs['connection_string'], tlsAllowInvalidCertificates=True)

    # Execution Pre-Sets
    if kwargs['drop']:
        drop_collection_if_has_docs(kwargs['database'], kwargs['collection'], client_params)

    if FORK_CLIENT:
        global CLIENT
        client = get_client(client_params)
        client["test"].command("ping")
        CLIENT = client
    else:
        client = None

    # Setup Execution
    qin = Queue()  # Queue for main Task
    done = Event()  # Main Task Flag
    done_prep = Event()  # Prep Queue Flag

    print(f"\r{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} Starting Prep\r")
    time_start = time.time()
    procs = []

    # pq = Process(target=add_to_queue, args=(args.iterations,
    #                                         [client_params, args.database, args.collection, args.docs_count],
    #                                         qin, prep_event), daemon=True)
    # pq.start()
    _thread.start_new_thread(task_gen_queue, (kwargs['iterations'],
                                              [client_params, kwargs['database'], kwargs['collection'],
                                               kwargs['docs_count'], kwargs['sample']],
                                              qin, done_prep)
                             )

    print(f"\r{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} Starting Execution\r")

    # Execution Start
    ot = Thread(target=task_progress_output,
                args=(kwargs['collection'], kwargs['database'], client_params, qin, procs, done, client,))
    ot.start()

    for i in range(min(kwargs['iterations'], kwargs['concurrency'])):
        try:
            p = Process(target=populate_db_insert_task, args=(i, qin, done_prep, kwargs.get('bulk')), daemon=True)
            p.start()
            procs.append(p)
        except RuntimeError:
            print("\rError starting Thread\r")
    warmup_time = round(time.time() - time_start, 2)

    # Wait for prep and work
    while not done_prep.is_set() or not qin.empty():
        time.sleep(1)
        # Remove failed processes
        for p in procs:
            if p.exitcode:
                p.join(0)
                procs.remove(p)
                print(f"\rProcess {p.pid} exited with code {p.exitcode}\n")
        # Check if all processes failed
        if len(procs) == 0:
            print(f"\n*** All processes failed. Stopping! ***\nPlease review the output for the failure errors")
            break

    # Execution End
    for p in procs:
        p.join(10)

    done.set()
    ot.join(10)

    total_time = round(time.time() - time_start, 2)

    print(f"\n\n{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} Done.")
    print(f"Execution Duration: {total_time}")
    print(f"Collection \"{kwargs['collection']}\" has "
          f"{count_collection_documents(kwargs['collection'], kwargs['database'], client_params):,d} documents")


def task_gen_queue(iterations, params, queue, prep_event: Event):
    ts = time.time()
    print(f"\r{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} Queueing iterations")
    for i in range(iterations):
        queue.put([i] + params)
    dr = time.time() - ts
    print(f"\r{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} Queueing iteration done\n")
    prep_event.set()


def get_value(x):
    return x.get() if isinstance(x, tk.StringVar) or isinstance(x, tk.IntVar) or isinstance(x, tk.BooleanVar) else x


def task_progress_output(c, d, cp, qin: Queue, workers, done: Event, client):
    if not client:
        client = pymongo.MongoClient(**cp)
    collection = client[d][c]

    while not done.is_set():
        time.sleep(0.3)
        print(end=f"\rCollection \"{c}\" Estimated Documents: "
                  f"{collection.estimated_document_count():,d} "  # ,  Iterations in Queue: {qin.qsize()}, "
                  f"Workers running: {len(workers)}")
    client.close()


def drop_collection_if_has_docs(db_name, collection_name, client_params, docs_threshold=0):
    client = pymongo.MongoClient(**client_params)
    db = client[db_name]
    collection = db[collection_name]
    if collection.estimated_document_count() > docs_threshold:
        collection.drop()
        print(f"Dropped collection \"{collection_name}\"\n")
    client.close()


def count_collection_documents(collection, database, client_params):
    client = pymongo.MongoClient(**client_params)
    collection = client[database][collection]
    # count = collection.count_documents({})
    count = collection.estimated_document_count()
    client.close()
    return count


def populate_db_insert_task(*args):
    worker_id, qin, prep_event, bulk = args
    worker_name = f"worker-{worker_id}"
    logging.info(f"\n{worker_name} starting.")

    while not prep_event.is_set() or not qin.empty():

        # Handle faster execution than prep
        if qin.empty():
            print(f'\r***{worker_name} Queue is empty but queueing is not finished.***\n')
            time.sleep(1)
            continue

        iteration, client_params, database, collection, docs_count, sample_doc_path = qin.get()

        if LOG_COLL:
            global LOG_COLL_NAME
            if not LOG_COLL_NAME:
                LOG_COLL_NAME = datetime.now().strftime("populate_db_log_%Y%m%d%H%M%S")
            log_coll = get_collection(db_name=database, coll_name=LOG_COLL_NAME,
                                      connection_params=client_params)
            log_coll.insert_one({"iteration": iteration, "start": datetime.now(), "worker": worker_name})
        # print(f"\n{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: Iteration {iteration} Start")

        collection = get_collection(database, collection, client_params)

        docs = list()
        Faker.seed(randint(1, 9999))
        id_1 = id_factory(value=(iteration * docs_count))
        id_2 = id_factory()

        for j in range(docs_count):
            if sample_doc_path:
                if bulk:
                    docs.append(pymongo.InsertOne(load_doc(sample_doc_path)))
                else:
                    docs.append(load_doc(sample_doc_path))
            else:
                d_id = next(id_2)
                id_val = next(id_1)
                if bulk:
                    docs.append(pymongo.InsertOne(get_doc(id_val, d_id, iteration)))
                else:
                    docs.append(get_doc(id_val, d_id, iteration))

        if bulk:
            resp = collection.bulk_write(docs)
        else:
            resp = collection.insert_many(docs)

        if not resp.acknowledged:
            print(
                f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: "
                f"Iteration {iteration}: Got result '{resp.acknowledged}'")
        else:
            if 'log_coll' in locals():
                log_coll.update_one(filter={"iteration": iteration}, update={"$set": {"end": datetime.now()}})
            # print(
            #     f"{datetime.now().strftime('[%Y-%m-%dT%H:%M:%S]')} {worker_name}: Iteration {iteration} Done. "
            #     f"Documents existing now: {collection.count_documents({})}"
            # )


def get_doc(doc_id, doc_internal_id, iteration):
    dd = datetime(datetime.now().year + randint(-5, 5), randint(1, 12), randint(1, 28), randint(0, 23), randint(0, 59),
                  randint(0, 59))

    doc_arr = list()
    for n in range(randint(0, 1)):
        t = randint(65, 88)
        doc_arr.append({
            "in_id": randint(1, 5),
            "str_id": f"identifier{n + 1}",
            "type": f"{chr(t)}{chr(t + 1)}{chr(t + 2)}"
        })

    doc = {
        ################################################################################################
        # THE DOCUMENT                                                                                 #
        ################################################################################################
        "id": doc_id,
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
            "id": doc_internal_id,
            "iteration": iteration,
            "date_created": datetime.now(),
        },

        ################################################################################################
        # THE DOCUMENT                                                                                 #
        ################################################################################################
    }

    # Add partial field to documents
    if randint(0, 1):
        doc["timeStamp_partially_existing"] = (datetime.timestamp(dd) * 1000) + randint(100, 999)

    return doc


def load_doc(doc_path):
    if "~" in doc_path:
        doc_path = doc_path.replace("~", os.path.expanduser("~"))
    with open(doc_path) as f:
        sample_text = f.read()
    try:
        sample = json.loads(sample_text)
    except json.JSONDecodeError as err:
        print(f"{doc_path} is NOT a properly formatted JSON file!")
        raise err
    if isinstance(sample, list):
        sample = random.choice(sample)
    try:
        del sample['_id']
    except KeyError:
        pass

    # Translation Mapping
    # "setText()",
    # "setNumber(<MIN>,<MAX>)",
    # "setBool()",
    # "setTextArray(<SIZE>)",
    # "setNumArray(<SIZE>)",
    # "setDoc(<SIZE>)",
    # "setDate()"

    for k, v in sample.items():
        if isinstance(v, str):
            if "setNumber" in v:
                sample[k] = randint(int(v.split('(')[1].split(',')[0]), int(v.split(')')[0].split(',')[1]))
            elif "setBool" in v:
                sample[k] = bool(randint(0, 1))
            elif "setTextArray" in v:
                words = list()
                for i in range(int(v.split('(')[1].split(')')[0])):
                    words.append(FAKE.word())
                    sample[k] = words
            elif "setText" in v:
                sample[k] = FAKE.sentence()
            elif "setNumArray" in v:
                sample[k] = []
                for i in range(int(v.split('(')[1].split(')')[0])):
                    sample[k].append(randint(1, 1000))
            elif "setDoc" in v:
                sample[k] = dict()
                for i in range(int(v.split('(')[1].split(')')[0])):
                    sample[k][f"key{i}"] = f"Value_{i}"
            elif "setDate" in v:
                sample[k] = datetime(datetime.now().year + randint(-5, 5), randint(1, 12), randint(1, 28),
                                     randint(0, 23), randint(0, 59), randint(0, 59))

    return sample


def id_factory(value: int = 0, step: int = 1):
    while True:
        value += step
        yield value


def get_client(conn_params):
    global CLIENT
    if not CLIENT:
        CLIENT = pymongo.MongoClient(**conn_params)
        print(f"\rNew client opened by {multiprocessing.current_process().name}\n")
    return CLIENT


def get_collection(db_name=None, coll_name=None, connection_params=None, client=None):
    db_name = db_name if db_name is not None else get_value(TARGET_DB)
    coll_name = coll_name if coll_name is not None else get_value(TARGET_COLL)
    if not client:
        client = get_client(connection_params)
    db = client[db_name]
    collection = db[coll_name]
    return collection


def form():
    root = tk.Tk()
    root.resizable(False, False)
    root.title("Populate MongoDB")

    top_frame = tk.Frame(root)
    top_frame.pack(fill="x", anchor=tk.NW, side="top")
    tk.Label(top_frame, text="MongoDB Populate Database Script").pack(pady=5)

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

    # POPULATE
    tk.Label(main_frame, text="Execution Parameters", font=("helvetica", 18)).grid(row=9, columnspan=25)

    tk.Label(main_frame, text="Connection String: ").grid(row=10, column=10, sticky=tk.W)
    cs = tk.StringVar()
    cs.set("mongodb://localhost:27017")
    # cs.set("mongodb://localhost:6070,localhost:6071,localhost:6072")
    tk.Entry(main_frame, textvariable=cs).grid(row=10, column=20, sticky=tk.W)
    text = "Connection string. If target is Atlas, use \"Drivers\" connection string."
    tk.Label(main_frame, text=text).grid(row=10, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Database Name: ").grid(row=20, column=10, sticky=tk.W)
    db = tk.StringVar()
    db.set(datetime.now().strftime("myDb"))
    tk.Entry(main_frame, textvariable=db).grid(row=20, column=20, sticky=tk.W)
    text = "Target database to populate."
    tk.Label(main_frame, text=text).grid(row=20, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Collection Base Name: ").grid(row=30, column=10, sticky=tk.W)
    coll = tk.StringVar()
    coll.set(datetime.now().strftime("myColl_%Y-%m-%d"))
    tk.Entry(main_frame, textvariable=coll).grid(row=30, column=20, sticky=tk.W)
    text = "Target collection to populate."
    tk.Label(main_frame, text=text).grid(row=30, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Iterations: ").grid(row=40, column=10, sticky=tk.W)
    it = tk.IntVar()
    it.set(100)
    tk.Entry(main_frame, textvariable=it).grid(row=40, column=20, sticky=tk.W)
    text = "Number of operations. This multiplied by Documents per iteration is the eventual document count inserted."
    tk.Label(main_frame, text=text).grid(row=40, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Documents per iteration: ").grid(row=50, column=10, sticky=tk.W)
    docs_count = tk.IntVar()
    docs_count.set(1000)
    tk.Entry(main_frame, textvariable=docs_count).grid(row=50, column=20, sticky=tk.W)
    text = "Number of documents to inserted using insertMany. " \
           "This multiplied by Documents per iteration is the eventual document count inserted"
    tk.Label(main_frame, text=text).grid(row=50, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Concurrency: ").grid(row=60, column=10, sticky=tk.W)
    concurrency = tk.IntVar()
    concurrency.set(os.cpu_count())
    tk.Entry(main_frame, textvariable=concurrency).grid(row=60, column=20, sticky=tk.W)
    text = "Concurrent Workers. Keep within range of CPU cores. Values exceeding CPU cores may result in " \
           "bottlenecks, slow response times and your cat or workstation spontaneously combusting. "
    tk.Label(main_frame, text=text).grid(row=60, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Sample document path: ").grid(row=69, column=10, sticky=tk.W)
    sample = tk.StringVar()
    tk.Entry(main_frame, textvariable=sample).grid(row=69, column=20, sticky=tk.W)
    text = "OPTIONAL: Leave empty to auto generate documents."
    tk.Label(main_frame, text=text).grid(row=69, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Drop Collection: ").grid(row=70, column=10, sticky=tk.W)
    drop = tk.BooleanVar()
    drop.set(True)
    tk.Checkbutton(main_frame, text='', variable=drop, onvalue=1, offvalue=0).grid(row=70, column=20, sticky="w")
    text = "Drop the collection before starting If collection with the chosen collection name exists."
    tk.Label(main_frame, text=text).grid(row=70, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Use Bulk Writes: ").grid(row=71, column=10, sticky=tk.W)
    bulk = tk.BooleanVar()
    bulk.set(False)
    tk.Checkbutton(main_frame, text='', variable=bulk, onvalue=1, offvalue=0).grid(row=71, column=20, sticky="w")
    text = "Use Bulk insertOne operations instead of insertMany batches."
    tk.Label(main_frame, text=text).grid(row=71, column=30, sticky=tk.W)

    ttk.Separator(main_frame, orient='horizontal').grid(row=75, columnspan=100, sticky=tk.EW, pady=5)

    # AUTH PARAMETERS
    tk.Label(main_frame, text="Authentication Parameters", font=("helvetica", 18)).grid(row=79, columnspan=25)

    tk.Label(main_frame, text="Username: ").grid(row=80, column=10, sticky=tk.W)
    username = tk.StringVar()
    username.set("username")
    tk.Entry(main_frame, textvariable=username).grid(row=80, column=20, sticky=tk.W)
    text = "Username if applicable. Unused if the connection string doesn't include \"<username>\""
    tk.Label(main_frame, text=text).grid(row=80, column=30, sticky=tk.W)

    tk.Label(main_frame, text="Password: ").grid(row=90, column=10, sticky=tk.W)
    password = tk.StringVar()
    password.set("password")
    tk.Entry(main_frame, textvariable=password, show="*").grid(row=90, column=20, sticky=tk.W)
    text = "Password if applicable. Unused if the connection string doesn't include \"<password>\""
    tk.Label(main_frame, text=text).grid(row=90, column=30, sticky=tk.W)

    tk.Label(main_frame, text="TLS: ").grid(row=100, column=10, sticky=tk.W)
    tls = tk.BooleanVar()
    tls.set(False)
    tk.Checkbutton(main_frame, text='', variable=tls, onvalue=1, offvalue=0).grid(row=100, column=20, sticky="w")
    text = "TLS certificate required."
    tk.Label(main_frame, text=text).grid(row=100, column=30, sticky=tk.W)

    # # NOT IMPLEMENTED
    # ttk.Separator(my_frame, orient='horizontal').grid(row=999, columnspan=100, sticky=tk.EW, pady=5)
    # tk.Label(my_frame, text="Future Parameters", font=("helvetica", 18)).grid(row=1000, columnspan=25)
    #
    # tk.Label(my_frame, text="Log collection: ").grid(row=1001, column=10, sticky=tk.W)
    # _ = tk.IntVar()
    # _.set(0)
    # tk.Checkbutton(my_frame, text='', variable=_, onvalue=1, offvalue=0).grid(row=1001, column=20, sticky="w")
    # tk.Label(my_frame, text="NOT IMPLEMENTED: Create a collection with log of inserts").grid(
    #     row=1001, column=30, sticky=tk.W)
    #
    # tk.Label(my_frame, text="Log collection name: ").grid(row=1002, column=10, sticky=tk.W)
    # _ = tk.StringVar()
    # # _.set("log_collection")
    # tk.Entry(my_frame, textvariable=_).grid(row=1002, column=20, sticky=tk.W)
    # tk.Label(my_frame, text="NOT IMPLEMENTED: Name of the log collection (will generate default if empty)").grid(
    #     row=1002, column=30, sticky=tk.W)
    #
    # tk.Label(my_frame, text="Fork Client: ").grid(row=1003, column=10, sticky=tk.W)
    # _ = tk.IntVar()
    # _.set(0)
    # tk.Checkbutton(my_frame, text='', variable=_, onvalue=1, offvalue=0).grid(row=1003, column=20, sticky="w")
    # test = NOT IMPLEMENTED: One client to rule them all. Pymongo don't like that but it works
    # tk.Label(my_frame, text=text).grid(
    #     row=1003, column=30, sticky=tk.W)

    # Place buttons frame
    ttk.Separator(buttons_frame, orient='horizontal').pack(fill='x')
    buttons_frame.pack(anchor=tk.SE, expand=0, fill='x', ipady=20)  # , fill=tk.Y, expand=True)

    # Buttons Frame Buttons
    cmd = (lambda: populate_db(
        connection_string=cs.get(),
        database=db.get(),
        collection=coll.get(),
        iterations=it.get(),
        docs_count=docs_count.get(),
        concurrency=concurrency.get(),
        sample=sample.get(),
        drop=drop.get(),
        username=username.get(),
        password=password.get(),
        tls=tls.get(),
        formConfig=True,
        bulk=bulk)
           )
    tk.Button(buttons_frame, text="Execute", command=cmd).pack(side="right")
    tk.Button(buttons_frame, text="Exit", command=lambda: close_window(root)).pack(side='right')

    # Enable use of enter and escape.
    root.bind('<Return>', lambda event: cmd())
    root.bind('<Escape>', lambda event: close_window(root))

    return root


def close_window(window):
    window.destroy()


def check_params(*args, **kwargs):
    for arg in args:
        print(arg)

    for k, v in kwargs.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    freeze_support()
    main()
