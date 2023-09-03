import os
import json
import pymongo
import certifi


DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CONN_STR = None  # "mongodb+srv://<username>:<password>@cluster0.qsg3m.mongodb.net/?retryWrites=true&w=majority"
TARGET_DB = "pmdb"
TARGET_COLL = "movieDetails"
FILE_NAME = "movie_details_sample.json"


def main():
    sample_push()


def sample_push():
    global CONN_STR
    global DB_USER
    global DB_PASS
    if not CONN_STR:
        CONN_STR = input("Paste driver connection string:\n")
    connection_string = CONN_STR.replace("<username>", str(DB_USER)).replace("<password>", str(DB_PASS))
    client = pymongo.MongoClient(connection_string, tlsAllowInvalidCertificates=True)
    doc_path = os.path.join(os.path.dirname(__file__), "doc_push_sample.json")
    push_docs(client, doc_path, TARGET_DB, TARGET_COLL)


def push_docs(client, doc_path, db_name, collection_name):
    with open(doc_path, 'r') as f:
        docs = json.loads(f.read())

    db = client[db_name]
    collection = db[collection_name]

    if collection.estimated_document_count() > 0:
        print(f"Dropping collection {collection_name}")
        dr = collection.drop()

    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(f"Insert Failed")
    else:
        print(f"{len(resp.inserted_ids)} documents inserted.")


if __name__ == "__main__":
    main()
