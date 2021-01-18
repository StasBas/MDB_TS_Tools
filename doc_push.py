import os
import json
import pymongo
import certifi


DB_USER = os.environ.get("dbuser")
DB_PASS = os.environ.get("dbpass")
CLUSTER = f"cluster0.qsg3m.mongodb.net/pmdb"
TARGET_DB = "pmdb"
TARGET_COLL = "movieDetails"
FILE_NAME = "movie_details_sample.json"


def main():
    file_path = os.path.join(os.path.dirname(__file__), "movie_details_sample.json")
    with open(file_path, 'r') as f:
        docs = json.loads(f.read())

    client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASS}@{CLUSTER}?retryWrites=true&w=majority",
                                 ssl_ca_certs=certifi.where())
    db = client[TARGET_DB]
    collection = db[TARGET_COLL]

    if collection.estimated_document_count() > 0:
        dr = collection.drop()

    resp = collection.insert_many(docs)
    if not resp.acknowledged:
        print(f"Insert Failed")
    else:
        print(f"{len(resp.inserted_ids)} documents inserted.")


if __name__ == "__main__":
    main()
