## Populate DB Script

Script to create large volumes of content in MongoDB database.

The script can generate documents or use provided sample. 

Run from CLI or via form.

## Params


- **Connection String**: Connection string. Ex. "mongodb://localhost:27017". If using Atlas, select drivers connection string for python.
- **Database Name**: Name of database where documents will be added
- **Collection Base Name**: Name of collection where documents will be added.
- **Iterations**: How many requests will be made to the DB service. Final number of documents is this value multiplied by the "Documents per iteration" value.
- **Documents per iteration**: How many documents are inserted in a single request. Final number of documents is this value multiplied by the "Iterations" value.
- **Concurrency**: Concurrent Workers. Should not exceed the number of CPUs. If using form to run, the default value is the max cores.
- **Sample Document Path**: Full path to sample document. Ex. ~/Downloads/sample.json.  Target MUST be a properly formatted json file. Can be edited to allow random values. See "Sample Document Mod" section below.
- **Drop Collection**: If True, will drop the target collection if it exists prior to execution.
- **Use Bulk Writes**: If False, will use insertMany() operation type. If True, will use insertOne() bulk operation types.
- **username**: DB service username. Replaces the "<username>:<password>" in the connection string. 
- **password**: DB service password. Replaces the "<username>:<password>" in the connection string. 


### Default Document Structure


    {
        "id": int,
        "object": uuid,
        "date_rolled": date,
        "ts_ms": int,
        "description": text,
        "keyword": text,
        "active": bool,
        "public": bool,
        "location": array(geolocation),
        "words_array": array(text),
        "person": {
            "name": text,
            "lastname": text,
            "eye_color": text,
            "hair_color": text,
            "address": text,
        },
        "receiptNumber": text(numeric),
        "status": text(["created", "claimed", "other"]),
        "score": int(1, 10000),
        "source": text,
        "obj_array": [
            {
                "name": text,
                "type": int
            },
            {
                "name": text,
                "type": int
            },
            {
                "name": text,
                "type": int
            },
        ],
        "nest_obj_arr": {
            "ex_type": text(["t1", "t2", "t3"]),
            "obj_arr": array(json)
        },
        "nest_obj_obj": {
            "type": text(["t1", "t2", "t3"]),
            "properties": {
                "prop1": text(["p1", "p2", "p3"]),
                "prop2": text(["p1", "p2", "p3"]),
                "prop3": text(["p1", "p2", "p3"]),
                "prop4": text(["p1", "p2", "p3"])
            }
        },
        "internal": {
            "id": doc_internal_id,
            "iteration": iteration,
            "date_created": datetime,
        },
        timeStamp_partially_existing = ISODATE(),
    }

The field "timeStamp_partially_existing" will be added to half the documents at random.


### Sample Document Mod 

If using document sample, replace the document field value with one of the following to generate random data in the fields:

            "setText()",                : Generate random Text
            "setNumber(<MIN>,<MAX>)",   : Generate number between MIN and MAX
            "setBool()",                : Random boolean value
            "setTextArray(<SIZE>)",     : Array(text) with 'SIZE' number of members 
            "setNumArray(<SIZE>)",      : Array(int) with 'SIZE' number of members 
            "setDoc(<SIZE>)",           : Subdocument with 'SIZE' number of keys
            "setDate()"                 : IsoDate


### CLI

To run from CLI provide "path" param. 


Usage:

      populate_mongodb [-h] [-c CONNECTION_STRING] [-db DATABASE]
                            [-coll COLLECTION] [-i ITERATIONS]
                            [-dc DOCS_COUNT] [-w CONCURRENCY] [-drop DROP]
                            [-u USERNAME] [-p PASSWORD]


    options:
      -h, --help            show this help message and exit
      -c CONNECTION_STRING, --connection_string CONNECTION_STRING
                            Connection String. ex: "mongodb://localhost:5000"
      -db DATABASE, --database DATABASE
                            DB Name. ex: "myDb"
      -coll COLLECTION, --collection COLLECTION
                            Collection Name. ex: "myCollection"
      -i ITERATIONS, --iterations ITERATIONS
                            Number of iterations. ex: 10
      -dc DOCS_COUNT, --docs_count DOCS_COUNT
                            Number of documents per iteration. ex: 100
      -w CONCURRENCY, --concurrency CONCURRENCY
                            Concurrency to run keep under 30 on macs. ex: 100
      -s SAMPLE, --sample SAMPLE
                            Sample Document Path. Target must be JSON formatted.
                            ex ~/Documents/myDod.json
      -drop DROP, --drop DROP
                            Drop target database. (1/0)
      -b BULK, --bulk BULK  Use Bulk insertOne instead of insertMany (1/0)
      -u USERNAME, --username USERNAME
                            Atlas username
      -p PASSWORD, --password PASSWORD
                            Atlas password
      -tls TLS, --tls TLS   TLS Enabled