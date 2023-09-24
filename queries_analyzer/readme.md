## Queries Analyzer

Analyze log by shape of queries. Parses default structured MongoDB logs.


## Params

### Execution Parameters

- **Log File Path**: Full path to log file on your computer. ex. "~/Downloads/filename.log"
- **Start Time**: Earliest time stamp in log to include in analysis. 
- **End Time**: Latest time stamp in log to include in analysis. 
- **Decoder Error** Limit: Maximum decoder errors. Non JSON log formatted lines tolerance.
- **Max print length**: Number of top grouped results to include in report.
- **Log Examples**: Number of Log lines to include in search results.
- **Workers**: Workers/Concurrency. Default value is half of the max available.
- **Output Path**: Path to folder where reports will be saved. If empty - reports will be printed in console. 

### Analyzers Parameters

- **Text Search**: Comma separated phrases to include in search. Prefix of "-" will exclude the phrase. Ex: "hasSortStage,-COLLSCAN" will parse only lines that have "hasSortStage" and do not have "COLLSCAN".
- **Attribute Search**: Provide statistics for a specific attribute in the structured log. Ex: "bytesRead". For non-numeric values will simply group exact matches.
- **Perform Ratio Analysis**: Calculate ratio for each query that has "docsExamined" and "nreturned" attributes and provide statistics for the result.



### CLI

To run from CLI provide "path" param. 

For full usage guide run "queries_analyzer -h". Example:

    usage: queries_analyzer.py [-h] [-p PATH] [-s START_TIME] [-r END_TIME][-l ERROR_LIMIT] [-pl MAX_PRINT]
                           [-log_examples LOG_EXAMPLES] [-output OUTPUT_PATH]
                           [-fs SEARCH] [-ks KEY_SEARCH] [-ratio RATIO] [-w
                           WORKERS]




    options:
      -h, --help            show this help message and exit
      -p PATH, --path PATH  Log file path. ex: "~/Downloads/mongodb.log"
      -s START_TIME, --start_time START_TIME
                            Start Time. ex: "1970-01-01T00:00:00"
      -r END_TIME, --end_time END_TIME
                            End Time. ex: "3000-01-01T00:00:00"
      -l ERROR_LIMIT, --error_limit ERROR_LIMIT
                            Error limit. ex: 100
      -pl MAX_PRINT, --max_print MAX_PRINT
                            Max results to print. ex: 10
      -log_examples LOG_EXAMPLES, --log_examples LOG_EXAMPLES
                            Max log lines to print. ex: 10
      -output OUTPUT_PATH, --output_path OUTPUT_PATH
                            Path to save reports. ex: "~/Documents/reports"
      -fs SEARCH, --search SEARCH
                            Full search. All terms must be in line. Terms starting
                            with "-" must not be in line
                            ex:"-COLLSCAN,hasSortStage"
      -ks KEY_SEARCH, --key_search KEY_SEARCH
                            Search for pecific key values ex: bytesRead
      -ratio RATIO, --ratio RATIO
                            Include ratio analysis . ex: 1/0
      -w WORKERS, --workers WORKERS
                            Workers (cores) . ex: 4