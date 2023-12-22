## RS States Analyzer

Provide a report of all logged replica set changes.
- SELF - the node from which the log is. state transitions to newState from oldState
- RS_DOWN - Nodes marked as down for the perspective node due to heartbeat timeout
- Member state - RS members new state log entries.


## Params

### Execution Parameters

- **Log File Path**: Full path to log file on your computer. ex. "~/Downloads/filename.log"
- **Start Time**: Earliest time stamp in log to include in analysis. 
- **End Time**: Latest time stamp in log to include in analysis. 
- **Decoder Error** Limit: Maximum decoder errors. Non JSON log formatted lines tolerance.

###
Curently saves the raw log but returns only the parsed restructured log. 

### CLI

To run from CLI provide "path" param. 

For full usage guide run "queries_analyzer -h". Example:

    usage: state_changes_analyzer.py [-h] [-p PATH] [-s START_TIME] [-r END_TIME][-l ERROR_LIMIT] 