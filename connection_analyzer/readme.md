## MongoDB Log Connections Analyzer

Analyze connections. Parses default structured MongoDB logs.

Provides information about opened and closed connections grouped by the remote host base ip.

Includes reports for used client information and application.


## Params

### Execution Parameters
- **Path**: Path to file location Ex: "~/Downloads", running from CLI will use the location.
- **File Name**: Log file name
- **Start Time**: Earliest time stamp to provide analysis for.
- **End Time**: Latest time stamp to provide analysis for.
- **Decoder Error Limit**: Log structure error tolerance. How many "non JSON" failures before attempts to parse log are aborted.

### Analyzers Parameters
- **Max Entries**: Max top results to print in case of multiple results.
- **Applications Info**: Include logged applications for the established connections.
- **Drivers Info**: Include logged driver info for the established connections.


### CLI

To run from CLI provide "FILENAME" param.

    Usage: queries_analyzer [-h] [-p PATH] -f FILENAME [-s START]
                            [-e END] [-a APPS] [-d DRIVERS]
                            [-ll LIMIT]

### TODO: 

- Error connections
  - Unauthorized
  - Timed out
  - etc.
- 
