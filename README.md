# g2search

If you are beginning your journey with
[Senzing](https://senzing.com/),
please start with
[Senzing Quick Start guides](https://docs.senzing.com/quickstart/).

You are in the
[Senzing Garage](https://github.com/senzing-garage)
where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## Overview

The [G2Search.py](G2Search.py) utility reads a file of json formatted search records, calling the search API for each, and logs
the results to a csv file for analysis.  It employs a mapping file to control the output.

Usage:

```console
python G2Search.py --help
usage: G2Search.py [-h] [-c INI_FILE_NAME] [-m MAPPINGFILENAME]
                   [-i INPUTFILENAME] [-d DELIMITERCHAR] [-e FILEENCODING]
                   [-o OUTPUTFILENAME] [-l LOGFILENAME] [-nt THREAD_COUNT]

optional arguments:
  -h, --help            show this help message and exit
  -c INI_FILE_NAME, --config_file_name INI_FILE_NAME
                        name of the g2.ini file, defaults to
                        /etc/opt/senzing/G2Module.ini
  -m MAPPINGFILENAME, --mappingFileName MAPPINGFILENAME
                        the name of a mapping file
  -i INPUTFILENAME, --inputFileName INPUTFILENAME
                        the name of an input file
  -o OUTPUTFILENAME, --outputFileName OUTPUTFILENAME
                        the name of the output file
  -l LOGFILENAME, --log_file LOGFILENAME
                        optional statistics filename (json format)
  -nt THREAD_COUNT, --thread_count THREAD_COUNT
                        number of threads to start
```

## Contents

1. [Prerequisites](#prerequisites)
1. [Input file](#input-file)
1. [Mapping file](#mapping-file)
1. [Typical use](#typical-use)
1. [Sample output](#sample-output)

### Prerequisites

- Python 3.6 or higher
- Senzing API version 3.00 or higher

*If using an SSHD container, you should first max it out with at least 4 processors and 30g of ram as the more threads
you give it, the faster it will run.  Also, if the database container is not set to auto-scale, you should give it
additional resources as well.*

1. Place the following files in a directory of your choice:
    - [G2Search.py](G2Search.py)
    - [search_map_template.json](search_map_template.json)

2. Set PYTHONPATH environment variable to python directory where you installed Senzing.
    - Example: export PYTHONPATH=/opt/senzing/g2/python

3. The senzing environment must be set to your project by sourcing the setupEnv script created for it.

Its a good idea to place these settings in your .bashrc file to make sure the enviroment is always setup and ready to go.
*These will already be set if you are using a Senzing docker image such as the sshd or console.*

### Input file

The input file contains the [Senzing formatted](https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-Data-Mapping) JSON string for the search requests. Each search request should be on a single line.

```console
{"NAME_FULL":"ROBERT SMITH",  "DATE_OF_BIRTH":"11/12/1978"}
{"NAME_FULL":"JOHN DANIELS",  "SSN_NUMBER":"123-45-6789"}
```

### Mapping file

See the [search_map_template.json](search_map_template.json).   This is a template that containing the likely settings you
would use for each search you perform.   Feel free to clone it and adjust the settings to fit the goals you have for each
particular search you want to run.

There are several sections in this template:

#### Input section

This section describes the input source file.   Currently only json search messages are supported.

- fileFormat: JSON

#### Scoring section

This section dictates the weighted scoring of search results. For every matched entity in a search result, Senzing also supplies
a 1 to 100 score of how close the name was, how close the address was, etc. These scores can be used to create an overall score
for the matched record.

see the article [Scoring-Search-Results](https://senzing.zendesk.com/hc/en-us/articles/360047855193-Scoring-Search-Results)

#### Output section

This section contains search result filters and defines the output columns.

Filters include:

- matchLevelFilter: only return up to a certain match level (1=resolved, 2=possible match, etc)
- nameScoreFilter: only return records where the name scored high enough
- maxReturnCount: only return the top (n) matches
- dataSourceFilter: only return entities from a particular data source

Output columns:

Output columns can come from the search record, the api, or the matched entity
as indicated by the "source" attribute.

- input: This is helpful so that you can quickly see what record was searched for.
You can specify any of the following ...
    - ROW_ID: the row number in the json file
    - SEARCH_ENTITY_ID: the entity_id assigned to the search record if the search record was actually loaded
      *Note: some implementations involve a load and then a search to see what else it may have hit that didn't create a relationship.*
    - SEARCH_STRING: the entire json search record
    - Any root level attribute from the search record such a "PRIARY_ORG_NAME"

- api: you can specify any of the following :
    - MATCH_NUMBER: the ranked match number for the entity.
    - MATCH_LEVEL: 1=resolved, 2=possible match. 3=possibly related, 4-name only
    - MATCH_LEVEL_CODE: just the code portion of the match_level
    - MATCH_KEY: NAME+ADDRESS, etc
    - MATCH_SCORE: the weighted overall score for the entity
    - NAME_SCORE: just the best matching name score for the entity
    - ENTITY_ID: the entity_id for the matched entity
    - ENTITY_NAME: the best name for the entity
    - ENTITY_SOURCES: what data sources the entity came from 
    - SCORE_DATA: all of the score data

- record: you can specify any of the following
  - NAME_DATA: all of the names
  - ATTRIBUTE_DATA: dates of birth, gender, etc
  - IDENTIFIER_DATA: all of the identifiers such as passport, tax_id, etc
  - ADDRESS_DATA: all of the addresses
  - PHONE_DATA: all of the phone numbers
  - OTHER_DATA: all of the non resolving attributes such as dates, statuses and amounts

### Typical use

```console
python G2Search.py -m search_map_template.json -i /search_list.json -o search_result.csv -l search_result.json
```

The -nt THREAD_COUNT parameter will default to the max the resources of the computer or container you are
using.  However, you can override this to add more threads if desired.  For instance, if access to the database
is slow, you can increase the number of threads running as they spend a lot of time waiting on database queries.

### Sample output

*see the [sample_search_result.csv](sample_search_result.csv) file to see the result of all your searches*

All of the search results are output to a csv file.

There will be one or more rows for each search record.

- match_number: the match_number column will be zero if no rows are found match_number 1 will be the
best match found as determined by the weighted score. match_numbers 2-n are any additional matches
found also ranked by the weighed score.

*see the [sample_search_result.json](sample_search_result.json) file*

These accumulated statistics are displayed at the end of the run and captured in the log file if
specified.

- summary: The summary section shows the total number searches performed and
the total that returned any sort of a match (resolved, possible, name_only)

- resolution: the resolution section breaks down these matches by match level.  The
"best" section only counts the best matches found while "additional" section counts
the additional matches found.

- scoring: the scoring section breaks down these matches by the weighted match_score.
The "best" section only counts the best matches found while "additional" section counts
the additional matches found.  The "name" section is included as so you can see the pure
name scores beinbg returned.
