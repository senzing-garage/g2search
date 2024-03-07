# g2search

## Overview

The [G2Search.py](G2Search.py) utility reads a file of json formatted search records, calling the search API for each, and logs
the results to a csv file for analysis.  It employs a configuration file to control the output.

Usage:

```console
python G2Search.py --help
usage: G2Search.py [-h] [-c CONFIG_FILE_NAME] [-i INPUT_FILE_NAME] [-o OUTPUT_FILE_ROOT] [-nt THREAD_COUNT] [-D]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG_FILE_NAME, --config_file_name CONFIG_FILE_NAME
                        Path and name of optional G2Module.ini file to use.
  -i INPUT_FILE_NAME, --input_file_name INPUT_FILE_NAME
                        the name of a json input file
  -o OUTPUT_FILE_ROOT, --output_file_root OUTPUT_FILE_ROOT
                        root name for output files created, both a csv and a json stats file will be created
  -nt THREAD_COUNT, --thread_count THREAD_COUNT
                        number of threads to start, defaults to max available
  -D, --debug           run in debug mode
```

## Contents

1. [Prerequisites](#prerequisites)
1. [Input file](#input-file)
1. [Configuration file](#configuration-file)
1. [Typical use](#typical-use)
1. [Sample output](#sample-output)

### Prerequisites

- Python 3.6 or higher
- Senzing API version 3.00 or higher

1. Place the following files in a directory of your choice:
    - [G2Search.py](G2Search.py)
    - [search_config_template.json](search_config_template.json)

2. The senzing environment must be set to your project.

### Input file

The input file should contain a list of search records formatted according to the [Senzing formatted](https://senzing.zendesk.com/hc/en-us/articles/231925448-Generic-Entity-Specification-Data-Mapping)


### Configuration file

See the [search_config_template.json](search_config_template.json).   This is a template that containing the likely settings you
would use for each search you perform. Feel free to clone it and adjust the settings to fit your desired output.

There are three sections in this template:

#### Filtering section

Filters include:
- max_return_count: only return the top (n) matches
- match_score_filter: minimum computed match_score (see scoring section below)
- match_level_filter: maximum match level (1=resolved, 2=possible match, 3=possibly related)
- data_source_filter: entity must contain a record from a particular data source

#### Scoring section

This section dictates the weighted scoring of search results. see the article [Scoring-Search-Results](https://senzing.zendesk.com/hc/en-us/articles/360047855193-Scoring-Search-Results)

#### Output Columns section

The syntax for each ouput column is: 

- {"any column name": "{any python fstring expression}"}

python fstring values should come from either the search_record or the matched_entity values.  

The search_record is under your control as in you formatted the search json.  However, we do add a ROW_ID attribute to it.  So you can add columns such as:
- {"row_number": "{search_record['ROW_ID']}"},        <-- contains just the ROW_ID we assigned to each search record idn the input file
- {"row_number": "{search_record['NAME_FULL']}"},     <-- contains just the NAME_FULL of the search_record (if you mapped one)
- {"search_json": "{json.dumps(search_record)}"},     <-- contains the whole json search message

The matched_entity is under our control and it contains the following attributes:

- MATCH_NUMBER: The ranked order based on the computed match_score (highest score first)
- MATCH_SCORE: The computed match_score
- MATCH_LEVEL: Senzing match level (1=resolved, 2=possible match, 3=possibly related)
- MATCH_LEVEL_DESC: the descriptive text of the match level (not just the number)
- MATCH_KEY: the list of features that matched or detracted from the match such as: NAME+ADDRESS-DOB
- RULE_CODE: the Senzing principle or "rule" that was hit

- ENTITY_ID: The Senzing unique ID for the entity found
- ENTITY_NAME: The best name for the entity found
- DATA_SOURCES: The list of data sources the entity has records in



    "NAME_SCORE": 100,
    "DOB_SCORE": 100,
    "ADDRESS_SCORE": 100,
    "EMAIL_SCORE": 100,
    "SSN_SCORE": 100,
    "RECORD_TYPE_SCORE": 100,

    "RECORD_LIST": [
        {
            "DATA_SOURCE": "CUSTOMERS",
            "RECORD_ID": "1010"
        },
        {
            "DATA_SOURCE": "CUSTOMERS",
            "RECORD_ID": "1009"
        },
        {
            "DATA_SOURCE": "CUSTOMERS",
            "RECORD_ID": "1011"
        },
        {
            "DATA_SOURCE": "WATCHLIST",
            "RECORD_ID": "1012"
        },
        {
            "DATA_SOURCE": "WATCHLIST",
            "RECORD_ID": "1014"
        }
    ],



    "SEARCH_VALUES": "NAME: Edward Kusha | DOB: 3/1/1970 | ADDRESS: 1304 Poppy Hills Dr Blacklick OH 43004 | EMAIL: Kusha123@hmail.com | SSN: 294-66-9999 | RECORD_TYPE: PERSON ",
    "MATCHED_VALUES": "NAME: Edward Kusha (100) | DOB: 3/1/1970 (100) | ADDRESS: 1304 Poppy Hills Dr Blacklick OH 43004 (100) | EMAIL: Kusha123@hmail.com (100) | SSN: 294-66-9999 (100) | RECORD_TYPE: PERSON (100) ",


    "MATCH_KEY_DETAIL"
NAME: 100 

NAME(Edward Kusha|Edward Kusha|GNR_FN:100,)
DOB(3/1/1970|3/1/1970|FULL_SCORE:100)




    "RAW_SCORES": {
        "NAME": {
            "INBOUND_FEAT": "Edward Kusha",
            "CANDIDATE_FEAT": "Edward Kusha",
            "GNR_FN": 100,
            "GNR_SN": 100,
            "GNR_GN": 100,
            "GENERATION_MATCH": -1,
            "GNR_ON": -1
        },
        "DOB": {
            "INBOUND_FEAT": "3/1/1970",
            "CANDIDATE_FEAT": "3/1/1970",
            "FULL_SCORE": 100
        },
        "ADDRESS": {
            "INBOUND_FEAT": "1304 Poppy Hills Dr Blacklick OH 43004",
            "CANDIDATE_FEAT": "1304 Poppy Hills Dr Blacklick OH 43004",
            "FULL_SCORE": 100
        },
        "EMAIL": {
            "INBOUND_FEAT": "Kusha123@hmail.com",
            "CANDIDATE_FEAT": "Kusha123@hmail.com",
            "FULL_SCORE": 100
        },
        "SSN": {
            "INBOUND_FEAT": "294-66-9999",
            "CANDIDATE_FEAT": "294-66-9999",
            "FULL_SCORE": 100
        },
        "RECORD_TYPE": {
            "INBOUND_FEAT": "PERSON",
            "CANDIDATE_FEAT": "PERSON",
            "FULL_SCORE": 100
        }
    },


See the samples output.  Please note that since the program is multi-threaded, the search_record ROW_IDs do not necessarily come out in perfect order.   If you wish to review the results in a spreadsheet, a proper sort would be ROW_ID ascending and MATCH_NUMBER descending.






 the mns can come from either the search record or the matched entity.  





- input: This is helpful so that you can quickly see what record was searched for.
You can specify any of the following ...
    - ROW_ID: the row number in the json file
    - SEARCH_ENTITY_ID: the entity_id assigned to the search record if the search record was actually loaded
      *Note: some implementations involve a load and then a search to see what else it may have hit that didn't create a relationship.*
    - SEARCH_STRING: the entire json search record
    - Any root level attribute from the search record such a "PRIARY_ORG_NAME"

- api: you can specify any of the following :
    - MATCH_NUMBER: the ranked match number for the entity.
    - MATCH_LEVEL: 1=resolved, 2=possible match. 3=possibly related
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
python G2Search.py -c search_config_template.json -i /search_input.json -o search_result
```

You can also use the -nt THREAD_COUNT to increase the number of threads from the default max available.  For instance, if access to the database
is slow, you may want to double the number of threads as they spend a lot of time waiting on database queries.

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
