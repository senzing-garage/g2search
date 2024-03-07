# g2search

## Overview

The [G2Search.py](G2Search.py) utility reads a file of json formatted search records, calling the search API for each, and logs
the results to a csv file for analysis.  It employs a configuration file to control the output.

Usage:

```console
python3 G2Search.py --help
usage: G2Search.py [-h] [-c CONFIG_FILE_NAME] [-i INPUT_FILE_NAME] [-o OUTPUT_FILE_ROOT] [-nt THREAD_COUNT] [-A] [-D]

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
  -A, --do_audit        compute precision and recall (requires expected record_id in search record)
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

In addition to what is in the default [search_config_template.json](search_config_template.json), you can also choose any ohter attributes in the matched_entity structure computed in the score_entities function in the [G2Search.py](G2Search.py)

### Typical use

```console
python G2Search.py -c search_config_template.json -i /search_input.json -o search_result
```

You can also use the -nt THREAD_COUNT to increase the number of threads from the default max available.  For instance, if access to the database is slow, you may want to double the number of threads as they spend a lot of time waiting on database queries.

### Sample output

*see the [sample_search_result.csv](sample_search_result.csv) file to see the result of all your searches*

All of the search results are output to a csv file.

There will be one or more rows for each search record.

- match_number: the match_number column will be zero if no rows are found match_number 1 will be the
best match found as determined by the weighted score. match_numbers 2-n are any additional matches
found also ranked by the weighed score.

*see the [sample_search_result.json](sample_search_result.json) file*

These accumulated statistics are displayed at the end of the run and captured in the output json file.
