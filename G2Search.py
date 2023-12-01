#! /usr/bin/env python3

import os
import sys
import time
from datetime import datetime
import json
import csv
import argparse
import configparser
import signal
import itertools
import logging

import concurrent.futures

from senzing import G2Engine, G2EngineFlags, G2Exception


class SZSearch:
    """ senzing search wrapper class"""
    def __init__(self, g2module_params, **kwargs):

        self.g2_engine = G2Engine()
        self.g2_engine.init('G2Search', g2module_params, False)
        self.g2_engine.primeEngine()

        self.max_return_count = kwargs.get('max_return_count', 0)
        self.data_source_filter = kwargs.get('data_source_filter', '').upper()
        self.match_level_filter = kwargs.get('match_level_filter', None)
        self.scoring_config = kwargs.get('scoring_config', {})

        search_flag_list = ['G2_SEARCH_INCLUDE_STATS',
                            'G2_SEARCH_INCLUDE_FEATURE_SCORES',
                            'G2_ENTITY_INCLUDE_ENTITY_NAME',
                            'G2_ENTITY_INCLUDE_RECORD_DATA']
        if self.match_level_filter == 1:
            search_flag_list.append('G2_SEARCH_INCLUDE_RESOLVED')
        elif self.match_level_filter == 2:
            search_flag_list.append('G2_SEARCH_INCLUDE_RESOLVED')
            search_flag_list.append('G2_SEARCH_INCLUDE_POSSIBLY_SAME')
        else:
            search_flag_list.append('G2_SEARCH_INCLUDE_ALL_ENTITIES')
        self.search_flag_bits = G2EngineFlags.combine_flags(search_flag_list)
        self.feature_order = {'NAME': 1, 'DOB': 2, 'GENDER': 2, 'ADDRESS': 3, "PHONE": 3, "EMAIL": 3, "WEBSITE": 3, "SSN": 4, "NATIONAL_ID": 4, "PASSPORT": 4, "DRIVERS_LICENSE": 4}


    def __del__(self):
        self.g2_engine.destroy()


    def search(self, search_string):
        search_data = {"search_string": search_string}

        start_time_search = time.time()
        try:
            response = bytearray()
            self.g2_engine.searchByAttributes(search_string, response, self.search_flag_bits)
            search_response = json.loads(response)
        except G2Exception as ex:
            search_data['error'] = ex
            return search_data
        search_data['api_time'] = time.time() - start_time_search

        search_response = self.score_response(search_response)
        search_response = self.filter_response(search_response)

        search_data['search_time'] = time.time() - start_time_search
        search_data['search_response'] = search_response

        return search_data


    def score_response(self, json_response):
        scored_entities = []
        for entity_data in json_response.get('RESOLVED_ENTITIES'):
            matched_entity = {'ENTITY_ID': entity_data['ENTITY']['RESOLVED_ENTITY']['ENTITY_ID'],
                              'ENTITY_NAME': entity_data['ENTITY']['RESOLVED_ENTITY']['ENTITY_NAME'],
                              'RECORD_LIST': entity_data['ENTITY']['RESOLVED_ENTITY']['RECORDS'],
                              'DATA_SOURCES': '',
                              'MATCH_SCORE': 0,
                              'MATCH_LEVEL': entity_data['MATCH_INFO']['MATCH_LEVEL'],
                              'MATCH_CODE': entity_data['MATCH_INFO']['MATCH_LEVEL_CODE'],
                              'MATCH_KEY': entity_data['MATCH_INFO']['MATCH_KEY'][1:], # strips leading +
                              'RULE_CODE': entity_data['MATCH_INFO']['ERRULE_CODE'],
                              'SEARCH_VALUES': '',
                              'MATCHED_VALUES': '',
                              'RAW_SCORES': {}}


            data_sources = {}
            for record in entity_data['ENTITY']['RESOLVED_ENTITY']['RECORDS']:
                data_source = record['DATA_SOURCE']
                if data_source not in data_sources:
                    data_sources[data_source] = [record['RECORD_ID']]
                else:
                    data_sources[data_source].append(record['RECORD_ID'])
            matched_entity['DATA_SOURCES'] = ' | '.join(f"{x}: {data_sources[x][0]}" if len(data_sources[x]) == 1 else f"{x}: ({len(data_sources[x])})" for x in data_sources)

            for feature_code in sorted(entity_data['MATCH_INFO']['FEATURE_SCORES'].keys(), key=lambda x: (self.feature_order.get(x, 99), x)):
                score_code = 'GNR_FN' if feature_code == 'NAME' else 'FULL_SCORE'
                score_config = self.scoring_config.get(feature_code, {'threshold': 0, '+weight': 100})
                best_score_record = sorted(entity_data['MATCH_INFO']['FEATURE_SCORES'][feature_code], key=lambda x: x[score_code])[-1]
                matched_entity[f'{feature_code}_SCORE'] = best_score_record[score_code]

                if best_score_record[score_code] >= score_config['threshold']:
                    matched_entity['MATCH_SCORE'] += (best_score_record[score_code] * (score_config['+weight']/100))
                elif score_config.get('-weight'):
                    matched_entity['MATCH_SCORE'] -= score_config['-weight']

                matched_entity['SEARCH_VALUES'] += f"{feature_code}: {best_score_record['INBOUND_FEAT']} | "
                matched_entity['MATCHED_VALUES'] += f"{feature_code}: {best_score_record['CANDIDATE_FEAT']} ({best_score_record[score_code]}) | "
                matched_entity['RAW_SCORES'][feature_code] = best_score_record
            matched_entity['MATCH_SCORE'] = round(matched_entity['MATCH_SCORE'],0)
            matched_entity['SEARCH_VALUES'] = matched_entity['SEARCH_VALUES'][0:-2] if matched_entity['SEARCH_VALUES'] else ''
            matched_entity['MATCHED_VALUES'] = matched_entity['MATCHED_VALUES'][0:-2] if matched_entity['MATCHED_VALUES'] else ''

            scored_entities.append(matched_entity)
        return scored_entities

    def filter_response(self, scored_response):
        filtered_entities = []
        cntr = 0
        for entity_data in sorted(scored_response, key=lambda x: x['MATCH_SCORE'], reverse=True):
            if self.match_level_filter and entity_data['MATCH_LEVEL'] > self.match_level_filter:
                continue
            if self.data_source_filter and self.data_source_filter not in str(entity_data['RECORD_LIST']):
                continue
            cntr += 1
            entity_data['MATCH_NUMBER'] = cntr
            filtered_entities.append(entity_data)

            if self.max_return_count and cntr == self.max_return_count:
                break

        return filtered_entities


def prepare_output(output_columns):
    column_headers = []
    column_mappings = []
    for column_data in output_columns:
        column_headers.append(list(column_data.items())[0][0])
        column_map = f"{list(column_data.items())[0][1]}"
        column_mappings.append(f'f"{column_map}"')
    return column_headers, column_mappings


def prepare_input(row_id, input_record):
    if not isinstance(input_record, dict):
        input_record = json.loads(input_record)
    input_record['ROW_ID'] = row_id
    return json.dumps(input_record)


def record_in_list(data_source, record_id, record_list):
    for record in record_list:
        if record_id == record['RECORD_ID'] and (record['DATA_SOURCE'] == data_source or not data_source):
            return True 
    return False


def get_next_record(reader):
    try: 
        return next(reader)
    except StopIteration: 
        return None

def file_search(engine, input_file, output_file, output_columns):

    output_file_name, output_file_ext = os.path.splitext(output_file)
    csv_output_file = output_file_name + '.csv'
    json_output_file = output_file_name + '.json'
    column_headers, column_mappings = prepare_output(output_columns)

    stat_pack = {
        'timings': {'started': datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")},
        'counts': {'search_count': 0,
                   'error_count': 0,
                   'found_count': 0,
                   'matched_count': 0,
                   'possible_count': 0,
                   'related_count': 0}}

    stat_pack['audit'] = {}
    stat_pack['audit']['best'] = {'true_positive_count': 0, 'false_positive_count': 0, 'false_negative_count': 0}
    stat_pack['audit']['all'] = {'true_positive_count': 0, 'false_positive_count': 0, 'false_negative_count': 0}

    if args.thread_count > 0:
        max_workers = args.thread_count
    else:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            max_workers = executor._max_workers

    start_time = time.time()
    with open(csv_output_file, 'w') as out_file:

        csv_writer = csv.writer(out_file)
        csv_writer.writerow(column_headers)

        input_file_ext = os.path.splitext(input_file)[1].upper()

        with open(input_file, 'r', encoding='utf-8-sig') as in_file:
            if input_file_ext == '.CSV':
                reader = csv.DictReader(in_file)
            else:
                reader = in_file

            with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
                logging.info(f"starting {executor._max_workers} threads")

                futures = {}
                record_count = 0
                while record_count < executor._max_workers: # prime the work queue
                    record = get_next_record(reader)
                    if not record:
                        break
                    record_count += 1
                    futures[executor.submit(engine.search, prepare_input(record_count, record))] = record

                while futures:
                    for f in concurrent.futures.as_completed(futures.keys()):
                        stat_pack['counts']['search_count'] += 1
                        response_data = f.result()
                        search_string = response_data['search_string']
                        search_row = json.loads(search_string)
                        if 'error' in response_data:
                            logging.warning(f"search record {search_row['ROW_ID']} returned {response_data['error']}")
                            stat_pack['counts']['error_count'] += 1
                        else:
                            search_response = response_data['search_response']
                            logging.debug(f"search record {search_row['ROW_ID']} returned {len(search_response)} entities")
                            if len(search_response) > 0:
                                stat_pack['counts']['found_count'] += 1
                                if search_response[0]['MATCH_LEVEL'] == 1:
                                    stat_pack['counts']['matched_count'] += 1
                                elif search_response[0]['MATCH_LEVEL'] == 2:
                                    stat_pack['counts']['possible_count'] += 1
                                else:
                                    stat_pack['counts']['related_count'] += 1
                            else:
                                search_response = [{'MATCH_NUMBER': 0}]

                            search_data_source = search_row.get('DATA_SOURCE')
                            search_record_id = search_row.get('RECORD_ID') 

                            for matched_entity in search_response:
                                
                                audit_status = 'n/a'
                                if search_record_id:
                                    if matched_entity['MATCH_NUMBER'] == 0:
                                        audit_status = 'false_negative'
                                    else:
                                        if record_in_list(search_data_source, search_record_id, matched_entity['RECORD_LIST']):
                                            audit_status = 'true_positive'
                                        else:
                                            audit_status = 'false_positive'
                                    stat_pack['audit']['all'][audit_status+'_count'] += 1
                                    if matched_entity['MATCH_NUMBER'] <= 1:
                                        stat_pack['audit']['best'][audit_status+'_count'] += 1
                                matched_entity['AUDIT_STATUS'] = audit_status

                                csv_record = []
                                for column_map in column_mappings:
                                    try:
                                        csv_record.append(eval(column_map))
                                    except Exception as ex:
                                        csv_record.append('')
                                        if matched_entity['MATCH_NUMBER'] != 0:
                                            logging.warning(f"column map[{column_map}] failed with {ex}")
                                csv_writer.writerow(csv_record)

                        if stat_pack['counts']['search_count'] % 1000 == 0:
                            eps = int(float(stat_pack['counts']['search_count']) / (float(time.time() - start_time if time.time() - start_time != 0 else 0)))
                            elapsed_min = round((time.time() - start_time) / 60, 1)
                            logging.info(f"{stat_pack['counts']['search_count']} searches, {stat_pack['counts']['found_count']} found, {stat_pack['counts']['error_count']} errors, {elapsed_min} minutes elapsed, {eps} searches per second")

                        futures.pop(f)

                        if not shut_down:
                            record = get_next_record(reader)
                            if record:
                                record_count += 1
                                futures[executor.submit(engine.search, prepare_input(record_count, record))] = record


                if args.debug:
                    try:
                        response = bytearray()
                        engine.g2_engine.stats(response)
                        stats = response.decode()
                        logging.debug(f"\n{stats}")
                    except:
                        pass

    stat_pack['timings']['ended'] = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    stat_pack['timings']['total_run_time'] = round((time.time() - start_time) / 60, 1)
    stat_pack['timings']['searches_per_second'] = int(float(stat_pack['counts']['search_count']) / (float(time.time() - start_time if time.time() - start_time != 0 else 0)))
    stat_pack['timings']['status'] = 'completed successfully' if shut_down == 0 else 'ABORTED!'

    if stat_pack['audit']['best']['true_positive_count'] + stat_pack['audit']['best']['false_positive_count'] + stat_pack['audit']['best']['false_negative_count'] == 0:
        del stat_pack['audit']
    else:
        stat_pack['audit']['best']['precision'] = round(stat_pack['audit']['best']['true_positive_count'] / (stat_pack['audit']['best']['true_positive_count'] + stat_pack['audit']['best']['false_positive_count'] + .0), 5)
        stat_pack['audit']['best']['recall'] = round(stat_pack['audit']['best']['true_positive_count'] / (stat_pack['audit']['best']['true_positive_count'] + stat_pack['audit']['best']['false_negative_count'] + .0), 5)
        stat_pack['audit']['best']['f1-score'] = round(2 * ((stat_pack['audit']['best']['precision'] * stat_pack['audit']['best']['recall']) / (stat_pack['audit']['best']['precision'] + stat_pack['audit']['best']['recall'] + .0)), 5)
        stat_pack['audit']['all']['precision'] = round(stat_pack['audit']['all']['true_positive_count'] / (stat_pack['audit']['all']['true_positive_count'] + stat_pack['audit']['all']['false_positive_count'] + .0), 5)
        stat_pack['audit']['all']['recall'] = round(stat_pack['audit']['all']['true_positive_count'] / (stat_pack['audit']['all']['true_positive_count'] + stat_pack['audit']['all']['false_negative_count'] + .0), 5)
        stat_pack['audit']['all']['f1-score'] = round(2 * ((stat_pack['audit']['all']['precision'] * stat_pack['audit']['all']['recall']) / (stat_pack['audit']['all']['precision'] + stat_pack['audit']['all']['recall'] + .0)), 5)

    logging.info(f"\n{json.dumps(stat_pack, indent=4)}")
    with open(json_output_file, 'w') as out_file:
        out_file.write(json.dumps(stat_pack, indent=4))


def get_engine_config_from_ini():
    if not os.getenv('SENZING_ETC_PATH'):
        raise Exception('Senzing environment not initialized')
    ini_file_name = os.path.normpath(os.path.join(os.getenv('SENZING_ETC_PATH'), 'G2Module.ini'))
    if not os.path.exists(ini_file_name):
        raise Exception(f"G2Module.ini not found at {os.getenv('SENZING_ETC_PATH')}")
    ini_parser = configparser.ConfigParser(empty_lines_in_values=False, interpolation=None)
    ini_parser.read(ini_file_name)
    json_config = {}
    for group_name in ini_parser.sections():
        upper_group_name = group_name.upper()
        json_config[upper_group_name] = {}
        for var_name in ini_parser[group_name]:
            upper_var_name = var_name.upper()
            json_config[upper_group_name][upper_var_name] = ini_parser[group_name][var_name]
    return json.dumps(json_config)


def signal_handler(signal, frame):
    logging.warning('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = 9


if __name__ == "__main__":

    shut_down = 0
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file_name', help='Path and name of optional G2Module.ini file to use.')
    parser.add_argument('-i', '--input_file_name', help='the name of a json input file')
    parser.add_argument('-o', '--output_file_root', help='root name for output files created, both a csv and a json stats file will be created')
    parser.add_argument('-t', '--thread_count', type=int, default=0, help='number of threads to start, defaults to max available')
    parser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='run in debug mode')
    args = parser.parse_args()

    loggingLevel = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d %I:%M', level=loggingLevel)

    if not args.input_file_name or not os.path.exists(args.input_file_name):
        logging.error(f"{'the input file does not exist' if args.input_file_name else 'an input file is required'}")
        sys.exit(-1)

    if not args.output_file_root:
        logging.error('an output file root name is required')
        sys.exit(-1)

    search_kwargs = {}
    output_columns = None
    if args.config_file_name:
        if not os.path.exists(args.config_file_name):
            logging.error('the configuration file does not exist')
            sys.exit(-1)
        try:
            config_data = json.load(open(args.config_file_name, 'r'))
            search_kwargs['max_return_count'] = config_data.get('filtering', {}).get('max_return_count', 0)
            search_kwargs['data_source_filter'] = config_data.get('filtering', {}).get('data_source_filter', '').upper()
            search_kwargs['match_level_filter'] = config_data.get('filtering', {}).get('match_level_filter', 0)
            search_kwargs['scoring_config'] = config_data.get('scoring', {})
            output_columns = config_data.get('output_columns', None)
        except Exception as err:
            logging.error(f"error in configuration file {err}")
            sys.exit(1)

    if os.getenv('SENZING_ENGINE_CONFIGURATION_JSON'):
        engine_config_json = os.getenv('SENZING_ENGINE_CONFIGURATION_JSON')
    else:
        try:
            engine_config_json = get_engine_config_from_ini()
        except Exception as ex:
            logging.error(ex)
            sys.exit(-1)

    logging.info('initializing ...')
    try:
        sz_engine = SZSearch(engine_config_json, **search_kwargs)
    except Exception as ex:
        logging.error(f"shutdown: {ex}")
        sys.exit(-1)

    file_search(sz_engine, args.input_file_name, args.output_file_root, output_columns)
    del sz_engine
