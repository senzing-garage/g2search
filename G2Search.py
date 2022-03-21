#! /usr/bin/env python3

import os
import sys
import argparse
import configparser
import signal
import time
import logging
from datetime import datetime, timedelta
import csv
import json
import glob

#--multi-threading
from multiprocessing import Process, Queue, Value, Manager
from queue import Empty, Full
import threading
import math


#--senzing python classes
try: 
    import G2Paths
    from G2Product import G2Product
    from G2Engine import G2Engine
    from G2IniParams import G2IniParams
    from G2ConfigMgr import G2ConfigMgr
    from G2Exception import G2Exception
    from G2Diagnostic import G2Diagnostic
except:
    print('\nPlease export PYTHONPATH=<path to senzing python directory>\n')
    sys.exit(1)

#---------------------------------------
def queue_read(queue): 
    try: return queue.get(True, 1)
    except Empty:
        time.sleep(.1)
        return None

#---------------------------------------
def queue_write(queue, message): 
    while True:
        try: queue.put(message, True, 1)
        except Full:
            time.sleep(.01)
            continue
        break

#---------------------------------------
def wait_for_queues(search_queue, result_queue):
    #--currently not used in favor of waiting for each queue
    waits = 0
    while search_queue.qsize() or result_queue.qsize():
        time.sleep(5)
        waits += 1
        if waits >= 10:
            break
        elif search_queue.qsize() or result_queue.qsize():
            logging.info('waiting for %s search_queue and %s result_queue records' % (search_queue.qsize(), result_queue.qsize()))

    if (search_queue.qsize() or result_queue.qsize()):
        logging.warning('queues are not empty!')
        return False
    return True

#---------------------------------------
def wait_for_queue(qname, q):
    waits = 0
    while q.qsize() and shutDown.value == 0:
        time.sleep(5)
        waits += 1
        #--disabled in favor of control-c shutDown
        if False: #waits >= 10:  
            break
        elif q.qsize():
            logging.info(f'waiting for {q.qsize()} {qname} records')
    if (q.qsize()):
        logging.warning(f'{qname} not empty!')
        return False
    return True

#---------------------------------------
def setup_search_queue(thread_count, stop_search_threads, search_queue, result_queue, mappingDoc):
    try: 
        g2Engine = G2Engine()
        g2Engine.initV2('G2Search', iniParams, False)
    except G2Exception as ex:
        logging.error(f'G2Exception: {ex}')
        with shutDown.get_lock():
            shutDown.value = 1
        return

    #--use minimal format unless record data requested
    #--initialize search flags
    #--
    #-- G2_ENTITY_MINIMAL_FORMAT = ( 1 << 18 )
    #-- G2_ENTITY_BRIEF_FORMAT = ( 1 << 20 )
    #-- G2_ENTITY_INCLUDE_NO_FEATURES
    #--
    #-- G2_EXPORT_INCLUDE_RESOLVED = ( 1 << 2 )
    #-- G2_EXPORT_INCLUDE_POSSIBLY_SAME = ( 1 << 3 )
    #--
    #if apiVersion['VERSION'][0:1] > '1':
    searchFlags = g2Engine.G2_SEARCH_INCLUDE_ALL_ENTITIES
    searchFlags = searchFlags | g2Engine.G2_SEARCH_INCLUDE_FEATURE_SCORES
    searchFlags = searchFlags | g2Engine.G2_ENTITY_INCLUDE_ENTITY_NAME
    searchFlags = searchFlags | g2Engine.G2_ENTITY_INCLUDE_RECORD_FORMATTED_DATA
    searchFlags = searchFlags | g2Engine.G2_ENTITY_INCLUDE_RECORD_JSON_DATA
    searchFlags = searchFlags | g2Engine.G2_SEARCH_INCLUDE_STATS

    thread_list = []
    for thread_id in range(thread_count):
        thread_list.append(threading.Thread(target=process_search_queue, args=(thread_id, stop_search_threads, search_queue, result_queue, mappingDoc, g2Engine, searchFlags)))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    g2Engine.destroy()

#---------------------------------------
def process_search_queue(thread_id, stop_search_threads, search_queue, result_queue, mappingDoc, g2Engine, searchFlags):
    while stop_search_threads.value == 0: 
        queue_data = queue_read(search_queue)
        if queue_data:
            logging.debug('read search_queue: ' + str(queue_data)[0:50])  
            result_rows = process_search(queue_data, mappingDoc, g2Engine, searchFlags)
            if result_rows:
                queue_write(result_queue, result_rows)
    logging.debug('process_search_queue %s shut down with %s left in the queue' % (thread_id, search_queue.qsize()))

#---------------------------------------
def setup_result_queue(thread_id, stop_result_thread, result_queue, mappingDoc, statPack):
    try: 
        mappingDoc['output']['fileHandle'] = open(mappingDoc['output']['fileName'], 'w')
    except IOError as err: 
        logging.error('Cannot write to %s: %s' % (mappingDoc['output']['fileName'], err))
        with shutDown.get_lock():
            shutDown.value = 1
        return

    if mappingDoc['output']['fileFormat'] != 'JSON':
        mappingDoc['output']['fileWriter'] = csv.writer(mappingDoc['output']['fileHandle'], dialect=csv.excel, quoting=csv.QUOTE_MINIMAL)
        mappingDoc['output']['fileWriter'].writerow(mappingDoc['output']['outputHeaders'])

    process_result_queue(thread_id, stop_result_thread, result_queue, mappingDoc, statPack)

    #print('-'*10)
    #print(json.dumps(statPack, indent = 4))
    #print('-'*10)
    mgr_statPack.update(statPack)
    mappingDoc['output']['fileHandle'].close()

#---------------------------------------
def process_result_queue(thread_id, stop_result_thread, result_queue, mappingDoc, statPack):
    cntr = 0 
    while stop_result_thread.value == 0:
        queue_data = queue_read(result_queue)
        if queue_data:
            logging.debug('read result_queue: ' + str(queue_data)[0:50])  
            statPack = process_result(queue_data, mappingDoc, statPack)
            cntr += 1
            if cntr % progressInterval == 0:
                display = {'search_count': statPack['summary']['search_count'],
                           'match_count': statPack['summary']['match_count'],
                           'resolved': statPack['resolution']['best']['resolved'],
                           'possible': statPack['resolution']['best']['possible'],
                           'related': statPack['resolution']['best']['related'],
                           'name_only': statPack['resolution']['best']['name_only']}
                logging.info('interim result: ' + json.dumps(display))
    logging.debug('process_result_queue %s shut down with %s left in the queue' % (thread_id, result_queue.qsize()))

#---------------------------------------
def process_search(rowData, mappingDoc, g2Engine, searchFlags):

    #--clean garbage values
    #for key in rowData:
    #    rowData[key] = csv_functions.clean_value(key, rowData[key])

    #--perform calculations
    mappingErrors = 0
    if 'calculations' in mappingDoc:
        for calcDict in mappingDoc['calculations']:
            try: newValue = eval(list(calcDict.values())[0])
            except Exception as err: 
                logging.debug('%s [%s]' % (list(calcDict.keys())[0], err)) 
                #mappingErrors += 1
            else:
                if type(newValue) == list:
                    for newItem in newValue:
                        rowData.update(newItem)
                else:
                    rowData[list(calcDict.keys())[0]] = newValue

    logging.debug(json.dumps(rowData))

    if 'search' in mappingDoc and 'filter' in mappingDoc['search']:
        try: skipRow = eval(mappingDoc['search']['filter'])
        except Exception as err: 
            skipRow = False
            logging.debug(' filter error: %s [%s]' % (mappingDoc['search']['filter'], err))
        if skipRow:
            #mappingDoc['result']['rowsSkipped'] += 1
            return None

    if 'search' in mappingDoc and 'attributes' in mappingDoc['search']:
        rootValues = {}
        subListValues = {}
        for attrDict in mappingDoc['search']['attributes']:
            if attrDict['attribute'] == '<ignore>':
                continue

            attrValue = getValue(rowData, attrDict['mapping'])
            if attrValue:
                if 'subList' in attrDict:
                    if attrDict['subList'] not in subListValues:
                        subListValues[attrDict['subList']] = {}
                    subListValues[attrDict['subList']][attrDict['label_attribute']] = attrValue
                else:
                    rootValues[attrDict['label_attribute']] = attrValue

        #--create the search json
        searchData = {}
        for subList in subListValues:
            searchData[subList] = [subListValues[subList]]
        searchData.update(rootValues)
        logging.debug(json.dumps(searchData))
        searchStr = json.dumps(searchData)

    else: #--already json or already mapped csv header
        searchStr = json.dumps(rowData)

    rowData['SEARCH_STRING'] = searchStr
    #--empty searchResult = '{"SEARCH_RESPONSE": {"RESOLVED_ENTITIES": []}}'???
    try: 
        response = bytearray()
        retcode = g2Engine.searchByAttributesV2(searchStr, searchFlags, response)
        response = response.decode() if response else ''
        #if len(response) > 500:
        #    print(json.dumps(json.loads(response), indent=4))
        #    pause()
    except G2ModuleException as err:
        logging.error(err)
        with shutDown.get_lock():
            shutDown.value = 1
        return None
    jsonResponse = json.loads(response)
    logging.debug(json.dumps(jsonResponse))

    matchList = []
    for resolvedEntity in jsonResponse['RESOLVED_ENTITIES']:

        #--create a list of data sources we found them in
        dataSources = {}
        for record in resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']:
            dataSource = record['DATA_SOURCE']
            if dataSource not in dataSources:
                dataSources[dataSource] = [record['RECORD_ID']]
            else:
                dataSources[dataSource].append(record['RECORD_ID'])

        dataSourceList = []
        for dataSource in dataSources:
            if len(dataSources[dataSource]) == 1:
                dataSourceList.append(dataSource + ': ' + dataSources[dataSource][0])
            else:
                dataSourceList.append(dataSource + ': ' + str(len(dataSources[dataSource])) + ' records')

        #--determine the matching criteria
        matchLevel = int(resolvedEntity['MATCH_INFO']['MATCH_LEVEL'])
        matchLevelCode = resolvedEntity['MATCH_INFO']['MATCH_LEVEL_CODE']
        matchKey = resolvedEntity['MATCH_INFO']['MATCH_KEY'] if resolvedEntity['MATCH_INFO']['MATCH_KEY'] else '' 
        matchKey = matchKey.replace('+RECORD_TYPE', '')

        scoreData = []
        bestScores = {}
        bestScores['NAME'] = {}
        bestScores['NAME']['score'] = 0
        bestScores['NAME']['value'] = 'n/a'
        for featureCode in resolvedEntity['MATCH_INFO']['FEATURE_SCORES']:
            if featureCode == 'NAME':
                scoreCode = 'GNR_FN'
            else: 
                scoreCode = 'FULL_SCORE'
            for scoreRecord in resolvedEntity['MATCH_INFO']['FEATURE_SCORES'][featureCode]:
                matchingScore= scoreRecord[scoreCode]
                matchingValue = scoreRecord['CANDIDATE_FEAT']
                scoreData.append('%s|%s|%s|%s' % (featureCode, scoreCode, matchingScore, matchingValue))
                if featureCode not in bestScores:
                    bestScores[featureCode] = {}
                    bestScores[featureCode]['score'] = 0
                    bestScores[featureCode]['value'] = 'n/a'
                if matchingScore > bestScores[featureCode]['score']:
                    bestScores[featureCode]['score'] = matchingScore
                    bestScores[featureCode]['value'] = matchingValue

        logging.debug(json.dumps(bestScores))

        #--perform scoring (use stored match_score if not overridden in the mapping document)
        if 'scoring' not in mappingDoc:
            matchScore = str(((5-resolvedEntity['MATCH_INFO']['MATCH_LEVEL']) * 100) + int(resolvedEntity['MATCH_INFO']['MATCH_SCORE'])) + '-' + str(1000+bestScores['NAME']['score'])[-3:]
        else:
            matchScore = 0
            for featureCode in bestScores:
                if featureCode in mappingDoc['scoring']:
                    logging.debug(featureCode, mappingDoc['scoring'][featureCode])
                    if bestScores[featureCode]['score'] >= mappingDoc['scoring'][featureCode]['threshold']:
                        matchScore += int(round(bestScores[featureCode]['score'] * (mappingDoc['scoring'][featureCode]['+weight'] / 100),0))
                    elif '-weight' in mappingDoc['scoring'][featureCode]:
                        matchScore += -mappingDoc['scoring'][featureCode]['-weight'] #--actual score does not matter if below the threshold

        #--create the possible match entity one-line summary
        matchedEntity = {}
        matchedEntity['ENTITY_ID'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_ID']
        if 'ENTITY_NAME' in resolvedEntity['ENTITY']['RESOLVED_ENTITY']:
            matchedEntity['ENTITY_NAME'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_NAME'] + (('\n aka: ' + bestScores['NAME']['value']) if bestScores['NAME']['value'] and bestScores['NAME']['value'] != resolvedEntity['ENTITY']['RESOLVED_ENTITY']['ENTITY_NAME'] else '')
        else:
            matchedEntity['ENTITY_NAME'] = bestScores['NAME']['value'] if 'NAME' in bestScores else ''
        matchedEntity['ENTITY_SOURCES'] = '\n'.join(dataSourceList)
        matchedEntity['MATCH_LEVEL'] = matchLevel
        matchedEntity['MATCH_LEVEL_CODE'] = matchLevelCode
        matchedEntity['MATCH_KEY'] = matchKey[1:]
        matchedEntity['MATCH_SCORE'] = matchScore
        matchedEntity['NAME_SCORE'] = bestScores['NAME']['score']
        matchedEntity['SCORE_DATA'] = json.dumps(bestScores)  #'\n'.join(sorted(map(str, scoreData)))

        logging.debug(json.dumps(matchedEntity))

        matchedEntity['RECORDS'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']

        #--check the output filters
        filteredOut = False
        if matchLevel > mappingDoc['output']['matchLevelFilter']:
            filteredOut = True
            logging.debug('** did not meet matchLevelFilter **')
        if bestScores['NAME']['score'] < mappingDoc['output']['nameScoreFilter']:
            filteredOut = True
            logging.debug('** did not meet nameScoreFilter **')
        if mappingDoc['output']['dataSourceFilter'] and mappingDoc['output']['dataSourceFilter'] not in dataSources:
            filteredOut = True
            logging.debug('** did not meet dataSourceFiler **')
        if not filteredOut:
            matchList.append(matchedEntity)

    #--set the no match condition
    if len(matchList) == 0:
    #    if requiredFieldsMissing:
    #        rowsSkipped += 1
    #    else:
        #rowsNotMatched += 1
        matchedEntity = {}
        matchedEntity['ENTITY_ID'] = 0
        matchedEntity['ENTITY_NAME'] = ''
        matchedEntity['ENTITY_SOURCES'] = ''
        matchedEntity['MATCH_NUMBER'] = 0
        matchedEntity['MATCH_LEVEL'] = 0
        matchedEntity['MATCH_LEVEL_CODE'] = ''
        matchedEntity['MATCH_KEY'] = ''
        matchedEntity['MATCH_SCORE'] = ''
        matchedEntity['NAME_SCORE'] = ''
        matchedEntity['SCORE_DATA'] = ''
        matchedEntity['RECORDS'] = []
        matchList.append(matchedEntity)
        logging.debug('** no matches found **')
        
    result_rows = []
    matchNumber = 0
    for matchedEntity in sorted(matchList, key=lambda x: x['MATCH_SCORE'], reverse=True):
        matchNumber += 1
        matchedEntity['MATCH_NUMBER'] = matchNumber if matchedEntity['ENTITY_ID'] != 0 else 0
        result_rows.append(matchedEntity)
        if matchNumber > mappingDoc['output']['maxReturnCount']:
            break

    return [rowData, result_rows]

#----------------------------------------
def process_result(resultData, mappingDoc, statPack):
    rowData = resultData[0]
    matchList = resultData[1]
    statPack['summary']['search_count'] += 1
    for matchedEntity in matchList:
        if matchedEntity['MATCH_NUMBER'] == 1: 
            statPack['summary']['match_count'] += 1
            level = 'best' 
        else:
            level = 'additional'

        if matchedEntity['MATCH_SCORE']:
            score = int(matchedEntity['MATCH_SCORE'])
            statPack['scoring'][level]['total'] += 1
            if score >= 100:
                statPack['scoring'][level]['>=100'] += 1
            elif score >= 95:
                statPack['scoring'][level]['>=95'] += 1
            elif score >= 90:
                statPack['scoring'][level]['>=90'] += 1
            elif score >= 85:
                statPack['scoring'][level]['>=85'] += 1
            elif score >= 80:
                statPack['scoring'][level]['>=80'] += 1
            elif score >= 75:
                statPack['scoring'][level]['>=75'] += 1
            elif score >= 70:
                statPack['scoring'][level]['>=70'] += 1
            else:
                statPack['scoring'][level]['<70'] += 1

        if matchedEntity['NAME_SCORE']:
            score = int(matchedEntity['NAME_SCORE'])
            statPack['scoring']['name']['total'] += 1
            if score >= 100:
                statPack['scoring']['name']['=100'] += 1
            elif score >= 95:
                statPack['scoring']['name']['>=95'] += 1
            elif score >= 90:
                statPack['scoring']['name']['>=90'] += 1
            elif score >= 85:
                statPack['scoring']['name']['>=85'] += 1
            elif score >= 80:
                statPack['scoring']['name']['>=80'] += 1
            elif score >= 75:
                statPack['scoring']['name']['>=75'] += 1
            elif score >= 70:
                statPack['scoring']['name']['>=70'] += 1
            else:
                statPack['scoring']['name']['<70'] += 1

        #--get the column values
        #uppercasedJsonData = False
        rowValues = []
        for columnDict in mappingDoc['output']['columns']:
            columnValue = ''
            try: 
                if columnDict['source'].upper() in ('CSV', 'INPUT'):
                    columnValue = columnDict['value'] % rowData
                elif columnDict['source'].upper() == 'API':
                    columnValue = columnDict['value'] % matchedEntity
            except:
                if mappingDoc['input']['fileFormat'].upper() != 'JSON':
                    logging.warning('could not find %s in %s' % (columnDict['value'],columnDict['source'].upper())) 

            #--comes from the records
            if columnDict['source'].upper() == 'RECORD':
                #if not uppercasedJsonData:
                #    record['JSON_DATA'] = dictKeysUpper(record['JSON_DATA'])
                #    uppercasedJsonData = True
                columnValues = []
                for record in matchedEntity['RECORDS']:
                    if columnDict['value'].upper().endswith('_DATA'):
                        for item in record[columnDict['value'].upper()]:
                            columnValues.append(item)
                    else:
                        try: thisValue = columnDict['value'] % record['JSON_DATA']
                        except: pass
                        else:
                            if thisValue and thisValue not in columnValues:
                                columnValues.append(thisValue)
                if columnValues:
                    columnValue = '\n'.join(sorted(map(str, columnValues)))

            #if debugOn:
            #    print(columnDict['value'], columnValue)
            if len(columnValue) > 32000:
                columnValue = columnValue[0:32000]
                logging.info('column %s truncated at 32k' % columnDict['name'])
            rowValues.append(columnValue.replace('\n', '|'))
                    
        #--write the record
        if mappingDoc['output']['fileFormat'] != 'JSON':
            mappingDoc['output']['fileWriter'].writerow(rowValues)
        else:
            mappingDoc['output']['fileHandle'].write(json.dumps(rowValues) + '\n')
    
        #--update the counters
        if matchedEntity['MATCH_LEVEL'] != 0:
            statPack['resolution'][level]['total'] += 1
        if matchedEntity['MATCH_LEVEL'] == 1:
            statPack['resolution'][level]['resolved'] += 1
        elif matchedEntity['MATCH_LEVEL'] == 2:
            statPack['resolution'][level]['possible'] += 1
        elif matchedEntity['MATCH_LEVEL'] == 3:
            statPack['resolution'][level]['related'] += 1
        elif matchedEntity['MATCH_LEVEL'] == 4:
            statPack['resolution'][level]['name_only'] += 1

    return statPack

#----------------------------------------
def processFile(mappingDoc):
    
    #--initialize the stats
    statPack = {}
    statPack['resolution'] = {}
    statPack['resolution']['best'] = {}
    statPack['resolution']['best']['total'] = 0
    statPack['resolution']['best']['resolved'] = 0
    statPack['resolution']['best']['possible'] = 0
    statPack['resolution']['best']['related'] = 0
    statPack['resolution']['best']['name_only'] = 0
    statPack['resolution']['additional'] = {}
    statPack['resolution']['additional']['total'] = 0
    statPack['resolution']['additional']['resolved'] = 0
    statPack['resolution']['additional']['possible'] = 0
    statPack['resolution']['additional']['related'] = 0
    statPack['resolution']['additional']['name_only'] = 0
    statPack['scoring'] = {}
    statPack['scoring']['best'] = {}
    statPack['scoring']['best']['total'] = 0
    statPack['scoring']['best']['>=100'] = 0
    statPack['scoring']['best']['>=95'] = 0
    statPack['scoring']['best']['>=90'] = 0
    statPack['scoring']['best']['>=85'] = 0
    statPack['scoring']['best']['>=80'] = 0
    statPack['scoring']['best']['>=75'] = 0
    statPack['scoring']['best']['>=70'] = 0
    statPack['scoring']['best']['<70'] = 0
    statPack['scoring']['additional'] = {}
    statPack['scoring']['additional']['total'] = 0
    statPack['scoring']['additional']['>=100'] = 0
    statPack['scoring']['additional']['>=95'] = 0
    statPack['scoring']['additional']['>=90'] = 0
    statPack['scoring']['additional']['>=85'] = 0
    statPack['scoring']['additional']['>=80'] = 0
    statPack['scoring']['additional']['>=75'] = 0
    statPack['scoring']['additional']['>=70'] = 0
    statPack['scoring']['additional']['<70'] = 0
    statPack['scoring']['name'] = {}
    statPack['scoring']['name']['total'] = 0
    statPack['scoring']['name']['=100'] = 0
    statPack['scoring']['name']['>=95'] = 0
    statPack['scoring']['name']['>=90'] = 0
    statPack['scoring']['name']['>=85'] = 0
    statPack['scoring']['name']['>=80'] = 0
    statPack['scoring']['name']['>=75'] = 0
    statPack['scoring']['name']['>=70'] = 0
    statPack['scoring']['name']['<70'] = 0
    statPack['summary'] = {}
    statPack['summary']['search_count'] = 0
    statPack['summary']['match_count'] = 0

    #--upper case value replacements
    if 'columnHeaders' in mappingDoc['input']:
        mappingDoc['input']['columnHeaders'] = [x.upper() for x in mappingDoc['input']['columnHeaders']]

    if 'search' in mappingDoc and 'attributes' in mappingDoc['search']:
        for ii in range(len(mappingDoc['search']['attributes'])):
            mappingDoc['search']['attributes'][ii]['mapping'] = mappingDoc['search']['attributes'][ii]['mapping'].upper().replace(')S', ')s')

    for ii in range(len(mappingDoc['output']['columns'])):
        mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

    #--build output headers
    recordDataRequested = False
    outputHeaders = []
    for ii in range(len(mappingDoc['output']['columns'])):
        columnName = mappingDoc['output']['columns'][ii]['name'].upper()
        mappingDoc['output']['columns'][ii]['name'] = columnName
        outputHeaders.append(columnName)
        if mappingDoc['output']['columns'][ii]['source'].upper() == 'RECORD':
            recordDataRequested = True
    mappingDoc['output']['outputHeaders'] = outputHeaders

    if 'matchLevelFilter' not in mappingDoc['output'] or int(mappingDoc['output']['matchLevelFilter']) < 1:
        mappingDoc['output']['matchLevelFilter'] = 99
    else:
        mappingDoc['output']['matchLevelFilter'] = int(mappingDoc['output']['matchLevelFilter'])
        if mappingDoc['output']['matchLevelFilter'] == 1:
            searchFlags = searchFlags | g2Engine.G2_EXPORT_INCLUDE_RESOLVED
        elif mappingDoc['output']['matchLevelFilter'] == 2:
            searchFlags = searchFlags | g2Engine.G2_EXPORT_INCLUDE_RESOLVED | g2Engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME

    if 'nameScoreFilter' not in mappingDoc['output']:
        mappingDoc['output']['nameScoreFilter'] = 0
    else:
        mappingDoc['output']['nameScoreFilter'] = int(mappingDoc['output']['nameScoreFilter'])

    if 'dataSourceFilter' not in mappingDoc['output']:
        mappingDoc['output']['dataSourceFilter'] = None
    else:
        mappingDoc['output']['dataSourceFilter'] = mappingDoc['output']['dataSourceFilter'].upper()

    if 'maxReturnCount' not in mappingDoc['output']:
        mappingDoc['output']['maxReturnCount'] = 1
    else:
        mappingDoc['output']['maxReturnCount'] = int(mappingDoc['output']['maxReturnCount'])
       
    logging.info(f'starting {threadCount} threads ...')
    stop_search_threads = Value('i', 0)
    search_queue = Queue(threadCount * 100)
    stop_result_thread = Value('i', 0)
    result_queue = Queue(threadCount * 1000)

    search_process_list = []
    unusedThreads = threadCount - 1
    while unusedThreads > 0:
        if unusedThreads > args.max_threads_per_process:
            thisThreadCount = args.max_threads_per_process
            unusedThreads -= args.max_threads_per_process
        else:
            thisThreadCount = unusedThreads
            unusedThreads = 0
        search_process_list.append(Process(target=setup_search_queue, args=(thisThreadCount, stop_search_threads, search_queue, result_queue, mappingDoc)))
    for process in search_process_list:
        process.start()

    #--final thread processes the result 
    result_process = Process(target=setup_result_queue, args=(999, stop_result_thread, result_queue, mappingDoc, statPack))
    result_process.start()

    #--upper case value replacements
    #for ii in range(len(mappingDoc['search']['attributes'])):
    #    mappingDoc['search']['attributes'][ii]['value'] = mappingDoc['search']['attributes'][ii]['value'].upper().replace(')S', ')s')
    #for ii in range(len(mappingDoc['output']['columns'])):
    #    mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

    rowsSkipped = 0
    rowsMatched = 0
    rowsNotMatched = 0
    resolvedMatches = 0
    possibleMatches = 0
    possiblyRelateds = 0
    nameOnlyMatches = 0

    mappedList = []
    unmappedList = []
    ignoredList = []

    #--ensure uniqueness of attributes, especially if using labels (usage types)
    errorCnt = 0
    labelAttrList = []

    if 'search' in mappingDoc and 'attributes' in mappingDoc['search']:
        for i1 in range(len(mappingDoc['search']['attributes'])):
            if mappingDoc['search']['attributes'][i1]['attribute'] == '<ignore>':
                if 'mapping' in mappingDoc['search']['attributes'][i1]:
                    ignoredList.append(mappingDoc['search']['attributes'][i1]['mapping'].replace('%(','').replace(')s',''))
                continue
            elif csv_functions.is_senzing_attribute(mappingDoc['search']['attributes'][i1]['attribute']):
                mappedList.append(mappingDoc['search']['attributes'][i1]['attribute'])
            else:
                unmappedList.append(mappingDoc['search']['attributes'][i1]['attribute'])

            if 'label' in mappingDoc['search']['attributes'][i1]:
                mappingDoc['search']['attributes'][i1]['label_attribute'] = mappingDoc['search']['attributes'][i1]['label'].replace('_', '-') + '_'
            else:
                mappingDoc['search']['attributes'][i1]['label_attribute'] = ''
            mappingDoc['search']['attributes'][i1]['label_attribute'] += mappingDoc['search']['attributes'][i1]['attribute']
            if mappingDoc['search']['attributes'][i1]['label_attribute'] in labelAttrList:
                errorCnt += 1
                logging.warning('attribute %s (%s) is duplicated for output %s!' % (i1, mappingDoc['search']['attributes'][i1]['label_attribute'], i))
            else:
                labelAttrList.append(mappingDoc['search']['attributes'][i1]['label_attribute'])
        if errorCnt:
            return -1

    #--override mapping document with parameters
    #if fieldDelimiter or 'fieldDelimiter' not in mappingDoc['input']:
    #    mappingDoc['input']['fieldDelimiter'] = fieldDelimiter
    #if fileEncoding or 'fileEncoding' not in mappingDoc['input']:
    #    mappingDoc['input']['fileEncoding'] = fileEncoding
    if 'columnHeaders' not in mappingDoc['input']:
        mappingDoc['input']['columnHeaders'] = []

    #--get the file format
    if 'fileFormat' not in mappingDoc['input']:
        mappingDoc['input']['fileFormat'] = 'CSV'
    else:
        mappingDoc['input']['fileFormat'] = mappingDoc['input']['fileFormat'].upper()

    #--for each input file
    totalRowCnt = 0
    for fileName in fileList:
        logging.info('Processing %s ...' % fileName)

        currentFile = {}
        currentFile['name'] = fileName
        currentFile['rowCnt'] = 0
        currentFile['skipCnt'] = 0

        #--open the file
        if 'fileEncoding' in mappingDoc['input'] and mappingDoc['input']['fileEncoding']:
            currentFile['fileEncoding'] = mappingDoc['input']['fileEncoding']
            currentFile['handle'] = open(fileName, 'r', encoding=mappingDoc['input']['fileEncoding'])
        else:
            currentFile['handle'] = open(fileName, 'r')

        #--set the current file details
        currentFile['fileFormat'] = mappingDoc['input']['fileFormat']
        if currentFile['fileFormat'] == 'JSON':
            currentFile['csvDialect'] = 'n/a'
            currentFile['reader'] = currentFile['handle']
        else:
            #currentFile['fieldDelimiter'] = mappingDoc['input']['fieldDelimiter']
            if 'fieldDelimiter' not in mappingDoc['input']:
                currentFile['csvDialect'] = csv.Sniffer().sniff(currentFile['handle'].readline(), delimiters='|,\t')
                currentFile['handle'].seek(0)
                currentFile['fieldDelimiter'] = currentFile['csvDialect'].delimiter
                mappingDoc['input']['fieldDelimiter'] = currentFile['csvDialect'].delimiter
            elif mappingDoc['input']['fieldDelimiter'].lower() in ('csv', 'comma', ','):
                currentFile['csvDialect'] = 'excel'
            elif mappingDoc['input']['fieldDelimiter'].lower() in ('tab', 'tsv', '\t'):
                currentFile['csvDialect'] = 'excel-tab'
            elif mappingDoc['input']['fieldDelimiter'].lower() in ('pipe', '|'):
                csv.register_dialect('pipe', delimiter = '|', quotechar = '"')
                currentFile['csvDialect'] = 'pipe'
            elif len(mappingDoc['input']['fieldDelimiter']) == 1:
                csv.register_dialect('other', delimiter = delimiter, quotechar = '"')
                currentFile['csvDialect'] = 'other'
            elif len(mappingDoc['input']['fieldDelimiter']) > 1:
                currentFile['csvDialect'] = 'multi'
            else:
                currentFile['csvDialect'] = 'excel'

            #--set the reader (csv cannot be used for multi-char delimiters)
            if currentFile['csvDialect'] != 'multi':
                currentFile['reader'] = csv.reader(currentFile['handle'], dialect=currentFile['csvDialect'])
            else:
                currentFile['reader'] = currentFile['handle']

            #--get the current file header row and use it if not one already
            currentFile, currentHeaders = getNextRow(currentFile)
            if not mappingDoc['input']['columnHeaders']:
                mappingDoc['input']['columnHeaders'] = [x.upper() for x in currentHeaders]
            currentFile['header'] = mappingDoc['input']['columnHeaders']

        batchStartTime = time.time()
        while True:
            currentFile, rowData = getNextRow(currentFile)
            if rowData:
                totalRowCnt += 1
                rowData['ROW_ID'] = totalRowCnt
                queue_write(search_queue, rowData)

            if totalRowCnt % progressInterval == 0 or not rowData:
                logging.info(f'{totalRowCnt} rows read')

            #--break conditions
            if (not rowData) or shutDown.value:
                break

        currentFile['handle'].close()
        if shutDown.value:
            break

    #--finish the search queues
    logging.info('Finishing up ...')
    queuesEmpty = wait_for_queue('search_queue', search_queue)
    with stop_search_threads.get_lock():
        stop_search_threads.value = 1
    start = time.time()
    while time.time() - start <= 60:
        if not any(process.is_alive() for process in search_process_list):
            break
        alive_list = [process for process in search_process_list if process.is_alive()]
        logging.info(f'waiting for {len(alive_list)} processes')
        time.sleep(5)
    for process in search_process_list:
        if process.is_alive():
            logging.warning(process.name, 'did not terminate gracefully')
            process.terminate()
        else:
            process.join()
    search_queue.close() 

    #--finish the result queues
    queuesEmpty = wait_for_queue('result_queue', result_queue)
    with stop_result_thread.get_lock():
        stop_result_thread.value = 1
    start = time.time()
    while time.time() - start <= 60:
        if not result_process.is_alive():
            break
        time.sleep(5)
    if result_process.is_alive():
        logging.warning(result_process.name, 'did not terminate gracefully')
        result_process.terminate()
    else:
        result_process.join()
    result_queue.close() 

    #--bring in and finalize the statPack from the result queue process
    statPack = dict(mgr_statPack)
    if statPack['summary']['search_count'] > 0:
        statPack['summary']['match_percent'] = str(round(((float(statPack['summary']['match_count']) / float(statPack['summary']['search_count'])) * 100.00), 2)) + '%'
        display = {'search_count': statPack['summary']['search_count'],
                   'match_count': statPack['summary']['match_count'],
                   'resolved': statPack['resolution']['best']['resolved'],
                   'possible': statPack['resolution']['best']['possible'],
                   'related': statPack['resolution']['best']['related'],
                   'name_only': statPack['resolution']['best']['name_only']}
        logging.info('final result: ' + json.dumps(display))
        if logFileName:
            with open(logFileName, 'w') as outfile:
                json.dump(statPack, outfile, indent=4)    

    record_count = statPack['summary']['search_count']
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    eps = int(float(record_count) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
    logging.info(f'{record_count} records processed in {elapsedMins} minutes = {eps} records per second')

    return 

#----------------------------------------
def getNextRow(fileInfo):
    errCnt = 0
    rowData = None
    while not rowData:

        #--quit for consecutive errors
        if errCnt >= 10:
            logging.error('Shutdown due to too many errors')
            with shutDown.get_lock():
                shutDown.value = 1
            break
             
        try: line = next(fileInfo['reader'])
        except StopIteration:
            break
        except: 
            logging.warning(' row %s: %s' % (fileInfo['rowCnt'], sys.exc_info()[0]))
            fileInfo['skipCnt'] += 1
            errCnt += 1
            continue
        fileInfo['rowCnt'] += 1
        if line: #--skip empty lines

            if fileInfo['fileFormat'] == 'JSON':
                return fileInfo, json.loads(line)

            #--csv reader will return a list (mult-char delimiter must be manually split)
            if type(line) == list:
                row = line
            else:
                row = [removeQuoteChar(x.strip()) for x in line.split(fileInfo['delimiter'])]

            #--turn into a dictionary if there is a header
            if 'header' in fileInfo:

                #--column mismatch
                if len(row) != len(fileInfo['header']):
                    logging.warning(' row %s has %s columns, expected %s' % (fileInfo['rowCnt'], len(row), len(fileInfo['header'])))
                    fileInfo['skipCnt'] += 1
                    errCnt += 1
                    continue

                #--is it the header row
                elif str(row[0]).upper() == fileInfo['header'][0].upper() and str(row[len(row)-1]).upper() == fileInfo['header'][len(fileInfo['header'])-1].upper():
                    fileInfo['skipCnt'] += 1
                    if fileInfo['rowCnt'] != 1:
                        logging.info(' row %s contains the header' % fileInfo['rowCnt'])
                        errCnt += 1
                    continue

                #--return a good row
                else:
                    rowData = dict(zip(fileInfo['header'], [str(x).strip() for x in row]))

            else: #--if not just return what should be the header row
                fileInfo['skipCnt'] += 1
                rowData = [str(x).strip() for x in row]

        else:
            logging.info(' row %s is blank' % fileInfo['rowCnt'])
            fileInfo['skipCnt'] += 1
            continue

    return fileInfo, rowData

#----------------------------------------
def removeQuoteChar(s):
    if len(s)>2 and s[0] + s[-1] in ("''", '""'):
        return s[1:-1] 
    return s 

#----------------------------------------
def getValue(rowData, expression):
    try: rtnValue = expression % rowData
    except: 
        logging.warning('could not map %s' % (expression,)) 
        rtnValue = ''
    return rtnValue
    
#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    try: response = input(question)
    except KeyboardInterrupt:
        response = None
        with shutDown.get_lock():
            shutDown.value = 9
    return response

#----------------------------------------
def signal_handler(signal, frame):
    logging.warning('USER INTERUPT! Shutting down ... (please wait)')
    with shutDown.get_lock():
        shutDown.value = 9
    return

#----------------------------------------
if __name__ == "__main__":
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    shutDown = Value('i', 0)
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    progressInterval = 1000

    mgr = Manager()
    mgr_statPack = mgr.dict()
       
    try: iniFileName = G2Paths.get_G2Module_ini_path()
    except: iniFileName = '' 

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file_name', dest='ini_file_name', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='the name of a mapping file')
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of an input file')
    parser.add_argument('-d', '--delimiterChar', dest='delimiterChar', help='delimiter character')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='logFileName', help='optional statistics filename (json format)')
    parser.add_argument('-nt', '--thread_count', type=int, default=0, help='number of threads to start')
    parser.add_argument('-mtp', '--maxThreadsPerProcess', dest='max_threads_per_process', default=16, type=int, help='maximum threads per process, default=%(default)s')
    parser.add_argument('-D', '--debugOn', dest='debugOn', action='store_true', default=False, help='run in debug mode')
    args = parser.parse_args()
    ini_file_name = args.ini_file_name
    mappingFileName = args.mappingFileName
    inputFileName = args.inputFileName
    delimiterChar = args.delimiterChar
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    logFileName = args.logFileName
    threadCount = args.thread_count

    loggingLevel = logging.DEBUG if args.debugOn else logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d %I:%M', level=loggingLevel)

    #--read the mapping file
    if not os.path.exists(mappingFileName):
        logging.error(f'\n{mappingFileName} does not exist')
        sys.exit(1)
    try: mappingDoc = json.load(open(mappingFileName, 'r'))
    except ValueError as err:
        logging.error(f'\nmapping file error: {err} in{mappingFileName}')
        sys.exit(1)

    #--get the input file
    if inputFileName or 'inputFileName' not in mappingDoc['input']:
        mappingDoc['input']['inputFileName'] = inputFileName
    if not mappingDoc['input']['inputFileName']:
        logging.error('\nno input file supplied')
        sys.exit(1)
    fileList = glob.glob(mappingDoc['input']['inputFileName'])
    if len(fileList) == 0:
        logging.error(f'\n{inputFileName} not found')
        sys.exit(1)

    #--get the output file name
    if outputFileName:
        mappingDoc['output']['fileName'] = outputFileName
    if 'fileName' not in mappingDoc['output']:
        logging.error('\nan ouput file name is required')
        sys.exit(1)

    #--get the ini file parameters
    try:
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
    except G2Exception as err:
        logging.error('%s' % str(err))
        sys.exit(1)

    #--determine the number of threads to use
    if threadCount == 0:
        try:
            g2Diag = G2Diagnostic()
            g2Diag.initV2('pyG2Diagnostic', iniParams, False)
            physical_cores = g2Diag.getPhysicalCores()
            logical_cores = g2Diag.getLogicalCores()
            calc_cores_factor = 2 if physical_cores != logical_cores else 1.2
            threadCount = math.ceil(logical_cores * calc_cores_factor)
            logging.info(f'Physical cores: {logical_cores}, logical cores: {logical_cores}, default threads: {threadCount}')
        except G2Exception as err:
            logging.error('%s' % str(err))
            sys.exit(1)

    processFile(mappingDoc)

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if shutDown.value == 0:
        logging.info(f'Process completed successfully in {elapsedMins} minutes!\n')
    else:
        logging.warning(f'Process aborted after {elapsedMins} minutes!\n')

    sys.exit(shutDown.value)
