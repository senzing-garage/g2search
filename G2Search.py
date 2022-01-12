#! /usr/bin/env python3

import os
import sys
import argparse
import configparser
import signal
import time
from datetime import datetime, timedelta
import csv
import json
import glob

#--senzing python classes
try: 
    import G2Paths
    from G2Product import G2Product
    from G2Engine import G2Engine
    from G2IniParams import G2IniParams
    from G2ConfigMgr import G2ConfigMgr
    from G2Exception import G2Exception
except:
    print('')
    print('Please export PYTHONPATH=<path to senzing python directory>')
    print('')
    sys.exit(1)

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    try: response = input(question)
    except KeyboardInterrupt:
        response = None
        global shutDown
        shutDown = True
    return response

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True
    return
        
#----------------------------------------
def getNextRow(fileInfo):
    errCnt = 0
    rowData = None
    while not rowData:

        #--quit for consecutive errors
        if errCnt >= 10:
            fileInfo['ERROR'] = 'YES'
            print()
            print('Shutdown due to too many errors')
            break
             
        try: line = next(fileInfo['reader'])
        except StopIteration:
            break
        except: 
            print(' row %s: %s' % (fileInfo['rowCnt'], sys.exc_info()[0]))
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
                    print(' row %s has %s columns, expected %s' % (fileInfo['rowCnt'], len(row), len(fileInfo['header'])))
                    fileInfo['skipCnt'] += 1
                    errCnt += 1
                    continue

                #--is it the header row
                elif str(row[0]).upper() == fileInfo['header'][0].upper() and str(row[len(row)-1]).upper() == fileInfo['header'][len(fileInfo['header'])-1].upper():
                    fileInfo['skipCnt'] += 1
                    if fileInfo['rowCnt'] != 1:
                        print(' row %s contains the header' % fileInfo['rowCnt'])
                        errCnt += 1
                    continue

                #--return a good row
                else:
                    rowData = dict(zip(fileInfo['header'], [str(x).strip() for x in row]))

            else: #--if not just return what should be the header row
                fileInfo['skipCnt'] += 1
                rowData = [str(x).strip() for x in row]

        else:
            print(' row %s is blank' % fileInfo['rowCnt'])
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
        print('warning: could not map %s' % (expression,)) 
        rtnValue = ''
    return rtnValue
    
#----------------------------------------
def processFile():
    global shutDown
    
    #--read the mapping file
    if not os.path.exists(mappingFileName):
        print(f'\n{mappingFileName} does not exist')
        return -1
    
    try: mappingDoc = json.load(open(mappingFileName, 'r'))
    except ValueError as err:
        print(f'\nmapping file error: {err} in{mappingFileName}')
        return -1

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
       
    #--open the output file
    if outputFileName:
        fileName = outputFileName
    else:
        if 'fileName' not in mappingDoc['output']:
            print('\nan ouput file name is required')
            return -1
        fileName = mappingDoc['output']['fileName']
    outputFileHandle = open(fileName, 'w', encoding='utf-8', newline='')
    mappingDoc['output']['fileHandle'] = outputFileHandle
    if mappingDoc['output']['fileType'] != 'JSON':
        mappingDoc['output']['fileWriter'] = csv.writer(mappingDoc['output']['fileHandle'], dialect=csv.excel, quoting=csv.QUOTE_MINIMAL)
        mappingDoc['output']['fileWriter'].writerow(outputHeaders)

    #--upper case value replacements
    #for ii in range(len(mappingDoc['search']['attributes'])):
    #    mappingDoc['search']['attributes'][ii]['value'] = mappingDoc['search']['attributes'][ii]['value'].upper().replace(')S', ')s')
    #for ii in range(len(mappingDoc['output']['columns'])):
    #    mappingDoc['output']['columns'][ii]['value'] = mappingDoc['output']['columns'][ii]['value'].upper().replace(')S', ')s')

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

    rowsSkipped = 0
    rowsMatched = 0
    rowsNotMatched = 0
    resolvedMatches = 0
    possibleMatches = 0
    possiblyRelateds = 0
    nameOnlyMatches = 0

    mappingDoc['result'] = {}
    mappingDoc['result']['rowsSearched'] = 0
    mappingDoc['result']['rowsSkipped'] = 0
    mappingDoc['result']['mappedList'] = []
    mappingDoc['result']['unmappedList'] = []
    mappingDoc['result']['ignoredList'] = []
    mappingDoc['result']['statistics'] = {}

    #--ensure uniqueness of attributes, especially if using labels (usage types)
    errorCnt = 0
    labelAttrList = []

    if 'search' in mappingDoc and 'attributes' in mappingDoc['search']:
        for i1 in range(len(mappingDoc['search']['attributes'])):
            if mappingDoc['search']['attributes'][i1]['attribute'] == '<ignore>':
                if 'mapping' in mappingDoc['search']['attributes'][i1]:
                    mappingDoc['result']['ignoredList'].append(mappingDoc['search']['attributes'][i1]['mapping'].replace('%(','').replace(')s',''))
                continue
            elif csv_functions.is_senzing_attribute(mappingDoc['search']['attributes'][i1]['attribute']):
                mappingDoc['result']['mappedList'].append(mappingDoc['search']['attributes'][i1]['attribute'])
            else:
                mappingDoc['result']['unmappedList'].append(mappingDoc['search']['attributes'][i1]['attribute'])
            mappingDoc['result']['statistics'][mappingDoc['search']['attributes'][i1]['attribute']] = 0

            if 'label' in mappingDoc['search']['attributes'][i1]:
                mappingDoc['search']['attributes'][i1]['label_attribute'] = mappingDoc['search']['attributes'][i1]['label'].replace('_', '-') + '_'
            else:
                mappingDoc['search']['attributes'][i1]['label_attribute'] = ''
            mappingDoc['search']['attributes'][i1]['label_attribute'] += mappingDoc['search']['attributes'][i1]['attribute']
            if mappingDoc['search']['attributes'][i1]['label_attribute'] in labelAttrList:
                errorCnt += 1
                print('attribute %s (%s) is duplicated for output %s!' % (i1, mappingDoc['search']['attributes'][i1]['label_attribute'], i))
            else:
                labelAttrList.append(mappingDoc['search']['attributes'][i1]['label_attribute'])
        if errorCnt:
            return -1

    #--override mapping document with parameters
    if inputFileName or 'inputFileName' not in mappingDoc['input']:
        mappingDoc['input']['inputFileName'] = inputFileName
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

    #--get the input file
    if not mappingDoc['input']['inputFileName']:
        print('\nno input file supplied')
        return 1
    fileList = glob.glob(mappingDoc['input']['inputFileName'])
    if len(fileList) == 0:
        print('')
        print(f'\n{inputFileName} not found')
        return 1

    #--for each input file
    totalRowCnt = 0
    for fileName in fileList:
        print('')
        print('Processing %s ...' % fileName)
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
            if not rowData or shutDown:
                break

            totalRowCnt += 1
            rowData['ROW_ID'] = totalRowCnt

            #--clean garbage values
            #for key in rowData:
            #    rowData[key] = csv_functions.clean_value(key, rowData[key])

            #--perform calculations
            mappingErrors = 0
            if 'calculations' in mappingDoc:
                for calcDict in mappingDoc['calculations']:
                    try: newValue = eval(list(calcDict.values())[0])
                    except Exception as err: 
                        print('  error: %s [%s]' % (list(calcDict.keys())[0], err)) 
                        mappingErrors += 1
                    else:
                        if type(newValue) == list:
                            for newItem in newValue:
                                rowData.update(newItem)
                        else:
                            rowData[list(calcDict.keys())[0]] = newValue

            if debugOn:
                print(json.dumps(rowData, indent=4))
                pause()

            if 'search' in mappingDoc and 'filter' in mappingDoc['search']:
                try: skipRow = eval(mappingDoc['search']['filter'])
                except Exception as err: 
                    skipRow = False
                    print(' filter error: %s [%s]' % (mappingDoc['search']['filter'], err))
                if skipRow:
                    mappingDoc['result']['rowsSkipped'] += 1
                    continue

            if 'search' in mappingDoc and 'attributes' in mappingDoc['search']:
                rootValues = {}
                subListValues = {}
                for attrDict in mappingDoc['search']['attributes']:
                    if attrDict['attribute'] == '<ignore>':
                        continue

                    attrValue = getValue(rowData, attrDict['mapping'])
                    if attrValue:
                        mappingDoc['result']['statistics'][attrDict['attribute']] += 1
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
                if debugOn:
                    print(json.dumps(searchData, indent=4))
                    pause()
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
                print(err)
                shutDown = True
                break
            jsonResponse = json.loads(response)
            if debugOn:
                print(json.dumps(jsonResponse, indent=4))
                pause()

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

                if debugOn: 
                    print(json.dumps(bestScores, indent=4))


                #--perform scoring (use stored match_score if not overridden in the mapping document)
                if 'scoring' not in mappingDoc:
                    matchScore = str(((5-resolvedEntity['MATCH_INFO']['MATCH_LEVEL']) * 100) + int(resolvedEntity['MATCH_INFO']['MATCH_SCORE'])) + '-' + str(1000+bestScores['NAME']['score'])[-3:]
                else:
                    matchScore = 0
                    for featureCode in bestScores:
                        if featureCode in mappingDoc['scoring']:
                            if debugOn: 
                                print(featureCode, mappingDoc['scoring'][featureCode])
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

                if debugOn:
                    print(json.dumps(matchedEntity, indent=4))
                    pause()

                matchedEntity['RECORDS'] = resolvedEntity['ENTITY']['RESOLVED_ENTITY']['RECORDS']

                #--check the output filters
                filteredOut = False
                if matchLevel > mappingDoc['output']['matchLevelFilter']:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet matchLevelFilter **')
                if bestScores['NAME']['score'] < mappingDoc['output']['nameScoreFilter']:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet nameScoreFilter **')
                if mappingDoc['output']['dataSourceFilter'] and mappingDoc['output']['dataSourceFilter'] not in dataSources:
                    filteredOut = True
                    if debugOn:
                        print(' ** did not meet dataSourceFiler **')
                if not filteredOut:
                    matchList.append(matchedEntity)

            #--set the no match condition
            if len(matchList) == 0:
            #    if requiredFieldsMissing:
            #        rowsSkipped += 1
            #    else:
                rowsNotMatched += 1
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
                if debugOn:
                    print(' ** no matches found **')
            else:
                rowsMatched += 1
                
            #----------------------------------
            #--create the output rows
            matchNumber = 0
            for matchedEntity in sorted(matchList, key=lambda x: x['MATCH_SCORE'], reverse=True):
                matchNumber += 1
                matchedEntity['MATCH_NUMBER'] = matchNumber if matchedEntity['ENTITY_ID'] != 0 else 0
                level = 'best' if matchNumber == 1 else 'additional'

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

                if matchNumber > mappingDoc['output']['maxReturnCount']:
                    break

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
                        if currentFile['fileFormat'] != 'JSON':
                            print('warning: could not find %s in %s' % (columnDict['value'],columnDict['source'].upper())) 

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
                        print('column %s truncated at 32k' % columnDict['name'])
                    rowValues.append(columnValue.replace('\n', '|'))
                            
                #--write the record
                if mappingDoc['output']['fileType'] != 'JSON':
                    mappingDoc['output']['fileWriter'].writerow(rowValues)
                else:
                    mappingDoc['output']['fileHandle'].write(json.dumps(rowValues) + '\n')

                #--update the counters
                if matchedEntity['MATCH_LEVEL'] != 0:
                    statPack['resolution'][level]['total'] += 1
                if matchedEntity['MATCH_LEVEL'] == 1:
                    resolvedMatches += 1
                    statPack['resolution'][level]['resolved'] += 1
                elif matchedEntity['MATCH_LEVEL'] == 2:
                    possibleMatches += 1
                    statPack['resolution'][level]['possible'] += 1
                elif matchedEntity['MATCH_LEVEL'] == 3:
                    possiblyRelateds += 1
                    statPack['resolution'][level]['related'] += 1
                elif matchedEntity['MATCH_LEVEL'] == 4:
                    nameOnlyMatches += 1
                    statPack['resolution'][level]['name_only'] += 1
                        
            if totalRowCnt % sqlCommitSize == 0:
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                batchStartTime = time.time()
                print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (totalRowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))

            #--break conditions
            if shutDown:
                break
            elif 'ERROR' in currentFile:
                break

        currentFile['handle'].close()
        if shutDown:
            break

    #--all files completed
    now = datetime.now().strftime('%I:%M%p').lower()
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    eps = int(float(sqlCommitSize) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
    batchStartTime = time.time()

    if totalRowCnt > 0:
        statPack['summary'] = {}
        statPack['summary']['search_count'] = totalRowCnt
        statPack['summary']['match_count'] = rowsMatched
        statPack['summary']['match_percent'] = str(round(((float(rowsMatched) / float(totalRowCnt)) * 100.00), 2)) + '%'
        print(' %s rows searched at %s, %s per second ... %s rows matched, %s resolved matches, %s possible matches, %s possibly related, %s name only' % (totalRowCnt, now, eps, rowsMatched, resolvedMatches, possibleMatches, possiblyRelateds, nameOnlyMatches))
        print(json.dumps(statPack, indent = 4))    
        if logFileName:
            with open(logFileName, 'w') as outfile:
                json.dump(statPack, outfile, indent=4)    

    #--close all inputs and outputs
    outputFileHandle.close()

    return shutDown

#----------------------------------------
if __name__ == "__main__":
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    sqlCommitSize = 100
    
    try: iniFileName = G2Paths.get_G2Module_ini_path()
    except: iniFileName = '' 

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_file_name', dest='ini_file_name', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    parser.add_argument('-m', '--mappingFileName', dest='mappingFileName', help='the name of a mapping file')
    parser.add_argument('-i', '--inputFileName', dest='inputFileName', help='the name of an input file')
    parser.add_argument('-d', '--delimiterChar', dest='delimiterChar', help='delimiter character')
    parser.add_argument('-e', '--fileEncoding', dest='fileEncoding', help='file encoding')
    parser.add_argument('-o', '--outputFileName', dest='outputFileName', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='logFileName', help='optional statistics filename (json format).')
    parser.add_argument('-D', '--debugOn', dest='debugOn', action='store_true', default=False, help='run in debug mode')
    args = parser.parse_args()
    ini_file_name = args.ini_file_name
    mappingFileName = args.mappingFileName
    inputFileName = args.inputFileName
    delimiterChar = args.delimiterChar
    fileEncoding = args.fileEncoding
    outputFileName = args.outputFileName
    logFileName = args.logFileName
    debugOn = args.debugOn

    #--try to initialize the g2engine
    try:
        g2Engine = G2Engine()
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
        g2Engine.initV2('G2Snapshot', iniParams, False)
    except G2Exception as err:
        print('\n%s\n' % str(err))
        sys.exit(1)

    returnCode = processFile()

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if returnCode == 0:
        print(f'\nProcess completed successfully in {elapsedMins} minutes!\n')
    else:
        print(f'\nProcess aborted after {elapsedMins} minutes!\n')

    g2Engine.destroy()
    sys.exit(returnCode)
