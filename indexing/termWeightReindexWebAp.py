import json
import math
import re
import urllib
from concurrent.futures.thread import ThreadPoolExecutor

import elasticsearch
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search

timeBasedIndex = {}
indexedIds = set()
try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexedWebAP.txt', 'rb') as f2:
        indexedIds = set(x.strip().decode("utf-8") for x in f2)
except FileNotFoundError:
    print('file does not exist')
    indexedIds = set()

try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNewTest.txt', 'rb') as f3:
        timeBasedIndex = json.load(f3)
except Exception:
    print('file does not exist time based')
    timeBasedIndex = {}

es = Elasticsearch()
s = Search(using=es, index='webap-index', doc_type='article')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names
reg = re.compile("[a-z]{4,}")
e1 = {}
ids = []
faultyIds = []
counter = 0
baseYear = 2015


def getTermWeights(doc, e1=e1, baseYear = baseYear, counter=counter):
    termWeightsDict = {}
    actions = []
    #previous_count = 0
    print(f"doc here is {doc['_id']}")
    fields = doc['term_vectors']
    passage = fields['passage']
    terms = passage['terms']
    #print(f'terms value is {terms}')
    for term, values in terms.items():
        df = values['doc_freq']
        #print(f'df value is {df} and term is {term}')
        termWeight = 0
        if term in reg.findall(term):
            if term not in timeBasedIndex.keys():
                print(f'trying the etymonline for {term}')
                url = 'https://www.etymonline.com/search?q=' + term
                html = urllib.request.urlopen(url).read()
                soup = BeautifulSoup(html, "html.parser")
                # print(f'soup is {soup}')
                # break
                definitionTag = soup.findAll("section", {"class": re.compile(r'^word__defination')})
                #print(f'definitionTag for {term} is {definitionTag}')
                for i in definitionTag:
                    wordDefi = i.get_text()
                    temp = re.findall(r'\d+', wordDefi)
                    #print(f'temp values are {temp}')
                    res = list(map(int, temp))
                    try:
                        if len(str(res[0])) == 4:
                            timeBasedIndex[term] = str(res[0])
                            yearDiff = baseYear - res[0]
                            termWeight = math.log(df / yearDiff)
                            #print(f'term weight inside loop {term} is {termWeight}')
                            break

                    except IndexError:
                        pass
            else:
                originYear = timeBasedIndex[term]
                yearDiff = baseYear - int(originYear)
                termWeight = math.log(df / yearDiff)
            print(f'term weight for {term} is {termWeight}')
            termWeightsDict[term] = termWeight

    newText = ''
    for word in e1[doc['_id']]['passage'].split():
        termWeight1 = 0.000
        for key, value in termWeightsDict.items():
            if word.lower() == key:
                termWeight1 = value
                break
        newText = newText + word + '|' + str('%.3f' % termWeight1) + ' '

    e1[doc['_id']]['passage'] = newText
    try:
        es.index(index='webap-payload-index', id=doc['_id'], body=e1[doc['_id']],type="article")
        indexedIds.add(doc['_id'])
        print(f'indexed now {len(indexedIds)}')
    except elasticsearch.exceptions.RequestError:
        print('faulty id here')
        faultyIds.append(doc['_id'])

    '''actions.append(dict(_index="webap-payload-index", _type="article", _id=doc['_id'], _source=e1[doc['_id']]))
    indexedIds.add(doc['_id'])
    print(f'actions here is {actions}')
    #list_len = len(actions)
    print(f'length of action {len(actions)}')
    counter = counter + 1
    print(f'now indexing data {counter}')
    indexData(actions=actions)

    if counter>=10:
        print(f'now indexing data {counter}')
        counter = 0
        indexData(actions=actions)'''


def indexData(actions):
    try:
        #print(actions)
        helpers.bulk(es, actions)
        # indexedIds.add(doc['_id'])
    except elasticsearch.helpers.errors.BulkIndexError as exc:
        faultyIds.append(exc)

for h in s.scan():
    if h.meta.id not in indexedIds:
        ids.append(h.meta.id)
        e1[h.meta.id] = dict(docnum=h.docnum,target_qid=h.target_qid,passage=h.passage)

print(f'length of read ids is {len(ids)}')
try:
    if len(ids) != 0:
        for i in range(0, 1000, 100):
            idsChunk = ids[i:i + 100]
            resultSet = es.mtermvectors(index="webap-index", doc_type="article",
                                                    body=dict(ids=idsChunk,
                                                              parameters=dict(term_statistics=True,
                                                                              fields=["passage"])))['docs']
            print(f'len(resultSet) is {len(resultSet)}')
            proc = ThreadPoolExecutor()
            proc.map(getTermWeights,[doc for doc in resultSet])
finally:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNewTest.txt',
              'w') as filehandle:
        json.dump(timeBasedIndex, filehandle)

    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexedWebAP.txt',
              'a') as filehandle1:
        filehandle1.writelines("%s\n" % itemId for itemId in indexedIds)

    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/faultyItemsWebAP.txt',
              'a') as filehandle2:
        filehandle2.writelines("%s\n" % itemId for itemId in faultyIds)