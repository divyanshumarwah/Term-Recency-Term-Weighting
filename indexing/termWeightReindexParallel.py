import json
import math
import multiprocessing
import re
import urllib.request
from multiprocessing import Process
from multiprocessing import Pool
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures.process import ProcessPoolExecutor

import elasticsearch
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import urllib3

es = Elasticsearch()

# get all of the indices on the Elasticsearch cluster
all_indices = es.indices.get_alias("trec-news-index")

timeBasedIndex = {}
indexedIds = set()
alreadyReadItemList = []
faultyIds = []

try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexedTest.txt', 'rb') as f2:
        indexedIds = set(x.strip().decode("utf-8") for x in f2)
except FileNotFoundError:
    print('file does not exist')
    indexedIds = set()

try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNewTest.txt', 'rb') as f3:
        timeBasedIndex = json.load(f3)
except FileNotFoundError:
    print('file does not exist time based')
    timeBasedIndex = {}


# print(timeBasedIndex)
termWeightDictIDMapping = {}
e1 = {}
counter = 0
baseYear = 2017
previous_count = 0
#termWeightsDict = {}
def getTermWeights(doc):
    #print(f'doc inside function {doc}')
    termWeightsDict = {}
    #doc = json.loads(doc1)
    #print(f'doc type inside function {type(doc)}')
    fields = doc['term_vectors']
    # print(fields)
    try:
        text = fields['text']
        # print(text)
        field_stats = text['field_statistics']
        N = field_stats['doc_count']
        # print(N)
        terms = text['terms']
        # print(terms)
        newText = " "

        for term, values in terms.items():
            # print(term)
            # print(values['doc_freq'])
            #tf = values['term_freq']
            df = values['doc_freq']
            #idf = math.log(N / df)
            #tf_idf = tf * idf
            # print(f'tf:{tf} df:{df} idf:{idf} tf_idf:{tf_idf} term:{term}')
            termWeight = 0
            if term in reg.findall(term):
                if term not in timeBasedIndex.keys():
                    try:

                        url = 'https://www.etymonline.com/search?q=' + term
                        #http = urllib3.PoolManager()
                        #response = http.request('GET', url)
                        #request.add_header('User-agent', 'Mozilla/5.0 (Linux i686)')
                        #html = urllib3.urlopen(request).read()
                        html = urllib.request.urlopen(url)
                        if html.status == 200:
                            soup = BeautifulSoup(html.read(), "html.parser")
                            definitionTag = soup.findAll("section", {"class": re.compile(r'^word__defination')})
                            # print(len(definitionTag))
                            for i in definitionTag:
                                wordDefi = i.get_text()
                                # print(url)
                                # print(wordDefi)
                                temp = re.findall(r'\d+', wordDefi)
                                res = list(map(int, temp))
                                try:
                                    # print("The numbers list is : " + str(res[0]))
                                    if len(str(res[0])) is 4:
                                        # print('adding year over here')
                                        timeBasedIndex[term] = str(res[0])
                                        # print('word and year is ', term, str(res[0]))
                                        yearDiff = baseYear - res[0]
                                        termWeight = math.log(df / yearDiff)
                                        # print(f'term:{term} yearDifference:{yearDiff} termweight:{termWeight}')
                                        # print(timeBasedIndex)
                                        break
                                except IndexError:
                                    pass
                                    #i_error.with_traceback()
                                    #print('no origin year, skip the word')
                        #html.close()

                    except Exception:
                        pass
                        #u_error.with_traceback()
                        #print('exception word is ' + term)
                else:
                    originYear = timeBasedIndex[term]
                    yearDiff = baseYear - int(originYear)
                    termWeight = math.log(df / yearDiff)
                    # print(f'in else block, term:{term} yearDifference:{yearDiff} termweight:{termWeight}')

            # break
            # newText = newText + term + '|'+str('%.3f' % termWeight)+ ' '
            termWeightsDict[term] = termWeight
        #print(f'termWeightsDict is {termWeightsDict}')
    except Exception:
        print(f'error over here')
        pass

    print(f"doc id in function is {doc['_id']}")
    termWeightDictIDMapping[doc['_id']] = termWeightsDict

    #gc.collect()
    #return termWeightDictIDMapping

def indexData(actions):
    try:
        #print(actions)
        helpers.bulk(es, actions)
        # indexedIds.add(doc['_id'])
    except elasticsearch.helpers.errors.BulkIndexError as exc:
        faultyIds.append(exc)


# iterate over the list of Elasticsearch indices
for num, index in enumerate(all_indices):
    # declare a filter query dict object
    match_all = {
        "size": 500,
        "query": {
            "match_all": {}
        }
    }

    # make a search() request to get all docs in the index
    resp = es.search(
        index=index,
        body=match_all,
        scroll='100m'  # length of time to keep search context
    )
    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']
    try:
        # use a 'while' iterator to loop over document 'hits'
        while len(resp['hits']['hits']):
            # make a request using the Scroll API
            resp = es.scroll(
                scroll_id=old_scroll_id,
                scroll='100m'  # length of time to keep search context
            )

            # check if there's a new scroll ID
            if old_scroll_id != resp['_scroll_id']:
                print("NEW SCROLL ID:", resp['_scroll_id'])

            # keep track of pass scroll _id
            old_scroll_id = resp['_scroll_id']

            # result  = es.search(index="trec-news-index", body=query)

            ids = []
            # result = es.search(index="abc", doc_type=my_doc_type, body={"query": {"term": {"field":  "sky"}}})


            # print(f'alreadyReadItemList is {alreadyReadItemList}')
            actions = []
            for res in resp['hits']['hits']:
                if res['_id'] not in indexedIds:
                    ids.append(res['_id'])
                    e1[res['_id']] = dict(id=res['_source']['ID'], url=res['_source']['URL'],
                                          title=res['_source']['title'],
                                          author=res['_source']['author'],
                                          published_date=res['_source']['published_date'],
                                          text=res['_source']['text'], caption=res['_source']['caption'],
                                          authorBio=res['_source']['authorBio'],
                                          type=res['_source']['type'], source=res['_source']['source'])



            #print(f'ids are {ids}')
            # print(f'old e1 is {e1}')
            reg = re.compile("[a-z]{4,}")


            if len(ids) != 0:
                resultSet = es.mtermvectors(index="trec-news-index", doc_type="news",
                                            body=dict(ids=ids, parameters=dict(term_statistics=True, fields=["text"])))['docs']

                proc = Pool(processes=8)
                #proc = ThreadPoolExecutor()
                print(f'resultset length is {len(resultSet)}')
                proc.map(getTermWeights,[doc for doc in resultSet])
                list_len = len(list(termWeightDictIDMapping))
                print(f'length of dictionary over here {list_len}')
                if list_len !=0 and (list_len-previous_count)>=100:
                    for doc_id in list(termWeightDictIDMapping)[counter:]:

                        termWeightsDict = termWeightDictIDMapping[doc_id]
                        print(f'key is {doc_id}')
                        #print(f'term  weight dict  here is {termWeightsDict}')
                        newText = ''
                        for word in e1[doc_id]['text'].split():
                            # print(word)
                            termWeight1 = 0.000

                            for key, value in termWeightsDict.items():
                                if word.lower() == key:
                                    termWeight1 = value
                                    break
                            newText = newText + word + '|' + str('%.3f' % termWeight1) + ' '
                            #print(newText)
                            # complete the processes
                            # for proc in procs:
                            #    proc.join()

                        e1[doc_id]['text'] = newText

                        # print('new text is'+newText)
                        #print(f'updated e1 is {e1[doc_id]}')
                        #exit(0)
                        counter = counter+1
                        actions.append(dict(_index="trec-news-payloads-index1", _type="_doc", _id=doc_id, _source=e1[doc_id]))
                        indexedIds.add(doc_id)


                        #print(f'counter is {counter}')
                    print(f"length of actions here{len(actions)}")
                    indexData(actions=actions)
                    print(f'indexed now {len(actions)} and counter value is {counter}')
                    actions = []
                    previous_count = list_len
                #gc.collect()

    finally:
        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNewTest.txt','w') as filehandle:
            json.dump(timeBasedIndex, filehandle)

        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexedTest.txt','w') as filehandle1:
            filehandle1.writelines("%s\n" % itemId for itemId in indexedIds)

        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/faultyItemsTest.txt','a') as filehandle2:
            filehandle2.writelines("%s\n" % itemId for itemId in faultyIds)
