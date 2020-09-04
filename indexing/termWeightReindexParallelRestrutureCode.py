#import loadIndexedItems
import json
import math
from bs4 import BeautifulSoup
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Search
import re
from concurrent.futures.thread import ThreadPoolExecutor
import urllib.request

timeBasedIndex = {}
indexedIds = set()
try:
    with open('itemsIndexedTest.txt', 'rb') as f2:
        indexedIds = set(x.strip().decode("utf-8") for x in f2)
except FileNotFoundError:
    print('file does not exist')
    indexedIds = set()

try:
    with open('timeBasedIndexNewTest.txt', 'rb') as f3:
        timeBasedIndex = json.load(f3)
except Exception as e:
    print(e.with_traceback)
    try:
        with open('timeBasedIndexNewFixed.txt', 'rb') as f3:
            timeBasedIndex = json.load(f3)
    except Exception:
        print('file does not exist time based')
        timeBasedIndex = {}

es = Elasticsearch()
s = Search(using=es, index='trec-news-index', doc_type='_doc')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names
reg = re.compile("[a-z]{4,}")
e1 = {}
ids = []

baseYear = 2017
#previous_count = 0
counter = 0
faultyIds = []


def getTermWeights(doc):
    termWeightsDict = {}
    global actions
    previous_count=0
    print(f"doc here is {doc['_id']}")
    fields = doc['term_vectors']
    text = fields['text']
    terms = text['terms']
    for term, values in terms.items():
        df = values['doc_freq']
        termWeight = 0
        if term in reg.findall(term):
            if term not in timeBasedIndex.keys():
                try:
                    url = 'https://www.etymonline.com/search?q=' + term
                    html = urllib.request.urlopen(url)
                    if html.status == 200:
                        soup = BeautifulSoup(html.read(), "html.parser")
                        definitionTag = soup.findAll("section", {"class": re.compile(r'^word__defination')})
                        for i in definitionTag:
                            wordDefi = i.get_text()
                            temp = re.findall(r'\d+', wordDefi)
                            res = list(map(int, temp))
                            try:
                                if len(str(res[0])) == 4:
                                    timeBasedIndex[term] = str(res[0])
                                    yearDiff = baseYear - res[0]
                                    termWeight = math.log(df / yearDiff)
                                    break
                            except IndexError:
                                pass
                except Exception:
                    print('exception word is ' + term)
                    pass
            else:
                originYear = timeBasedIndex[term]
                yearDiff = baseYear - int(originYear)
                termWeight = math.log(df / yearDiff)
        termWeightsDict[term] = termWeight
    
    print(f"doc id in function is {doc['_id']}")
    newText = ''
    for word in e1[doc['_id']]['text'].split():
        termWeight1 = 0.000
        for key, value in termWeightsDict.items():
            if word.lower() == key:
                termWeight1 = value
                break
        newText = newText + word + '|' + str('%.3f' % termWeight1) + ' '

    e1[doc['_id']]['text'] = newText
    actions.append(dict(_index="trec-news-payloads-index1", _type="_doc", _id=doc['_id'], _source=e1[doc['_id']]))
    indexedIds.add(doc['_id'])
    print(f'actions here is {actions}')
    print(f'actions type here is {type(actions)}')
    list_len = len(actions)
    print(f'length of action {len(actions)}')
    if list_len != 0 and (list_len - previous_count) >= 10:
        print(f"length of actions here{len(actions)}")
        indexData(actions=actions)
        print(f'indexed now {len(actions)} and counter value is {counter}')
        #actions = []
        previous_count = list_len
    #yield actions
    #return actions
    #termWeightDictIDMapping[doc['_id']] = termWeightsDict

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
        e1[h.meta.id] = dict(id=h.ID,url=h.URL,title=h.title,author=h.author,published_date=h.published_date,
                                            text=h.text, caption=h.caption,
                                            authorBio=h.authorBio,
                                            type=h.type, source=h.source)

print(f'length of read ids is {len(ids)}')

if len(ids) != 0:
    for i in range(0, 1000, 100):
        idsChunk = ids[i:i + 100]
        resultSet = es.mtermvectors(index="trec-news-index", doc_type="_doc",
                                                body=dict(ids=idsChunk, parameters=dict(term_statistics=True,fields=["text"])))['docs']
        print(f'len(resultSet) is {len(resultSet)}')
        #getTermWeights(doc for doc in resultSet)
        proc = ThreadPoolExecutor()       
        proc.map(getTermWeights,[doc for doc in resultSet])
        
        '''
        
        #print(f'fn_result type is {type(fn_result)}')
        #print(f'fn_result here is {fn_result}')
        print(f'actions here is {actions}')
        print(f'actions type here is {type(actions)}')
        list_len = len(actions)
        print(f'length of action {len(actions)}')
        if list_len !=0 and (list_len-previous_count)>=99:
            print(f"length of actions here{len(actions)}")
            indexData(actions=actions)
            print(f'indexed now {len(actions)} and counter value is {counter}')
            actions = []
            previous_count = list_len
        '''

'''
finally:
    print('exception in the main block')
    with open('timeBasedIndexNewTest.txt','w') as filehandle:
        json.dump(timeBasedIndex, filehandle)

    with open('itemsIndexedTest.txt','w') as filehandle1:
        filehandle1.writelines("%s\n" % itemId for itemId in indexedIds)

    with open('faultyItemsTest.txt','a') as filehandle2:
        filehandle2.writelines("%s\n" % itemId for itemId in faultyIds)
'''