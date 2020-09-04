import json
import math
import re
import urllib

import elasticsearch
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

es = Elasticsearch()

# get all of the indices on the Elasticsearch cluster
all_indices = es.indices.get_alias("trec-news-index")

# keep track of the number of the documents returned
doc_count = 0

timeBasedIndex = {}
indexedIds = set()
alreadyReadItemList = []
faultyIds = []

try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexed1.txt', 'rb') as f2:
        indexedIds = set(x.strip().decode("utf-8") for x in f2)
except FileNotFoundError:
    print('file does not exist')
    indexedIds = set()

try:
    with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNew.txt', 'rb') as f3:
        timeBasedIndex = json.load(f3)
except FileNotFoundError:
    print('file does not exist time based')
    timeBasedIndex = {}

#print(timeBasedIndex)

# iterate over the list of Elasticsearch indices
for num, index in enumerate(all_indices):
    # declare a filter query dict object
    match_all = {
        "size": 100,
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


            #result  = es.search(index="trec-news-index", body=query)

            ids = []
            #result = es.search(index="abc", doc_type=my_doc_type, body={"query": {"term": {"field":  "sky"}}})

            e1 = {}
            #print(f'alreadyReadItemList is {alreadyReadItemList}')

            for res in resp['hits']['hits']:
                if res['_id'] not in indexedIds:
                    ids.append(res['_id'])
                    e1[res['_id']] = dict(id=res['_source']['ID'],url=res['_source']['URL'],title=res['_source']['title'],
                                          author=res['_source']['author'],published_date=res['_source']['published_date'],
                                          text=res['_source']['text'],caption=res['_source']['caption'],authorBio=res['_source']['authorBio'],
                                          type=res['_source']['type'],source=res['_source']['source'])

            print(f'ids are {ids}')
            print(f'old e1 is {e1}')
            reg = re.compile("[a-z]{4,}")

            baseYear = 2017
            if len(ids)!=0:
                resultSet = es.mtermvectors(index="trec-news-index",doc_type="news",
                                        body=dict(ids=ids,parameters=dict(term_statistics=True,fields=["text"])))['docs']
                for doc in resultSet:
                    #print(doc)
                    fields = doc['term_vectors']
                    #print(fields)
                    text = fields['text']
                    #print(text)
                    field_stats = text['field_statistics']
                    N = field_stats['doc_count']
                    #print(N)
                    terms = text['terms']
                    #print(terms)
                    newText = " "
                    termWeightsDict = {}
                    for term,values in terms.items():
                        #print(term)
                        #print(values['doc_freq'])
                        tf = values['term_freq']
                        df =  values['doc_freq']
                        idf = math.log(N / df)
                        tf_idf = tf*idf
                        print(f'tf:{tf} df:{df} idf:{idf} tf_idf:{tf_idf} term:{term}')
                        termWeight = 0
                        if term in reg.findall(term):
                            if term not in timeBasedIndex.keys():
                                try:
                                    url = 'https://www.etymonline.com/search?q=' + term
                                    html = urllib.request.urlopen(url).read()
                                    soup = BeautifulSoup(html, "html.parser")
                                    definitionTag = soup.findAll("section", {"class": re.compile(r'^word__defination')})
                                    #print(len(definitionTag))
                                    for i in definitionTag:
                                        wordDefi = i.get_text()
                                        #print(url)
                                        #print(wordDefi)
                                        temp = re.findall(r'\d+', wordDefi)
                                        res = list(map(int, temp))
                                        try:
                                            #print("The numbers list is : " + str(res[0]))
                                            if len(str(res[0])) is 4:
                                                print('adding year over here')
                                                timeBasedIndex[term] = str(res[0])
                                                print('word and year is ', term, str(res[0]))
                                                yearDiff = baseYear-res[0]
                                                termWeight = math.log(df/yearDiff)
                                                print(f'term:{term} yearDifference:{yearDiff} termweight:{termWeight}')
                                                print(timeBasedIndex)
                                                break
                                        except IndexError:
                                            print('no origin year, skip the word')
                                except UnicodeEncodeError:
                                    print('exception word is ' + term)
                            else:
                                originYear = timeBasedIndex[term]
                                yearDiff = baseYear - int(originYear)
                                termWeight = math.log(df / (yearDiff+1))
                                print(f'in else block, term:{term} yearDifference:{yearDiff} termweight:{termWeight}')

                        #break
                        #newText = newText + term + '|'+str('%.3f' % termWeight)+ ' '
                        termWeightsDict[term] = termWeight

                    print(f'termWeightsDict is {termWeightsDict}')
                    for word in e1[doc['_id']]['text'].split():
                        print(word)
                        termWeight1 = 0.000
                        for key, value in termWeightsDict.items():
                            if word.lower() == key:
                                termWeight1 = value
                                break
                        newText = newText + word + '|' + str('%.3f' % termWeight1) + ' '

                    e1[doc['_id']]['text'] = newText
                    print('new text is'+newText)
                    print(f'updated e1 is {e1}')
                    try:
                        es.index(index='trec-news-payloads-index1', id=doc['_id'], body=e1[doc['_id']])
                        indexedIds.add(doc['_id'])
                    except elasticsearch.exceptions.RequestError:
                        faultyIds.append(doc['_id'])


                    #break
    finally:
        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndexNew.txt', 'w') as filehandle:
            json.dump(timeBasedIndex, filehandle)

        #for key,value in e1.items():
        #    es.index(index='trec-news-payloads-index', id = key, body=value)
        #    indexedIds.add(key)

        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/itemsIndexed1.txt', 'a') as filehandle1:
            filehandle1.writelines("%s\n" % itemId for itemId in indexedIds)

        with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/faultyItems1.txt', 'w') as filehandle2:
            filehandle2.writelines("%s\n" % itemId for itemId in faultyIds)