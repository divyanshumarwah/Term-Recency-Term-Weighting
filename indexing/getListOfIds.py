import csv

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
es = Elasticsearch()
s = Search(using=es, index='test_index1', doc_type='_doc')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names


payloadIndexIds = []
for h in s.scan():
    payloadIndexIds.append(h.id)

print(len(payloadIndexIds))
'''
s1 = Search(using=es, index='trec-news-index', doc_type='_doc')
s1 = s1.script_fields()  # only get ids, otherwise `fields` takes a list of field names

trecNewsIds = []
for h in s1.scan():
    trecNewsIds.append(h.ID)

print(len(trecNewsIds))

'''
f2 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updated_qrels.csv', 'r')
#f2 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/news2019_qrels.csv', 'r')
c2 = csv.reader(f2)
givenResultsList = list(c2)
giveListofIDs = []
i = 0
for givenResultRow in givenResultsList:
    if i==0:
        i=1
        continue

    #print(givenResultRow[2])
    giveListofIDs.append(givenResultRow[2])
    i = i+1

print(i)
listOfIDsToBeIndexed = []
for id in giveListofIDs:
    if id not in payloadIndexIds:
        listOfIDsToBeIndexed.append(id)
print(len(listOfIDsToBeIndexed))
