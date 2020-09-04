from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

es = Elasticsearch()
s = Search(using=es, index='test_index2', doc_type='_doc')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names
indexList = []

for h in s.scan():
    indexList.append(h.id)

s1 = Search(using=es, index='trec-news-test-index', doc_type='_doc')
s1 = s1.script_fields()  # only get ids, otherwise `fields` takes a list of field names
indexedList = []

for h1 in s1.scan():
    indexedList.append(h1.id)

notIndexedItems = []

for id in indexList:
    if id not in indexedList:
        notIndexedItems.append(id)

print(len(notIndexedItems))

