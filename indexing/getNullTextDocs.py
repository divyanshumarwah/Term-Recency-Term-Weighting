from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
es = Elasticsearch()
s = Search(using=es, index='test_index1', doc_type='_doc')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names

nullIndexIds = []
for h in s.scan():
    if len(h.text)==0:
        nullIndexIds.append(h.id)

#print((nullIndexIds))
print(len(nullIndexIds))