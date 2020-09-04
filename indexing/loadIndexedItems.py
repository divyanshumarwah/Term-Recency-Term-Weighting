from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import datetime
#from datetime import timedelta
start=datetime.now()
es = Elasticsearch()
s = Search(using=es, index='test_index1', doc_type='_doc')

s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names
ids = [h.meta.id for h in s.scan()]
#print(ids)

with open('itemsIndexedTest.txt','w') as filehandle1:
    filehandle1.writelines("%s\n" % itemId for itemId in ids)


timer = datetime.now()-start
if timer.seconds>= 1:
    print('hello')

