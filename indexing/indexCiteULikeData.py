from elasticsearch import Elasticsearch
import csv
import codecs

es = Elasticsearch()
file_name = "/Users/divyanshumarwah/Documents/dissertation/cite-u-like/citeulike-a-master/raw-data.csv"
with codecs.open(file_name, 'r', encoding='utf-8',errors='ignore') as fdata:
    raw_data = list(csv.reader(fdata))
print(len(raw_data))
counter = 0
for data_row in raw_data:
    if counter == 0:
        counter = counter+1
        continue
    else:
        try:
            e1 = {}
            e1['docid'] = data_row[0]
            e1['title'] = data_row[1]
            e1['citeulike_id'] = data_row[2]
            e1['raw_title'] = data_row[3]
            e1['abstract'] = data_row[4]
            print(e1)
            counter = counter+1
            es.index(index='citulike-index', doc_type='article', body=e1)
        except Exception:
            pass

print(f'{counter} articles  indexed')

