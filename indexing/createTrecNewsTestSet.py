import elasticsearch
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search

es = Elasticsearch()
e1 = {}
actions = []
indexList = []

'''s = Search(using=es, index='test_index2', doc_type='_doc')
s = s.script_fields()  # only get ids, otherwise `fields` takes a list of field names
for h in s.scan():
    indexList.append(h.id)

print(len(indexList))'''
from getUnindexedItemsTrecNewsTestSet import notIndexedItems
indexList = notIndexedItems

for i in range(0, 100, 50):
    idsChunk = indexList[i:i + 50]
    matchString = ""
    for id in idsChunk:
        matchString = id + " " + matchString

    matchString = matchString.strip()

    matchQuery = {
        "query": {
            "match": {
                "ID": {
                    "query": matchString,
                    "operator": "or"
                }
            }
        }
    }

    resusltSet = es.search(index="trec-news-index", body=matchQuery, size="50")
    print(f'fetched the resultset {len(resusltSet)}')
    for res in resusltSet['hits']['hits']:
        e1[res['_id']] = dict(id=res['_source']['ID'], url=res['_source']['URL'], title=res['_source']['title'],
                             author=res['_source']['author'], published_date=res['_source']['published_date'],
                             text=res['_source']['text'], caption=res['_source']['caption'],
                             authorBio=res['_source']['authorBio'],
                             type=res['_source']['type'], source=res['_source']['source'])
        actions.append(dict(_index="trec-news-test-index", _type="_doc", _id=res['_id'], _source=e1[res['_id']]))

    try:
        print(f'length of actions here {len(actions)}')
        helpers.bulk(es, actions)
        actions = []
        # indexedIds.add(doc['_id'])
    except elasticsearch.helpers.errors.BulkIndexError as exc:
        print(exc)
