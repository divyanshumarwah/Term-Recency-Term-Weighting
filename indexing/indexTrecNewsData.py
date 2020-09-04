
from elasticsearch import Elasticsearch
import json
import json_lines
import pandas as pd

es = Elasticsearch()

#es.indices.create(index = 'trec-news-index', body = request_body)

def json_validator(data):
    try:
        json.loads(data)
        return True
    except ValueError as error:
        print("invalid json %s" % error)
        return False


with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/WashingtonPost.v2/data/TREC_Washington_Post_collection.v2.jl','rb') as f:
    for item in json_lines.reader(f):
        print(item['id'])
        e1 = {}
        e1['ID'] = item['id']
        e1['URL'] = item['article_url']
        e1['title'] = item['title']
        e1['author'] = item['author']
        e1['published_date'] = item['published_date']
        text=''
        fullcaption=''
        authorBio=''
        i=1
        exceptionValues ={}
        for x in item['contents']:
            try:
                if x['content'] is not None and isinstance(x['content'],str):
                    text = text + " " + x['content']
                    #print('text is'+text)
            except:
                try:
                    if x['fullcaption'] is not None:
                        fullcaption = fullcaption+x['fullcaption']
                        #print('caption is'+fullcaption)
                except:
                    try:
                        if x['bio'] is not None:
                            authorBio = authorBio+x['bio']
                            #print('authorBio is '+authorBio)
                    except:
                        print("do nothing "+item['id'])
                        exceptionValues[i] = item['id']
                        i = i+1

        e1['text'] = text
        e1['caption'] = fullcaption
        e1['authorBio'] = authorBio
        e1['type'] = item['type']
        e1['source'] = item['source']

        #print(e1)

        #exit(0)
        es.index(index='trec-news-index', doc_type='news', body=e1)

faultValues = pd.DataFrame.from_dict(exceptionValues)
faultValues.to_csv("Fault_values.csv")
    #print(json_validator(f.read()))

