import xml.etree.cElementTree as ET
import re
from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MoreLikeThis
from nltk.corpus import stopwords

es = Elasticsearch()

with open("/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/newsir18-topics.xml") as f:
#with open("/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/newsir19-background-linking-topics.xml") as f:
    xml = f.read()
tree = ET.fromstring(re.sub(r"(<\?xml[^>]+\?>)", r"\1<root>", xml) + "</root>")

resultset =[]
#titles = []
listOfWords=[]
#parsing the input xml fileß
for child in tree.findall('top'):
    url = child.find('url').text
    docid = child.find('docid').text
    num = child.find('num').text
    colonIndex = num.index(':')
    indexNum = num[colonIndex+2:-1]
    #print(indexNum)

    #hitting the elasticsearch server to get the results

    res = es.search(index='trec-news-index', body={"query":
                                                       {"match":
                                                            {"URL": url}
                                                        }
                                                   } , size=1)
    #print("%d documents found" % res['hits']['total'])

    #get the title of the input URL
    for doc in res['hits']['hits']:
        #print(doc['_score'])
        title = doc['_source']['title']
        #print(title)
        '''words = title.split()
        for word in words:
            listOfWords.append(word.lower())'''


    #titles.append(title) #creating a separate title array

    #search the input title text using more like this
    #s = Search(using=es,index='trec-news-index')
    #s = Search(using=es, index='test_index2')
    matchQuery = {
        "query": {
            "position_match":{
                "query":{
                    "match":{
                        "text":title
                }
            }
        }
        }
    }
    resultSet = es.search(index="test_index2", body=matchQuery, size="100",request_timeout=200)
    '''s = s.query(MoreLikeThis(like=title, fields=['title', 'URL', 'text'],min_term_freq=1,max_query_terms=100))
    s = s[0:500] #defining the size of the retrieved results
    result1 = s.execute()'''

    #iterating over the retrieved results
    for doc in resultSet['hits']['hits']:
        #print(doc['_score'])
        values = []
        values.append(indexNum)
        values.append(title)
        values.append(doc['_source']['id'])
        values.append(doc['_source']['title'])
        values.append(doc['_score'])
        print(values)
        resultset.append(values)

#print(resultset)

#for words in titles:
'''print('before removing duplicates ',len(listOfWords))
listOfWords = list(dict.fromkeys(listOfWords))
print('after removing duplicates ',len(listOfWords))
print(listOfWords)

stop_words = set(stopwords.words('english'))
filteredList = [w for w in listOfWords if not w in stop_words]
print('after removing stop words ',len(filteredList))
print(filteredList)'''

#creating a dataframe and CSV of the result set
df = pd.DataFrame(resultset, columns = ['Number', 'inputTitle','DocID','resultTitle','score'])
df.to_csv(path_or_buf='/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset2019-BM25-time_top100-16-june.csv',index=False)
