import json
import time

from elasticsearch import Elasticsearch
import pandas as pd

# Use tensorflow 1 behavior to match the Universal Sentence Encoder
# examples (https://tfhub.dev/google/universal-sentence-encoder/2).
import tensorflow.compat.v1 as tf
import tensorflow_hub as hub
import xml.etree.cElementTree as ET
import re

##### SEARCHING #####

def run_query():
    #with open("/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/newsir19-background-linking-topics.xml") as f:
    with open("/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/newsir18-topics.xml") as f:
        xml = f.read()
    tree = ET.fromstring(re.sub(r"(<\?xml[^>]+\?>)", r"\1<root>", xml) + "</root>")

    resultset = []
    # parsing the input xml file
    for child in tree.findall('top'):
        url = child.find('url').text
        docid = child.find('docid').text
        num = child.find('num').text
        colonIndex = num.index(':')
        indexNum = num[colonIndex + 2:-1]
        # print(indexNum)

        # hitting the elasticsearch server to get the results

        res = client.search(index='trec-news-index', body={"query":
                                                           {"match":
                                                                {"URL": url}
                                                            }
                                                       }, size=1)
        # print("%d documents found" % res['hits']['total'])

        # get the title of the input URL
        for doc in res['hits']['hits']:
            # print(doc['_score'])
            title = doc['_source']['title']
        embedding_start = time.time()
        query_vector = embed_text([title])[0]
        embedding_time = time.time() - embedding_start

        '''script_query = {
            "position_match":{
                "query":{
                    "match":{
                        "text": title
                    }
                }
            }
        }'''

        script_query = {
            "script_score": {
                "query": {"match":{"text": title}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, doc['title_vector']) + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }

        '''script_query = {
            "function_score": {
                "query": {
                    "position_match":{
                        "query":{"match":{"text": title
                                          }
                                 }
                    }
                },
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, doc['title_vector']) + 1.0",
                                "params": {"query_vector": query_vector}
                            }
                        }
                    }
                ]
            }
        }'''

        search_start = time.time()
        response = client.search(
            index=INDEX_NAME,
            body={
                "size": SEARCH_SIZE,
                "query": script_query,
                "_source": {"includes": ["title", "id"]}
            },
            request_timeout=200
        )
        search_time = time.time() - search_start

        print()
        print("{} total hits.".format(response["hits"]["total"]["value"]))
        print("embedding time: {:.2f} ms".format(embedding_time * 1000))
        print("search time: {:.2f} ms".format(search_time * 1000))
        for hit in response["hits"]["hits"]:
            values = []
            values.append(indexNum)
            values.append(title)
            values.append(hit['_source']['id'])
            values.append(hit['_source']['title'])
            values.append(hit['_score'])
            print(values)
            resultset.append(values)
    df = pd.DataFrame(resultset, columns=['Number', 'inputTitle', 'DocID', 'resultTitle', 'score'])
    df.to_csv(path_or_buf='/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_USE_100_17june.csv',
        index=False)

##### EMBEDDING #####

def embed_text(text):
    try:
        vectors = session.run(embeddings, feed_dict={text_ph: text})
        return [vector.tolist() for vector in vectors]
    except Exception:
        print(f'error in text {text}')


if __name__ == '__main__':
    INDEX_NAME = "trec-use-index"
    INDEX_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/index.json"

    #DATA_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/test_index2_data.json"
    #BATCH_SIZE = 10

    SEARCH_SIZE = 100

    GPU_LIMIT = 0.5

    print("Downloading pre-trained embeddings from tensorflow hub...")
    embed = hub.Module("https://tfhub.dev/google/universal-sentence-encoder/2")
    text_ph = tf.placeholder(tf.string)
    embeddings = embed(text_ph)
    print("Done.")

    print("Creating tensorflow session...")
    config = tf.ConfigProto()
    config.gpu_options.per_process_gpu_memory_fraction = GPU_LIMIT
    session = tf.Session(config=config)
    session.run(tf.global_variables_initializer())
    session.run(tf.tables_initializer())
    print("Done.")

    client = Elasticsearch()

    run_query()

    print("Closing tensorflow session...")
    session.close()
    print("Done.")
