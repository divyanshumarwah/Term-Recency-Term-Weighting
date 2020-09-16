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
    with open("/Users/divyanshumarwah/Documents/dissertation/WebAP-Dataset/WebAP/gradedText/gov2.query.json") as f:
        queries = json.load(f)

    resultset = []
    # parsing the input xml file
    for x in queries['queries']:
        indexNum = x['number']
        # print(indexNum)
        # hitting the elasticsearch server to get the results
        title = x['text']
        embedding_start = time.time()
        query_vector = embed_text([title])[0]
        embedding_time = time.time() - embedding_start
        print(f'title is {title}')
        print(f'query vector is {query_vector}')

        '''script_query = {
            "script_score": {
                "query": {"match":{"passage": title}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, doc['title_vector']) + 1.0",
                    "params": {"query_vector": query_vector}
                }
            }
        }
        '''
        script_query = {
            "function_score": {
                "query": {
                    "position_match":{
                        "query":{"match":{"passage": title
                                          }
                                 }
                    }
                },
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, doc['passage_vector']) + 1.0",
                                "params": {"query_vector": query_vector}
                            }
                        }
                    }
                ]
            }
        }

        search_start = time.time()
        response = client.search(
            index=INDEX_NAME,
            body={
                "size": SEARCH_SIZE,
                "query": script_query,
                "_source": {"includes": ["docnum", "target_qid"]}
            },
            request_timeout=100
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
            values.append(hit['_source']['docnum'])
            values.append(hit['_source']['target_qid'])
            values.append(hit['_score'])
            print(values)
            resultset.append(values)
    df = pd.DataFrame(resultset, columns=['Number', 'inputTitle', 'docnum', 'target_qid', 'score'])
    df.to_csv(path_or_buf='/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_webUSE1-time-new-top-10.csv',
        index=False)

##### EMBEDDING #####

def embed_text(text):
    try:
        vectors = session.run(embeddings, feed_dict={text_ph: text})
        return [vector.tolist() for vector in vectors]
    except Exception:
        print(f'error in text {text}')


if __name__ == '__main__':
    INDEX_NAME = "webap-use-test"
    INDEX_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/index.json"

    #DATA_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/test_index2_data.json"
    #BATCH_SIZE = 10

    SEARCH_SIZE = 10

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