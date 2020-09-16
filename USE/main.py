import json
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Use tensorflow 1 behavior to match the Universal Sentence Encoder
# examples (https://tfhub.dev/google/universal-sentence-encoder/2).
#import tensorflow.compat.v1 as tf
#import tensorflow_hub as hub

##### INDEXING #####

def index_data():
    print("Creating the 'web ap' index.")
    client.indices.delete(index=INDEX_NAME, ignore=[404])

    with open(INDEX_FILE) as index_file:
        source = index_file.read().strip()
        client.indices.create(index=INDEX_NAME, body=source)

    docs = []
    count = 0

    with open(DATA_FILE) as data_file:
        for line in data_file:
            line = line.strip()

            doc = json.loads(line)
            doc = doc['_source']
            if doc["passage"] is not None and doc["passage"]!="":
                docs.append(doc)
                count += 1

            if count % BATCH_SIZE == 0:
                index_batch(docs)
                docs = []
                print("Indexed {} documents.".format(count))

        if docs:
            index_batch(docs)
            print("Indexed {} documents.".format(count))

    client.indices.refresh(index=INDEX_NAME)
    print("Done indexing.")

def index_batch(docs):
    #titles = [doc["title"] for doc in docs]
    #title_vectors = embed_text(titles)

    requests = []
    for i, doc in enumerate(docs):
        request = doc
        request["_op_type"] = "index"
        request["_index"] = INDEX_NAME
        #request["title_vector"] = title_vectors[i]
        requests.append(request)
    bulk(client, requests)

##### EMBEDDING #####

'''def embed_text(text):
    try:
        vectors = session.run(embeddings, feed_dict={text_ph: text})
        return [vector.tolist() for vector in vectors]
    except Exception:
        print(f'error in text {text}')
'''
##### MAIN SCRIPT #####

if __name__ == '__main__':
    INDEX_NAME = "webap-test-index1"
    INDEX_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/index4.json"

    DATA_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/webap_index_payload_data.json"
    BATCH_SIZE = 10

    SEARCH_SIZE = 5

    GPU_LIMIT = 0.5

    '''print("Downloading pre-trained embeddings from tensorflow hub...")
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
    print("Done.")'''

    client = Elasticsearch()

    index_data()
    #run_query_loop()

    print("Closing tensorflow session...")
    #session.close()
    print("Done.")
