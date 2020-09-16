import json
import tensorflow.compat.v1 as tf
import tensorflow_hub as hub

def embed_text(text):
    vectors = session.run(embeddings, feed_dict={text_ph: text})
    return [vector.tolist() for vector in vectors]


def index_batch(docs):
    titles = ['Rick Santorum talks to voters in N.H. — and talks and talks and talks',
              'Case dismissed against man accused of drawing blood from people at D.C. apartment',
              'Olney artistic director Jim Petosa to step down',
              'Michigan football enters 2012 Sugar Bowl with a new attitude on defense',
              'The White House can’t seem to perform simple arithmetic', None,
              'Newtonian exceptionalism',
              'small theaters',
              'How to add wine to your cocktails, in 6 recipes',
              'State lawmakers from Prince George’s seek broad probe of graduation rates']

    title_vectors = embed_text(titles)
    print(titles)
    requests = []
    for i, doc in enumerate(docs):
        request = doc
        request["_op_type"] = "index"
        request["_index"] = "trec-use-index"
        request["title_vector"] = title_vectors[i]
        requests.append(request)
    print(f'requests are {requests}')
    #bulk(client, requests)

DATA_FILE = "/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/USE-test/data/test_index2_data.json"

docs = []
count = 0
print("Downloading pre-trained embeddings from tensorflow hub...")
embed = hub.Module("https://tfhub.dev/google/universal-sentence-encoder/2")
text_ph = tf.placeholder(tf.string)
embeddings = embed(text_ph)
print("Done.")

print("Creating tensorflow session...")
config = tf.ConfigProto()
config.gpu_options.per_process_gpu_memory_fraction = 0.5
session = tf.Session(config=config)
session.run(tf.global_variables_initializer())
session.run(tf.tables_initializer())
print("Done.")

with open(DATA_FILE) as data_file:
    for line in data_file:
        line = line.strip()

        doc = json.loads(line)
        doc = doc['_source']
        docs.append(doc)
        count += 1
        if count % 10 == 0:
            index_batch(docs)
            docs = []
            print("Indexed {} documents.".format(count))
            #print(doc['title'])
            break

