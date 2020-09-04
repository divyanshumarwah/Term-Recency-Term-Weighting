import re

from elasticsearch import Elasticsearch
import xml.etree.cElementTree as ET

es = Elasticsearch()


with open('/Users/divyanshumarwah/Documents/dissertation/WebAP-Dataset/WebAP/gradedText/grade.trectext_patched','rb') as f:
    xml = f.read()

root = ET.parse('/Users/divyanshumarwah/Documents/dissertation/WebAP-Dataset/WebAP/gradedText/grade.trectext_patched').getroot()

for child in root.findall('DOC'):
    e1 = {}
    e1['docnum'] = child.find('DOCNO').text
    e1['target_qid'] = child.find('TARGET_QID').text
    e1['orginal_doc_num'] = child.find('ORIGINAL_DOCNO').text
    text = ''
    for sentence in child.findall('TEXT/NONE/SENTENCE'):
        print(sentence.text)
        text = text+' '+sentence.text
    e1['passage'] = text.strip()
    es.index(index='webap-index', doc_type='article', body=e1)
