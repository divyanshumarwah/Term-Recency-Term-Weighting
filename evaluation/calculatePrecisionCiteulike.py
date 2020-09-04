import pandas as pd

resultSet = pd.read_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updatedresultset_citeulike-BM25-time-16-june.csv')
original_docids = list(resultSet["docid"].unique())

resultSet = resultSet[resultSet["found/not found"]!="NOT FOUND in master list"]


precisionValues = resultSet.groupby('docid').count()['DocID']

'''updated_citation = pd.read_csv('/Users/divyanshumarwah/Documents/dissertation/cite-u-like/citeulike-a-master/citations_long_10.csv')
original_docids = list(updated_citation["docid"].unique())
'''
for item in original_docids:
    if item not in precisionValues.keys():
        precisionValues[item] = 0

#print(precisionValues)
precisionValues.to_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/precision_Score-BM25-time-16-june-10_citeulike.csv')
