import pandas as pd

resultSet = pd.read_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/updatedresultset_webAP1-TFIDF-new-top-10_28june.csv')
original_docids = list(resultSet["Number"].unique())

resultSet = resultSet[resultSet["found/not found"]!="NOT FOUND in master list"]


precisionValues = resultSet.groupby('Number').count()['docnum']

'''updated_citation = pd.read_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_webAP1-time-BM25-top-10.csv')
original_docids = list(updated_citation["Number"].unique())'''

for item in original_docids:
    if item not in precisionValues.keys():
        precisionValues[item] = 0

print(precisionValues)
precisionValues.to_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/precision_Score_TFIDF-top-10_28June.csv')

#precisionValues.to_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/precision_Score_USE-10_webAP.csv')