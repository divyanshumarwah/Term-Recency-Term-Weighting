import pandas as pd

resultSet = pd.read_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updatedresultset_USE_100_17june.csv')

original_docids = list(resultSet["Number"].unique())

resultSet = resultSet[resultSet["Gain Value(2^r)"]!=0]

precisionValues = resultSet.groupby('Number').count()['DocID']

for item in original_docids:
    if item not in precisionValues.keys():
        precisionValues[item] = 0

#print(precisionValues)

precisionValues.to_csv('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/precision_Score_USE_100_17_june.csv')