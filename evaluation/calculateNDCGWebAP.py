import csv
import math
import pandas as pd

f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/updatedresultset_webUSE1-top-10-5june.csv', 'r')
c1 = csv.reader(f1)

resultSet = list(c1)

ground_truth = pd.read_csv("/Users/divyanshumarwah/Documents/dissertation/WebAP-Dataset/gov2_top10_doclist.csv")
ground_truth = ground_truth.groupby('Number').head(10)[["Number","relevance"]]

#ground_truth["relevance"] = ground_truth["relevance"].apply(lambda x:int(math.log2(x)))
docsList = [ground_truth['Number'].unique()][0]
groundTruthDict = {}
scoresDict = {}

# using formula at https://machinelearningmedium.com/2017/07/24/discounted-cumulative-gain/
#calculating log2(i+1)
logNormalizeValue  = []
for i in range(1,10):
    logNormalizeValue.append(float('%.3f'%math.log2(i+1)))
print(logNormalizeValue)

def pad(l, content, width):
    l.extend([content] * (width - len(l)))
    return l

def computeNDCG(idealTruth,scoresArr):

    # division of lists
    # using zip() + list comprehension
    iDCGValues = [i / j for i, j in zip(idealTruth, logNormalizeValue)]
    print(iDCGValues)
    iDCG = sum(iDCGValues)
    DCGValues = [i / j for i, j in zip(scoresArr, logNormalizeValue)]
    print(DCGValues)
    DCG = sum(DCGValues)

    nDCG = DCG/iDCG
    print(nDCG)

    return float('%3f'%nDCG)

for id in docsList:
    scores = []
    for row in resultSet[1:]:
        if id == int(row[0]):
            score = int(row[7])
            if score!=0:
                score = int(math.log2(score))
            scores.append(score)
    scoresDict[int(id)] = scores
    groundTruthDict[id] = ground_truth[["relevance"]][ground_truth["Number"] == id]['relevance'].tolist()

print(f'score dict values {scoresDict}')
print(f'groundTruthDict is {groundTruthDict}')

ndcgscoreValues = {}
for key,value in groundTruthDict.items():
    for key1,value1 in scoresDict.items():
        if key == key1:
            #ground truth values
            true_relevance = pad(value, 0, 10)
            print(true_relevance)
            scores_arr = value1
            print(scores_arr)

            ndcgscoreValues[key] = computeNDCG(true_relevance,scores_arr)

print(ndcgscoreValues)

with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/nDCG-10_Score_USE.csv', 'w') as f:
    for key in ndcgscoreValues.keys():
        f.write("%d,%f\n"%(key,ndcgscoreValues[key]))