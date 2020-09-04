#https://stackoverflow.com/questions/5268929/compare-two-csv-files-and-search-for-similar-items
import csv
import math
#f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_url.csv', 'r')
#f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_publish_date.csv','r')
f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/resultset_webAP1-TFIDF-new-top-10_28june.csv', 'r')
f2 = open('/Users/divyanshumarwah/Documents/dissertation/WebAP-Dataset/gov2_top10_doclist.csv', 'r')
f3 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/webap-results/updatedresultset_webAP1-TFIDF-new-top-10_28june.csv', 'w')
#f3 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updatedresultset_syn_test.csv', 'w')

c1 = csv.reader(f1)
c2 = csv.reader(f2)
c3 = csv.writer(f3)

givenResultsList = list(c2)

for tfRow in c1:
    row = 1
    found = False
    for givenResultRow in givenResultsList:
        results_row = tfRow
        #for normal elastic based approaches
        #if tfRow[0] == givenResultRow[0] and tfRow[2] == givenResultRow[2]:

        #for time based approach
        if tfRow[3] == givenResultRow[4] and givenResultRow[2]!='relevance':
            results_row.append('FOUND in master list (row ' + str(row) + ')')
            results_row.append(math.pow(2,int(givenResultRow[2])))
            found = True
            break
        row = row + 1
    if not found:
        results_row.append('NOT FOUND in master list')
        results_row.append(0)
    c3.writerow(results_row)

f1.close()
f2.close()
f3.close()
