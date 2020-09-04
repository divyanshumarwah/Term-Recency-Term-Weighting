#https://stackoverflow.com/questions/5268929/compare-two-csv-files-and-search-for-similar-items
import csv

#f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_url.csv', 'r')
#f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_publish_date.csv','r')
f1 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/resultset_USE_100_17june.csv', 'r')
f2 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updated_qrels.csv', 'r')
#f2 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/news2019_qrels.csv','r')
f3 = open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/updatedresultset_USE_100_17june.csv', 'w')
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
        if tfRow[3] == givenResultRow[3]:
            results_row.append('FOUND in master list (row ' + str(row) + ')')
            results_row.append(givenResultRow[4])
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
