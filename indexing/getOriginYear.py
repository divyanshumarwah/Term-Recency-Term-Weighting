import json
import re
from bs4 import BeautifulSoup
import urllib.request
import logging
#from searchQueries import filteredList
from nltk.corpus import stopwords
from elasticsearch import Elasticsearch

'''
testString = """ WorldViews Despite a big year for women in politics, national legislatures are still dominated by men By Melissa Etehad and Jeremy C.F. Lin It's a big year for women in politics. In a historic first, Hillary Clinton was named the Democratic Party’s presidential nominee in the upcoming U.S. elections. If she wins, she will join Theresa May of Britain and Angela Merkel of Germany in the ranks of women who lead prominent Western democracies. They're not alone, either. In recent years, the number of women holding positions in both parliament and executive government has grown. As of June 2016, women’s membership in parliament doubled from 11.3 percent in 1995 to 22.1 percent in 2015, according to a recent <a href="http://www.ipu.org/pdf/publications/WIP20Y-en.pdf">study</a> by the Inter-Parliamentary Union. And this year alone, there have been many historic firsts. In Iran, women won 17 seats during the Islamic Republic’s parliamentary elections in February. “This is a record and we are happy that our dear women are taking part in all stages, especially in politics,” Iranian President Hassan Rouhani <a href="http://iranprimer.usip.org/blog/2016/may/27/iran%E2%80%99s-runoff-election-parliament">said</a> in a speech back in May. Africa has had some of the most dramatic breakthroughs over the past 20 years. As of June, four African nations were part of the top 10 as far as the number of women represented in parliament. Rwanda, for example, tops the chart as the country with the highest number of women represented in parliament in both the lower (63.8 percent) and upper houses (38.5 percent) in 2016. But women remain a relatively small group in parliaments around the world, and progress is slow. Data from the IPU shows that in a vast majority of countries, men still dominate the political stage. In both the lower and upper houses combined, women around the world account for around 22 percent of seats held in parliament. "Yes it is an improvement considering that it started at a low base," Executive Director of U.N. Women Phumzile Mlambo Ngcuka said. "But if we continue at this pace, it's going to take us too long. We need to fast-track the attainment of gender equality." The U.S. is one country that lags the global average of women in the national legislature. It ranks behind 95 countries, including low- and middle-income countries such as Ethiopia, El Salvador and Suriname. Other countries that outperform the United States include Iraq, Afghanistan and Saudi Arabia. One way that countries have been fast-tracking female participation in politics is through gender quotas, and the results have left many surprised. Countries that that have relatively low socioeconomic development or have just emerged from conflict, such as Afghanistan, Tanzania and Ecuador, have shown tremendous improvement and have higher levels of female representation in parliaments. Jennifer Rosen, assistant professor of sociology at Pepperdine University, says emerging democracies give leaders a unique opportunity for leaders to enshrine gender quotas in their new constitutions, helping bypass cultural barriers that would have otherwise stood in the way of women participating in politics for many years. Different electoral systems also influence women's participation in parliament. While scholars agree that proportional representation systems, which allow people to vote for a party list rather than a particular candidate, have a positive influence on female representation in government, some are divided as to whether it has a positive impact on all countries or only on Western democracies. One way for countries to shift their mentalities is to encourage people in executive leadership positions to set an example, Rosen said. Ngcuka, the U.N. women executive director, agrees and said men play an important role and that women shouldn't be the only ones trying to "move the glass ceiling," noting how attitudes change when influential leaders such as Canadian Prime Minister Justin Trudeau nominated a cabinet in 2015 that was half-female. Political parties that are supposed to provide gender quotas need to ensure that they place women in winnable positions, IPU Secretary General Martin Chungong said. "In many instances they are not walking the walk," he said. "It's not enough to have quotas. We need to provide incentives for people who don't want to implement them." What's also clear is that having women in national legislatures can change the way a country is governed. Research shows that when women participate in politics, the conversation gets steered toward issues that their male counterparts often <a href="https://www.inclusivesecurity.org/wp-content/uploads/2013/05/Bringing-Women-into-Government_FINAL.pdf">fail </a>to address, such as family planning, education and gender-based violence. "We have seen that there are certain issues women are better able to articulate," Chungong said. "Women tend to take leading roles not because it is a women's issue, but because it has to do with livelihood of society as a whole."   <strong>More on WorldViews</strong> <a href="https://www.washingtonpost.com/news/worldviews/wp/2016/08/13/the-world-is-getting-better-at-paid-maternity-leave-the-u-s-is-not/">The world is getting better at paid maternity leave. The U.S. is not.</a>"""

wordList = testString.split()
stop_words = set(stopwords.words('english'))
#filteredList1 = [w for w in wordList if not w in stop_words]
'''
logging.basicConfig(filename='/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/originYearp.log',
                    filemode='w', format='%(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
filteredList1 = set()
with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/listfile.txt', 'r') as filehandle:
    filecontents = filehandle.readlines()

    for line in filecontents:
        # remove linebreak which is the last character of the string
        current_place = line[:-1]

        # add item to the list
        filteredList1.add(current_place)

#filteredList1 = getListofWords()
print('after removing stop words ',len(filteredList1))
logging.info('after removing stop words ',len(filteredList1))
timeBasedIndex = {}
#reg = re.compile(r'\b\w+\b')
reg = re.compile("[a-z]{4,}")
for word in filteredList1:
    if word in reg.findall(word):
        try:
            url = 'https://www.etymonline.com/search?q=' + word
            html = urllib.request.urlopen(url).read()
            soup = BeautifulSoup(html, "html.parser")
            definitionTag = soup.findAll("section",{"class":re.compile(r'^word__defination')})
            logging.info(len(definitionTag))
            for i in definitionTag:
                wordDefi = i.get_text()
                logging.info(url)
                logging.info(wordDefi)
                temp = re.findall(r'\d+', wordDefi)
                res = list(map(int, temp))
                try:
                    logging.info("The numbers list is : " + str(res[0]))
                    if len(str(res[0])) is 4:
                        print('adding year over here')
                        logging.info('adding year over here')
                        timeBasedIndex[word] = str(res[0])
                        print('word and year is ', word, str(res[0]))
                        logging.info('word and year is ',word, str(res[0]))
                        print(timeBasedIndex)
                        break
                except IndexError:
                    logging.info('no origin year, skip the word')
        except UnicodeEncodeError:
            logging.info('exception word is '+word)
    else:
        logging.info('This is not a word ',word)
        continue

logging.info(timeBasedIndex)

with open('/Users/divyanshumarwah/Documents/dissertation/trec-news-2018/timeBasedIndex.txt', 'w') as filehandle:
    json.dump(timeBasedIndex, filehandle)



'''

import lxml.html
from searchQueries import filteredList
from io import StringIO
import requests

for word in  filteredList:
    searchString = 'http://www.etymonline.com/index.php?term=' + word
    html =  requests.get(searchString)
    doc = lxml.html.fromstring(html.content)
    print(doc)
    divContent = doc.xpath('//div[@class="word-"]')
    etymString = divContent.xpath('.//section[@class="word__defination"]/text()')
    print(etymString)


'''
