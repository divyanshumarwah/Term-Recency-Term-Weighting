# Term Recency Term Weighting
MSc-Computer Science(Data Science) - Thesis

### Implementation 
Tools used and needed- 
Elasticsearch, Python, Java, Gradle

Dataset used - https://1drv.ms/u/s!AtfAgPR4VDcEscQXcKFf0eWUOejhqQ?e=j3LBex

Steps to setup and test the algorithm:
1. Download the dataset from the shared location
2. Setup Elasticsearch server using reference from https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
3. Exeute the code given in the Indexing folder to setup different indices from the downloaded dataset in Elasticsearch server. 
Or alternatively use elasticdump to replicate the indices and their structure from (https://1drv.ms/u/s!AtfAgPR4VDcEscRdZ495eYCUIH3s2A?e=hUn4T8).
4. Build the plugin project separately using Gradle and set it up with Elasticsearch and restart the Elastic server.
5. for a regular search, search it directly using Kibana Dev console. 
for algorithm test purpose, use the files given in the Search Folder, that will search the given set of queries and store the results in CSV file.
6. To evaluate the results retrieved against the gold standard resultset, use the nDCG and Precision evaluation scripts in the evaluation folder.

results Folder has the files for the results fetched from the experiments done.

### Literature Files
dissertation latex-score- this has the latex source files for the disseratation.

Overleaf Link- https://www.overleaf.com/read/tbtprmyncvbm

Research Paper - https://drive.google.com/file/d/1Cp8PsHjMHmldpuWH57L3s4IzF1T9TxbZ/view?usp=sharing

Presentation - https://drive.google.com/file/d/1LKhV4-sc8qINhD60UWlNiqoO7NxjvFXG/view?usp=sharing

Thesis Report - https://drive.google.com/file/d/1Zxpp23mdfqiSt4Ou-izgLH6knIqhTlY_/view?usp=sharing
(Access Limited to Trinity College Dublin Members only)
