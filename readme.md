# Simple DDOS Attacks Detector

### Prerequisite
- Project is setup on Cloudera Quickstart Docker Image.
- Python APIs for Apache Spark 1.6.0

`Spark 1.6.0 (Python API)`  
`Python 2.7` 

## Run Program
Run this command to train on the log file, and then run the prediction of the DDOS cluster and output a text file.
`spark-submit train_ddos_detector.py`


### Apache log message sample 

`155.156.168.116 - - [25/May/2015:23:11:15 +0000] “GET / HTTP/1.0” 200 3557 “-“ “Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; acc=baadshah; acc=none; freenet DSL 1.1; (none))”`  
For more information, please read the [apache log format](https://httpd.apache.org/docs/2.2/logs.html).

### Note 
The program is not run at scale. It is done with a single node pseudo cluster.  # ddos_detector
