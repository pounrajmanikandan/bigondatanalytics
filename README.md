# bigondatanalytics
POCs for big on data analytics, used Hortonworks Data platform to explore.

### Sample Word Statistics by HDP
Copy file datasets/shakespeare.txt to HDFS file system.
```
(Ambari / FilesView -> /tmp/data/)
```

Submit to spark as below
```
spark-submit ./Main.py
```

Run Netcat (often abbreviated to nc) is a computer networking utility for reading from and writing to network connections using TCP or UDP.
Login into HDP snadbox and run below 
```
nc -l sandbox-hdp.hortonworks.com 3333
```
Submit streaming program to spark
```
spark-submit ./spark-streaming-demo.py
```
Anything we input in Netcat, will be processed by spark streaming program.

### 360 degree customer view
