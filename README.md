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
The reason that insurers continue to increase investments in data and analytics are that they are ultimately looking for an edge in three key areas:
##### Product/Risk:
Analytics  can  be  harnessed  to  achieve  more  granular  exposure  analysis,  enhance  product design, improve underwriting, enable better pricing precision, and more efficiently and effectively adjudicate claims. Analytics are especially vital to address claims fraud.  
The result
 – increased profitability of the core business.

##### Customer Experience:
Improving the customer experience requires actionable insights to better understand customer  needs  and  journeys,  support  omni-channel  expectations,  personalize  interactions,  and  propose best next actions. Gathering, processing, and managing data at scale is required to develop the necessary understanding of all customers as individuals.
The result
 – new customer acquisition, improved retention, and maximized customer lifetime value. In a recent study, 28% of insurers saw increased retention and improvement in lifetime value due to a focus on improving the customer experience

##### Operational Efficiencies: 
In order to identify opportunities to improve efficiencies, move to digital operations, and capitalize on the new economies of IT, insurers must have real-time access and insights for operational data. They must have access to a variety of data to explore, question, and extract actionable insight to apply to operational activities. 
The result
 – business optimization and a reduced cost structure

#### EFFECTIVE ADJUDICATE CLAIMS: Realtime Claim analytics - POC

Millions of pieces of data can be mined based on several parameters such as the type of provider, procedure codes, and member eligibility. Claims need to be analyzed to understand the procedure codes, billed amount and other important attributes to identify overpayment risks.
Proper adoption of data technologies and having all the data in a streamlined process connected effectively with front and backend creates efficiencies. 

##### Early Intervention
High-risk claims that require attention can benefit from early intervention while allowing lower-risk claims to be processed more quickly through automation and auto-adjudication. 

--- 

##### Source of data
Domain | No. of records
--- | --- | 
Customers Profile | 1m
Customers Policy | 1m
Complaints | 50k
Claims | 0.5m
Premium Payment Histories | 1m

##### Spark Cluster - Hortonworks DataPlatform
Managed from http://104.42.217.185:8080/ (Inbound Rules needed to be updated for any machine to access this.)

###### YARN
![alt text](https://github.com/pounrajmanikandan/bigondatanalytics/blob/master/yarn.png)

###### SPARK
![alt text](https://github.com/pounrajmanikandan/bigondatanalytics/blob/master/spark.png)

Default HDFS folder is hdfs:///tmp/data/
##### Scripts
transaction_processor.py
- Step-1: Initializes Spark Context with default configuration.
- Step-2: Reads customers.csv as TextFile from HDFS and maps (CustomerId, CustomerProfile)
- Step-3: Reads policies.csv as TextFile from HDFS and maps (PolicyId, PolicyData)
- Step-4: Joins customers with policies and maps (PolicyId, CustomerId)
- Step-5: Reads complaints.csv as TextFile from HDFS and maps (PolicyId, ComplaintResolutionStatus)
- Step-6: Joins customer_policies with compliants and collects (PolicyId, Complaint Count)
- Step-7: Reads claims.csv as TextFile from HDFS and maps (PolicyId, ClaimsStatus)
- Step-8: Joins customer_policies with claims and collects (PolicyId, Claims Count)
- Step-9: Joins customer_policies complains & claims
- Step-10: Reads payments.csv as TextFile from HDFS and collects (PolicyId, TotalPayment, InvalidPayment) - Invalid is something rejected by system
- Step-11: Generates payments by marking invalid policies payment as 'unfair'
- Step-12: Joins customer_policies complains & claims with payments
- Step-13: Serializes policyid, customerid, complaint count, claims count, payment status to transactions HDFS directory

claim_riskanalysis_processor.py
- Step-1: Initializes Spark Context with default configuration.
- Step-2: Reads policies.csv as TextFile from HDFS and maps (PolicyId, ClaimAmount, Tenure, PolicyStartYear)
- Step-3: Maps each policy and calculate Payoff based on tenure and rate (0.25)
- Step-4: Deserializes HDFS transactions generated Transaction_Processor (PolicyId, policyid, customerid, complaint count, claims count, payment status)
- Step-5: Joins policies with total insured value with transactions, validates using payment status, default_deduct_tiv_claim_rate and max_deduct_tiv_claim_rate then maps (PolicyId, Transaction Data, Risk (1=Low, -1=High))
- Step-6: Writes policyid, customerid, complaint count, claims count, payment status, risk value to customer_policies HDFS directory

```
Note: Python2 requires index to format strings
```