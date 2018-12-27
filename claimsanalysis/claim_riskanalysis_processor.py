from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from operator import add,ge
from operator import floordiv
from datetime import datetime
import pandas
import time


tiv_year_rate = 0.25
default_deduct_tiv_claim_rate = 0.10
max_deduct_tiv_claim_rate = 2
current_year = datetime.now().year


def execute():
    spark_config = SparkConf().setAppName('ClaimRiskAnalysisProcessor')
    spark_context = SparkContext(conf=spark_config)
    sql_context = SQLContext(spark_context)

    policy_file = spark_context.textFile("../datasets/hrlr-analysis-policies.csv")
    policy_header = policy_file.first()

    policies = policy_file.filter(
        lambda line_data: line_data not in policy_header).map(lambda line: line.split(",")).map(
        lambda line_cols: (line_cols[2], (line_cols[4],line_cols[5],str(line_cols[6]).split("-")[0])))

    policies_tiv = policies.map(lambda dat_o: (dat_o[0], calculate_payoff(int(dat_o[1][0]), int(dat_o[1][1]), int(dat_o[1][2]))))

    transaction_file = spark_context.sequenceFile("./output/transactions/*")
    transactions = transaction_file.map(lambda dat_o: str(dat_o[0]).split(";")).map(lambda dat_o: (dat_o[0], dat_o))

    policy_with_transactions = policies_tiv.leftOuterJoin(transactions)

    policies_tiv_risk_state = policy_with_transactions.map(lambda dat_o: (dat_o[0], dat_o[1][1], validate_payoff(dat_o[1][0], dat_o[1][1])))

    customer_policies_processed = policies_tiv_risk_state.\
        map(lambda dat_o: (dat_o[0], dat_o[1][2], dat_o[1][3], dat_o[1][4], dat_o[1][5], dat_o[1][6], str(dat_o[2])))

    print(customer_policies_processed.collect())

    data_frame = sql_context.createDataFrame(customer_policies_processed)
    panda_dataframe = data_frame.toPandas()
    panda_dataframe.to_csv("./output/customer_policies.csv")  # Save in DB (Hive)


def validate_payoff(policy_payoff, transactions):
    current_payoff = 0
    matured_payoff = 0
    payment_state = 'fair'

    if transactions is not None:
        payment_state = transactions[6]

    deduct_tiv_claim_rate = default_deduct_tiv_claim_rate

    if payment_state == 'unfair':
        deduct_tiv_claim_rate = max_deduct_tiv_claim_rate

    for x in policy_payoff:
        data = str(x).split(";")
        matured_payoff = int(data[1])
        if int(data[0]) == current_year:
            current_payoff = int(data[1])

    difference_tiv = matured_payoff - current_payoff
    max_tiv_claim_difference_value = current_payoff * deduct_tiv_claim_rate

    if difference_tiv > max_tiv_claim_difference_value:
        return 1 #less risk

    return -1


def calculate_payoff(amount, tenure, policy_year):
    data = []
    for x in range(0,tenure):
        amount = int((amount * tiv_year_rate) + amount)
        data.append("{};{}".format(int(policy_year+(x+1)), amount))

    return data


if __name__ == "__main__":
    print("Started")
    start = time.time( )
    execute( )
    end = time.time( )
    print("Completed. Took Time {} secs".format(round(end - start, 3)))
