from pyspark import SparkContext, SparkConf
from operator import add,ge
from operator import floordiv
import time


class TransactionProcessor:
    def __init__(self):
        self.spark_config = SparkConf().setAppName('InsuranceTransactionProcessor')
        self.spark_context = SparkContext(conf=self.spark_config)
        self.transaction_data = []
        self.payment_threshold = 2;

    def calculate(self, x):
        print("Reading data {}".format(x))
        return 1

    def execute(self):
        customer_file = self.spark_context.textFile("../datasets/hrlr-analysis-customers.csv")
        local_payment = self.payment_threshold

        customers = customer_file.map(lambda line: line.split(",")).filter(
            lambda line_data: line_data[0] not in "customerid,uniqueid").map(
            lambda line_cols: (line_cols[0], line_cols[1]))

        policy_file = self.spark_context.textFile("../datasets/hrlr-analysis-policies.csv")
        policy_header = policy_file.first()
        policies = policy_file.filter(lambda line_data: line_data not in policy_header).\
            map(lambda line: line.split(",")).map(lambda line_cols: (line_cols[0], line_cols))

        customer_policies = customers.join(policies).map(lambda dat_o: (dat_o[1][1][2], [dat_o[0], dat_o[1][0], dat_o[1][1][1]]))

        complaints_file = self.spark_context.textFile("../datasets/hrlr-analysis-complaints.csv")
        compliants_header = policy_file.first()
        complaints = complaints_file.map(lambda line: line.split(",")).filter(
            lambda line_data: line_data[0] not in "compliantid,transactionref").map(
            lambda line_cols: (line_cols[2], line_cols[4]))

        customer_policies_with_com = customer_policies.join(complaints).\
            map(lambda dat_o: (str(dat_o[0]+";"+dat_o[1][0][0]+";"+dat_o[1][0][1]+";"+dat_o[1][0][2]), 1)).\
                reduceByKey(lambda old, new: old + new);

        claims_file = self.spark_context.textFile("../datasets/hrlr-analysis-claims.csv")
        claims = claims_file.map(lambda line: line.split(",")).filter(
            lambda line_data: line_data[0] not in "claimid,transactionref").map(
            lambda line_cols: (line_cols[3], line_cols))

        customer_policies_with_clm = customer_policies.leftOuterJoin(claims).\
            map(lambda dat_o: (str(dat_o[0]+";"+dat_o[1][0][0]+";"+dat_o[1][0][1]+";"+dat_o[1][0][2]), 1)).\
                reduceByKey(lambda old, new: old + new);

        policies_with_com_clm_count = customer_policies_with_clm.leftOuterJoin(customer_policies_with_com).\
            map(lambda dat_o: (dat_o[0], str(dat_o[1][0])+";"+str(dat_o[1][1])))

        payments_file = self.spark_context.textFile("../datasets/hrlr-analysis-payments.csv")
        payments = payments_file.map(lambda line: line.split(",")).filter(
            lambda line_data: line_data[0] not in "paymentid,")\

        total_payments = payments.map(lambda dat_a: (dat_a[2], 1)).reduceByKey(add)
        invalid_payments = payments.filter(lambda dat_a: dat_a[3] not in '"received"').map(lambda dat_a: (dat_a[2], 1)).reduceByKey(add)

        payments_data = total_payments.leftOuterJoin(invalid_payments).\
            filter(lambda dat_a: isinstance(dat_a[1][1], int)).map(lambda dat_a: (dat_a[0], floordiv(dat_a[1][0], dat_a[1][1]))).\
                filter(lambda dat_a: ge(dat_a[1], local_payment)).map(lambda dat_a: (dat_a[0], "unfair"))

        customer_policies_with_payments = customer_policies.leftOuterJoin(payments_data). \
            map(lambda dat_o: (str(dat_o[0] + ";" + dat_o[1][0][0] + ";" + dat_o[1][0][1] + ";" + dat_o[1][0][2]), dat_o[1][1]))

        policies_with_com_clm_count_pay_state = policies_with_com_clm_count.join(customer_policies_with_payments).\
            map(lambda dat_o: (str(dat_o[0]+";"+dat_o[1][0]+";"+str(dat_o[1][1])),1))

        print(policies_with_com_clm_count_pay_state.collect())
        policies_with_com_clm_count_pay_state.saveAsSequenceFile("./output/transactions")


if __name__ == "__main__":
    transaction_processor = TransactionProcessor()
    print("Started")
    start = time.time()
    transaction_processor.execute()
    end = time.time()
    print("Completed. Took Time {} secs".format(round(end - start, 3)))
