import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType
import datetime
import logging
import json
import random

## Glue Params

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

path = "./bank_data_export"
dest_path = "./bank_data_export_parquet"

def create_temp_view_from_csv(file, view_name):
    df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(file)

    df.createOrReplaceTempView(view_name)
    print(f"Temporary view '{view_name}' created from {file}")

    return df

# Create temporary views for each CSV file
account_status_types_df = create_temp_view_from_csv(path+"/account_status_types.csv", "account_status_types")
account_types_df = create_temp_view_from_csv(path+"/account_types.csv", "account_types")
create_temp_view_from_csv(path+"/accounts.csv", "accounts")
branch_df = create_temp_view_from_csv(path+"/branches.csv", "branches")
create_temp_view_from_csv(path+"/card_types.csv", "card_types")
create_temp_view_from_csv(path+"/cards.csv", "cards")
create_temp_view_from_csv(path+"/customer_status_types.csv", "customer_status_types")
create_temp_view_from_csv(path+"/customer_types.csv", "customer_types")
create_temp_view_from_csv(path+"/customers.csv", "customers")
employees_df = create_temp_view_from_csv(path+"/employees.csv", "employees")
create_temp_view_from_csv(path+"/loan_payments.csv", "loan_payments")
create_temp_view_from_csv(path+"/loan_status_types.csv", "loan_status_types")
create_temp_view_from_csv(path+"/loan_types.csv", "loan_types")
create_temp_view_from_csv(path+"/loans.csv", "loans")
create_temp_view_from_csv(path+"/transaction_types.csv", "transaction_types")
create_temp_view_from_csv(path+"/transactions.csv", "transactions")

# Account
account_df = spark.sql("""
    SELECT 
        a.account_id,
        a.customer_id,
        a.branch_id,
        at.type_name AS type,
        ast.status_name AS status,
        a.number,
        a.balance,
        a.min_balance,
        a.date_opened,
        a.date_closed
    FROM accounts a
    JOIN account_types at ON a.type = at.account_type_id
    JOIN account_status_types ast ON a.status = ast.status_id
    """)

hide_account_number = spark.udf.register("hide_account_number", lambda x: "****" + str(x)[-4:], StringType())

account_df = account_df.withColumn(
    "number", f.call_function("hide_account_number", f.col("number"))
)

account_df.show()

# Customer
customer_df = spark.sql("""
    SELECT 
        c.*,
        ct.type_name AS c_type,
        cs.status_name AS c_status
    FROM customers c
    JOIN customer_types ct ON c.customer_type_id = ct.customer_type_id
    JOIN customer_status_types cs ON c.status = cs.status_id
    """)

hide_phone_number = spark.udf.register("hide_phone_number", lambda x: "****" + str(x)[-3:], StringType())

customer_df = customer_df
customer_df = customer_df.drop("customer_type_id", "status")\
    .withColumnsRenamed({"c_type": "customer_type","c_status": "status"})\
    .withColumn("phone_number", f.call_function("hide_phone_number", f.col("phone_number")))

customer_df.show()

# Card
card_df = spark.sql("""
    SELECT 
        c.*,
        ct.type_name AS card_type
    FROM cards c
    JOIN card_types ct ON c.card_type_id = ct.card_type_id
    """)

add_number = spark.udf.register("add_number", lambda x: "****" + str(random.randint(1000, 9999)), StringType())

card_df = card_df.drop("card_type_id")\
    .withColumn("number", f.col("customer_id"))\
    .withColumn(
        "number", f.call_function("add_number", f.col("number"))
    )


# Loan
loan_df = spark.sql("""
    SELECT 
        l.*,
        lt.type_name AS loan_type,
        ls.status_name AS loan_status
    FROM loans l
    JOIN loan_types lt ON l.loan_type_id = lt.loan_type_id
    JOIN loan_status_types ls ON l.loan_status_id = ls.status_id
    """)

loan_df = loan_df.drop("loan_type_id", "loan_status_id")

# Transaction
transaction_df = spark.sql("""
    SELECT 
        t.*,
        tt.type_name AS transaction_type
    FROM transactions t
    JOIN transaction_types tt ON t.type = tt.transaction_type_id
    """)

transaction_df = transaction_df.drop("type", "receiver_account_id")
transaction_df = transaction_df.withColumn(
    "year", f.year(f.col("date")))\
    .withColumn(
        "month", f.month(f.col("date")))\
    .withColumn(
        "day", f.dayofmonth(f.col("date")))


loan_payments_df = spark.sql("""
    SELECT * FROM loan_payments
""")

loan_payments_df = loan_payments_df \
    .withColumn(
    "year", f.year(f.col("paid_date")))\
    .withColumn(
        "month", f.month(f.col("paid_date")))\
    .withColumn(
        "day", f.dayofmonth(f.col("paid_date")))


# To Parquet
spark.conf.set("spark.sql.files.maxPartitionBytes", 256*1024*1024)
# Account
account_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/accounts")
## Customer
customer_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/customers")
## Card
card_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/cards")
## Loan
loan_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/loans")
## Transaction
transaction_df.repartition(1).write.partitionBy("year", "month", "day").mode("overwrite").option("compression", "snappy").parquet(dest_path + "/transactions/")
## Branch
branch_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/branches")
## Employee
employees_df.repartition(1).write.mode("overwrite").option("compression", "snappy").parquet(dest_path + "/employees")
## Loan Payments
loan_payments_df.repartition(1).write.partitionBy("year", "month", "day").mode("overwrite").parquet(dest_path + "/loan_payments/")
