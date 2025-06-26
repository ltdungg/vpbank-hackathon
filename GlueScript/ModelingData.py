import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, BooleanType
import datetime
import logging
import json
import random

CATALOG_NAME = 'glue_catalog'
CLEAN_ZONE = 's3://vpbank-banking-lakehouse/clean-zone/'
DATABASE_NAME = 'vpbank_clean_zone'
LANDING_ZONE = 's3://vpbank-banking-lakehouse/landing-zone/'
## Spark Config

conf = SparkConf()
conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
conf.set(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
conf.set(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
conf.set(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", CLEAN_ZONE)
conf.set("spark.sql.catalog.glue_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")

## Glue Params
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

DATE_NOW = datetime.datetime.now()

def read_parquet_from_s3(path, key, schema=None):
    if schema:
        return spark.read.schema(schema).parquet(path + key)
    else:
        return spark.read.parquet(path + key, inferSchema=True)


def create_dim_date_table():
    # DIM_DATE
    print("Creating dim_date table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_date (
        date_id INT,
        full_date DATE,
        day INT,
        month INT,
        year INT,
        quarter INT,
        day_of_week INT,
        is_weekend BOOLEAN
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_date/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_date created successfully.")

    START_DATE = datetime.date(2000, 1, 1)
    END_DATE = datetime.date(2050, 12, 31)

    dim_date_schema = StructType([
        StructField("date_id", IntegerType(), True),
        StructField("full_date", DateType(), True),
        StructField("day", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True)
    ])


    dim_date_dict = {
        'date_id': [(START_DATE + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range((END_DATE - START_DATE).days + 1)],
        'full_date': [(START_DATE + datetime.timedelta(days=i)) for i in range((END_DATE - START_DATE).days + 1)],
        'day': [(START_DATE + datetime.timedelta(days=i)).day for i in range((END_DATE - START_DATE).days + 1)],
        'month': [(START_DATE + datetime.timedelta(days=i)).month for i in range((END_DATE - START_DATE).days + 1)],
        'year': [(START_DATE + datetime.timedelta(days=i)).year for i in range((END_DATE - START_DATE).days + 1)],
        'quarter': [((START_DATE + datetime.timedelta(days=i)).month-1)//3+1 for i in range((END_DATE - START_DATE).days + 1)],
        'day_of_week': [(START_DATE + datetime.timedelta(days=i)).weekday() for i in range((END_DATE - START_DATE).days + 1)],
        'is_weekend': [(START_DATE + datetime.timedelta(days=i)).weekday() >= 5 for i in range((END_DATE - START_DATE).days + 1)],
    }

    dim_date_data = []
    for i in range(len(dim_date_dict['date_id'])):
        dim_date_data.append((
            int(dim_date_dict['date_id'][i]),
            dim_date_dict['full_date'][i],
            dim_date_dict['day'][i],
            dim_date_dict['month'][i],
            dim_date_dict['year'][i],
            dim_date_dict['quarter'][i],
            dim_date_dict['day_of_week'][i],
            dim_date_dict['is_weekend'][i]
        ))

    dim_date_df = spark.createDataFrame(dim_date_data, schema=dim_date_schema)

    print("Inserting data into dim_date table...")
    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_date
    SELECT 
        date_id,
        full_date,
        day,
        month,
        year,
        quarter,
        day_of_week,
        is_weekend
    FROM {{dim_date_df}}""", dim_date_df=dim_date_df)

    print("Data inserted into dim_date table successfully.")

    return 200

# Create dim_accounts_table
def create_dim_accounts_table():

    print("Creating dim_accounts table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_accounts (
            account_id STRING,
            customer_id STRING,
            branch_id STRING,
            type STRING,
            status STRING,
            number STRING,
            min_balance INT,
            date_opened DATE,
            date_closed DATE
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_accounts/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_accounts created successfully.")

    account_df = read_parquet_from_s3(LANDING_ZONE, 'accounts/')
    account_df = account_df.drop('balance')

    print("Inserting data into dim_accounts table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_accounts
    SELECT 
        account_id,
        customer_id,
        branch_id,
        type,
        status,
        number,
        min_balance,
        date_opened,
        date_closed
    FROM {{account_df}}
    """, account_df=account_df)

    print("Data inserted into dim_accounts table successfully.")

    return 200

def create_dim_branches():
    print("Creating dim_branches table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_branches (
            branch_id STRING,
            name STRING,
            address STRING,
            phone_number STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_active BOOLEAN
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_branches/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_branches created successfully.")

    branch_df = read_parquet_from_s3(LANDING_ZONE, 'branches/')

    branch_df = branch_df.withColumn('created_at', f.current_timestamp()) \
        .withColumn('updated_at', f.current_timestamp()) \
        .withColumn('is_active', f.lit(True))

    print("Inserting data into dim_branches table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_branches
    SELECT 
        branch_id,
        name,
        address,
        phone_number,
        created_at,
        updated_at,
        is_active
    FROM {{branch_df}}
    """, branch_df=branch_df)

    print("Data inserted into dim_branches table successfully.")

    return 200

def create_dim_cards():
    print("Creating dim_cards table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_cards (
            card_id STRING,
            account_id STRING,
            monthly_fee INT,
            interest_rate FLOAT,
            expiration_date DATE,
            issuance_date DATE,
            card_type STRING,
            number STRING
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_cards/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_cards created successfully.")

    card_df = read_parquet_from_s3(LANDING_ZONE, 'cards/')
    card_df = card_df.drop('customer_id')

    print("Inserting data into dim_cards table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_cards
    SELECT 
        card_id,
        account_id,
        monthly_fee,
        interest_rate,
        expiration_date,
        issuance_date,
        card_type,
        number
    FROM {{card_df}}
    """, card_df=card_df)

    print("Data inserted into dim_cards table successfully.")

    return 200

def create_dim_customers():
    print("Creating dim_customers table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_customers (
            customer_id STRING,
            name STRING,
            country STRING,
            city STRING,
            street STRING,
            phone_number STRING,
            registration_date DATE,
            email STRING,
            customer_type STRING,
            status STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_active BOOLEAN
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_customers/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_customers created successfully.")

    customer_df = read_parquet_from_s3(LANDING_ZONE, 'customers/')

    customer_df = customer_df.withColumn('created_at', f.current_timestamp()) \
        .withColumn('updated_at', f.current_timestamp()) \
        .withColumn('is_active', f.lit(True))

    print("Inserting data into dim_customers table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_customers
    SELECT 
        customer_id,
        name,
        country,
        city,
        street,
        phone_number,
        registration_date,
        email,
        customer_type,
        status,
        created_at,
        updated_at,
        is_active
    FROM {{customer_df}}
    """, customer_df=customer_df)

    print("Data inserted into dim_customers table successfully.")

    return 200

def create_dim_employees():
    print("Creating dim_employees table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_employees (
            employee_id STRING,
            name STRING,
            position STRING,
            branch_id STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_active BOOLEAN
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_employees/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_employees created successfully.")

    employee_df = read_parquet_from_s3(LANDING_ZONE, 'employees/')

    employee_df = employee_df.withColumn('created_at', f.current_timestamp()) \
        .withColumn('updated_at', f.current_timestamp()) \
        .withColumn('is_active', f.lit(True))

    print("Inserting data into dim_employees table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_employees
    SELECT 
        employee_id,
        name,
        position,
        branch_id,
        created_at,
        updated_at,
        is_active
    FROM {{employee_df}}
    """, employee_df=employee_df)

    print("Data inserted into dim_employees table successfully.")

    return 200

def create_dim_loans():
    print("Creating dim_loans table...")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.dim_loans (
            loan_id STRING,
            customer_id STRING,
            amount LONG,
            interest_rate FLOAT,
            term INT,
            start_date DATE,
            end_date DATE,
            type STRING,
            status STRING
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}dim_loans/'
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table dim_loans created successfully.")

    loan_df = read_parquet_from_s3(LANDING_ZONE, 'loans/').withColumnRenamed('loan_type', 'type').withColumnRenamed('loan_status', 'status')

    print("Inserting data into dim_loans table...")

    spark.sql(f"""
    INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.dim_loans
    SELECT 
        loan_id,
        customer_id,
        amount,
        interest_rate,
        term,
        start_date,
        end_date,
        type,
        status
    FROM {{loan_df}}
    """, loan_df=loan_df)

    print("Data inserted into dim_loans table successfully.")

    return 200

def create_loan_payment_fact(key):
    loan_payment_df = spark.read.option("recursiveFileLookup", "true").parquet(LANDING_ZONE + 'loan_payments/' + key)

    return loan_payment_df

def batch_loan_payment_fact_6m(start_date):
    month = start_date.month
    year = start_date.year

    end_month = month + 6 if month + 6 <= 12 else month + 6 - 12
    end_year = year if month + 6 <= 12 else year + 1

    if end_year > year:
        start_year_key = f'year={year}/month=' + '{' + ','.join([str(m) for m in range(month, 13)]) + '}/'
        end_year_key = f'year={end_year}/month=' + '{' + ','.join([str(m) for m in range(1, end_month + 1)]) + '}/'
        loan_payment_df1 = create_loan_payment_fact(start_year_key)
        loan_payment_df2 = create_loan_payment_fact(end_year_key)

        loan_payment_df = loan_payment_df1.union(loan_payment_df2)
    else:
        start_year_key = f'year={year}/month=' + '{' + ','.join([str(m) for m in range(month, end_month + 1)]) + '}/'
        loan_payment_df = create_loan_payment_fact(start_year_key)

    return loan_payment_df

def create_fact_loan_payments():
    # LOAN PAYMENTS
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{DATABASE_NAME}.fact_loan_payments (
            payment_id STRING,
            loan_id STRING,
            transaction_id STRING,
            scheduled_payment_date_id INT,
            payment_date_id INT,
            paid_date_id INT,
            payment_amount LONG,
            principal_amount LONG,
            paid_amount LONG,
            scheduled_payment_date DATE,
            payment_date DATE,
            paid_date DATE
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}fact_loan_payments/'
        PARTITIONED BY (YEAR(paid_date))
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    print("Table loan payments created successfully.")

    spark.sql(f"ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.fact_loan_payments ADD PARTITION FIELD month(paid_date)")

    spark.udf.register("date_to_int", lambda date: int(date.strftime('%Y%m%d')), IntegerType())
    for i in range(2):
        loan_payment_df = batch_loan_payment_fact_6m(DATE_NOW - datetime.timedelta(days=365 // (i + 1)))

        loan_payment_df = loan_payment_df.withColumn(
            'scheduled_payment_date_id', f.call_function("date_to_int", f.col('scheduled_payment_date'))) \
            .withColumn(
            'payment_date_id', f.call_function("date_to_int", f.col('paid_date'))) \
            .withColumn(
            'paid_date_id', f.call_function("date_to_int", f.col('paid_date')))

        spark.sql(f"""
        INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.fact_loan_payments
        SELECT 
            payment_id,
            loan_id,
            transaction_id,
            scheduled_payment_date_id,
            payment_date_id,
            paid_date_id,
            payment_amount,
            principal_amount,
            paid_amount,
            scheduled_payment_date,
            payment_date,
            paid_date
        FROM {{loan_payment_df}}
    """, loan_payment_df=loan_payment_df)

        print(f"Inserted loan payments for 6 months starting from {DATE_NOW - datetime.timedelta(days=365 // (i + 1))} into fact_loan_payments table.")

# TRANSACTION
def create_transaction_fact_1m(year, month):

    try:
        transaction_df = spark.read.option("recursiveFileLookup", "true").parquet(LANDING_ZONE + 'transactions/' + 'year=' + str(year) + '/month=' + str(month) + '/')
        return transaction_df
    except Exception as e:
        logging.error(f"No transactions for {year}-{month}: {e}")
        return spark.createDataFrame([], schema=StructType([]))

def create_fact_transactions():
    print("Creating fact_transactions table...")

    spark.sql(f"""
    DROP TABLE IF EXISTS {CATALOG_NAME}.{DATABASE_NAME}.fact_transactions;
    """)

    spark.sql(f"""
        CREATE TABLE {CATALOG_NAME}.{DATABASE_NAME}.fact_transactions (
            transaction_id STRING,
            account_id STRING,
            employee_id STRING,
            date_id INT,
            `timestamp` TIMESTAMP,
            currency STRING,
            type STRING,
            amount LONG,
            balance_after LONG
        )
        USING iceberg
        LOCATION '{CLEAN_ZONE}fact_transactions/'
        PARTITIONED BY (years(timestamp))
        TBLPROPERTIES (
          'write.target-file-size-bytes' = '134217728'  -- 128MB
        )
    """)

    spark.sql(f"ALTER TABLE {CATALOG_NAME}.{DATABASE_NAME}.fact_transactions ADD PARTITION FIELD months(timestamp)")

    print("Table fact_transactions created successfully.")
    spark.udf.register("date_to_int", lambda date: int(date.strftime('%Y%m%d')), IntegerType())
    for i in range(12):
        month = (DATE_NOW.month - i - 1) % 12 + 1
        year = DATE_NOW.year if month <= DATE_NOW.month else DATE_NOW.year - 1
        transaction_fact_df = create_transaction_fact_1m(year, month)
        transaction_fact_df = transaction_fact_df.withColumn(
            'date_id', f.call_function("date_to_int", f.col('date')))\
            .withColumn('year', f.year(f.col('date'))) \
            .withColumn('month', f.month(f.col('date'))) \
            .withColumn('day', f.dayofmonth(f.col('date'))) \
        .withColumnRenamed('date', 'timestamp').withColumnRenamed('transaction_type', 'type')

        spark.sql(f"""
        INSERT INTO {CATALOG_NAME}.{DATABASE_NAME}.fact_transactions
        SELECT 
            transaction_id,
            account_id,
            employee_id,
            date_id,
            `timestamp`,
            currency,
            type,
            amount,
            balance_after
        FROM {{transaction_fact_df}}
        """, transaction_fact_df=transaction_fact_df)

        print(f"Inserted transactions for {year}-{month} into fact_transactions table.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create dimensions
    # create_dim_date_table()
    create_dim_accounts_table()
    # create_dim_branches()
    # create_dim_cards()
    # create_dim_customers()
    # create_dim_employees()
    # create_dim_loans()
    #
    # # Create fact tables
    # create_fact_loan_payments()
    # create_fact_transactions()

    logging.info("Data modeling completed successfully.")


