import time
from faker import Faker
import random
from datetime import datetime, timedelta
import json
import csv
import os
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import requests
import pandas as pd
import awswrangler as wr
import boto3
import threading

# GET DATA FROM S3
CLEAN_ZONE = 's3://vpbank-banking-lakehouse/clean-zone/'
DATABASE_NAME = 'vpbank_clean_zone'
transaction_stream_name = "vpbank-transaction-stream"
loan_payment_stream_name = "vpbank-loan-payment-stream"
loan_stream_name = "vpbank-loan-stream"


def get_data_from_catalog(table_name: str):
    """Lấy dữ liệu từ AWS Glue Data Catalog"""
    print(f"Retrieving data from table: {table_name} in database: {DATABASE_NAME}")
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {table_name}",
        database=DATABASE_NAME,
        ctas_approach=False,
    )

    dicts_data = df.to_dict(orient='records')

    return dicts_data

def get_last_id_from_catalog(table_name: str, id_column: str, prefix: str) -> int:
    """Lấy ID lớn nhất từ bảng trong AWS Glue Data Catalog"""
    print(f"Retrieving last ID from table: {table_name}, column: {id_column} with prefix: {prefix}")
    df = wr.athena.read_sql_query(
        sql=f"SELECT MAX({id_column}) AS max_id FROM {table_name}",
        database=DATABASE_NAME
    )

    max_id = df['max_id'].iloc[0]
    if max_id is not None:
        max_id = int(max_id.replace(prefix, ''))
        return max_id
    else:
        return 0

def put_record_to_kinesis(stream_name, stream_arn, data, partition_key):
    """Gửi bản ghi đến Kinesis Data Stream"""
    print(f"Putting record to Kinesis stream: {stream_name}")
    client = boto3.client('kinesis', region_name='ap-southeast-1')
    response = client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key,
        StreamARN=stream_arn
    )
    print(f"Record put to Kinesis stream: {stream_name}, response: {response}")

def create_stream(stream_name: str):
    """Tạo Kinesis Data Stream nếu chưa tồn tại"""
    client = boto3.client('kinesis', region_name='ap-southeast-1')
    try:
        response = client.describe_stream(StreamName=stream_name)
        print(f"Stream {stream_name} already exists.")
        return response['StreamDescription']['StreamARN']
    except Exception as e:
        print(f"Creating stream {stream_name}...")
        client.create_stream(
            StreamName=stream_name,
            ShardCount=2
        )
        print(f"Stream {stream_name} created successfully.")
        response = client.describe_stream(StreamName=stream_name)
        return response['StreamDescription']['StreamARN']

@dataclass
class BankDataConfig:
    """Cấu hình tham số cho việc tạo dữ liệu ngân hàng"""

    # Batch size
    batch_size: int = 500000

    # Số lượng bản ghi cho từng bảng
    num_employees: int = 50
    num_customers: int = 100
    num_accounts: int = 200
    num_transactions: int = 1
    num_cards: int = 150
    num_loans: int = 1
    num_loan_payments: int = 1

    # Cấu hình thời gian
    max_years_back: int = 5  # Khách hàng đăng ký tối đa bao nhiêu năm trước
    account_years_back: int = 3  # Tài khoản mở tối đa bao nhiêu năm trước
    transaction_years_back: int = 1  # Giao dịch trong bao nhiêu năm trước
    loan_years_back: int = 1  # Khoản vay tối đa bao nhiêu năm trước
    card_years_back: int = 2  # Thẻ phát hành tối đa bao nhiêu năm trước

    # Cấu hình ID
    id_length: int = 7
    transaction_id_length: int = 10
    customer_prefix: str = "CUS"
    account_prefix: str = "ACC"
    branch_prefix: str = "BRA"
    employee_prefix: str = "EMP"
    transaction_prefix: str = "TXN"
    card_prefix: str = "CRD"
    loan_prefix: str = "LOA"
    payment_prefix: str = "PAY"

    # Cấu hình tiền tệ và số tiền
    currencies: List[str] = None

    # Cấu hình số dư tài khoản (VND)
    savings_balance_range: tuple = (5_000_000, 500_000_000)
    checking_balance_range: tuple = (1_000_000, 100_000_000)
    investment_balance_range: tuple = (10_000_000, 1_000_000_000)
    business_balance_range: tuple = (50_000_000, 2_000_000_000)

    # Cấu hình giao dịch
    deposit_amount_range: tuple = (100_000, 50_000_000)
    withdrawal_amount_range: tuple = (100_000, 10_000_000)
    transfer_amount_range: tuple = (100_000, 20_000_000)

    # Cấu hình khoản vay
    mortgage_amount_range: tuple = (500_000_000, 5_000_000_000)
    personal_loan_range: tuple = (10_000_000, 500_000_000)
    business_loan_range: tuple = (100_000_000, 2_000_000_000)
    vehicle_loan_range: tuple = (50_000_000, 800_000_000)

    # Cấu hình lãi suất
    interest_rate_range: tuple = (6.0, 15.0)

    # Tỷ lệ tài khoản/khoản vay đã đóng
    closed_account_ratio: float = 0.1
    paid_loan_ratio: float = 0.3

    # Locale cho Faker
    locale: str = 'vi_VN'

    def __post_init__(self):
        if self.currencies is None:
            self.currencies = ['VND']


class BankDataGenerator:
    """Class để tạo dữ liệu fake cho hệ thống ngân hàng"""

    def __init__(self, config: BankDataConfig = None):
        self.config = config or BankDataConfig()
        self.fake = Faker(self.config.locale)
        Faker.seed(42)  # Để có kết quả nhất quán

    def generate_id(self, prefix: str, num: int, is_transactions: bool = False) -> str:
        """Tạo ID theo format yêu cầu"""
        return f"{prefix}{num:0{self.config.id_length if not is_transactions else self.config.transaction_id_length}d}"

    def create_lookup_tables(self) -> Dict[str, List[Dict]]:
        """Tạo các bảng tra cứu"""
        return {
            'customer_types': [
                {'customer_type_id': 1, 'type_name': 'Cá nhân'},
                {'customer_type_id': 2, 'type_name': 'Doanh nghiệp'},
                {'customer_type_id': 3, 'type_name': 'Tổ chức'}
            ],
            'customer_status_types': [
                {'status_id': 1, 'status_name': 'Hoạt động'},
                {'status_id': 2, 'status_name': 'Tạm khóa'},
                {'status_id': 3, 'status_name': 'Đóng'}
            ],
            'account_types': [
                {'account_type_id': 1, 'type_name': 'Tiết kiệm'},
                {'account_type_id': 2, 'type_name': 'Thanh toán'},
                {'account_type_id': 3, 'type_name': 'Đầu tư'},
                {'account_type_id': 4, 'type_name': 'Doanh nghiệp'}
            ],
            'account_status_types': [
                {'status_id': 1, 'status_name': 'Hoạt động'},
                {'status_id': 2, 'status_name': 'Tạm khóa'},
                {'status_id': 3, 'status_name': 'Đóng'}
            ],
            'transaction_types': [
                {'transaction_type_id': 1, 'type_name': 'Gửi tiền'},
                {'transaction_type_id': 2, 'type_name': 'Rút tiền'},
                {'transaction_type_id': 3, 'type_name': 'Chuyển khoản'},
                {'transaction_type_id': 4, 'type_name': 'Thanh toán khoản vay'},
                {'transaction_type_id': 5, 'type_name': 'Lãi suất'}
            ],
            'card_types': [
                {'card_type_id': 1, 'type_name': 'ATM', 'monthly_fee': 0, 'interest_rate': 0},
                {'card_type_id': 2, 'type_name': 'Visa Debit', 'monthly_fee': 50000, 'interest_rate': 0},
                {'card_type_id': 3, 'type_name': 'Mastercard Credit', 'monthly_fee': 100000, 'interest_rate': 2.5},
                {'card_type_id': 4, 'type_name': 'Visa Credit Gold', 'monthly_fee': 200000, 'interest_rate': 2.0}
            ],
            'loan_types': [
                {'loan_type_id': 1, 'type_name': 'Thế chấp nhà'},
                {'loan_type_id': 2, 'type_name': 'Tín chấp cá nhân'},
                {'loan_type_id': 3, 'type_name': 'Kinh doanh'},
                {'loan_type_id': 4, 'type_name': 'Xe máy/Ô tô'}
            ],
            'loan_status_types': [
                {'status_id': 1, 'status_name': 'Đang vay'},
                {'status_id': 2, 'status_name': 'Quá hạn'},
                {'status_id': 3, 'status_name': 'Đã thanh toán'},
                {'status_id': 4, 'status_name': 'Nợ xấu'}
            ]
        }

    def create_transactions(self, accounts: List[Dict], employees: List[Dict],
                            transaction_types: List[Dict], n: int, start_index: int) -> List[Dict]:
        """Tạo dữ liệu giao dịch"""
        transactions = []

        amount_ranges = {
            'Gửi tiền': self.config.deposit_amount_range,
            'Rút tiền': self.config.withdrawal_amount_range,
            'Chuyển khoản': self.config.transfer_amount_range,
            'Thanh toán khoản vay': self.config.transfer_amount_range,
            'Lãi suất': (50_000, 5_000_000)
        }

        for i in range(1, n + 1):
            account = random.choice(accounts)
            transaction_type = random.choice(transaction_types)

            # Số tiền giao dịch
            type_name = transaction_type['type_name']
            amount_range = amount_ranges.get(type_name, self.config.transfer_amount_range)
            amount = random.uniform(*amount_range)

            # Số âm cho rút tiền và một số giao dịch khác
            if type_name in ['Rút tiền', 'Thanh toán khoản vay']:
                amount = -amount
            elif type_name == 'Chuyển khoản':
                amount = random.choice([1, -1]) * amount

            transaction = {
                'transaction_id': self.generate_id(self.config.transaction_prefix, start_index + i, is_transactions=True),
                'account_id': account['account_id'],
                'employee_id': random.choice(employees)['employee_id'],
                'type': transaction_type['type_name'],
                'currency': random.choice(self.config.currencies),
                'date': self.fake.date_time_between(
                    start_date=f'-{self.config.transaction_years_back}y',
                    end_date='now'
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'amount': round(amount)
            }

            transactions.append(transaction)
        return transactions

    def create_loans(self, customers: List[Dict], loan_types: List[Dict],
                     loan_status_types: List[Dict], start_index: int) -> List[Dict]:
        """Tạo dữ liệu khoản vay"""
        loans = []

        amount_ranges = {
            'Thế chấp nhà': self.config.mortgage_amount_range,
            'Tín chấp cá nhân': self.config.personal_loan_range,
            'Kinh doanh': self.config.business_loan_range,
            'Xe máy/Ô tô': self.config.vehicle_loan_range
        }

        for i in range(start_index, start_index + self.config.num_loans):
            customer = random.choice(customers)
            loan_type = random.choice(loan_types)

            # Số tiền vay theo loại
            amount_range = amount_ranges[loan_type['type_name']]
            amount = random.uniform(*amount_range)

            interest_rate = random.uniform(*self.config.interest_rate_range)
            term = random.choice([12, 24, 36, 48, 60, 84, 120])  # Kỳ hạn (tháng)

            start_date = self.fake.date_between(
                start_date=f'-{self.config.loan_years_back}y',
                end_date='today'
            )
            end_date = start_date + timedelta(days=term * 30)

            # Trạng thái khoản vay
            status_id = random.choice(loan_status_types)['status_name']
            if random.random() < self.config.paid_loan_ratio:
                status_id = 3  # Đã thanh toán

            loan = {
                'loan_id': self.generate_id(self.config.loan_prefix, i),
                'customer_id': customer['customer_id'],
                'type': loan_type['type_name'],
                'amount': round(amount),
                'currency': random.choice(self.config.currencies),
                'interest_rate': round(interest_rate, 2),
                'term': term,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'status': status_id,
            }
            loans.append(loan)
        return loans

    def create_loan_payments(self, loans: List[Dict], transaction_last_id, start_index: int) -> List[Dict]:

        loan_payments = []
        today = datetime.today().date()

        for i in range(start_index, start_index + self.config.num_loan_payments):
            loan = random.choice(loans)

            monthly_payment = loan['amount'] / loan['term']
            payment_amount = monthly_payment * random.uniform(0.8, 1.2)
            principal_amount = payment_amount * random.uniform(0.6, 0.8)

            start_date_obj = loan['start_date']
            end_date_obj = loan['end_date']

            # scheduled_date không được vượt quá hôm nay
            scheduled_date = self.fake.date_between(
                start_date=start_date_obj,
                end_date=min(end_date_obj, today)
            )

            actual_payment_date = scheduled_date + timedelta(days=random.randint(-5, 15))
            # Đảm bảo không vượt quá ngày hôm nay
            if actual_payment_date > today:
                actual_payment_date = today

            loan_payment = {
                'payment_id': self.generate_id(self.config.payment_prefix, i + start_index),
                'loan_id': loan['loan_id'],
                'transaction_id': self.generate_id(self.config.transaction_prefix,
                                                   random.randint(1, transaction_last_id),
                                                   is_transactions=True),
                'payment_amount': round(payment_amount),
                'scheduled_payment_date': scheduled_date.strftime('%Y-%m-%d'),
                'payment_date': actual_payment_date.strftime('%Y-%m-%d'),
                'principal_amount': round(principal_amount),
                'paid_amount': round(payment_amount),
                'paid_date': actual_payment_date.strftime('%Y-%m-%d')
            }
            loan_payments.append(loan_payment)
        return loan_payments

def real_time_transaction_generator(stream_name: str, stream_arn: str, accounts, employees, transaction_types, last_transaction_id):
    """Hàm để gửi dữ liệu giao dịch đến Kinesis Data Stream"""

    records_per_day = 1000000
    time_per_record = 24 * 60 * 60 / records_per_day

    i = last_transaction_id + 1
    while True:
        generated_transactions = generator.create_transactions(
            accounts=accounts,
            employees=employees,
            transaction_types=transaction_types,
            n=1,
            start_index=i
        )

        for transaction in generated_transactions:
            put_record_to_kinesis(
                stream_name=stream_name,
                stream_arn=stream_arn,
                data=transaction,
                partition_key=transaction['account_id']
            )
            print(f"Transaction {transaction['transaction_id']} sent to Kinesis stream {stream_name}")

        i += 1
        time.sleep(time_per_record)

def real_time_loan_payment_generator(stream_name: str, stream_arn: str, loans, transaction_last_id, last_loan_payment_id):
    """Hàm để gửi dữ liệu thanh toán khoản vay đến Kinesis Data Stream"""

    i = last_loan_payment_id + 1
    while True:
        generated_loan_payments = generator.create_loan_payments(
            loans=loans,
            transaction_last_id=transaction_last_id,
            start_index=last_loan_payment_id + 1
        )

        for payment in generated_loan_payments:
            put_record_to_kinesis(
                stream_name=stream_name,
                stream_arn=stream_arn,
                data=payment,
                partition_key=payment['loan_id']
            )
            print(f"Loan payment {payment['payment_id']} sent to Kinesis stream {stream_name}")

        i += 1
        time.sleep(random.uniform(20, 30))

def real_time_loan_generator(stream_name: str, stream_arn: str, customers, loan_types, loan_status_types, start_index):
    """Hàm để gửi dữ liệu khoản vay đến Kinesis Data Stream"""
    i = start_index + 1
    while True:
        generated_loans = generator.create_loans(
            customers=customers,
            loan_types=loan_types,
            loan_status_types=loan_status_types,
            start_index=i
        )

        for loan in generated_loans:
            put_record_to_kinesis(
                stream_name=stream_name,
                stream_arn=stream_arn,
                data=loan,
                partition_key=loan['customer_id']
            )
            print(f"Loan {loan['loan_id']} sent to Kinesis stream {stream_name}")

        i += 1
        time.sleep(random.uniform(60, 120))


# Chạy ví dụ
if __name__ == "__main__":
    # Create Kinesis stream
    transaction_stream_arn = create_stream(transaction_stream_name)
    loan_payment_stream_arn = create_stream(loan_payment_stream_name)

    loan_stream_arn = create_stream(loan_stream_name)

    print(f"Transaction Stream ARN: {transaction_stream_arn}")
    print(f"Loan Payment Stream ARN: {loan_payment_stream_arn}")
    print(f"Loan Stream ARN: {loan_stream_arn}")

    config = BankDataConfig()
    generator = BankDataGenerator(config)

    # Tạo các bảng tra cứu
    lookup_tables = generator.create_lookup_tables()

    # Lấy dữ liệu từ AWS Glue Data Catalog
    customers = get_data_from_catalog('dim_customers')
    accounts = get_data_from_catalog('dim_accounts')
    employees = get_data_from_catalog('dim_employees')
    branches = get_data_from_catalog('dim_branches')
    loans = get_data_from_catalog('dim_loans')

    last_transaction_id = get_last_id_from_catalog('fact_transactions', 'transaction_id', config.transaction_prefix)
    last_loan_payment_id = get_last_id_from_catalog('fact_loan_payments', 'payment_id', config.payment_prefix)

    # Transactions generation thread
    transaction_thread = threading.Thread(
        target=real_time_transaction_generator,
        args=(transaction_stream_name, transaction_stream_arn, accounts, employees, lookup_tables['transaction_types'], last_transaction_id)
    )
    transaction_thread.start()

    # Loan payments generation thread
    loan_payment_thread = threading.Thread(
        target=real_time_loan_payment_generator,
        args=(loan_payment_stream_name, loan_payment_stream_arn, loans, last_transaction_id, last_loan_payment_id)
    )
    loan_payment_thread.start()

    # Loans generation thread
    loan_thread = threading.Thread(
        target=real_time_loan_generator,
        args=(loan_stream_name, loan_stream_arn, customers, lookup_tables['loan_types'], lookup_tables['loan_status_types'], len(loans) + 1)
    )
    loan_thread.start()