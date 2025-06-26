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


@dataclass
class BankDataConfig:
    """Cấu hình tham số cho việc tạo dữ liệu ngân hàng"""

    # Batch size
    batch_size: int = 500000

    # Số lượng bản ghi cho từng bảng
    num_employees: int = 50
    num_customers: int = 100
    num_accounts: int = 200
    num_transactions: int = 1000
    num_cards: int = 150
    num_loans: int = 80
    num_loan_payments: int = 200

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

    def create_branches(self) -> List[Dict]:
        """Tạo dữ liệu chi nhánh"""
        branches = []
        req = requests.get('https://www.vpbank.com.vn/api/branchandatm/searchbranchesandatms?keyword=&city=&district=')
        data = req.json()
        branch_id = 1
        for item in data['branches_and_atm']:
            if item['IsBranch'] == True and (item['Address'].find('Hà Nội') != -1 or item['Address'].find('Hồ Chí Minh') != -1):
                branches.append({
                    'branch_id': self.generate_id(self.config.branch_prefix, branch_id),
                    'name': item['Name'].replace('\n', ' ').strip(),
                    'address': item['Address'],
                    'phone_number': item['Phone']
                })
                branch_id += 1

        return branches

    def create_employees(self, branches: List[Dict]) -> List[Dict]:
        """Tạo dữ liệu nhân viên"""
        positions = ['Giám đốc chi nhánh', 'Trưởng phòng', 'Giao dịch viên',
                     'Tư vấn viên', 'Kế toán', 'Chuyên viên tín dụng']
        employees = []

        for i in range(1, self.config.num_employees + 1):
            employee = {
                'employee_id': self.generate_id(self.config.employee_prefix, i),
                'name': self.fake.name(),
                'position': random.choice(positions),
                'branch_id': random.choice(branches)['branch_id']
            }
            employees.append(employee)
        return employees

    def create_customers(self, customer_types: List[Dict], customer_status_types: List[Dict]) -> List[Dict]:
        """Tạo dữ liệu khách hàng"""
        customers = []
        for i in range(1, self.config.num_customers + 1):
            customer = {
                'customer_id': self.generate_id(self.config.customer_prefix, i),
                'name': self.fake.name(),
                'country': 'Việt Nam',
                'city': random.choice(['Hà Nội', 'Hồ Chí Minh']),
                'street': self.fake.street_address(),
                'phone_number': self.fake.phone_number(),
                'registration_date': self.fake.date_between(
                    start_date=f'-{self.config.max_years_back}y',
                    end_date='today'
                ).strftime('%Y-%m-%d'),
                'email': self.fake.email(),
                'customer_type_id': random.choice(customer_types)['customer_type_id'],
                'status': random.choice(customer_status_types)['status_id']
            }
            customers.append(customer)
        return customers

    def create_accounts(self, customers: List[Dict], branches: List[Dict],
                        account_types: List[Dict], account_status_types: List[Dict]) -> List[Dict]:
        """Tạo dữ liệu tài khoản"""
        accounts = []
        balance_ranges = {
            'Tiết kiệm': self.config.savings_balance_range,
            'Thanh toán': self.config.checking_balance_range,
            'Đầu tư': self.config.investment_balance_range,
            'Doanh nghiệp': self.config.business_balance_range
        }

        min_balances = {
            'Tiết kiệm': 500_000,
            'Thanh toán': 50_000,
            'Đầu tư': 1_000_000,
            'Doanh nghiệp': 5_000_000
        }

        for i in range(1, self.config.num_accounts + 1):
            customer = random.choice(customers)
            account_type = random.choice(account_types)
            currency = random.choice(self.config.currencies)

            # Tạo số tài khoản 12 chữ số
            account_number = ''.join([str(random.randint(0, 9)) for _ in range(12)])

            # Số dư theo loại tài khoản
            balance_range = balance_ranges[account_type['type_name']]
            balance = random.uniform(*balance_range)
            min_balance = min_balances[account_type['type_name']]

            date_opened = self.fake.date_between(
                start_date=f'-{self.config.account_years_back}y',
                end_date='today'
            )

            # Tài khoản đóng
            date_closed = None
            if random.random() < self.config.closed_account_ratio:
                date_closed = self.fake.date_between(
                    start_date=date_opened,
                    end_date='today'
                ).strftime('%Y-%m-%d')

            account = {
                'account_id': self.generate_id(self.config.account_prefix, i),
                'customer_id': customer['customer_id'],
                'branch_id': random.choice(branches)['branch_id'],
                'type': account_type['account_type_id'],
                'status': random.choice(account_status_types)['status_id'],
                'currency': currency,
                'number': account_number,
                'balance': round(balance),
                'min_balance': min_balance,
                'date_opened': date_opened.strftime('%Y-%m-%d'),
                'date_closed': date_closed
            }
            accounts.append(account)
        return accounts

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

            balance_after = round(account['balance'] + amount)
            # Đảm bảo số dư không âm
            if balance_after < 0 and type_name in ['Rút tiền', 'Chuyển khoản']:
                amount *= -1
                type_name = 'Gửi tiền' if type_name == 'Rút tiền' else 'Chuyển khoản'

            transaction = {
                'transaction_id': self.generate_id(self.config.transaction_prefix, start_index + i, is_transactions=True),
                'account_id': account['account_id'],
                'receiver_account_id': random.choice(accounts)['account_id'] if type_name == 'Chuyển khoản' else None,
                'employee_id': random.choice(employees)['employee_id'],
                'type': transaction_type['transaction_type_id'],
                'currency': random.choice(self.config.currencies),
                'date': self.fake.date_time_between(
                    start_date=f'-{self.config.transaction_years_back}y',
                    end_date='now'
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'amount': round(amount),
                'balance_after': round(account['balance'] + amount)
            }

            transactions.append(transaction)
        return transactions

    def create_cards(self, accounts: List[Dict], customers: List[Dict],
                     card_types: List[Dict]) -> List[Dict]:
        """Tạo dữ liệu thẻ"""
        cards = []

        for i in range(1, self.config.num_cards + 1):
            account = random.choice(accounts)
            card_type = random.choice(card_types)
            customer_id = account['customer_id']

            issuance_date = self.fake.date_between(
                start_date=f'-{self.config.card_years_back}y',
                end_date='today'
            )
            expiration_date = issuance_date + timedelta(days=365 * 3)  # 3 năm

            card = {
                'card_id': self.generate_id(self.config.card_prefix, i),
                'account_id': account['account_id'],
                'customer_id': customer_id,
                'card_type_id': card_type['card_type_id'],
                'monthly_fee': card_type['monthly_fee'],
                'interest_rate': card_type['interest_rate'],
                'expiration_date': expiration_date.strftime('%Y-%m-%d'),
                'issuance_date': issuance_date.strftime('%Y-%m-%d')
            }
            cards.append(card)
        return cards

    def create_loans(self, customers: List[Dict], loan_types: List[Dict],
                     loan_status_types: List[Dict]) -> List[Dict]:
        """Tạo dữ liệu khoản vay"""
        loans = []

        amount_ranges = {
            'Thế chấp nhà': self.config.mortgage_amount_range,
            'Tín chấp cá nhân': self.config.personal_loan_range,
            'Kinh doanh': self.config.business_loan_range,
            'Xe máy/Ô tô': self.config.vehicle_loan_range
        }

        for i in range(1, self.config.num_loans + 1):
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
            status_id = random.choice(loan_status_types)['status_id']
            if random.random() < self.config.paid_loan_ratio:
                status_id = 3  # Đã thanh toán

            loan = {
                'loan_id': self.generate_id(self.config.loan_prefix, i),
                'customer_id': customer['customer_id'],
                'loan_type_id': loan_type['loan_type_id'],
                'amount': round(amount),
                'currency': random.choice(self.config.currencies),
                'interest_rate': round(interest_rate, 2),
                'term': term,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'loan_status_id': status_id,
                # Lưu thêm datetime object để sử dụng trong loan_payments
                '_start_date_obj': start_date,
                '_end_date_obj': end_date
            }
            loans.append(loan)
        return loans

    def create_loan_payments(self, loans: List[Dict], transactions: List[Dict], start_index: int) -> List[Dict]:
        """Tạo dữ liệu thanh toán khoản vay - ĐÃ SỬA LỖI"""
        loan_payments = []
        transactions = [t for t in transactions if t['type'] == 4]  # Chỉ lấy giao dịch thanh toán khoản vay
        today = datetime.today().date()

        for i in range(1, self.config.num_loan_payments + 1):
            loan = random.choice(loans)
            transaction = random.choice(transactions)

            monthly_payment = loan['amount'] / loan['term']
            payment_amount = monthly_payment * random.uniform(0.8, 1.2)
            principal_amount = payment_amount * random.uniform(0.6, 0.8)

            start_date_obj = loan['_start_date_obj']
            end_date_obj = loan['_end_date_obj']

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
                'transaction_id': transaction['transaction_id'],
                'payment_amount': round(payment_amount),
                'scheduled_payment_date': scheduled_date.strftime('%Y-%m-%d'),
                'payment_date': actual_payment_date.strftime('%Y-%m-%d'),
                'principal_amount': round(principal_amount),
                'paid_amount': round(payment_amount),
                'paid_date': actual_payment_date.strftime('%Y-%m-%d')
            }
            loan_payments.append(loan_payment)
        return loan_payments


    def generate_all_data(self):
        """Tạo tất cả dữ liệu ngân hàng"""
        print("Đang tạo dữ liệu ngân hàng với cấu hình:")
        print(f"- Nhân viên: {self.config.num_employees}")
        print(f"- Khách hàng: {self.config.num_customers}")
        print(f"- Tài khoản: {self.config.num_accounts}")
        print(f"- Giao dịch: {self.config.num_transactions}")
        print(f"- Thẻ: {self.config.num_cards}")
        print(f"- Khoản vay: {self.config.num_loans}")
        print(f"- Thanh toán vay: {self.config.num_loan_payments}")

        # Tạo bảng tra cứu
        lookup_tables = self.create_lookup_tables()

        # Tạo dữ liệu chính theo thứ tự phụ thuộc
        branches = self.create_branches()

        branches_df = pd.DataFrame(branches)
        branches_df.to_csv('./bank_data/branches.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(branches)} chi nhánh ngân hàng.")

        employees = self.create_employees(branches)

        employees_df = pd.DataFrame(employees)
        employees_df.to_csv('./bank_data/employees.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(employees)} nhân viên ngân hàng.")

        customers = self.create_customers(
            lookup_tables['customer_types'],
            lookup_tables['customer_status_types']
        )
        customers_df = pd.DataFrame(customers)
        customers_df.to_csv('./bank_data/customers.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(customers)} khách hàng ngân hàng.")

        accounts = self.create_accounts(
            customers, branches,
            lookup_tables['account_types'],
            lookup_tables['account_status_types']
        )
        accounts_df = pd.DataFrame(accounts)
        accounts_df.to_csv('./bank_data/accounts.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(accounts)} tài khoản ngân hàng.")

        cards = self.create_cards(accounts, customers, lookup_tables['card_types'])
        cards_df = pd.DataFrame(cards)
        cards_df.to_csv('./bank_data/cards.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(cards)} thẻ ngân hàng.")

        loans = self.create_loans(
            customers,
            lookup_tables['loan_types'],
            lookup_tables['loan_status_types']
        )
        loans_df = pd.DataFrame(loans)
        loans_df.to_csv('./bank_data/loans.csv', index=False, encoding='utf-8')
        print(f"Đã tạo {len(loans)} khoản vay ngân hàng.")

        numbers_of_batches = self.config.num_transactions // self.config.batch_size
        transaction_start_index = 0
        loan_payment_start_index = 0

        for batch in range(numbers_of_batches + 1):
            print(f"Đang tạo giao dịch batch {batch + 1}/{numbers_of_batches + 1}...")
            # Tạo giao dịch theo từng batch

            transactions = self.create_transactions(
                accounts, employees,
                lookup_tables['transaction_types'],
                n=batch * self.config.batch_size + self.config.batch_size if batch < numbers_of_batches else self.config.num_transactions - batch * self.config.batch_size,
                start_index=transaction_start_index
            )

            transaction_start_index += len(transactions)

            transactions_df = pd.DataFrame(transactions)
            transactions_df.to_csv(f'./bank_data/transactions.csv', index=False, encoding='utf-8', mode='a', header=not os.path.exists('./bank_data/transactions.csv'))

            loan_payments = self.create_loan_payments(loans, transactions,
                                                      start_index=loan_payment_start_index)

            loan_payment_start_index += len(loan_payments)

            loan_payments_df = pd.DataFrame(loan_payments)
            loan_payments_df.to_csv(f'./bank_data/loan_payments.csv', index=False, encoding='utf-8', mode='a', header=not os.path.exists('./bank_data/loan_payments.csv'))


        print("Tạo dữ liệu hoàn tất!")


# Chạy ví dụ
if __name__ == "__main__":

    custom_config = BankDataConfig(
        num_employees=5000,
        num_customers=500000,
        num_accounts=700000,
        num_transactions=365000000,
        num_cards=600000,
        num_loans=30000,
        num_loan_payments=10000
    )

    generator = BankDataGenerator(custom_config)
    data = generator.generate_all_data()
