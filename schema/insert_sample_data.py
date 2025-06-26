from datetime import datetime

from pymongo import MongoClient

client = MongoClient('mongodb://admin:admin@localhost:27017/')
db = client['financial_db']


def create_customer_data():
    db.customers.insert_one({
        '_id': 'CUST_12345',
        'personal_info': {
            'name': 'Nguyễn Văn An',
            'email': 'an.nguyen@email.com',
            'phone': '0901234567',
            'address': {
                'street': '123 Nguyen Hue Street',
                'city': 'Ho Chi Minh City',
                'country': 'Vietnam'
            },
            'registration_date': datetime(2020, 3, 15),
            'customer_since_months': 62,
            'customer_type': 'Individual',
            'kyc_status': 'Verified'
        },
        'accounts': [1001, 1002],
        'cards': [2001],
        'loans': [3001],
        'primary_branch': 'District 1 Branch'
    })


def create_account_data():
    db.accounts.insert_many([
        {
            '_id': 1001,
            'customer_id': 'CUST_12345',
            'account_type': 'Savings Account',
            'account_number': '****7890',
            'balance': 150000000.00,
            'currency': 'VND',
            'interest_rate': 4.5,
            'min_balance': 500000.00,
            'status': 'Active',
            'date_opened': datetime(2020, 3, 20)
        },
        {
            '_id': 1002,
            'customer_id': 'CUST_12345',
            'account_type': 'Checking Account',
            'account_number': '****7891',
            'balance': 45000000.00,
            'currency': 'VND',
            'overdraft_limit': 5000000.00,
            'status': 'Active'
        }
    ])


def create_cards_data():
    db.cards.insert_one({
        '_id': 2001,
        'customer_id': 'CUST_12345',
        'card_type': 'Credit Card',
        'masked_number': '4567****1234',
        'linked_account': 1002,
        'credit_limit': 50000000.00,
        'outstanding_balance': 8500000.00,
        'available_credit': 41500000.00,
        'monthly_fee': 50000.00,
        'interest_rate': 24.5,
        'expiration_date': datetime(2027, 3, 31)
    })


def create_loans_data():
    db.loans.insert_one({
        '_id': 3001,
        'customer_id': 'CUST_12345',
        'loan_type': 'Personal Loan',
        'original_amount': 100000000.00,
        'remaining_balance': 85000000.00,
        'interest_rate': 15.5,
        'monthly_payment': 3200000.00,
        'next_payment_date': datetime(2025, 7, 15),
        'term_remaining_months': 8,
        'payment_history': 'Excellent'
    })


def transaction_data():
    db.transactions.insert_many([
        {
            '_id': 'TXN_20250604_001',
            'customer_id': 'CUST_12345',
            'account_id': 1002,
            'date': datetime(2025, 6, 4, 15, 30),
            'amount': 1250000.00,
            'type': 'Deposit',
            'payment_method': 'credit_card'
        },
        {
            '_id': 'TXN_20250604_002',
            'customer_id': 'CUST_12345',
            'account_id': 1002,
            'date': datetime(2025, 6, 4, 9, 15),
            'amount': -450000.00,
            'type': 'Withdrawal',
            'payment_method': 'debit_card'
        }
    ])


def create_analytics_data():
    db.analytics.insert_one({
        '_id': 'CUST_12345_202506',
        'customer_id': 'CUST_12345',
        'period': 'last_12_months',
        'transaction_analytics': {
            'total_transactions': 892,
            'total_inflow': 280000000.00,
            'total_outflow': 205000000.00,
            'net_cashflow': 75000000.00,
            'avg_monthly_transactions': 74,
            'spending_patterns': {
                'by_type': [
                    {'type': 'Deposit', 'count': 156, 'amount': 280000000.00, 'percentage': 57.7},
                    {'type': 'Withdrawal', 'count': 234, 'amount': 145000000.00, 'percentage': 29.9},
                    {'type': 'Transfer', 'count': 389, 'amount': 60000000.00, 'percentage': 12.4}
                ],
                'by_channel': {
                    'branch': 63.6,
                    'atm': 25.4,
                    'online': 11.0
                }
            }
        },
        'financial_health_score': {
            'overall_score': 785,
            'breakdown': {
                'payment_history': 95,
                'account_balance': 88,
                'credit_utilization': 17,
                'relationship_depth': 92
            },
            'debt_to_income_ratio': 13.6,
            'savings_rate': 22.5,
            'liquidity_ratio': 3.8,
            'risk_category': 'Low'
        },
        'last_updated': datetime(2025, 6, 5, 0, 33)
    })


if __name__ == '__main__':
    create_customer_data()
    create_account_data()
    create_cards_data()
    create_loans_data()
    transaction_data()
    create_analytics_data()
    print('Sample data inserted successfully!')
