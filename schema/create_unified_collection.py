from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import json

# Connect to MongoDB
client = MongoClient('mongodb://admin:admin@localhost:27017/')
db = client['financial_db']

# Drop existing collections to ensure a clean setup
db.drop_collection('customers')
db.drop_collection('accounts')
db.drop_collection('cards')
db.drop_collection('loans')
db.drop_collection('transactions')
db.drop_collection('analytics')

def create_customer_collection():
    db.create_collection('customers', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'personal_info', 'accounts', 'cards', 'loans', 'primary_branch'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'personal_info': {
                    'bsonType': 'object',
                    'required': ['name', 'email', 'phone', 'address', 'registration_date', 'customer_since_months', 'customer_type', 'kyc_status'],
                    'properties': {
                        'name': {'bsonType': 'string'},
                        'email': {'bsonType': 'string'},
                        'phone': {'bsonType': 'string'},
                        'address': {
                            'bsonType': 'object',
                            'required': ['street', 'city', 'country'],
                            'properties': {
                                'street': {'bsonType': 'string'},
                                'city': {'bsonType': 'string'},
                                'country': {'bsonType': 'string'}
                            }
                        },
                        'registration_date': {'bsonType': 'date'},
                        'customer_since_months': {'bsonType': 'int'},
                        'customer_type': {'bsonType': 'string'},
                        'kyc_status': {'bsonType': 'string'}
                    }
                },
                'accounts': {'bsonType': 'array', 'items': {'bsonType': 'int'}},
                'cards': {'bsonType': 'array', 'items': {'bsonType': 'int'}},
                'loans': {'bsonType': 'array', 'items': {'bsonType': 'int'}},
                'primary_branch': {'bsonType': 'string'}
            }
        }
    })

    # Create indexes for customers
    db.customers.create_index('personal_info.email', unique=True)
    db.customers.create_index('personal_info.phone')

def create_accounts_collection():
    db.create_collection('accounts', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'customer_id', 'account_type', 'account_number', 'balance', 'currency', 'status'],
            'properties': {
                '_id': {'bsonType': 'int'},
                'customer_id': {'bsonType': 'string'},
                'account_type': {'bsonType': 'string'},
                'account_number': {'bsonType': 'string'},
                'balance': {'bsonType': 'double'},
                'currency': {'bsonType': 'string'},
                'interest_rate': {'bsonType': 'double'},
                'min_balance': {'bsonType': 'double'},
                'overdraft_limit': {'bsonType': 'double'},
                'status': {'bsonType': 'string'},
                'date_opened': {'bsonType': 'date'}
            }
        }
    })

    # Create indexes for accounts
    db.accounts.create_index('customer_id')
    db.accounts.create_index('account_number', unique=True)

def create_loans_collection():
    db.create_collection('loans', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'customer_id', 'loan_type', 'original_amount', 'remaining_balance', 'interest_rate',
                         'monthly_payment', 'next_payment_date', 'term_remaining_months'],
            'properties': {
                '_id': {'bsonType': 'int'},
                'customer_id': {'bsonType': 'string'},
                'loan_type': {'bsonType': 'string'},
                'original_amount': {'bsonType': 'double'},
                'remaining_balance': {'bsonType': 'double'},
                'interest_rate': {'bsonType': 'double'},
                'monthly_payment': {'bsonType': 'double'},
                'next_payment_date': {'bsonType': 'date'},
                'term_remaining_months': {'bsonType': 'int'},
                'payment_history': {'bsonType': 'string'}
            }
        }
    })

    # Create indexes for loans
    db.loans.create_index('customer_id')

def create_cards_collection():
    db.create_collection('cards', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'customer_id', 'card_type', 'masked_number', 'linked_account', 'credit_limit',
                         'outstanding_balance', 'available_credit'],
            'properties': {
                '_id': {'bsonType': 'int'},
                'customer_id': {'bsonType': 'string'},
                'card_type': {'bsonType': 'string'},
                'masked_number': {'bsonType': 'string'},
                'linked_account': {'bsonType': 'int'},
                'credit_limit': {'bsonType': 'double'},
                'outstanding_balance': {'bsonType': 'double'},
                'available_credit': {'bsonType': 'double'},
                'monthly_fee': {'bsonType': 'double'},
                'interest_rate': {'bsonType': 'double'},
                'expiration_date': {'bsonType': 'date'}
            }
        }
    })

    # Create indexes for cards
    db.cards.create_index('customer_id')
    db.cards.create_index('linked_account')

def create_transactions_collection():
    db.create_collection('transactions', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'customer_id', 'date', 'amount', 'type', 'payment_method'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'customer_id': {'bsonType': 'string'},
                'account_id': {'bsonType': 'int'},
                'date': {'bsonType': 'date'},
                'amount': {'bsonType': 'double'},
                'type': {'bsonType': 'string'},
                'payment_method': {'bsonType': 'string'}
            }
        }
    })

    # Create indexes for transactions
    db.transactions.create_index([('customer_id', 1), ('date', -1)])
    db.transactions.create_index([('account_id', 1), ('date', -1)])

def create_analytics_collection():
    db.create_collection('analytics', validator={
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'customer_id', 'period', 'transaction_analytics', 'financial_health_score',
                         'last_updated'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'customer_id': {'bsonType': 'string'},
                'period': {'bsonType': 'string'},
                'transaction_analytics': {
                    'bsonType': 'object',
                    'required': ['total_transactions', 'total_inflow', 'total_outflow', 'net_cashflow',
                                 'avg_monthly_transactions', 'spending_patterns'],
                    'properties': {
                        'total_transactions': {'bsonType': 'int'},
                        'total_inflow': {'bsonType': 'double'},
                        'total_outflow': {'bsonType': 'double'},
                        'net_cashflow': {'bsonType': 'double'},
                        'avg_monthly_transactions': {'bsonType': 'int'},
                        'spending_patterns': {
                            'bsonType': 'object',
                            'required': ['by_type', 'by_channel'],
                            'properties': {
                                'by_type': {
                                    'bsonType': 'array',
                                    'items': {
                                        'bsonType': 'object',
                                        'required': ['type', 'count', 'amount', 'percentage'],
                                        'properties': {
                                            'type': {'bsonType': 'string'},
                                            'count': {'bsonType': 'int'},
                                            'amount': {'bsonType': 'double'},
                                            'percentage': {'bsonType': 'double'}
                                        }
                                    }
                                },
                                'by_channel': {
                                    'bsonType': 'object',
                                    'required': ['branch', 'atm', 'online'],
                                    'properties': {
                                        'branch': {'bsonType': 'double'},
                                        'atm': {'bsonType': 'double'},
                                        'online': {'bsonType': 'double'}
                                    }
                                }
                            }
                        }
                    }
                },
                'financial_health_score': {
                    'bsonType': 'object',
                    'required': ['overall_score', 'breakdown', 'debt_to_income_ratio', 'savings_rate',
                                 'liquidity_ratio', 'risk_category'],
                    'properties': {
                        'overall_score': {'bsonType': 'int'},
                        'breakdown': {
                            'bsonType': 'object',
                            'required': ['payment_history', 'account_balance', 'credit_utilization',
                                         'relationship_depth'],
                            'properties': {
                                'payment_history': {'bsonType': 'int'},
                                'account_balance': {'bsonType': 'int'},
                                'credit_utilization': {'bsonType': 'int'},
                                'relationship_depth': {'bsonType': 'int'}
                            }
                        },
                        'debt_to_income_ratio': {'bsonType': 'double'},
                        'savings_rate': {'bsonType': 'double'},
                        'liquidity_ratio': {'bsonType': 'double'},
                        'risk_category': {'bsonType': 'string'}
                    }
                },
                'last_updated': {'bsonType': 'date'}
            }
        }
    })

    # Create indexes for analytics
    db.analytics.create_index([('customer_id', 1), ('period', 1)])

if __name__ == '__main__':
    create_customer_collection()
    create_accounts_collection()
    create_loans_collection()
    create_cards_collection()
    create_transactions_collection()
    create_analytics_collection()