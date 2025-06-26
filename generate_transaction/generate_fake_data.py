import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid
from psycopg2 import pool
from tqdm import tqdm

# Initialize Faker
fake = Faker()

# Database connection parameters
DB_PARAMS = {
    'dbname': 'vpbank',
    'user': 'vpbank',
    'password': 'vpbank',
    'host': 'localhost',
    'port': '5432'
}

connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_PARAMS)


def get_db_connection():
    return connection_pool.getconn()


# def connect_db():
#     return psycopg2.connect(**DB_PARAMS)

def release_db_connection(conn):
    connection_pool.putconn(conn)


def insert_branches(cursor, count=5):
    for _ in range(count):
        cursor.execute("""
                       INSERT INTO branch (name, address, phone_number)
                       VALUES (%s, %s, %s) RETURNING id
                       """, (
                           fake.company() + " Branch",
                           fake.address().replace('\n', ', '),
                           fake.phone_number()
                       ))


def insert_employees(cursor, branch_ids, count_per_branch=3):
    positions = ['Manager', 'Teller', 'Loan Officer', 'Customer Service']
    for branch_id in branch_ids:
        for _ in range(count_per_branch):
            cursor.execute("""
                           INSERT INTO employee (branch_id, name, position)
                           VALUES (%s, %s, %s)
                           """, (
                               branch_id,
                               fake.name(),
                               random.choice(positions)
                           ))


def insert_transaction_types(cursor):
    types = [
        ('Deposit', 'Cash or check deposit'),
        ('Withdrawal', 'Cash withdrawal'),
        ('Transfer', 'Funds transfer between accounts'),
        ('Payment', 'Bill payment')
    ]
    for name, desc in types:
        cursor.execute("""
                       INSERT INTO transactionType (name, description, is_active)
                       VALUES (%s, %s, %s)
                       """, (name, desc, True))


def insert_account_types(cursor):
    types = [
        ('Checking', 'Standard checking account', False),
        ('Savings', 'Savings account with interest', False),
        ('Business', 'Business checking account', True),
        ('Premium', 'Premium account with benefits', True)
    ]
    for name, desc, overdraft in types:
        cursor.execute("""
                       INSERT INTO accountType (name, description, allows_overdraft)
                       VALUES (%s, %s, %s)
                       """, (name, desc, overdraft))


def insert_card_types(cursor):
    types = [
        ('Debit', 'Standard debit card', None, 1000.00),
        ('Credit', 'Standard credit card', 5000.00, 2000.00),
        ('Premium Credit', 'Premium credit card', 10000.00, 5000.00)
    ]
    for name, desc, credit_limit, trans_limit in types:
        cursor.execute("""
                       INSERT INTO cardType (name, description, credit_limit, transaction_limit)
                       VALUES (%s, %s, %s, %s)
                       """, (name, desc, credit_limit, trans_limit))


def insert_customer_types(cursor):
    types = [
        ('Individual', 'Personal banking customer'),
        ('Business', 'Commercial banking customer'),
        ('VIP', 'High-net-worth individual')
    ]
    for name, desc in types:
        cursor.execute("""
                       INSERT INTO customerType (name, description)
                       VALUES (%s, %s)
                       """, (name, desc))


def insert_customer_status_types(cursor):
    statuses = [
        ('Active', 'Active customer'),
        ('Inactive', 'Inactive customer'),
        ('Suspended', 'Suspended account')
    ]
    for name, desc in statuses:
        cursor.execute("""
                       INSERT INTO customerStatusType (name, description)
                       VALUES (%s, %s)
                       """, (name, desc))


def insert_loan_types(cursor):
    types = [
        ('Personal', 'Personal loan', 60),
        ('Mortgage', 'Home mortgage loan', 360),
        ('Auto', 'Auto loan', 84)
    ]
    for name, desc, term in types:
        cursor.execute("""
                       INSERT INTO loanType (name, description, max_term)
                       VALUES (%s, %s, %s)
                       """, (name, desc, term))


def insert_loan_status_types(cursor):
    statuses = [
        ('Active', 'Active loan'),
        ('Paid', 'Fully paid loan'),
        ('Defaulted', 'Defaulted loan')
    ]
    for name, desc in statuses:
        cursor.execute("""
                       INSERT INTO loanStatusType (name, description)
                       VALUES (%s, %s)
                       """, (name, desc))


def insert_account_status_types(cursor):
    statuses = [
        ('Open', 'Active account'),
        ('Closed', 'Closed account'),
        ('Frozen', 'Frozen account')
    ]
    for name, desc in statuses:
        cursor.execute("""
                       INSERT INTO accountStatusType (name, description)
                       VALUES (%s, %s)
                       """, (name, desc))


def insert_customers(cursor, customer_type_ids, status_type_ids, count=1000, batch_size=100):
    used_emails = set()
    batch = []
    for i in tqdm(range(count), desc="Inserting customers"):
        name = fake.name()
        base_email = fake.email()
        email = base_email
        counter = 1
        # Ensure unique email
        while email in used_emails:
            email = f"{base_email.split('@')[0]}{counter}@{base_email.split('@')[1]}"
            counter += 1
        used_emails.add(email)

        batch.append((
            name,
            fake.country(),
            fake.city(),
            fake.street_address(),
            fake.phone_number(),
            email,
            random.choice(customer_type_ids),
            random.choice(status_type_ids)
        ))

        if len(batch) >= batch_size:
            cursor.executemany("""
                               INSERT INTO customer (name, country, city, street, phone_number, email,
                                                             customer_type_id, status_id)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                               """, batch)
            batch = []

    # Insert remaining customers
    if batch:
        cursor.executemany("""
                           INSERT INTO customer (name, country, city, street, phone_number, email,
                                                         customer_type_id, status_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                           """, batch)

    cursor.execute("SELECT id FROM customer")
    customer_ids = [row[0] for row in cursor.fetchall()]
    return customer_ids


def insert_accounts(cursor, customer_ids, branch_ids, account_type_ids, account_status_ids, count=30):
    currencies = ['USD', 'EUR', 'GBP']
    for _ in range(count):
        balance = round(random.uniform(100.0, 10000.0), 2)
        cursor.execute("""
                       INSERT INTO account (customer_id, branch_id, type, status, currency, number, balance,
                                            min_balance)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                       """, (
                           random.choice(customer_ids),
                           random.choice(branch_ids),
                           random.choice(account_type_ids),
                           random.choice(account_status_ids),
                           random.choice(currencies),
                           fake.iban(),
                           balance,
                           round(balance * 0.1, 2)
                       ))


def insert_cards(cursor, account_ids, customer_ids, card_type_ids, count=25):
    for _ in range(count):
        account_id = random.choice(account_ids)
        cursor.execute("SELECT customer_id FROM account WHERE id = %s", (account_id,))
        customer_id = cursor.fetchone()[0]
        issuance_date = fake.date_this_decade()
        expiration_date = issuance_date + timedelta(days=1095)  # 3 years
        cursor.execute("""
                       INSERT INTO card (account_id, customer_id, card_type_id, monthly_fee, interest_rate,
                                         expiration_date, issuance_date, credit_limit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       """, (
                           account_id,
                           customer_id,
                           random.choice(card_type_ids),
                           round(random.uniform(0.0, 50.0), 2),
                           round(random.uniform(0.0, 20.0), 2),
                           expiration_date,
                           issuance_date,
                           round(random.uniform(1000.0, 10000.0), 2)
                       ))


def insert_loans(cursor, customer_ids, loan_type_ids, loan_status_ids, count=15):
    for _ in range(count):
        start_date = fake.date_this_decade()
        term = random.randint(12, 60)
        end_date = start_date + timedelta(days=term * 30)
        cursor.execute("""
                       INSERT INTO loan (customer_id, loan_type_id, amount, currency, interest_rate, term,
                                         start_date, end_date, loan_status_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                       """, (
                           random.choice(customer_ids),
                           random.choice(loan_type_ids),
                           round(random.uniform(1000.0, 100000.0), 2),
                           'USD',
                           round(random.uniform(2.0, 10.0), 2),
                           term,
                           start_date,
                           end_date,
                           random.choice(loan_status_ids)
                       ))


def insert_transactions(cursor, account_ids, employee_ids, transaction_type_ids, total_count=1000000, batch_size=10000):
    # Disable indexes for faster insertion
    cursor.execute("DROP INDEX IF EXISTS idx_transaction_account")
    cursor.execute("DROP INDEX IF EXISTS idx_transaction_date")
    cursor.execute("DROP INDEX IF EXISTS idx_transaction_type")

    batch = []
    for i in tqdm(range(total_count), desc="Inserting transactions"):
        batch.append((
            random.choice(account_ids),
            random.choice(employee_ids) if random.random() > 0.2 else None,
            random.choice(transaction_type_ids),
            'USD',
            round(random.uniform(50.0, 5000.0), 2),
            str(uuid.uuid4())
        ))

        if len(batch) >= batch_size:
            cursor.executemany("""
                               INSERT INTO transaction (account_id, employee_id, type, currency, amount, reference_number)
                               VALUES (%s, %s, %s, %s, %s, %s)
                               """, batch)
            batch = []

    # Insert remaining transactions
    if batch:
        cursor.executemany("""
                           INSERT INTO transaction (account_id, employee_id, type, currency, amount, reference_number)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           """, batch)
    # Re-create indexes
    cursor.execute("CREATE INDEX idx_transaction_account ON transaction(account_id)")
    cursor.execute("CREATE INDEX idx_transaction_date ON transaction(date)")
    cursor.execute("CREATE INDEX idx_transaction_type ON transaction(type)")


def insert_loan_payments(cursor, loan_ids, transaction_ids, count=30):
    for _ in range(count):
        loan_id = random.choice(loan_ids)
        transaction_id = random.choice(transaction_ids)
        payment_amount = round(random.uniform(100.0, 5000.0), 2)
        cursor.execute("""
                       INSERT INTO loanPayment (loan_id, transaction_id, payment_amount, scheduled_payment_date,
                                                payment_date, principal_amount, paid_amount)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       """, (
                           loan_id,
                           transaction_id,
                           payment_amount,
                           fake.date_this_year(),
                           fake.date_this_year(),
                           round(payment_amount * 0.7, 2),
                           round(payment_amount * 0.3, 2)
                       ))


def insert_transaction_logs(cursor, transaction_ids, account_ids, transaction_type_ids, count=50):
    statuses = ['Completed', 'Pending', 'Failed']
    for _ in range(count):
        cursor.execute("""
                       INSERT INTO transactionLog (transaction_id, account_id, amount, transaction_type, status,
                                                   description)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       """, (
                           random.choice(transaction_ids),
                           random.choice(account_ids),
                           round(random.uniform(50.0, 5000.0), 2),
                           random.choice(transaction_type_ids),
                           random.choice(statuses),
                           fake.sentence()
                       ))


def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Insert reference data
        insert_branches(cursor, count=10)
        cursor.execute("SELECT id FROM branch")
        branch_ids = [row[0] for row in cursor.fetchall()]

        insert_employees(cursor, branch_ids, count_per_branch=5)
        cursor.execute("SELECT id FROM employee")
        employee_ids = [row[0] for row in cursor.fetchall()]

        insert_transaction_types(cursor)
        cursor.execute("SELECT id FROM transactionType")
        transaction_type_ids = [row[0] for row in cursor.fetchall()]

        insert_account_types(cursor)
        cursor.execute("SELECT id FROM accountType")
        account_type_ids = [row[0] for row in cursor.fetchall()]

        insert_card_types(cursor)
        cursor.execute("SELECT id FROM cardType")
        card_type_ids = [row[0] for row in cursor.fetchall()]

        insert_customer_types(cursor)
        cursor.execute("SELECT id FROM customerType")
        customer_type_ids = [row[0] for row in cursor.fetchall()]

        insert_customer_status_types(cursor)
        cursor.execute("SELECT id FROM customerStatusType")
        customer_status_ids = [row[0] for row in cursor.fetchall()]

        insert_loan_types(cursor)
        cursor.execute("SELECT id FROM loanType")
        loan_type_ids = [row[0] for row in cursor.fetchall()]

        insert_loan_status_types(cursor)
        cursor.execute("SELECT id FROM loanStatusType")
        loan_status_ids = [row[0] for row in cursor.fetchall()]

        insert_account_status_types(cursor)
        cursor.execute("SELECT id FROM accountStatusType")
        account_status_ids = [row[0] for row in cursor.fetchall()]

        # Insert main data
        insert_customers(cursor, customer_type_ids, customer_status_ids, count=10000)
        cursor.execute("SELECT id FROM customer")
        customer_ids = [row[0] for row in cursor.fetchall()]

        insert_accounts(cursor, customer_ids, branch_ids, account_type_ids, account_status_ids, count=20000)
        cursor.execute("SELECT id FROM account")
        account_ids = [row[0] for row in cursor.fetchall()]

        insert_cards(cursor, account_ids, customer_ids, card_type_ids, count=15000)

        insert_loans(cursor, customer_ids, loan_type_ids, loan_status_ids, count=5000)
        cursor.execute("SELECT id FROM loan")
        loan_ids = [row[0] for row in cursor.fetchall()]

        insert_transactions(cursor, account_ids, employee_ids, transaction_type_ids, total_count=1000000)
        cursor.execute("SELECT id FROM transaction")
        transaction_ids = [row[0] for row in cursor.fetchall()]

        insert_loan_payments(cursor, loan_ids, transaction_ids, count=10000)
        insert_transaction_logs(cursor, transaction_ids, account_ids, transaction_type_ids, count=50000)

        conn.commit()
        print("Data generated successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        release_db_connection(conn)
        connection_pool.closeall()


if __name__ == "__main__":
    main()
