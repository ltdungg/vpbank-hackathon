
-- create schema banking;
-- Create Branch table
CREATE TABLE if not exists Branch (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    phone_number VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT branch_name_unique UNIQUE (name)
);

-- Create Employee table
CREATE TABLE if not exists Employee (
    id SERIAL PRIMARY KEY,
    branch_id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (branch_id) REFERENCES Branch(id)
    
);

-- Create TransactionType table
CREATE TABLE if not exists TransactionType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create AccountType table
CREATE TABLE if not exists AccountType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    allows_overdraft BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CardType table
CREATE TABLE if not exists CardType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    credit_limit NUMERIC(15,2),
    transaction_limit NUMERIC(15,2) DEFAULT 1000.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CustomerType table
CREATE TABLE if not exists CustomerType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CustomerStatusType table
CREATE TABLE if not exists CustomerStatusType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Customer table
CREATE TABLE if not exists Customer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(255),
    city VARCHAR(255),
    street TEXT,
    phone_number VARCHAR(255),
    registration_date DATE DEFAULT CURRENT_DATE,
    email VARCHAR(255) UNIQUE,
    customer_type_id INTEGER NOT NULL,
    status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_type_id) REFERENCES CustomerType(id),
    FOREIGN KEY (status_id) REFERENCES CustomerStatusType(id)
);

-- Create LoanType table
CREATE TABLE if not exists LoanType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    max_term INTEGER DEFAULT 60
);

-- Create LoanStatusType table
CREATE TABLE if not exists LoanStatusType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

-- Create AccountStatusType table
CREATE TABLE if not exists AccountStatusType (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT
);

-- Create Account table
CREATE TABLE if not exists Account (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    branch_id INTEGER NOT NULL,
    type INTEGER NOT NULL,
    status INTEGER NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    number VARCHAR(50) UNIQUE,
    balance NUMERIC(15,2) CHECK (balance >= 0),
    min_balance NUMERIC(15,2) DEFAULT 0.0,
    date_opened DATE DEFAULT CURRENT_DATE,
    date_closed DATE,
    FOREIGN KEY (customer_id) REFERENCES Customer(id),
    FOREIGN KEY (branch_id) REFERENCES Branch(id),
    FOREIGN KEY (type) REFERENCES AccountType(id),
    FOREIGN KEY (status) REFERENCES AccountStatusType(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Card table
CREATE TABLE if not exists Card (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    card_type_id INTEGER NOT NULL,
    monthly_fee NUMERIC(15,2) DEFAULT 0.0,
    interest_rate NUMERIC(5,2) DEFAULT 0.0,
    expiration_date DATE,
    issuance_date DATE DEFAULT CURRENT_DATE,
    credit_limit NUMERIC(15,2) CHECK (credit_limit >= 0),
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (account_id) REFERENCES Account(id),
    FOREIGN KEY (customer_id) REFERENCES Customer(id),
    FOREIGN KEY (card_type_id) REFERENCES CardType(id),
    CHECK (expiration_date > issuance_date),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Loan table
CREATE TABLE if not exists Loan (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    loan_type_id INTEGER NOT NULL,
    amount NUMERIC(15,2) CHECK (amount > 0),
    currency VARCHAR(3) DEFAULT 'USD',
    interest_rate NUMERIC(5,2),
    term INTEGER,
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE,
    loan_status_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customer(id),
    FOREIGN KEY (loan_type_id) REFERENCES LoanType(id),
    FOREIGN KEY (loan_status_id) REFERENCES LoanStatusType(id),
    CHECK (end_date >= start_date)
);

-- Create Transaction table
CREATE TABLE if not exists Transaction (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL,
    employee_id INTEGER,
    type INTEGER NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount NUMERIC(15,2) CHECK (amount > 0),
    reference_number VARCHAR(255) UNIQUE,
    FOREIGN KEY (account_id) REFERENCES Account(id),
    FOREIGN KEY (employee_id) REFERENCES Employee(id),
    FOREIGN KEY (type) REFERENCES TransactionType(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create LoanPayment table
CREATE TABLE if not exists LoanPayment (
    id SERIAL PRIMARY KEY,
    loan_id INTEGER NOT NULL,
    transaction_id INTEGER NOT NULL,
    payment_amount NUMERIC(15,2) CHECK (payment_amount > 0),
    scheduled_payment_date DATE,
    payment_date DATE DEFAULT CURRENT_DATE,
    principal_amount NUMERIC(15,2),
    paid_amount NUMERIC(15,2),
    paid_date DATE,
    FOREIGN KEY (loan_id) REFERENCES Loan(id),
    FOREIGN KEY (transaction_id) REFERENCES Transaction(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create TransactionLog table
CREATE TABLE if not exists TransactionLog (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL,
    account_id INTEGER NOT NULL,
    amount NUMERIC(15,2),
    transaction_type INTEGER,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(255),
    description TEXT,
    FOREIGN KEY (transaction_id) REFERENCES Transaction(id),
    FOREIGN KEY (account_id) REFERENCES Account(id),
    FOREIGN KEY (transaction_type) REFERENCES TransactionType(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for performance
CREATE INDEX idx_account_customer ON Account(customer_id);
CREATE INDEX idx_transaction_account ON Transaction(account_id);
CREATE INDEX idx_transaction_date ON Transaction(date);
CREATE INDEX idx_transaction_type ON Transaction(type);
CREATE INDEX idx_loan_customer ON Loan(customer_id);
CREATE INDEX idx_card_account ON Card(account_id);
CREATE INDEX idx_loan_payment_loan ON LoanPayment(loan_id);