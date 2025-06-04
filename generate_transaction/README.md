# Banking Data Generator

A Python script for generating synthetic banking data for testing and development purposes. This tool creates realistic banking data including branches, employees, customers, accounts, transactions, and more.

## Prerequisites

- Python 3.6+
- PostgreSQL 16+
- Docker and Docker Compose (for running the database)

## Required Python Packages

```bash
pip install psycopg2-binary faker tqdm
```

## Database Setup

1. Start the PostgreSQL database using Docker Compose:

```bash
docker-compose up -d
```

This will start a PostgreSQL instance with the following configuration:
- Database: vpbank
- User: vpbank
- Password: vpbank
- Port: 5432

## Database Schema

The database schema includes the following tables:
- Branch
- Employee
- TransactionType
- AccountType
- CardType
- CustomerType
- CustomerStatusType
- Customer
- LoanType
- LoanStatusType
- AccountStatusType
- Account
- Card
- Loan
- Transaction
- LoanPayment
- TransactionLog

## Usage

1. First, ensure your database is running and the schema is created:

```bash
# Create the database schema
psql -U hackathon -d banking -f banking_transaction.sql
```

2. Run the data generation script:

```bash
python generate_fake_data.py
```

### Default Data Generation

The script will generate the following data by default:
- 10 branches
- 50 employees (5 per branch)
- 10,000 customers
- 20,000 accounts
- 15,000 cards
- 5,000 loans
- 1,000,000 transactions
- 10,000 loan payments
- 50,000 transaction logs

### Customizing Data Generation

You can modify the data generation parameters in the `main()` function of `generate_fake_data.py`:

```python
# Example of modifying the number of records
insert_branches(cursor, count=20)  # Generate 20 branches
insert_customers(cursor, customer_type_ids, customer_status_ids, count=50000)  # Generate 50,000 customers
```

## Database Backup and Restore

### Backup Database
To create a backup of your database:

```bash
pg_dump -U hackathon -h localhost -p 5432 banking > dump.sql
```

### Restore Database
To restore the database from a backup:

```bash
psql -U hackathon -h localhost -p 5432 -d banking -f dump.sql
```

## Data Generation Features

1. **Unique Constraints**:
   - Branch names are unique
   - Customer emails are unique
   - Account numbers are unique
   - Transaction reference numbers are unique

2. **Realistic Data**:
   - Realistic names, addresses, and contact information
   - Proper date ranges for transactions and loans
   - Realistic monetary amounts
   - Proper relationships between entities

3. **Performance Optimizations**:
   - Batch processing for large data sets
   - Connection pooling
   - Index management for faster inserts

## Error Handling

The script includes error handling for:
- Database connection issues
- Duplicate key violations
- Data integrity constraints
- Transaction rollback on errors

## Cleanup

To stop and remove the database:

```bash
docker-compose down -v
```

## Notes

- The generated data is synthetic and should not be used in production
- All monetary values are in USD by default
- Dates are generated within the last decade
- The script uses connection pooling for better performance
- Indexes are temporarily disabled during bulk inserts for better performance

## Troubleshooting

1. **Connection Issues**:
   - Ensure PostgreSQL is running
   - Check database credentials in DB_PARAMS
   - Verify port 5432 is available

2. **Duplicate Key Errors**:
   - The script handles duplicate prevention automatically
   - If you encounter duplicate errors, try reducing the batch size

3. **Memory Issues**:
   - For large data sets, consider reducing the batch size
   - Monitor system memory usage during generation

## Contributing

Feel free to submit issues and enhancement requests! 