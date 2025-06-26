WITH last_transaction AS (
    SELECT account_id, balance_after,
        row_number() OVER(PARTITION BY account_id ORDER BY timestamp DESC) AS rn
    FROM fact_transactions
)
SELECT account_id, balance_after
FROM last_transaction
WHERE rn = 1;

