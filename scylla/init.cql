CREATE KEYSPACE IF NOT EXISTS ads_db
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ads_db;

CREATE TABLE IF NOT EXISTS daily_customer_totals (
  customer_id TEXT,
  transaction_date DATE,
  daily_total FLOAT,
  PRIMARY KEY (customer_id, transaction_date)
);