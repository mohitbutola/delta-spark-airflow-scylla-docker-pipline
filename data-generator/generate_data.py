import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

# -------------------------------
# CONFIG
# -------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "data",
    "delta-lake",
    "customer_transactions",
    "raw_data.csv"
)

NUM_RECORDS = 1000
# OUTPUT_PATH = "../data/delta-lake/customer_transactions/raw_data.csv"

MERCHANTS = [
    "STORE_23",
    "ONLINE_SHOP",
    "GAS_STATION",
    "RESTAURANT",
    "APP_STORE",
    "SUPERMARKET"
]

# -------------------------------
# Helper: generate random timestamp
# -------------------------------

def random_timestamp():
    start_date = datetime.utcnow() - timedelta(days=10)
    random_seconds = random.randint(0, 10 * 24 * 60 * 60)
    return (start_date + timedelta(seconds=random_seconds)).isoformat() + "Z"

# -------------------------------
# Step 1: Generate valid records
# -------------------------------

def generate_valid_records(n):
    data = []

    for _ in range(n):
        record = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": f"C{random.randint(10000,99999)}",
            "amount": round(random.uniform(1, 500), 2),
            "timestamp": random_timestamp(),
            "merchant": random.choice(MERCHANTS)
        }
        data.append(record)

    return data

# -------------------------------
# Step 2: Introduce duplicates
# -------------------------------

def introduce_duplicates(data, percentage=0.1):
    num_duplicates = int(len(data) * percentage)
    duplicates = random.sample(data, num_duplicates)
    return data + duplicates

# -------------------------------
# Step 3: Introduce invalid amounts
# -------------------------------

def introduce_invalid_amounts(data, percentage=0.1):
    num_invalid = int(len(data) * percentage)
    invalid_rows = random.sample(range(len(data)), num_invalid)

    for idx in invalid_rows:
        data[idx]["amount"] = random.choice([0, -10.5, -50])

    return data

# -------------------------------
# MAIN
# -------------------------------

def main():

    print("Generating valid records...")
    data = generate_valid_records(NUM_RECORDS)

    print("Adding duplicates...")
    data = introduce_duplicates(data, percentage=0.1)

    print("Adding invalid amounts...")
    data = introduce_invalid_amounts(data, percentage=0.1)

    df = pd.DataFrame(data)

    # print(df.head())

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    print(f"Saving dataset to {OUTPUT_PATH}")
    df.to_csv(OUTPUT_PATH, index=False)

    print("Data generation complete!")
    print(f"Total records generated: {len(df)}")

if __name__ == "__main__":
    main()
