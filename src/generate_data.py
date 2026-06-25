import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

TOTAL_RECORDS = 1_000_000
OUTPUT_PATH = "/opt/spark/data/raw/transactions.csv"

CATEGORIES = [
    "Electronics", "electronics", "ELECTRONICS",
    "Clothing", "clothing", "CLOTHING",
    "Grocery", "grocery", "GROCERY",
    "Furniture", "furniture",
    "Sports", "sports", "SPORTS",
    "Books", "books",
    "Toys", "toys", "TOYS",
]

PRODUCTS = {
    "electronics": ["Laptop", "Smartphone", "Tablet", "Headphones", " Monitor", "Keyboard "],
    "clothing": ["T-Shirt", "Jeans", "Jacket", " Sneakers", "Dress", "Shorts "],
    "grocery": ["Rice", "Wheat Flour", "Cooking Oil", " Sugar", "Salt", "Lentils "],
    "furniture": ["Sofa", "Dining Table", "Chair", " Bookshelf", "Bed Frame"],
    "sports": ["Cricket Bat", "Football", " Tennis Racket", "Yoga Mat", "Dumbbells"],
    "books": ["Fiction Novel", "Data Science Book", "History Book", " Cook Book"],
    "toys": ["Action Figure", "Board Game", " Lego Set", "Puzzle", "Doll"],
}

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad",
    " Pune", "Kolkata", "Ahmedabad", "Jaipur ", "Lucknow",
    "Surat", "Bhopal", "Coimbatore", "Kochi", "Nagpur",
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash", None]

STORE_TYPES = ["Online", "Offline", "offline", "online", "ONLINE"]


def random_date(start_year=2022, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    rand_days = random.randint(0, delta.days)
    dt = start + timedelta(days=rand_days)

    fmt_choice = random.random()
    if fmt_choice < 0.85:
        return dt.strftime("%Y-%m-%d")
    elif fmt_choice < 0.92:
        return dt.strftime("%d/%m/%Y")
    else:
        return dt.strftime("%m-%d-%Y")


def generate_transaction_id(index):
    if random.random() < 0.005:
        return None
    return f"TXN{str(index).zfill(8)}"


def generate_customer_id():
    if random.random() < 0.03:
        return None
    return f"CUST{random.randint(1000, 99999)}"


def get_category():
    return random.choice(CATEGORIES)


def get_product(category):
    key = category.strip().lower()
    products = PRODUCTS.get(key, ["Unknown Product"])
    return random.choice(products)


def get_price(category):
    key = category.strip().lower()
    price_ranges = {
        "electronics": (500, 150000),
        "clothing": (200, 10000),
        "grocery": (20, 2000),
        "furniture": (3000, 100000),
        "sports": (300, 25000),
        "books": (100, 2000),
        "toys": (150, 8000),
    }
    low, high = price_ranges.get(key, (100, 50000))
    price = round(random.uniform(low, high), 2)

    issue = random.random()
    if issue < 0.01:
        return -abs(price)
    elif issue < 0.02:
        return 0.0
    elif issue < 0.025:
        return None
    return price


def get_quantity():
    qty = random.randint(1, 20)
    issue = random.random()
    if issue < 0.01:
        return -qty
    elif issue < 0.02:
        return 0
    elif issue < 0.025:
        return None
    return qty


def get_discount():
    discount = round(random.uniform(0, 0.5), 2)
    issue = random.random()
    if issue < 0.01:
        return round(random.uniform(1.1, 2.0), 2)
    elif issue < 0.015:
        return round(random.uniform(-0.5, -0.01), 2)
    return discount


def generate_batch(batch_size, start_index):
    records = []
    for i in range(batch_size):
        idx = start_index + i
        category = get_category()
        price = get_price(category)
        quantity = get_quantity()
        discount = get_discount()

        record = {
            "transaction_id": generate_transaction_id(idx),
            "customer_id": generate_customer_id(),
            "transaction_date": random_date(),
            "category": category,
            "product_name": get_product(category),
            "price": price,
            "quantity": quantity,
            "discount": discount,
            "city": random.choice(CITIES),
            "store_type": random.choice(STORE_TYPES),
            "payment_method": random.choice(PAYMENT_METHODS),
        }
        records.append(record)
    return records


def inject_duplicates(df, duplicate_fraction=0.02):
    n_duplicates = int(len(df) * duplicate_fraction)
    duplicate_rows = df.sample(n=n_duplicates, random_state=SEED)
    df_with_dupes = pd.concat([df, duplicate_rows], ignore_index=True)
    df_with_dupes = df_with_dupes.sample(frac=1, random_state=SEED).reset_index(drop=True)
    return df_with_dupes


def main():
    print("Starting data generation...")
    print(f"Target records: {TOTAL_RECORDS:,}")

    BATCH_SIZE = 100_000
    all_records = []
    batches = TOTAL_RECORDS // BATCH_SIZE

    for batch_num in range(batches):
        start_idx = batch_num * BATCH_SIZE
        print(f"Generating batch {batch_num + 1}/{batches} | Records {start_idx:,} to {start_idx + BATCH_SIZE:,}")
        batch = generate_batch(BATCH_SIZE, start_idx)
        all_records.extend(batch)

    print("Creating DataFrame...")
    df = pd.DataFrame(all_records)

    print("Injecting duplicate records...")
    df = inject_duplicates(df, duplicate_fraction=0.02)

    print(f"Total records after duplicates: {len(df):,}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    print(f"Writing to CSV: {OUTPUT_PATH}")
    df.to_csv(OUTPUT_PATH, index=False)

    print("")
    print("Data generation complete.")
    print(f"Output file : {OUTPUT_PATH}")
    print(f"Total rows  : {len(df):,}")
    print(f"Columns     : {list(df.columns)}")
    print("")
    print("Sample data quality issues:")
    print(f"  Null transaction_id  : {df['transaction_id'].isnull().sum():,}")
    print(f"  Null customer_id     : {df['customer_id'].isnull().sum():,}")
    print(f"  Null price           : {df['price'].isnull().sum():,}")
    print(f"  Null quantity        : {df['quantity'].isnull().sum():,}")
    print(f"  Null payment_method  : {df['payment_method'].isnull().sum():,}")
    print(f"  Negative price       : {(df['price'] < 0).sum():,}")
    print(f"  Zero price           : {(df['price'] == 0).sum():,}")
    print(f"  Negative quantity    : {(df['quantity'] < 0).sum():,}")
    print(f"  Zero quantity        : {(df['quantity'] == 0).sum():,}")
    print(f"  Invalid discount     : {((df['discount'] > 1) | (df['discount'] < 0)).sum():,}")


if __name__ == "__main__":
    main()