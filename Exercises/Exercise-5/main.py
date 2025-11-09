import psycopg2
import csv
import os

DB_PARAMS = {
    "host": "postgres",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def connect_db():
    """Establish PostgreSQL connection."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    print("Connected to PostgreSQL.")
    return conn


def create_tables(conn):
    """Execute SQL DDL to create tables."""
    ddl = """
   CREATE TABLE accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address_1 VARCHAR(100),
    address_2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    join_date DATE
);

-- Products Table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(10),
    product_description VARCHAR(100)
);

-- Transactions Table
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE,
    product_id INT REFERENCES products(product_id),
    product_code VARCHAR(10),
    product_description VARCHAR(100),
    quantity INT,
    account_id INT REFERENCES accounts(customer_id)
);

-- Optional Indexes for performance
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_product_id ON transactions(product_id);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        print("Tables created successfully.")


def load_csv_to_table(conn, csv_file, table_name):
    """Load data from a CSV file into a PostgreSQL table."""
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        columns = next(reader)
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        from psycopg2.extras import execute_values
        data = [tuple(row) for row in reader]

        with conn.cursor() as cur:
            execute_values(cur, query, data)
            conn.commit()
            print(f"Inserted data from {os.path.basename(csv_file)} into {table_name}")


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    conn = connect_db()
    create_tables(conn)

    csv_table_map = {
        "accounts.csv": "accounts",
        "products.csv": "products",
        "transactions.csv": "transactions"
    }

    for file_name, table_name in csv_table_map.items():
        file_path = os.path.join(data_dir, file_name)
        if os.path.exists(file_path):
            load_csv_to_table(conn, file_path, table_name)
        else:
            print(f" File not found: {file_path}")

    conn.close()
    print("\n All CSV files successfully loaded into PostgreSQL!")


if __name__ == "__main__":
    main()
