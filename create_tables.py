import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(15),
    address TEXT,
    credit_limit NUMERIC(10,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    stock_qty INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id INTEGER,
    invoice_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount NUMERIC(10,2) NOT NULL,
    previous_balance NUMERIC(10,2) DEFAULT 0,
    new_balance NUMERIC(10,2) DEFAULT 0,
    payment_status VARCHAR(20) DEFAULT 'PENDING',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS invoice_items (
    item_id SERIAL PRIMARY KEY,
    invoice_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    rate NUMERIC(10,2) NOT NULL,
    line_total NUMERIC(10,2) NOT NULL,
    FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount NUMERIC(10,2) NOT NULL,
    payment_mode VARCHAR(20) NOT NULL,
    reference_no VARCHAR(100),
    remarks TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS returns (
    return_id SERIAL PRIMARY KEY,
    return_number VARCHAR(50) NOT NULL UNIQUE,
    customer_id INTEGER,
    return_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_return_amount NUMERIC(10,2) NOT NULL,
    note TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS return_items (
    item_id SERIAL PRIMARY KEY,
    return_id INTEGER,
    product_id INTEGER,
    quantity INTEGER NOT NULL,
    rate NUMERIC(10,2) NOT NULL,
    line_total NUMERIC(10,2) NOT NULL,
    FOREIGN KEY (return_id) REFERENCES returns(return_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS stock_inward (
    inward_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    quantity INTEGER,
    remaining_qty INTEGER,
    received_date DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
""")

conn.commit()
cur.close()
conn.close()

print("✅ Tables created successfully!")