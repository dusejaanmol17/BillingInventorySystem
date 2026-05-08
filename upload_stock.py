import psycopg2
import csv
import os

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

with open('customers.csv', 'r', encoding='utf-8') as file:

    reader = csv.DictReader(file)

    for row in reader:

        name = row['name'].strip()
        phone = row['phone'].strip()
        address = row['address'].strip()

        # SAFE CREDIT LIMIT
        try:
            credit_limit = float(row['credit_limit']) if row['credit_limit'] else 0
        except:
            credit_limit = 0

        # SAFE OPENING BALANCE
        try:
            opening_balance = float(row['opening_balance']) if row['opening_balance'] else 0
        except:
            opening_balance = 0

        # OPTIONAL DATE
        opening_balance_date = row['opening_balance_date'].strip()

        if opening_balance_date == "":
            opening_balance_date = None

        # DUPLICATE CHECK
        cur.execute("""
            SELECT customer_id
            FROM customers
            WHERE LOWER(name) = LOWER(%s)
               OR phone = %s
        """, (name, phone))

        existing = cur.fetchone()

        if existing:
            print(f"⚠️ Skipped duplicate: {name}")
            continue

        # INSERT CUSTOMER
        cur.execute("""
            INSERT INTO customers (
                name,
                phone,
                address,
                credit_limit,
                opening_balance,
                opening_balance_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            name,
            phone,
            address,
            credit_limit,
            opening_balance,
            opening_balance_date
        ))

        print(f"✅ Inserted: {name}")

conn.commit()
cur.close()
conn.close()

print("🎉 Customers uploaded successfully!")