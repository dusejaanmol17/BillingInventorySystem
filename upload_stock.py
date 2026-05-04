import psycopg2
import csv
import os

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

with open('stock.csv', 'r') as file:
    reader = csv.DictReader(file)

    for row in reader:

        name = row['name']
        price = float(row['price'])
        qty = int(row['quantity'])
        date = row['received_date']

        # ✅ Insert product
        cur.execute("""
            INSERT INTO products (name, price, stock_qty)
            VALUES (%s, %s, %s)
            RETURNING product_id
        """, (name, price, qty))

        product_id = cur.fetchone()[0]

        # ✅ Insert stock
        cur.execute("""
            INSERT INTO stock_inward (product_id, quantity, remaining_qty, received_date)
            VALUES (%s, %s, %s, %s)
        """, (product_id, qty, qty, date))

conn.commit()
cur.close()
conn.close()

print("🎉 65 products inserted successfully!")