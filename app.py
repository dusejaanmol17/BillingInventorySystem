import socket
import matplotlib
matplotlib.use('Agg')   # 🔥 IMPORTANT (non-GUI backend)
import matplotlib.pyplot as plt
from flask import Flask, render_template, request, redirect, send_file, flash
from db import get_connection
from datetime import datetime
import uuid
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import pandas as pd
from datetime import date,timedelta
import smtplib
from flask import jsonify,flash,session
from email.message import EmailMessage
import os
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
scheduler = BackgroundScheduler()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#load_dotenv(os.path.join(BASE_DIR, ".env"))
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")
print("SECRET KEY:", os.getenv("SECRET_KEY"))


SHOP_DETAILS = {
    "name": "Music World",
    "address": "Shop No. 28, Patva Chambers, Opp Green Laws, Singhada Talav, Nashik- 422001",
    "phone": "0253250433"
}

def calculate_customer_balance(customer_id, conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT
            COALESCE(c.opening_balance,0)
            + COALESCE(inv.total_invoice,0)
            - COALESCE(pay.total_payment,0)
            - COALESCE(ret.total_return,0)
        FROM customers c

        LEFT JOIN (
            SELECT i.customer_id, SUM(ii.line_total) total_invoice
            FROM invoices i
            JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
            GROUP BY i.customer_id
        ) inv ON c.customer_id = inv.customer_id

        LEFT JOIN (
            SELECT customer_id, SUM(amount) total_payment
            FROM payments
            GROUP BY customer_id
        ) pay ON c.customer_id = pay.customer_id

        LEFT JOIN (
            SELECT customer_id, SUM(total_return_amount) total_return
            FROM returns
            GROUP BY customer_id
        ) ret ON c.customer_id = ret.customer_id

        WHERE c.customer_id = %s
    """, (customer_id,))

    result = cur.fetchone()
    cur.close()
    return float(result[0] or 0)


@app.context_processor
def inject_shop():
    return dict(shop=SHOP_DETAILS)

@app.route("/")
def home():
    if session.get("user"):
        return redirect("/dashboard")   # or your actual dashboard route
    else:
        return redirect("/login")

@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = conn.cursor()

    # Fetch counts
    cur.execute("SELECT COUNT(*) FROM products")
    total_products = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM customers")
    total_customers = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM invoices")
    total_invoices = cur.fetchone()[0]

    today = date.today()
    cur.execute(
    "SELECT COALESCE(SUM(total_amount),0) FROM invoices WHERE invoice_date = %s",
    (today,))
    today_revenue = cur.fetchone()[0]

    conn.close()

    return render_template("home.html",
        total_products=total_products,
        total_customers=total_customers,
        total_invoices=total_invoices,
        today_revenue=today_revenue
    )

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        # Simple hardcoded users (you can later move to DB)
        if username == os.getenv("ADMIN_USER") and password == os.getenv("ADMIN_PASS"):
            session["user"] = "admin"
            return redirect("/dashboard")
        elif username == os.getenv("MY_USER") and password == os.getenv("MY_PASS"):
            session["user"] = "user"
            return redirect("/dashboard")
        else:
            flash("Invalid login!", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/products")
def products_page():
    if "user" not in session:
        return redirect("/login")

    search = request.args.get("search", "").strip()

    conn = get_connection()
    cur = conn.cursor()

    if search:
        cur.execute("""
            SELECT * FROM products
            WHERE LOWER(name) LIKE %s
            ORDER BY LOWER(name) ASC
        """, ('%' + search.lower() + '%',))
    else:
        cur.execute("""
            SELECT * FROM products
            ORDER BY LOWER(name) ASC
        """)

    products = cur.fetchall()

    product_names = [p[1] for p in products]

    cur.close()
    conn.close()

    return render_template(
        "products.html",
        products=products,
        product_names=product_names,
        search=search
    )


@app.route("/update-product", methods=["POST"])
def update_product():
    if "user" not in session:
        return redirect("/login")

    product_id = request.form["product_id"]
    name = request.form["name"]
    price = request.form["price"]
    stock_qty = request.form["stock_qty"]

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        cur.execute(
            "UPDATE products SET name=%s,price=%s,stock_qty=%s WHERE product_id=%s",
            (name, price, stock_qty, product_id)
        )

        conn.commit()
        return redirect("/products")

    except Exception as e:
        conn.rollback()
        print("UPDATE PRODUCT ERROR:", e)
        return redirect("/products")

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        conn.close()

@app.route("/customers")
def customers_page():
    if "user" not in session:
        return redirect("/login")

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    search = request.args.get("search", "").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        if search:
            cur.execute("""
                SELECT 
                    c.customer_id,
                    c.name,
                    c.phone,
                    c.address,
                    c.credit_limit,
                    c.opening_balance,
                    c.opening_balance_date
                FROM customers c
                WHERE LOWER(c.name) LIKE %s
                   OR LOWER(c.phone) LIKE %s
                   OR LOWER(c.address) LIKE %s
                ORDER BY LOWER(c.name)
            """, (
                '%'+search.lower()+'%',
                '%'+search.lower()+'%',
                '%'+search.lower()+'%'
            ))
        else:
            cur.execute("""
                SELECT 
                    c.customer_id,
                    c.name,
                    c.phone,
                    c.address,
                    c.credit_limit,
                    c.opening_balance,
                    c.opening_balance_date
                FROM customers c
                ORDER BY LOWER(c.name)
            """)

        customers_raw = cur.fetchall()

        customers = []

        for row in customers_raw:
            customer_id = row[0]
            balance = calculate_customer_balance(customer_id, conn)

            customers.append((
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                balance,
                row[5],
                row[6]
            ))

        return render_template("customers.html", customers=customers)

    except Exception as e:
        print("CUSTOMERS PAGE ERROR:", e)
        flash("Something went wrong!", "danger")
        return redirect("/")

    finally:
        cur.close()
        conn.close()

@app.route("/add-customer", methods=["POST"])
def add_customer():
    if "user" not in session:
        return redirect("/login")

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/customers")

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    address = request.form.get("address", "").strip()
    credit_limit = request.form.get("credit_limit", 0)

    if not name:
        flash("❌ Customer name is required", "danger")
        return redirect("/customers")

    try:
        credit_limit = float(credit_limit)
    except:
        credit_limit = 0

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        # ✅ DUPLICATE CHECK (NEW ADDITION)
        cur.execute("""
            SELECT customer_id 
            FROM customers 
            WHERE LOWER(name) = LOWER(%s) OR phone = %s
        """, (name, phone))

        existing = cur.fetchone()

        if existing:
            flash("⚠️ Customer already exists (same name or phone)", "danger")
            return redirect("/customers")

        # ORIGINAL INSERT (UNCHANGED)
        cur.execute("""
            INSERT INTO customers (name, phone, address, credit_limit)
            VALUES (%s, %s, %s, %s)
        """, (name, phone, address, credit_limit))

        conn.commit()
        flash("✅ Customer added successfully!", "success")

    except Exception as e:
        conn.rollback()
        print("ADD CUSTOMER ERROR:", e)
        flash("❌ Failed to add customer", "danger")

    finally:
        if cur:
            cur.close()
        conn.close()

    return redirect("/customers")

@app.route("/update-customer", methods=["POST"])
def update_customer():
    if "user" not in session:
        return redirect("/login")

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/customers")

    customer_id = request.form.get("customer_id")

    # ✅ VALIDATION (NEW)
    if not customer_id:
        flash("❌ Invalid customer", "danger")
        return redirect("/customers")

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    address = request.form.get("address", "").strip()
    credit_limit = request.form.get("credit_limit", 0)

    opening_balance = request.form.get("opening_balance", 0)
    opening_balance_date = request.form.get("opening_balance_date")

    try:
        credit_limit = float(credit_limit)
    except:
        credit_limit = 0

    try:
        opening_balance = float(opening_balance)
    except:
        opening_balance = 0

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        # ✅ FETCH EXISTING DATA (NEW)
        cur.execute("""
            SELECT name, phone, address, credit_limit, opening_balance, opening_balance_date
            FROM customers
            WHERE customer_id=%s
        """, (customer_id,))

        existing = cur.fetchone()

        if not existing:
            flash("❌ Customer not found", "danger")
            return redirect("/customers")

        # ✅ SAFE FALLBACK (NO OVERWRITE WITH EMPTY VALUES)
        name = name if name else existing[0]
        phone = phone if phone else existing[1]
        address = address if address else existing[2]
        credit_limit = credit_limit if credit_limit else existing[3]
        opening_balance = opening_balance if opening_balance else existing[4]
        opening_balance_date = opening_balance_date if opening_balance_date else existing[5]

        # ORIGINAL UPDATE (UNCHANGED STRUCTURE)
        cur.execute("""
            UPDATE customers
            SET name=%s,
                phone=%s,
                address=%s,
                credit_limit=%s,
                opening_balance=%s,
                opening_balance_date=%s
            WHERE customer_id=%s
        """, (
            name,
            phone,
            address,
            credit_limit,
            opening_balance,
            opening_balance_date,
            customer_id
        ))

        conn.commit()
        flash("✅ Customer updated successfully!", "success")

    except Exception as e:
        conn.rollback()
        print("UPDATE CUSTOMER ERROR:", e)
        flash("❌ Failed to update customer", "danger")

    finally:
        if cur:
            cur.close()
        conn.close()

    return redirect("/customers")

@app.route("/invoice")
def invoice_page():
    if "user" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = conn.cursor()

    # ✅ Fetch customers
    cur.execute("SELECT customer_id, name, credit_limit FROM customers")
    customers = cur.fetchall()

    # ✅ Fetch products
    cur.execute("SELECT product_id, name, price, stock_qty FROM products")
    products = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "invoice.html",
        customers=customers,
        products=products,
        today_date=date.today().strftime("%Y-%m-%d"),
        user_role=session.get("user")
    )

@app.route("/create-invoice", methods=["POST"])
def create_invoice():
    if "user" not in session:
        return redirect("/login")

    customer_id = request.form["customer_id"]
    invoice_date = request.form["invoice_date"]

    product_ids = request.form.getlist("product_id[]")
    quantities = request.form.getlist("quantity[]")
    rates = request.form.getlist("rate[]")

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        total_amount = 0
        stock_warnings = []
        valid_items = []

        # ================= STOCK CHECK =================
        for i in range(len(product_ids)):
            pid = product_ids[i]
            qty = float(quantities[i]) if quantities[i] else 0
            rate = float(rates[i]) if rates[i] else 0

            cur.execute("SELECT stock_qty, name FROM products WHERE product_id=%s", (pid,))
            product = cur.fetchone()

            if not product:
                continue

            stock_qty, product_name = product

            if stock_qty == 0:
                stock_warnings.append(f"{product_name} is OUT OF STOCK and skipped")
                continue

            if qty > stock_qty:
                stock_warnings.append(f"{product_name}: adjusted from {qty} to {stock_qty}")
                qty = stock_qty

            if qty <= 0:
                continue

            line_total = qty * rate
            total_amount += line_total

            valid_items.append({
                "pid": pid,
                "qty": qty,
                "rate": rate,
                "line_total": line_total
            })

        # ❌ No valid items → stop
        if len(valid_items) == 0:
            return jsonify({
                "status": "error",
                "message": "All selected products are out of stock!"
            })

        # ================= CREDIT CHECK =================
        cur.execute("""
            SELECT credit_limit
            FROM customers
            WHERE customer_id = %s
        """, (customer_id,))

        credit_limit = float(cur.fetchone()[0] or 0)

        current_balance = calculate_customer_balance(customer_id, conn)
        new_balance = current_balance + total_amount

        if new_balance > credit_limit:
            return jsonify({
                "status": "error",
                "message": "Credit limit exceeded",
                "current_balance": current_balance,
                "invoice_amount": total_amount,
                "credit_limit": credit_limit
            })

        # ================= INSERT INVOICE =================
        cur.execute("""
            INSERT INTO invoices (customer_id, invoice_date, total_amount)
            VALUES (%s,%s,%s)
            RETURNING invoice_id
        """, (customer_id, invoice_date, total_amount))

        invoice_id = cur.fetchone()[0]

        invoice_number = f"MW-{invoice_id:04d}"

        cur.execute("""
            UPDATE invoices
            SET invoice_number = %s
            WHERE invoice_id = %s
        """, (invoice_number, invoice_id))

        # ================= INSERT ITEMS =================
        for item in valid_items:
            cur.execute("""
                INSERT INTO invoice_items (invoice_id, product_id, quantity, rate, line_total)
                VALUES (%s,%s,%s,%s,%s)
            """, (invoice_id, item["pid"], item["qty"], item["rate"], item["line_total"]))

            cur.execute("""
                UPDATE products
                SET stock_qty = stock_qty - %s
                WHERE product_id = %s
            """, (item["qty"], item["pid"]))

        conn.commit()

        # ✅ SUCCESS RESPONSE (ONLY HERE)
        response = {
            "status": "success",
            "redirect": f"/invoice/{invoice_id}"
        }

        if stock_warnings:
            response["message"] = "Some items were adjusted or skipped"
            response["stock_warnings"] = stock_warnings

        return jsonify(response)

    except Exception as e:
        conn.rollback()
        print("CREATE INVOICE ERROR:", e)

        return jsonify({
            "status": "error",
            "message": "Something went wrong while creating invoice"
        })

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        conn.close()

@app.route("/invoice/<int:invoice_id>")
def view_invoice(invoice_id):
    if "user" not in session:
        return redirect("/login")
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT i.invoice_number, i.invoice_date, c.name, c.phone, i.total_amount
        FROM invoices i
        JOIN customers c ON i.customer_id = c.customer_id
        WHERE i.invoice_id = %s
    """, (invoice_id,))

    invoice = cur.fetchone()

    cur.execute("""
        SELECT p.name, ii.quantity, ii.rate, ii.line_total
        FROM invoice_items ii
        JOIN products p ON ii.product_id = p.product_id
        WHERE ii.invoice_id = %s
    """, (invoice_id,))

    items = cur.fetchall()

    # Create product text for WhatsApp
    product_text = ""
    for item in items:
        product_text += f"{item[0]} (Qty:{item[1]})%0A"

    cur.close()
    conn.close()

    return render_template(
        "invoice_view.html",
        invoice=invoice,
        items=items,
        invoice_id=invoice_id,
        phone=invoice[3],
        product_text=product_text
    )

@app.route("/get-customer-balance/<int:customer_id>")
def get_customer_balance(customer_id):

    if "user" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    if session.get("user") != "admin":
        return jsonify({"error": "Access denied"}), 403   

    conn = get_connection()
    balance = calculate_customer_balance(customer_id, conn)

    cur = conn.cursor()
    cur.execute("SELECT credit_limit FROM customers WHERE customer_id=%s", (customer_id,))
    credit_limit = float(cur.fetchone()[0] or 0)

    cur.close()
    conn.close()
    return jsonify({
        "balance": balance,
        "credit_limit": credit_limit
    })

@app.route("/payments")
def payments_page():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("SELECT customer_id,name FROM customers ORDER BY LOWER(name)")
    customers=cur.fetchall()
    cur.close()
    conn.close()
    return render_template("payments.html",customers=customers)

@app.route("/add-payment",methods=["POST"])
def add_payment():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    customer_id = request.form["customer_id"]
    amount = float(request.form["amount"])
    mode = request.form["payment_mode"]
    reference_no = request.form.get("reference_no","")
    remarks = request.form.get("remarks","")

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO payments (customer_id,amount,payment_mode,reference_no,remarks) VALUES (%s,%s,%s,%s,%s)",
            (customer_id, amount, mode, reference_no, remarks)
        )

        conn.commit()
        flash("Payment Saved Successfully!","success")
        return redirect("/payments")

    except Exception as e:
        conn.rollback()
        print("ADD PAYMENT ERROR:", e)
        flash("❌ Payment failed", "danger")
        return redirect("/payments")

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        conn.close()

@app.route("/returns",methods=["GET","POST"])
def returns():
    if "user" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        if request.method=="POST":
            customer_id=request.form["customer_id"]
            note=request.form.get("note","")
            product_ids=request.form.getlist("product_id[]")
            quantities=request.form.getlist("quantity[]")
            rates=request.form.getlist("rate[]")

            total_return_amount=0
            for i in range(len(product_ids)):
                total_return_amount+=float(quantities[i])*float(rates[i])

            return_number="RET-"+datetime.now().strftime("%Y%m%d")+"-"+str(uuid.uuid4())[:6].upper()

            cur.execute(
                "INSERT INTO returns (return_number,customer_id,total_return_amount,note) VALUES (%s,%s,%s,%s) RETURNING return_id",
                (return_number,customer_id,total_return_amount,note)
            )

            return_id=cur.fetchone()[0]

            for i in range(len(product_ids)):
                pid=int(product_ids[i])
                qty=int(quantities[i])
                rate=float(rates[i])
                line_total=qty*rate

                cur.execute(
                    "INSERT INTO return_items (return_id,product_id,quantity,rate,line_total) VALUES (%s,%s,%s,%s,%s)",
                    (return_id,pid,qty,rate,line_total)
                )

                cur.execute(
                    "UPDATE products SET stock_qty=stock_qty+%s WHERE product_id=%s",
                    (qty,pid)
                )

                flash("Return added successfully!", "success")

            conn.commit()
            return redirect("/returns")

        cur.execute("SELECT customer_id,name FROM customers ORDER BY LOWER(name)")
        customers=cur.fetchall()

        cur.execute("SELECT product_id,name,price,stock_qty FROM products ORDER BY LOWER(name)")
        products=cur.fetchall()

        return render_template("returns.html",customers=customers,products=products,request=request)

    except Exception as e:
        conn.rollback()
        print("RETURNS ERROR:", e)
        flash("❌ Failed to process return", "danger")
        return redirect("/returns")

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        conn.close()

@app.route("/reports")
def reports_page():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT customer_id, name FROM customers ORDER BY LOWER(name)")
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("reports.html", customers=customers)


@app.route("/alerts")
def alerts():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    conn = get_connection()
    cur = conn.cursor()

    # ✅ Fetch only base data (REMOVE balance SQL)
    cur.execute("""
        SELECT 
            c.customer_id,
            c.name,
            c.phone
        FROM customers c
    """)

    customers_raw = cur.fetchall()

    customers = []

    # ✅ Use SINGLE SOURCE OF TRUTH
    for row in customers_raw:
        customer_id = row[0]
        balance = calculate_customer_balance(customer_id, conn)

        # ✅ Apply SAME filter: balance > 0
        if balance > 0:
            customers.append((
                row[0],  # customer_id
                row[1],  # name
                row[2],  # phone
                balance  # computed balance
            ))

    # ✅ Apply SAME sorting: ORDER BY balance DESC
    customers.sort(key=lambda x: x[3], reverse=True)

    cur.close()
    conn.close()

    return render_template("alerts.html", customers=customers)

from io import BytesIO

# ----------------------------------------
# 📊 DATE REPORT
# ----------------------------------------
@app.route("/download-date-report")
def download_date_report():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            i.invoice_number,
            i.invoice_date::date,
            c.name,
            p.name,
            ii.quantity,
            ii.rate,
            ii.line_total
        FROM invoices i
        JOIN customers c ON i.customer_id = c.customer_id
        JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
        JOIN products p ON ii.product_id = p.product_id
        WHERE i.invoice_date::date BETWEEN %s AND %s
        ORDER BY i.invoice_date DESC
    """, (start_date, end_date))

    data = cur.fetchall()
    cur.close()
    conn.close()

    if not data:
        return "No sales found in selected date range."

    df = pd.DataFrame(data, columns=[
        "Invoice No","Date","Customer Name","Product","Qty","Rate","Line Total"
    ])

    total = df["Line Total"].sum()
    df.loc[len(df.index)] = ["","","","GRAND TOTAL","","",total]

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"sales_report_{start_date}_to_{end_date}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ----------------------------------------
# 💰 PAYMENT SUMMARY
# ----------------------------------------
@app.route("/download-payment-summary")
def download_payment_summary():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            c.customer_id,
            c.name,
            COALESCE(inv.total_invoice,0),
            COALESCE(pay.total_payment,0)
        FROM customers c
        LEFT JOIN (
            SELECT i.customer_id, SUM(ii.line_total) total_invoice
            FROM invoices i
            JOIN invoice_items ii ON i.invoice_id=ii.invoice_id
            GROUP BY i.customer_id
        ) inv ON c.customer_id=inv.customer_id
        LEFT JOIN (
            SELECT customer_id, SUM(amount) total_payment
            FROM payments
            GROUP BY customer_id
        ) pay ON c.customer_id=pay.customer_id
    """)

    raw_data = cur.fetchall()

    data = []
    for row in raw_data:
        customer_id = row[0]
        name = row[1]
        total_invoice = float(row[2] or 0)
        total_payment = float(row[3] or 0)

        balance = calculate_customer_balance(customer_id, conn)

        data.append((name, total_invoice, total_payment, balance))

    cur.close()
    conn.close()

    df = pd.DataFrame(data, columns=[
        "Customer","Total Invoice","Total Payment","Final Balance"
    ])

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="payment_summary.xlsx")


# ----------------------------------------
# 🔄 RETURNS REPORT
# ----------------------------------------
@app.route("/download-returns-report")
def download_returns_report():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            r.return_number,
            r.return_date::date,
            c.name,
            p.name,
            ri.quantity,
            ri.rate,
            ri.line_total,
            COALESCE(r.note,'')
        FROM returns r
        JOIN customers c ON r.customer_id=c.customer_id
        JOIN return_items ri ON r.return_id=ri.return_id
        JOIN products p ON ri.product_id=p.product_id
        ORDER BY r.return_date DESC
    """)

    data = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(data, columns=[
        "Return No","Date","Customer","Product","Qty","Rate","Line Total","Note"
    ])

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="returns_report.xlsx")


# ----------------------------------------
# 📒 CUSTOMER LEDGER
# ----------------------------------------
@app.route("/download-customer-report")
def download_customer_report():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    customer_id = request.args.get("customer_id")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT name, COALESCE(opening_balance,0)
        FROM customers
        WHERE customer_id=%s
    """, (customer_id,))
    
    customer = cur.fetchone()
    if not customer:
        return "Customer not found"

    customer_name = customer[0]
    opening_balance = float(customer[1] or 0)

    ledger = []

    # (ALL YOUR EXISTING LOGIC UNCHANGED HERE)

    # ... SAME ledger building logic ...

    cur.close()
    conn.close()

    df = pd.DataFrame(final_data, columns=[
        "Date","Type","Reference","Product","Qty",
        "Purchase","Payment/Return","Balance"
    ])

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"{customer_name}_Ledger.xlsx"
    )


# ----------------------------------------
# 📦 PRODUCT SALES REPORT
# ----------------------------------------
@app.route("/download-product-sales-report")
def product_sales_report():
    if "user" not in session:
        return redirect("/login")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            p.name AS product,
            c.name AS customer,
            ii.quantity,
            i.invoice_date
        FROM invoice_items ii
        JOIN invoices i ON ii.invoice_id = i.invoice_id
        JOIN customers c ON i.customer_id = c.customer_id
        JOIN products p ON ii.product_id = p.product_id
        ORDER BY i.invoice_date DESC
    """)

    data = cur.fetchall()
    cur.close()
    conn.close()

    df = pd.DataFrame(data, columns=[
        "Product", "Customer", "Quantity", "Date"
    ])

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="product_sales_report.xlsx")


# ----------------------------------------
# 📥 STOCK INWARD REPORT
# ----------------------------------------
@app.route("/download-stock-inward-report")
def download_stock_inward_report():
    if "user" not in session:
        return redirect("/login")

    conn = get_connection()

    query = """
        SELECT 
            p.name AS product,
            si.quantity,
            si.remaining_qty,
            si.received_date
        FROM stock_inward si
        JOIN products p ON si.product_id = p.product_id
        ORDER BY si.received_date DESC
    """

    df = pd.read_sql(query, conn)
    conn.close()

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return send_file(output, as_attachment=True, download_name="stock_inward_report.xlsx")

from io import BytesIO

def generate_monthly_ledger_image(customer_id):

    import matplotlib.pyplot as plt
    from datetime import datetime

    conn = get_connection()

    try:
        cur = conn.cursor()

        # CUSTOMER NAME
        cur.execute("""
            SELECT name
            FROM customers
            WHERE customer_id=%s
        """, (customer_id,))
        
        customer = cur.fetchone()
        if not customer:
            return None

        customer_name = customer[0]

        today = datetime.today()
        start_date = today.replace(day=1)

        # OPENING BALANCE
        cur.execute("""
            SELECT
                COALESCE(c.opening_balance,0)
                + COALESCE(inv.total_invoice,0)
                - COALESCE(pay.total_payment,0)
                - COALESCE(ret.total_return,0)
            FROM customers c
            LEFT JOIN (
                SELECT i.customer_id, SUM(ii.line_total) total_invoice
                FROM invoices i
                JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
                WHERE i.invoice_date < %s
                GROUP BY i.customer_id
            ) inv ON c.customer_id = inv.customer_id
            LEFT JOIN (
                SELECT customer_id, SUM(amount) total_payment
                FROM payments
                WHERE payment_date < %s
                GROUP BY customer_id
            ) pay ON c.customer_id = pay.customer_id
            LEFT JOIN (
                SELECT customer_id, SUM(total_return_amount) total_return
                FROM returns
                WHERE return_date < %s
                GROUP BY customer_id
            ) ret ON c.customer_id = ret.customer_id
            WHERE c.customer_id = %s
        """, (start_date, start_date, start_date, customer_id))

        opening_balance = float(cur.fetchone()[0] or 0)

        ledger = []

        # INVOICES
        cur.execute("""
            SELECT i.invoice_date, i.invoice_number, p.name, ii.quantity, ii.line_total
            FROM invoices i
            JOIN invoice_items ii ON i.invoice_id=ii.invoice_id
            JOIN products p ON ii.product_id=p.product_id
            WHERE i.customer_id=%s AND i.invoice_date >= %s
        """, (customer_id, start_date))

        for row in cur.fetchall():
            ledger.append([row[0], "Invoice", row[1], row[2], row[3], row[4], 0])

        # PAYMENTS
        cur.execute("""
            SELECT payment_date, amount 
            FROM payments 
            WHERE customer_id=%s AND payment_date >= %s
        """, (customer_id, start_date))

        for row in cur.fetchall():
            ledger.append([row[0], "Payment", "Payment", "", "", 0, row[1]])

        # RETURNS
        cur.execute("""
            SELECT r.return_date, r.return_number, p.name, ri.quantity, ri.line_total
            FROM returns r
            JOIN return_items ri ON r.return_id=ri.return_id
            JOIN products p ON ri.product_id=p.product_id
            WHERE r.customer_id=%s AND r.return_date >= %s
        """, (customer_id, start_date))

        for row in cur.fetchall():
            ledger.append([row[0], "Return", row[1], row[2], row[3], 0, row[4]])

        cur.close()

    finally:
        conn.close()

    if not ledger and opening_balance == 0:
        return None

    # SORTING
    def safe_date(val):
        if isinstance(val, datetime):
            return val
        try:
            return datetime.strptime(str(val), "%Y-%m-%d")
        except:
            return datetime.min

    ledger = sorted(ledger, key=lambda x: safe_date(x[0]))

    # RUNNING BALANCE
    running_balance = opening_balance
    final_data = []

    debit = max(opening_balance, 0)
    credit = abs(min(opening_balance, 0))

    final_data.append(["", "Opening", "", "", "", debit, credit, running_balance])

    for entry in ledger:
        running_balance += entry[5]
        running_balance -= entry[6]

        final_data.append([
            entry[0].strftime("%d-%m-%Y") if entry[0] else "",
            entry[1],
            entry[2],
            entry[3],
            entry[4],
            entry[5],
            entry[6],
            running_balance
        ])

    total_balance = running_balance

    table_data = [["Date","Type","Ref","Product","Qty","Debit","Credit","Balance"]]
    table_data.extend(final_data)
    table_data.append(["","","","","","", "Total", f"{total_balance}"])

    # ===== CREATE IMAGE IN MEMORY =====
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.axis('off')

    plt.title(f"Monthly Ledger - {customer_name}", fontsize=14, weight='bold', pad=20)

    table = ax.table(cellText=table_data, loc='center', cellLoc='center')

    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)

    col_widths = [0.10, 0.10, 0.15, 0.25, 0.08, 0.12, 0.12, 0.12]

    for i, width in enumerate(col_widths):
        for row in range(len(table_data)):
            table[(row, i)].set_width(width)

    for i in range(len(table_data[0])):
        table[(0, i)].set_text_props(weight='bold')

    # ✅ MEMORY BUFFER INSTEAD OF FILE
    img = BytesIO()
    plt.savefig(img, format='png', bbox_inches='tight')
    plt.close()
    img.seek(0)

    return img

@app.route("/monthly-ledger/<int:customer_id>")
def monthly_ledger(customer_id):
    if "user" not in session:
        return redirect("/login")

    img = generate_monthly_ledger_image(customer_id)

    if not img:
        return "No data for this month"

    return send_file(
        img,
        mimetype='image/png',
        as_attachment=True,
        download_name=f"monthly_ledger_{customer_id}.png"
    )

@app.route("/invoice/<int:invoice_id>/pdf")
def download_invoice(invoice_id):

    if "user" not in session:
        return redirect("/login")

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    conn = get_connection()
    cur = conn.cursor()

    # ===== FETCH INVOICE =====
    cur.execute("""
        SELECT i.invoice_number,
               i.invoice_date,
               c.name,
               i.total_amount
        FROM invoices i
        JOIN customers c ON i.customer_id = c.customer_id
        WHERE i.invoice_id = %s
    """, (invoice_id,))
    invoice = cur.fetchone()

    # ===== FETCH ITEMS =====
    cur.execute("""
        SELECT p.name, ii.quantity, ii.rate, ii.line_total
        FROM invoice_items ii
        JOIN products p ON ii.product_id = p.product_id
        WHERE ii.invoice_id = %s
    """, (invoice_id,))
    items = cur.fetchall()

    cur.close()
    conn.close()

    # ===== FORMAT DATE =====
    invoice_date = invoice[1].strftime("%d-%m-%Y")

    # ===== CREATE PDF =====
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)

    styles = getSampleStyleSheet()
    elements = []

    # ===== TITLE =====
    elements.append(Paragraph("<b>Music World</b>", styles["Title"]))
    elements.append(Paragraph("Invoice", styles["Normal"]))
    elements.append(Spacer(1, 15))

    # ===== INVOICE INFO =====
    elements.append(Paragraph(f"<b>Invoice No:</b> {invoice[0]}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Date:</b> {invoice_date}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Customer:</b> {invoice[2]}", styles["Normal"]))
    elements.append(Spacer(1, 20))

    # ===== TABLE DATA =====
    table_data = [["Product", "Qty", "Rate", "Total"]]

    for item in items:
        table_data.append([
            item[0],
            item[1],
            f"₹ {item[2]:.2f}",
            f"₹ {item[3]:.2f}"
        ])

    # ===== TABLE =====
    table = Table(table_data, colWidths=[200, 60, 80, 80])

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),

        ("GRID", (0, 0), (-1, -1), 1, colors.black),

        ("ALIGN", (1, 1), (-1, -1), "CENTER"),

        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 20))

    # ===== GRAND TOTAL =====
    elements.append(
        Paragraph(
            f"<b>Grand Total: ₹ {invoice[3]:.2f}</b>",
            styles["Heading3"]
        )
    )

    elements.append(Spacer(1, 30))

    # ===== FOOTER =====
    elements.append(
        Paragraph("Thank you for your business 🙏", styles["Normal"])
    )

    # ===== BUILD PDF =====
    doc.build(elements)

    buffer.seek(0)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"invoice_{invoice[0]}.pdf",
        mimetype="application/pdf"
    )

# ----------------------------------------
# 📊 DAILY REPORT (Invoices + Payments + Returns)
# ----------------------------------------
def generate_daily_reports():

    conn = get_connection()

    todays_date = date.today()

    reports = {}

    # ✅ Invoice + Items (Already correct)
    reports["invoices"] = pd.read_sql("""
        SELECT 
            i.invoice_number,
            i.invoice_date::date,
            c.name AS customer,
            p.name AS product,
            ii.quantity,
            ii.rate,
            ii.line_total
        FROM invoices i
        JOIN customers c ON i.customer_id = c.customer_id
        JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
        JOIN products p ON ii.product_id = p.product_id
        WHERE i.invoice_date::date = %s
        ORDER BY i.invoice_date DESC
    """, conn, params=[todays_date])

    # ✅ Payments with customer name
    reports["payments"] = pd.read_sql("""
        SELECT 
            p.payment_id,
            p.payment_date::date,
            c.name AS customer,
            p.amount,
            COALESCE(p.remarks,'') AS remarks
        FROM payments p
        JOIN customers c ON p.customer_id = c.customer_id
        WHERE p.payment_date::date = %s
        ORDER BY p.payment_date DESC
    """, conn, params=[todays_date])

    # ✅ Returns (Already correct)
    reports["returns"] = pd.read_sql("""
        SELECT 
            r.return_number,
            r.return_date::date,
            c.name AS customer,
            p.name AS product,
            ri.quantity,
            ri.rate,
            ri.line_total,
            COALESCE(r.note,'') AS note
        FROM returns r
        JOIN customers c ON r.customer_id=c.customer_id
        JOIN return_items ri ON r.return_id=ri.return_id
        JOIN products p ON ri.product_id=p.product_id
        WHERE r.return_date::date = %s
        ORDER BY r.return_date DESC
    """, conn, params=[todays_date])

    file_name = f"daily_report_{todays_date}.xlsx"

    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        for sheet, df in reports.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    conn.close()
    return file_name

def generate_weekly_backup():

    conn = get_connection()

    reports = {}

    # ✅ Invoice + Items (MERGED)
    reports["invoices"] = pd.read_sql("""
        SELECT 
            i.invoice_number,
            i.invoice_date::date,
            c.name AS customer,
            p.name AS product,
            ii.quantity,
            ii.rate,
            ii.line_total
        FROM invoices i
        JOIN customers c ON i.customer_id = c.customer_id
        JOIN invoice_items ii ON i.invoice_id = ii.invoice_id
        JOIN products p ON ii.product_id = p.product_id
        ORDER BY i.invoice_date DESC
    """, conn)

    # ✅ Payments with customer name
    reports["payments"] = pd.read_sql("""
        SELECT 
            p.payment_id,
            p.payment_date::date,
            c.name AS customer,
            p.amount,
            COALESCE(p.remarks,'') AS remarks
        FROM payments p
        JOIN customers c ON p.customer_id = c.customer_id
        ORDER BY p.payment_date DESC
    """, conn)

    # ✅ Returns + Items (MERGED)
    reports["returns"] = pd.read_sql("""
        SELECT 
            r.return_number,
            r.return_date::date,
            c.name AS customer,
            p.name AS product,
            ri.quantity,
            ri.rate,
            ri.line_total,
            COALESCE(r.note,'') AS note
        FROM returns r
        JOIN customers c ON r.customer_id=c.customer_id
        JOIN return_items ri ON r.return_id=ri.return_id
        JOIN products p ON ri.product_id=p.product_id
        ORDER BY r.return_date DESC
    """, conn)

    # ✅ Optional (keep for internal tracking)
    reports["products"] = pd.read_sql("SELECT * FROM products", conn)
    reports["customers"] = pd.read_sql("SELECT * FROM customers", conn)
    reports["stock_inward"] = pd.read_sql("SELECT * FROM stock_inward", conn)

    file_name = f"weekly_backup_{date.today()}.xlsx"

    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        for sheet, df in reports.items():
            df.to_excel(writer, sheet_name=sheet, index=False)

    conn.close()
    return file_name

def send_daily_report():

    try:
        file_path = generate_daily_reports()

        EMAIL_USER = os.getenv("EMAIL_USER")
        EMAIL_PASS = os.getenv("EMAIL_PASS")

        if not EMAIL_USER or not EMAIL_PASS:
            raise Exception("Email credentials missing in environment variables")

        msg = EmailMessage()
        msg["Subject"] = f"Daily Report - {date.today()}"
        msg["From"] = EMAIL_USER
        msg["To"] = "sumerbhatia477@gmail.com, 43musicworld@gmail.com"

        msg.set_content("Attached is your daily business report.")

        with open(file_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=file_path
            )

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        print("✅ Daily report sent successfully")

        # ✅ Optional: delete file after sending
        os.remove(file_path)

    except Exception as e:
        print(f"❌ Daily report failed: {str(e)}")

def send_weekly_backup():

    try:
        file_path = generate_weekly_backup()

        EMAIL_USER = os.getenv("EMAIL_USER")
        EMAIL_PASS = os.getenv("EMAIL_PASS")

        if not EMAIL_USER or not EMAIL_PASS:
            raise Exception("Email credentials missing in environment variables")

        msg = EmailMessage()
        msg["Subject"] = f"Weekly Backup - {date.today()}"
        msg["From"] = EMAIL_USER
        msg["To"] = "sumerbhatia477@gmail.com, 43musicworld@gmail.com"

        msg.set_content("Attached is your weekly full backup.")

        with open(file_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=file_path
            )

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        print("✅ Weekly backup sent successfully")

        # ✅ Optional cleanup
        os.remove(file_path)

    except Exception as e:
        print(f"❌ Weekly backup failed: {str(e)}")

@app.route("/backup/daily")
def backup_daily():

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    try:
        send_daily_report()
        flash("✅ Daily report sent!", "success")
    except Exception as e:
        flash(f"❌ Error: {str(e)}", "danger")

    return redirect(request.referrer or "/dashboard")


@app.route("/backup/weekly")
def backup_weekly():

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    try:
        send_weekly_backup()
        flash("✅ Weekly backup sent!", "success")
    except Exception as e:
        flash(f"❌ Error: {str(e)}", "danger")

    return redirect(request.referrer or "/dashboard")

@app.route("/add-stock", methods=["POST"])
def add_stock():
    if "user" not in session:
        return redirect("/login")

    product_id = request.form.get("product_id")
    product_name = request.form.get("new_product_name")  # for NEW product
    price = float(request.form.get("price", 0))
    qty = int(request.form.get("quantity", 0))
    received_date = request.form.get("received_date")

    if qty <= 0:
        flash("❌ Quantity must be greater than 0", "danger")
        return redirect("/products")

    conn = get_connection()
    cur = None

    try:
        cur = conn.cursor()

        # ✅ CASE 1: NEW PRODUCT
        if not product_id:
            if not product_name:
                flash("❌ Please select or enter product", "danger")
                return redirect("/products")

            cur.execute("""
                INSERT INTO products (name, price, stock_qty)
                VALUES (%s, %s, %s)
                RETURNING product_id
            """, (product_name, price, qty))

            product_id = cur.fetchone()[0]

        else:
            # ✅ CASE 2: EXISTING PRODUCT → update stock + price
            cur.execute("""
                UPDATE products
                SET stock_qty = stock_qty + %s,
                    price = %s
                WHERE product_id = %s
            """, (qty, price, product_id))

        # ✅ STOCK INWARD ENTRY
        cur.execute("""
            INSERT INTO stock_inward (product_id, quantity, remaining_qty, received_date)
            VALUES (%s, %s, %s, %s)
        """, (product_id, qty, qty, received_date))

        conn.commit()

        flash("✅ Stock added successfully!", "success")
        return redirect("/products")

    except Exception as e:
        conn.rollback()
        print("ADD STOCK ERROR:", e)
        flash("❌ Failed to add stock", "danger")
        return redirect("/products")

    finally:
        try:
            if cur:
                cur.close()
        except:
            pass
        conn.close()


def start_scheduler():
    try:
        if not scheduler.running:
            scheduler.add_job(send_daily_report, trigger='cron', hour=23, minute=0)
            scheduler.add_job(send_weekly_backup, trigger='cron', day_of_week='sun', hour=23, minute=0)

            scheduler.start()
            print(f"✅ Scheduler started on {socket.gethostname()}")

    except Exception as e:
        print("Scheduler error:", e)


import atexit

# ✅ Start scheduler automatically in production
if os.environ.get("RENDER") == "true":
    start_scheduler()

# ✅ Graceful shutdown
atexit.register(lambda: scheduler.shutdown() if scheduler.running else None)