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
from datetime import date
import smtplib
from flask import jsonify
from email.message import EmailMessage
from apscheduler.schedulers.background import BackgroundScheduler
from flask import flash
from flask import session
import os
from datetime import timedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
print("SECRET_KEY:", os.getenv("SECRET_KEY"))
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY")


SHOP_DETAILS = {
    "name": "Music World",
    "address": "Shop No. 28, Patva Chambers, Opp Green Laws, Singhada Talav, Nashik- 422001",
    "phone": "0253250433"
}

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
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("SELECT * FROM products ORDER BY LOWER(name) ASC")
    products = cur.fetchall()
    # 👉 Send names separately for dropdown
    product_names = [p[1] for p in products]
    cur.close()
    conn.close()
    return render_template("products.html",
                           products=products,
                           product_names=product_names)



@app.route("/update-product", methods=["POST"])
def update_product():
    if "user" not in session:
        return redirect("/login")
    product_id=request.form["product_id"]
    name=request.form["name"]
    price=request.form["price"]
    stock_qty=request.form["stock_qty"]
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("UPDATE products SET name=%s,price=%s,stock_qty=%s WHERE product_id=%s",(name,price,stock_qty,product_id))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/products")

@app.route("/customers")
def customers_page():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("""
    SELECT c.customer_id,c.name,c.phone,c.address,c.credit_limit,
    COALESCE(inv.total_invoice,0)-COALESCE(pay.total_payment,0)-COALESCE(ret.total_return,0) AS balance
    FROM customers c
    LEFT JOIN (SELECT i.customer_id,SUM(ii.line_total) AS total_invoice FROM invoices i JOIN invoice_items ii ON i.invoice_id=ii.invoice_id GROUP BY i.customer_id) inv ON c.customer_id=inv.customer_id
    LEFT JOIN (SELECT customer_id,SUM(amount) AS total_payment FROM payments GROUP BY customer_id) pay ON c.customer_id=pay.customer_id
    LEFT JOIN (SELECT customer_id,SUM(total_return_amount) AS total_return FROM returns GROUP BY customer_id) ret ON c.customer_id=ret.customer_id
    ORDER BY LOWER(c.name) ASC
    """)
    customers=cur.fetchall()
    cur.close()
    conn.close()
    return render_template("customers.html",customers=customers)

@app.route("/add-customer",methods=["POST"])
def add_customer():
    if "user" not in session:
        return redirect("/login")
    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")
    name=request.form["name"]
    phone=request.form["phone"]
    address=request.form["address"]
    credit_limit=request.form["credit_limit"]
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("INSERT INTO customers (name,phone,address,credit_limit) VALUES (%s,%s,%s,%s)",(name,phone,address,credit_limit))
    conn.commit()
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
    cur = conn.cursor()

    # Generate invoice number
    cur.execute("SELECT COALESCE(MAX(invoice_id),0) + 1 FROM invoices")
    next_id = cur.fetchone()[0]
    invoice_number = f"MW-{next_id:04d}"

    total_amount = 0
    stock_warnings = []

    valid_items = []   # ✅ store only valid items

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

        # ❌ Skip completely if stock = 0
        if stock_qty == 0:
            stock_warnings.append(f"{product_name} is OUT OF STOCK and skipped")
            continue

        # Adjust if qty > stock
        if qty > stock_qty:
            stock_warnings.append(f"{product_name}: adjusted from {qty} to {stock_qty}")
            qty = stock_qty

        # ❌ Skip if qty becomes 0
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
        cur.close()
        conn.close()
        return jsonify({
            "status": "error",
            "message": "All selected products are out of stock!"
        })

    # ================= CREDIT CHECK =================
    cur.execute("""
        SELECT
            c.credit_limit,
            COALESCE(inv.total_invoice,0)
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
        WHERE c.customer_id=%s
    """, (customer_id,))

    data = cur.fetchone()
    credit_limit = float(data[0] or 0)
    current_balance = float(data[1] or 0)
    new_balance = current_balance + total_amount

    if new_balance > credit_limit:
        cur.close()
        conn.close()
        return jsonify({
            "status": "error",
            "message": "Credit limit exceeded",
            "current_balance": current_balance,
            "invoice_amount": total_amount,
            "credit_limit": credit_limit
        })

    # ================= INSERT INVOICE =================
    cur.execute("""
        INSERT INTO invoices (invoice_number, customer_id, invoice_date, total_amount)
        VALUES (%s,%s,%s,%s)
        RETURNING invoice_id
    """, (invoice_number, customer_id, invoice_date, total_amount))

    invoice_id = cur.fetchone()[0]

    # ================= INSERT ITEMS =================
    for item in valid_items:
        cur.execute("""
            INSERT INTO invoice_items (invoice_id, product_id, quantity, rate, line_total)
            VALUES (%s,%s,%s,%s,%s)
        """, (invoice_id, item["pid"], item["qty"], item["rate"], item["line_total"]))

        # Update stock
        cur.execute("""
            UPDATE products
            SET stock_qty = stock_qty - %s
            WHERE product_id = %s
        """, (item["qty"], item["pid"]))

    conn.commit()
    cur.close()
    conn.close()

    response = {
        "status": "success",
        "redirect": f"/invoice/{invoice_id}"
    }

    if stock_warnings:
        response["message"] = "Some items were adjusted or skipped"
        response["stock_warnings"] = stock_warnings

    return jsonify(response)

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
    cur = conn.cursor()

    cur.execute("""
        SELECT
        c.credit_limit,
        COALESCE(inv.total_invoice,0)
        - COALESCE(pay.total_payment,0)
        - COALESCE(ret.total_return,0) AS balance

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

    data = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({
        "balance": float(data[1] or 0),
        "credit_limit": float(data[0] or 0)
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
    customer_id=request.form["customer_id"]
    amount=float(request.form["amount"])
    mode=request.form["payment_mode"]
    reference_no=request.form.get("reference_no","")
    remarks=request.form.get("remarks","")
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("INSERT INTO payments (customer_id,amount,payment_mode,reference_no,remarks) VALUES (%s,%s,%s,%s,%s)",(customer_id,amount,mode,reference_no,remarks))
    conn.commit()
    flash("Payment Saved Successfully!","success")
    cur.close()
    conn.close()
    return redirect("/payments")

@app.route("/returns",methods=["GET","POST"])
def returns():
    if "user" not in session:
        return redirect("/login")
    conn=get_connection()
    cur=conn.cursor()
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
        cur.execute("INSERT INTO returns (return_number,customer_id,total_return_amount,note) VALUES (%s,%s,%s,%s) RETURNING return_id",(return_number,customer_id,total_return_amount,note))
        return_id=cur.fetchone()[0]
        for i in range(len(product_ids)):
            pid=int(product_ids[i])
            qty=int(quantities[i])
            rate=float(rates[i])
            line_total=qty*rate
            cur.execute("INSERT INTO return_items (return_id,product_id,quantity,rate,line_total) VALUES (%s,%s,%s,%s,%s)",(return_id,pid,qty,rate,line_total))
            cur.execute("UPDATE products SET stock_qty=stock_qty+%s WHERE product_id=%s",(qty,pid))
            flash("Return added successfully!", "success")
        conn.commit()
        cur.close()
        conn.close()
        return redirect("/returns")
    cur.execute("SELECT customer_id,name FROM customers ORDER BY LOWER(name)")
    customers=cur.fetchall()
    cur.execute("SELECT product_id,name,price,stock_qty FROM products ORDER BY LOWER(name)")
    products=cur.fetchall()
    cur.close()
    conn.close()
    return render_template("returns.html",customers=customers,products=products,request=request)

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
    filename = f"sales_report_{start_date}_to_{end_date}.xlsx"
    df.to_excel(filename,index=False)
    return send_file(filename, as_attachment=True)

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
    cur.execute("SELECT name FROM customers WHERE customer_id=%s",(customer_id,))
    customer = cur.fetchone()
    if not customer:
        return "Customer not found"
    customer_name = customer[0]
    ledger = []
    cur.execute("""
        SELECT i.invoice_date,i.invoice_number,p.name,ii.quantity,ii.line_total
        FROM invoices i
        JOIN invoice_items ii ON i.invoice_id=ii.invoice_id
        JOIN products p ON ii.product_id=p.product_id
        WHERE i.customer_id=%s
    """,(customer_id,))
    invoices = cur.fetchall()
    for row in invoices:
        ledger.append([row[0],"Invoice",row[1],row[2],row[3],row[4],0])
    cur.execute("SELECT payment_date,amount FROM payments WHERE customer_id=%s",(customer_id,))
    payments = cur.fetchall()
    for row in payments:
        ledger.append([row[0],"Payment","Payment","", "",0,row[1]])
    cur.execute("""
        SELECT r.return_date,r.return_number,p.name,ri.quantity,ri.line_total
        FROM returns r
        JOIN return_items ri ON r.return_id=ri.return_id
        JOIN products p ON ri.product_id=p.product_id
        WHERE r.customer_id=%s
    """,(customer_id,))
    returns = cur.fetchall()
    for row in returns:
        ledger.append([row[0],"Return",row[1],row[2],row[3],0,row[4]])
    cur.close()
    conn.close()
    ledger = sorted(ledger,key=lambda x:x[0])
    running_balance = 0
    final_data = []
    for entry in ledger:
        running_balance += entry[5]
        running_balance -= entry[6]
        final_data.append(entry + [running_balance])
    df = pd.DataFrame(final_data,columns=[
        "Date","Type","Reference","Product","Qty",
        "Purchase","Payment/Return","Balance"
    ])
    filename = f"{customer_name}_Ledger.xlsx"
    df.to_excel(filename,index=False)
    return send_file(filename,as_attachment=True)

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
            c.name,
            COALESCE(inv.total_invoice,0),
            COALESCE(pay.total_payment,0),
            COALESCE(inv.total_invoice,0)
            - COALESCE(pay.total_payment,0)
            - COALESCE(ret.total_return,0)
        FROM customers c
        LEFT JOIN (
            SELECT i.customer_id,SUM(ii.line_total) total_invoice
            FROM invoices i
            JOIN invoice_items ii ON i.invoice_id=ii.invoice_id
            GROUP BY i.customer_id
        ) inv ON c.customer_id=inv.customer_id
        LEFT JOIN (
            SELECT customer_id,SUM(amount) total_payment
            FROM payments
            GROUP BY customer_id
        ) pay ON c.customer_id=pay.customer_id
        LEFT JOIN (
            SELECT customer_id,SUM(total_return_amount) total_return
            FROM returns
            GROUP BY customer_id
        ) ret ON c.customer_id=ret.customer_id
    """)
    data = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(data,columns=[
        "Customer","Total Invoice","Total Payment","Final Balance"
    ])
    filename = "payment_summary.xlsx"
    df.to_excel(filename,index=False)
    return send_file(filename,as_attachment=True)

@app.route("/download-payments-report")
def download_payments_report():
    if "user" not in session:
        return redirect("/login")

    if session.get("user") != "admin":
        flash("Access denied!", "danger")
        return redirect("/")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            p.payment_id,
            p.payment_date,
            c.name AS customer,
            p.amount,
            p.payment_mode,
            p.reference_no,
            COALESCE(p.remarks,'')
        FROM payments p
        JOIN customers c ON p.customer_id = c.customer_id
        ORDER BY p.payment_date DESC
    """)

    data = cur.fetchall()

    cur.close()
    conn.close()

    if not data:
        return "No payment data found."

    df = pd.DataFrame(data, columns=[
        "Payment ID", "Date", "Customer",
        "Amount", "Mode", "Reference No", "Remarks"
    ])

    filename = "detailed_payments_report.xlsx"
    df.to_excel(filename, index=False)

    return send_file(filename, as_attachment=True)

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
    df = pd.DataFrame(data,columns=[
        "Return No","Date","Customer","Product","Qty","Rate","Line Total","Note"
    ])
    filename = "returns_report.xlsx"
    df.to_excel(filename,index=False)
    return send_file(filename,as_attachment=True)

@app.route("/alerts")
def alerts():
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
        c.phone,
        COALESCE(inv.total_invoice,0)
        - COALESCE(pay.total_payment,0)
        - COALESCE(ret.total_return,0) AS balance

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

    WHERE
        COALESCE(inv.total_invoice,0)
        - COALESCE(pay.total_payment,0)
        - COALESCE(ret.total_return,0) > 0

    ORDER BY balance DESC
    """)

    customers = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("alerts.html", customers=customers)


def generate_monthly_ledger_image(customer_id):

    import matplotlib.pyplot as plt
    from datetime import datetime
    import os

    conn = get_connection()
    cur = conn.cursor()

    # ===== CUSTOMER NAME =====
    cur.execute("SELECT name FROM customers WHERE customer_id=%s", (customer_id,))
    customer = cur.fetchone()
    if not customer:
        return None

    customer_name = customer[0]

    # ===== CURRENT MONTH =====
    today = datetime.today()
    start_date = today.replace(day=1)

    ledger = []

    # ===== INVOICES =====
    cur.execute("""
        SELECT i.invoice_date, i.invoice_number, p.name, ii.quantity, ii.line_total
        FROM invoices i
        JOIN invoice_items ii ON i.invoice_id=ii.invoice_id
        JOIN products p ON ii.product_id=p.product_id
        WHERE i.customer_id=%s AND i.invoice_date >= %s
    """, (customer_id, start_date))

    for row in cur.fetchall():
        ledger.append([row[0], "Invoice", row[1], row[2], row[3], row[4], 0])

    # ===== PAYMENTS =====
    cur.execute("""
        SELECT payment_date, amount 
        FROM payments 
        WHERE customer_id=%s AND payment_date >= %s
    """, (customer_id, start_date))

    for row in cur.fetchall():
        ledger.append([row[0], "Payment", "Payment", "", "", 0, row[1]])

    # ===== RETURNS =====
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
    conn.close()

    if not ledger:
        return None

    # ===== SORT =====
    ledger = sorted(ledger, key=lambda x: x[0])

    # ===== RUNNING BALANCE =====
    running_balance = 0
    final_data = []

    for entry in ledger:
        running_balance += entry[5]
        running_balance -= entry[6]

        final_data.append([
            entry[0].strftime("%d-%m-%Y"),  # ✅ FULL DATE
            entry[1],
            entry[2],
            entry[3],
            entry[4],
            entry[5],
            entry[6],
            running_balance
        ])

    # ===== TOTAL BALANCE =====
    total_balance = running_balance

    # ===== TABLE DATA =====
    table_data = [["Date","Type","Ref","Product","Qty","Debit","Credit","Balance"]]
    table_data.extend(final_data)

    # Add total row
    table_data.append(["","","","","","", "Total", f"{total_balance}"])

    # ===== FIGURE SIZE (IMPORTANT FIX) =====
    fig, ax = plt.subplots(figsize=(14, 6))  # 👈 Bigger width
    ax.axis('off')

    # ===== ADD TITLE =====
    plt.title(f"Monthly Ledger - {customer_name}", fontsize=14, weight='bold', pad=20)

    table = ax.table(
        cellText=table_data,
        loc='center',
        cellLoc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.6)

    # ===== COLUMN WIDTH FIX (MAIN ISSUE) =====
    col_widths = [0.10, 0.10, 0.15, 0.25, 0.08, 0.12, 0.12, 0.12]

    for i, width in enumerate(col_widths):
        for row in range(len(table_data)):
            table[(row, i)].set_width(width)

    # ===== BOLD HEADER =====
    for i in range(len(table_data[0])):
        table[(0, i)].set_text_props(weight='bold')

    # ===== SAVE =====
    os.makedirs("static", exist_ok=True)

    filename = f"ledger_{customer_id}.png"
    filepath = os.path.join("static", filename)

    plt.savefig(filepath, bbox_inches='tight')
    plt.close()

    return filepath

@app.route("/monthly-ledger/<int:customer_id>")
def monthly_ledger(customer_id):
    if "user" not in session:
        return redirect("/login")

    image_path = generate_monthly_ledger_image(customer_id)

    if not image_path:
        return "No data for this month"

    return send_file(image_path)

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
    cur.close()
    conn.close()

    flash("✅ Stock added successfully!", "success")
    return redirect("/products")

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

    filename = "product_sales_report.xlsx"
    df.to_excel(filename, index=False)

    return send_file(filename, as_attachment=True)

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

    filename = "stock_inward_report.xlsx"
    df.to_excel(filename, index=False)

    conn.close()

    return send_file(filename, as_attachment=True)

scheduler = BackgroundScheduler()

def start_scheduler():
    if not scheduler.get_jobs():
        try:
            scheduler.add_job(send_daily_report, 'interval', minutes=1)
            #scheduler.add_job(send_daily_report, trigger='cron', hour=23, minute=0)
            scheduler.add_job(send_weekly_backup, trigger='cron', day_of_week='sun', hour=23, minute=0)

            scheduler.start()
            print(f"✅ Scheduler started on {socket.gethostname()}")

        except Exception as e:
            print("Scheduler error:", e)


import atexit

# ✅ Start scheduler automatically in production
start_scheduler()

# ✅ Graceful shutdown
atexit.register(lambda: scheduler.shutdown())