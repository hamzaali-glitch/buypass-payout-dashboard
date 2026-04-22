"""
Load payout Excel into dashboard database.
Usage: python load_payout.py "path_to_excel" "Period Label"
Same parsing approach as the email/invoice script (proven to work).
Fetches product names from BigQuery order_details table.
"""

import sys, os, sqlite3, openpyxl
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), 'payout_data.db')
BQ_KEY = r'C:\Users\TrendZone.Pk\OneDrive\Desktop\Company Documents\buypass-ai-readonly.json'


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.execute('''CREATE TABLE IF NOT EXISTS periods (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_label TEXT NOT NULL,
        upload_time TEXT NOT NULL,
        total_sellers INTEGER,
        total_orders INTEGER,
        total_payout REAL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS sellers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_id INTEGER NOT NULL,
        store TEXT NOT NULL,
        amount REAL,
        total_orders INTEGER,
        biz_type TEXT,
        iban TEXT,
        bank TEXT,
        account_title TEXT
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_id INTEGER NOT NULL,
        store TEXT NOT NULL,
        bpid TEXT,
        category TEXT,
        product_name TEXT,
        customer TEXT,
        user_city TEXT,
        quantity INTEGER,
        price REAL,
        commission REAL,
        delivery_date TEXT,
        order_date TEXT
    )''')
    # Add product_name column if missing (for existing databases)
    try:
        db.execute('ALTER TABLE orders ADD COLUMN product_name TEXT')
        print("  Added product_name column to orders table")
    except Exception:
        pass  # column already exists
    db.execute('CREATE INDEX IF NOT EXISTS idx_sellers_period ON sellers(period_id)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_orders_period_store ON orders(period_id, store)')
    db.commit()
    db.close()


def fetch_order_details_bq(order_ids):
    """Fetch product names, customer, city, order date, delivery date from BigQuery."""
    try:
        from google.cloud import bigquery
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_KEY
        client = bigquery.Client(project='buypass-ai')

        product_map = {}
        order_info_map = {}

        # Fetch product names from order_details
        for i in range(0, len(order_ids), 200):
            batch = order_ids[i:i+200]
            ids_str = ', '.join(f"'{oid}'" for oid in batch)
            query = f"""
                SELECT buypassid, nameen
                FROM `buypass-ai.BuyPASS.order_details`
                WHERE buypassid IN ({ids_str})
            """
            results = client.query(query).result()
            for row in results:
                product_map[row.buypassid] = row.nameen or '-'

        print(f"  Product names fetched: {len(product_map)} / {len(order_ids)}")

        # Fetch customer, city, order date, delivery date from orders table
        for i in range(0, len(order_ids), 200):
            batch = order_ids[i:i+200]
            ids_str = ', '.join(f"'{oid}'" for oid in batch)
            query = f"""
                SELECT buypassid, userdetails_name, userdetails_city,
                       createdon, modifiedon
                FROM `buypass-ai.BuyPASS.orders`
                WHERE buypassid IN ({ids_str}) AND status = 'delivered'
            """
            results = client.query(query).result()
            for row in results:
                order_info_map[row.buypassid] = {
                    'customer': row.userdetails_name or '-',
                    'user_city': row.userdetails_city or '-',
                    'order_date': row.createdon,
                    'delivery_date': row.modifiedon,
                }

        print(f"  Order details fetched: {len(order_info_map)} / {len(order_ids)}")
        return product_map, order_info_map

    except Exception as e:
        print(f"  WARNING: Could not fetch from BigQuery: {e}")
        print("  Continuing without BigQuery data...")
        return {}, {}


def fetch_biz_types(store_names):
    """Fetch business types from BigQuery business table."""
    try:
        from google.cloud import bigquery
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = BQ_KEY
        client = bigquery.Client(project='buypass-ai')

        biz_map = {}
        for i in range(0, len(store_names), 100):
            batch = store_names[i:i+100]
            q = 'SELECT TRIM(storename) as store, businesstype FROM `buypass-ai.BuyPASS.business` WHERE TRIM(storename) IN UNNEST(@names)'
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter('names', 'STRING', batch)]
            )
            for r in client.query(q, job_config=job_config).result():
                if r.businesstype:
                    biz_map[r.store] = r.businesstype.title()

        print(f"  Business types fetched: {len(biz_map)} / {len(store_names)}")
        return biz_map

    except Exception as e:
        print(f"  WARNING: Could not fetch business types: {e}")
        return {}


def fmt_date(d):
    if not d:
        return '-'
    s = str(d)
    try:
        if 'UTC' in s:
            s = s.replace(' UTC', '')
        dt = datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%d/%m/%Y')
    except Exception:
        return str(d)[:10]


def safe_float(val):
    if val is None:
        return 0
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return 0


def load_payout(excel_path, period_label, payment_date=None, sort_date=None):
    print(f"Loading: {excel_path}")
    print(f"Period:  {period_label}")

    wb = openpyxl.load_workbook(excel_path, data_only=True)
    ws = wb[wb.sheetnames[0]]

    # === Read headers from row 1 ===
    headers = [str(c.value).strip().lower() if c.value else '' for c in ws[1]]

    def col(name):
        for i, h in enumerate(headers):
            if h == name:
                return i
        return None

    c_store = col('storename') or col('store name')
    c_bpid = col('buypassid') or col('order id')
    c_price = col('base_price') or col('base price') or col('amount')
    c_comm = col('ordersummary_commission')
    c_deliv = col('delivery_date')
    c_created = col('createdon')
    c_cat = col('category')
    c_biz = col('businesstype')
    c_cust = col('customer_name')
    c_ucity = col('user_city')
    c_qty = col('quantity')
    c_iban_detail = col('iban')
    c_acct_detail = col('account title')

    # === Find summary section (row with "S.no" or "Store name"/"Store Name" header) ===
    summary_row = None
    for ri in range(2, ws.max_row + 1):
        for cell in ws[ri]:
            if cell.value and str(cell.value).strip().lower() in ('s.no', 'store name', 'storename'):
                # Check if this is a summary header row (not a data row)
                # Summary header has "store name" + "amount" nearby
                row_vals = [str(c.value).strip().lower() if c.value else '' for c in ws[ri]]
                if 's.no' in row_vals or ('amount' in row_vals and any(x in row_vals for x in ['store name', 'storename'])):
                    summary_row = ri
                    break
        if summary_row:
            break

    if not summary_row:
        print("ERROR: Could not find summary section")
        return

    # Read summary headers from that row
    sum_headers = {}
    for cell in ws[summary_row]:
        if cell.value:
            sum_headers[str(cell.value).strip().lower()] = cell.column - 1

    s_store = sum_headers.get('store name', sum_headers.get('storename'))
    s_amount = sum_headers.get('amount')

    # For IBAN/Bank/Account — read from first data row directly
    # because headers can be offset due to merged cells
    s_iban = sum_headers.get('iban')

    # Read first data row to detect actual bank/account positions
    first_data = [c.value for c in ws[summary_row + 1]]
    s_bank = None
    s_acct = None
    if s_iban is not None and (s_iban + 1) < len(first_data):
        val_after_iban = first_data[s_iban + 1]
        if val_after_iban and isinstance(val_after_iban, str) and not str(val_after_iban).startswith('PK'):
            # col after IBAN is bank name
            s_bank = s_iban + 1
            s_acct = s_iban + 2
        else:
            # headers are correct
            s_bank = sum_headers.get('bank')
            s_acct = sum_headers.get('account title', sum_headers.get('accounttitle'))
    else:
        s_bank = sum_headers.get('bank')
        s_acct = sum_headers.get('account title', sum_headers.get('accounttitle'))

    print(f"  Detail rows: 2 to {summary_row - 1}")
    print(f"  Summary starts: row {summary_row + 1}")
    print(f"  Store col={s_store}, Amount col={s_amount}, IBAN col={s_iban}, Bank col={s_bank}, Acct col={s_acct}")

    # === Parse order details (row 2 to summary_row - 1) ===
    orders_by_store = {}
    store_bank_info = {}  # fallback bank info from detail rows
    total_orders = 0
    all_bpids = []

    for row in ws.iter_rows(min_row=2, max_row=summary_row - 1, values_only=True):
        store = str(row[c_store]).strip() if c_store is not None and row[c_store] else None
        if not store:
            continue

        total_orders += 1
        bpid = str(row[c_bpid]) if c_bpid is not None and row[c_bpid] else '-'
        if bpid != '-':
            all_bpids.append(bpid)

        # Capture bank info from detail rows as fallback
        if store not in store_bank_info and c_iban_detail is not None:
            iban_val = str(row[c_iban_detail]).strip() if row[c_iban_detail] else '-'
            acct_val = str(row[c_acct_detail]).strip() if c_acct_detail is not None and row[c_acct_detail] else '-'
            store_bank_info[store] = {'iban': iban_val, 'account_title': acct_val}

        order = {
            'bpid': bpid,
            'category': str(row[c_cat]) if c_cat is not None and row[c_cat] else '-',
            'customer': str(row[c_cust]) if c_cust is not None and row[c_cust] else '-',
            'user_city': str(row[c_ucity]) if c_ucity is not None and row[c_ucity] else '-',
            'quantity': int(row[c_qty]) if c_qty is not None and row[c_qty] else 1,
            'price': safe_float(row[c_price] if c_price is not None else None),
            'commission': safe_float(row[c_comm] if c_comm is not None else None),
            'delivery_date': fmt_date(row[c_deliv] if c_deliv is not None else None),
            'order_date': fmt_date(row[c_created] if c_created is not None else None),
            'biz_type': str(row[c_biz]).title() if c_biz is not None and row[c_biz] else '-',
        }
        if store not in orders_by_store:
            orders_by_store[store] = []
        orders_by_store[store].append(order)

    print(f"  Orders parsed: {total_orders} across {len(orders_by_store)} stores")

    # === Fetch product names + order details from BigQuery ===
    print(f"  Fetching data for {len(all_bpids)} orders from BigQuery...")
    product_map, order_info_map = fetch_order_details_bq(all_bpids)

    # Attach product names and fill missing fields from BigQuery
    for store, orders in orders_by_store.items():
        for o in orders:
            o['product_name'] = product_map.get(o['bpid'], '-')
            # Fill missing fields from BigQuery if not in Excel
            bq_info = order_info_map.get(o['bpid'])
            if bq_info:
                if o['customer'] == '-':
                    o['customer'] = bq_info['customer']
                if o['user_city'] == '-':
                    o['user_city'] = bq_info['user_city']
                if o['order_date'] == '-' and bq_info['order_date']:
                    o['order_date'] = fmt_date(bq_info['order_date'])
                if o['delivery_date'] == '-' and bq_info['delivery_date']:
                    o['delivery_date'] = fmt_date(bq_info['delivery_date'])

    # === Parse summary section ===
    sellers = []
    for row in ws.iter_rows(min_row=summary_row + 1, max_row=ws.max_row, values_only=True):
        store = row[s_store] if s_store is not None and s_store < len(row) else None
        amount = row[s_amount] if s_amount is not None and s_amount < len(row) else None
        if not store or not amount:
            continue
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            continue

        store = str(store).strip()
        # Skip total/summary rows
        if store.lower() in ('total', 'grand total', 'sum'):
            continue
        store_orders = orders_by_store.get(store, [])

        def safe(col_idx):
            if col_idx is not None and col_idx < len(row) and row[col_idx]:
                return str(row[col_idx]).strip()
            return '-'

        # Get bank info: prefer summary section, fallback to detail rows
        iban = safe(s_iban)
        bank = safe(s_bank)
        acct = safe(s_acct)
        if iban == '-' and store in store_bank_info:
            iban = store_bank_info[store].get('iban', '-')
        if acct == '-' and store in store_bank_info:
            acct = store_bank_info[store].get('account_title', '-')

        sellers.append({
            'store': store,
            'amount': amount,
            'total_orders': len(store_orders),
            'biz_type': store_orders[0]['biz_type'] if store_orders else '-',
            'iban': iban,
            'bank': bank,
            'account_title': acct,
        })

    total_payout = sum(s['amount'] for s in sellers)
    print(f"  Sellers: {len(sellers)}")
    print(f"  Total payout: Rs. {total_payout:,.2f}")

    # Fetch missing business types from BigQuery
    missing_biz = [s['store'] for s in sellers if s['biz_type'] == '-']
    if missing_biz:
        print(f"  Fetching business types for {len(missing_biz)} sellers...")
        biz_map = fetch_biz_types(missing_biz)
        for s in sellers:
            if s['biz_type'] == '-' and s['store'] in biz_map:
                s['biz_type'] = biz_map[s['store']]

    # Print first 3 sellers for verification
    print("\n  Sample sellers:")
    for s in sellers[:3]:
        print(f"    {s['store']}: Rs.{s['amount']:,.0f} | {s['total_orders']} orders | Bank: {s['bank']} | IBAN: {s['iban'][:20]}...")

    # === Save to database ===
    db = sqlite3.connect(DB_PATH)
    cur = db.cursor()

    # Delete existing period with same label
    cur.execute('SELECT id FROM periods WHERE period_label = ?', (period_label,))
    existing = cur.fetchone()
    if existing:
        pid = existing[0]
        cur.execute('DELETE FROM orders WHERE period_id = ?', (pid,))
        cur.execute('DELETE FROM sellers WHERE period_id = ?', (pid,))
        cur.execute('DELETE FROM periods WHERE id = ?', (pid,))
        print(f"\n  Replaced existing period '{period_label}'")

    upload_time = datetime.now().strftime('%d %b %Y, %I:%M %p')
    cur.execute('INSERT INTO periods (period_label, upload_time, total_sellers, total_orders, total_payout, payment_date, sort_date) VALUES (?,?,?,?,?,?,?)',
                (period_label, upload_time, len(sellers), total_orders, total_payout, payment_date, sort_date))
    pid = cur.lastrowid

    for s in sellers:
        cur.execute('INSERT INTO sellers (period_id, store, amount, total_orders, biz_type, iban, bank, account_title) VALUES (?,?,?,?,?,?,?,?)',
                    (pid, s['store'], s['amount'], s['total_orders'], s['biz_type'], s['iban'], s['bank'], s['account_title']))

    for store, orders in orders_by_store.items():
        for o in orders:
            cur.execute('INSERT INTO orders (period_id, store, bpid, category, product_name, customer, user_city, quantity, price, commission, delivery_date, order_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                        (pid, store, o['bpid'], o['category'], o['product_name'], o['customer'], o['user_city'], o['quantity'], o['price'], o['commission'], o['delivery_date'], o['order_date']))

    db.commit()
    db.close()
    print(f"\n  DONE! Saved as period_id={pid}")
    print(f"  Dashboard: http://localhost:5000/dashboard/{pid}")


if __name__ == '__main__':
    init_db()
    if len(sys.argv) < 3:
        print("Usage: python load_payout.py <excel_path> <period_label> [payment_date]")
        sys.exit(1)
    pay_date = sys.argv[3] if len(sys.argv) > 3 else None
    s_date = sys.argv[4] if len(sys.argv) > 4 else None
    load_payout(sys.argv[1], sys.argv[2], pay_date, s_date)
