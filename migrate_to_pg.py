"""
Migrate SQLite data to PostgreSQL (Railway/Supabase).
Usage: python migrate_to_pg.py <DATABASE_URL>
"""

import sys, sqlite3, psycopg2

SQLITE_PATH = 'payout_data.db'


def migrate(pg_url):
    print("Connecting to PostgreSQL...")
    pg = psycopg2.connect(pg_url)
    cur = pg.cursor()

    # Create tables
    print("Creating tables...")
    cur.execute('''CREATE TABLE IF NOT EXISTS periods (
        id SERIAL PRIMARY KEY,
        period_label TEXT NOT NULL,
        upload_time TEXT NOT NULL,
        total_sellers INTEGER,
        total_orders INTEGER,
        total_payout REAL,
        payment_date TEXT,
        sort_date TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS sellers (
        id SERIAL PRIMARY KEY,
        period_id INTEGER NOT NULL,
        store TEXT NOT NULL,
        amount REAL,
        total_orders INTEGER,
        biz_type TEXT,
        iban TEXT,
        bank TEXT,
        account_title TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
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
    cur.execute('CREATE INDEX IF NOT EXISTS idx_sellers_period ON sellers(period_id)')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_orders_period_store ON orders(period_id, store)')
    pg.commit()

    # Read from SQLite
    print("Reading SQLite data...")
    sq = sqlite3.connect(SQLITE_PATH)
    sq.row_factory = sqlite3.Row

    # Clear existing PG data
    cur.execute('DELETE FROM orders')
    cur.execute('DELETE FROM sellers')
    cur.execute('DELETE FROM periods')
    pg.commit()

    # Migrate periods
    periods = sq.execute('SELECT * FROM periods ORDER BY id').fetchall()
    id_map = {}  # old_id -> new_id
    for p in periods:
        cur.execute(
            'INSERT INTO periods (period_label, upload_time, total_sellers, total_orders, total_payout, payment_date, sort_date) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id',
            (p['period_label'], p['upload_time'], p['total_sellers'], p['total_orders'], p['total_payout'],
             p['payment_date'] if 'payment_date' in p.keys() else None,
             p['sort_date'] if 'sort_date' in p.keys() else None)
        )
        new_id = cur.fetchone()[0]
        id_map[p['id']] = new_id
        print(f"  Period: {p['period_label']} (id {p['id']} -> {new_id})")

    # Migrate sellers
    sellers = sq.execute('SELECT * FROM sellers ORDER BY id').fetchall()
    print(f"Migrating {len(sellers)} sellers...")
    for s in sellers:
        new_pid = id_map.get(s['period_id'])
        if not new_pid:
            continue
        cur.execute(
            'INSERT INTO sellers (period_id, store, amount, total_orders, biz_type, iban, bank, account_title) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
            (new_pid, s['store'], s['amount'], s['total_orders'], s['biz_type'], s['iban'], s['bank'], s['account_title'])
        )

    # Migrate orders
    orders = sq.execute('SELECT * FROM orders ORDER BY id').fetchall()
    print(f"Migrating {len(orders)} orders...")
    batch = []
    for o in orders:
        new_pid = id_map.get(o['period_id'])
        if not new_pid:
            continue
        batch.append((new_pid, o['store'], o['bpid'], o['category'],
                       o['product_name'] if 'product_name' in o.keys() else None,
                       o['customer'], o['user_city'], o['quantity'], o['price'], o['commission'],
                       o['delivery_date'], o['order_date']))
        if len(batch) >= 500:
            cur.executemany(
                'INSERT INTO orders (period_id, store, bpid, category, product_name, customer, user_city, quantity, price, commission, delivery_date, order_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                batch
            )
            batch = []

    if batch:
        cur.executemany(
            'INSERT INTO orders (period_id, store, bpid, category, product_name, customer, user_city, quantity, price, commission, delivery_date, order_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            batch
        )

    pg.commit()
    sq.close()
    pg.close()

    print(f"\nDONE! Migrated {len(periods)} periods, {len(sellers)} sellers, {len(orders)} orders")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_to_pg.py <DATABASE_URL>")
        sys.exit(1)
    migrate(sys.argv[1])
