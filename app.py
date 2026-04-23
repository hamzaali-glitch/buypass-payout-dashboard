"""
BuyPass Seller Payout Dashboard
- Google SSO authentication (restricted to @buypass.ai)
- PostgreSQL cloud database (Railway/Supabase)
- Falls back to SQLite for local development
- Invoice download per seller
"""

import os, base64
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, g, session
from authlib.integrations.flask_client import OAuth
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
app.secret_key = os.environ.get('SECRET_KEY', 'buypass-dashboard-dev-2026')

# ===== Database Setup =====
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    # PostgreSQL (Railway/Cloud)
    import psycopg2
    import psycopg2.extras

    def get_db():
        if 'db' not in g:
            g.db = psycopg2.connect(DATABASE_URL)
            g.db.autocommit = False
        return g.db

    def db_execute(query, params=None):
        db = get_db()
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(query, params)
        return cur

    def db_fetchall(query, params=None):
        cur = db_execute(query, params)
        return [dict(r) for r in cur.fetchall()]

    def db_fetchone(query, params=None):
        cur = db_execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None

    @app.teardown_appcontext
    def close_db(exc):
        db = g.pop('db', None)
        if db:
            if exc:
                db.rollback()
            else:
                db.commit()
            db.close()
else:
    # SQLite (local development)
    import sqlite3

    DB_PATH = os.path.join(os.path.dirname(__file__), 'payout_data.db')

    def get_db():
        if 'db' not in g:
            g.db = sqlite3.connect(DB_PATH)
            g.db.row_factory = sqlite3.Row
        return g.db

    def db_fetchall(query, params=None):
        db = get_db()
        rows = db.execute(query, params or ()).fetchall()
        return [dict(r) for r in rows]

    def db_fetchone(query, params=None):
        db = get_db()
        row = db.execute(query, params or ()).fetchone()
        return dict(row) if row else None

    @app.teardown_appcontext
    def close_db(exc):
        db = g.pop('db', None)
        if db:
            db.close()


# ===== Google SSO Setup =====
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '').strip() or None
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET', '').strip() or None
ALLOWED_DOMAIN = 'buypass.ai'

oauth = OAuth(app)
if GOOGLE_CLIENT_ID:
    google = oauth.register(
        name='google',
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
        client_kwargs={'scope': 'openid email profile'},
    )


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Skip auth in local dev (no GOOGLE_CLIENT_ID)
        if not GOOGLE_CLIENT_ID:
            return f(*args, **kwargs)
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


@app.route('/login')
def login():
    if not GOOGLE_CLIENT_ID:
        return redirect(url_for('index'))
    redirect_uri = url_for('auth_callback', _external=True)
    return google.authorize_redirect(redirect_uri, hd=ALLOWED_DOMAIN)


@app.route('/auth/callback')
def auth_callback():
    token = google.authorize_access_token()
    user_info = token.get('userinfo')
    if not user_info:
        flash('Authentication failed.', 'danger')
        return redirect(url_for('login'))

    email = user_info.get('email', '')
    if not email.endswith(f'@{ALLOWED_DOMAIN}'):
        flash(f'Access denied. Only @{ALLOWED_DOMAIN} emails are allowed.', 'danger')
        return redirect(url_for('login'))

    session['user'] = {
        'email': email,
        'name': user_info.get('name', email),
        'picture': user_info.get('picture', ''),
    }
    return redirect(url_for('index'))


@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))


# ===== Dashboard Routes =====

def get_all_periods():
    return db_fetchall('SELECT * FROM periods ORDER BY sort_date DESC')


def get_period_data(period_id):
    period = db_fetchone('SELECT * FROM periods WHERE id = %s' if DATABASE_URL else 'SELECT * FROM periods WHERE id = ?', (period_id,))
    if not period:
        return None, None, None, None

    ph = '%s' if DATABASE_URL else '?'
    sellers = db_fetchall(f'SELECT * FROM sellers WHERE period_id = {ph} ORDER BY amount DESC', (period_id,))
    totals = {
        'total_sellers': period['total_sellers'],
        'total_orders': period['total_orders'],
        'total_payout': period['total_payout'],
    }

    # Avg order value
    if totals['total_orders'] > 0:
        totals['avg_order'] = totals['total_payout'] / totals['total_orders']
    else:
        totals['avg_order'] = 0

    # Period-over-period comparison
    prev = db_fetchone(
        f'SELECT * FROM periods WHERE sort_date < {ph} ORDER BY sort_date DESC LIMIT 1' if period.get('sort_date')
        else f'SELECT * FROM periods WHERE id < {ph} ORDER BY id DESC LIMIT 1',
        (period.get('sort_date') or period_id,)
    )
    comparison = None
    if prev:
        comparison = {
            'prev_label': prev['period_label'],
            'sellers_delta': period['total_sellers'] - prev['total_sellers'],
            'orders_delta': period['total_orders'] - prev['total_orders'],
            'payout_delta': period['total_payout'] - prev['total_payout'],
            'sellers_pct': round((period['total_sellers'] - prev['total_sellers']) / max(prev['total_sellers'], 1) * 100, 1),
            'orders_pct': round((period['total_orders'] - prev['total_orders']) / max(prev['total_orders'], 1) * 100, 1),
            'payout_pct': round((period['total_payout'] - prev['total_payout']) / max(prev['total_payout'], 1) * 100, 1),
        }
        prev_avg = prev['total_payout'] / max(prev['total_orders'], 1)
        comparison['avg_delta'] = round(totals['avg_order'] - prev_avg, 1)
        comparison['avg_pct'] = round((totals['avg_order'] - prev_avg) / max(prev_avg, 1) * 100, 1)

    return period, sellers, totals, comparison


def get_seller_orders(period_id, store_name):
    ph = '%s' if DATABASE_URL else '?'
    orders = db_fetchall(f'SELECT * FROM orders WHERE period_id = {ph} AND store = {ph}', (period_id, store_name))
    seller = db_fetchone(f'SELECT * FROM sellers WHERE period_id = {ph} AND store = {ph}', (period_id, store_name))
    return orders, seller


def get_all_periods_data():
    """Aggregate sellers across all periods for consolidated ledger view."""
    ph = '%s' if DATABASE_URL else '?'
    # Aggregate sellers: group by store, sum amounts and orders
    sellers = db_fetchall('''
        SELECT s.store,
               SUM(s.amount) as amount,
               SUM(s.total_orders) as total_orders,
               MAX(s.biz_type) as biz_type,
               MAX(s.iban) as iban,
               MAX(s.bank) as bank,
               MAX(s.account_title) as account_title,
               COUNT(DISTINCT s.period_id) as periods_count
        FROM sellers s
        GROUP BY s.store
        ORDER BY SUM(s.amount) DESC
    ''')
    total_sellers = len(sellers)
    total_orders = sum(s['total_orders'] for s in sellers)
    total_payout = sum(s['amount'] for s in sellers)
    totals = {
        'total_sellers': total_sellers,
        'total_orders': total_orders,
        'total_payout': total_payout,
        'avg_order': total_payout / total_orders if total_orders > 0 else 0,
    }
    period = {
        'id': 'all',
        'period_label': 'All Periods',
        'payment_date': None,
    }
    return period, sellers, totals


def get_all_periods_seller_orders(store_name):
    """Get all orders for a seller across all periods, with period labels."""
    ph = '%s' if DATABASE_URL else '?'
    orders = db_fetchall(f'''
        SELECT o.*, p.period_label
        FROM orders o
        JOIN periods p ON o.period_id = p.id
        WHERE o.store = {ph}
        ORDER BY p.sort_date DESC, o.id
    ''', (store_name,))
    # Aggregate seller info across all periods
    seller = db_fetchone(f'''
        SELECT store,
               SUM(amount) as amount,
               SUM(total_orders) as total_orders,
               MAX(biz_type) as biz_type,
               MAX(iban) as iban,
               MAX(bank) as bank,
               MAX(account_title) as account_title
        FROM sellers
        WHERE store = {ph}
        GROUP BY store
    ''', (store_name,))
    return orders, seller


@app.route('/')
@login_required
def index():
    periods = get_all_periods()
    if periods:
        return redirect(url_for('dashboard', period_id=periods[0]['id']))
    return render_template('no_data.html', user=session.get('user'))


@app.route('/dashboard/all')
@login_required
def dashboard_all():
    periods = get_all_periods()
    period, sellers, totals = get_all_periods_data()

    logo_path = os.path.join(os.path.dirname(__file__), 'static', 'logo.png')
    with open(logo_path, 'rb') as lf:
        logo_b64 = 'data:image/png;base64,' + base64.b64encode(lf.read()).decode()

    return render_template('dashboard.html',
                           sellers=sellers,
                           totals=totals,
                           period=period,
                           periods=periods,
                           current_period_id='all',
                           comparison=None,
                           logo_b64=logo_b64,
                           is_all_periods=True,
                           user=session.get('user'))


@app.route('/dashboard/<int:period_id>')
@login_required
def dashboard(period_id):
    periods = get_all_periods()
    period, sellers, totals, comparison = get_period_data(period_id)
    if not period:
        return redirect(url_for('index'))

    # Load logo as base64 for invoice generation
    logo_path = os.path.join(os.path.dirname(__file__), 'static', 'logo.png')
    with open(logo_path, 'rb') as lf:
        logo_b64 = 'data:image/png;base64,' + base64.b64encode(lf.read()).decode()

    return render_template('dashboard.html',
                           sellers=sellers,
                           totals=totals,
                           period=period,
                           periods=periods,
                           current_period_id=period_id,
                           comparison=comparison,
                           logo_b64=logo_b64,
                           is_all_periods=False,
                           user=session.get('user'))


@app.route('/api/seller/all/<store_name>')
@login_required
def seller_orders_all_api(store_name):
    orders, seller = get_all_periods_seller_orders(store_name)
    return jsonify({'orders': orders, 'seller': seller, 'is_all_periods': True})


@app.route('/api/seller/<int:period_id>/<store_name>')
@login_required
def seller_orders_api(period_id, store_name):
    orders, seller = get_seller_orders(period_id, store_name)
    return jsonify({'orders': orders, 'seller': seller, 'is_all_periods': False})


@app.route('/health')
def health():
    return jsonify({
        'auth_enabled': GOOGLE_CLIENT_ID is not None,
        'db_type': 'postgresql' if DATABASE_URL else 'sqlite',
        'client_id_set': bool(GOOGLE_CLIENT_ID),
        'client_id_prefix': GOOGLE_CLIENT_ID[:20] + '...' if GOOGLE_CLIENT_ID else None,
    })


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
