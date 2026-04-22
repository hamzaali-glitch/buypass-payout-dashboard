"""
BuyPass Seller Payout Dashboard
- Data is loaded via load_payout.py (run by Claude or manually)
- MEMs view seller details, orders, deliveries, amounts
- Switch between payout periods
- Invoice download per seller
"""

import os, base64, sqlite3
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, g

app = Flask(__name__)
app.secret_key = 'buypass-dashboard-2026'
DB_PATH = os.path.join(os.path.dirname(__file__), 'payout_data.db')


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop('db', None)
    if db:
        db.close()


def get_all_periods():
    db = get_db()
    rows = db.execute('SELECT * FROM periods ORDER BY sort_date DESC').fetchall()
    return [dict(r) for r in rows]


def get_period_data(period_id):
    db = get_db()
    period = db.execute('SELECT * FROM periods WHERE id = ?', (period_id,)).fetchone()
    if not period:
        return None, None, None, None
    sellers = [dict(r) for r in db.execute('SELECT * FROM sellers WHERE period_id = ? ORDER BY amount DESC', (period_id,)).fetchall()]
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

    # Top city
    city_rows = db.execute('''
        SELECT user_city, COUNT(*) as cnt FROM orders
        WHERE period_id = ? AND user_city != '-'
        GROUP BY user_city ORDER BY cnt DESC LIMIT 1
    ''', (period_id,)).fetchone()
    totals['top_city'] = dict(city_rows)['user_city'] if city_rows else '-'
    totals['top_city_orders'] = dict(city_rows)['cnt'] if city_rows else 0

    # Period-over-period comparison
    prev = db.execute('SELECT * FROM periods WHERE id < ? ORDER BY id DESC LIMIT 1', (period_id,)).fetchone()
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
        # Avg order comparison
        prev_avg = prev['total_payout'] / max(prev['total_orders'], 1)
        comparison['avg_delta'] = round(totals['avg_order'] - prev_avg, 1)
        comparison['avg_pct'] = round((totals['avg_order'] - prev_avg) / max(prev_avg, 1) * 100, 1)

    return dict(period), sellers, totals, comparison


def get_seller_orders(period_id, store_name):
    db = get_db()
    orders = [dict(r) for r in db.execute('SELECT * FROM orders WHERE period_id = ? AND store = ?', (period_id, store_name)).fetchall()]
    seller = db.execute('SELECT * FROM sellers WHERE period_id = ? AND store = ?', (period_id, store_name)).fetchone()
    return orders, dict(seller) if seller else None


@app.route('/')
def index():
    periods = get_all_periods()
    if periods:
        return redirect(url_for('dashboard', period_id=periods[0]['id']))
    return render_template('no_data.html')


@app.route('/dashboard/<int:period_id>')
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
                           logo_b64=logo_b64)


@app.route('/api/seller/<int:period_id>/<store_name>')
def seller_orders_api(period_id, store_name):
    orders, seller = get_seller_orders(period_id, store_name)
    return jsonify({'orders': orders, 'seller': seller})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
