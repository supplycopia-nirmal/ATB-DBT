import os
import math
import json
import urllib.request
from flask import Flask, request, jsonify, send_from_directory, send_file
import duckdb
import csv
from io import StringIO
from werkzeug.wrappers import Response
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))
load_dotenv('.env')

app = Flask(__name__, static_folder='.')

DB_PATH = '../uc_health/uc_health.duckdb'

# Define allowed tables to prevent SQL injection
ALLOWED_TABLES = {
    'consumption': 'main.int_consumption_profiled',
    'purchase_orders': 'main.stg_purchase_orders',
    'invoices': 'main.stg_invoice',
    'item_master': 'main.int_item_master_profiled',
    'contracts': 'main.stg_contracts',
    'transformed': 'main.fct_cost_savings_analysis',
    'po_cost_savings_v2': 'main.fct_po_cost_savings_analysis_v2',
    'cons_cost_savings_v2': 'main.fct_consumption_cost_savings_analysis_v2',
    'po_cost_savings_v3': 'main.fct_po_cost_savings_analysis_v3',
    'cons_cost_savings_v3': 'main.fct_consumption_cost_savings_analysis_v3',
    'missing_po_v3': 'main.missing_po_rows_v3',
    'missing_cons_v3': 'main.missing_cons_rows_v3',
    'po_cost_savings_v4': 'main.fct_po_cost_savings_v4',
    'cons_cost_savings_v4': 'main.fct_consumption_cost_savings_v4',
    'gap_analysis_v4': 'main.fct_gap_analysis_v4'
}

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def static_files(path):
    return send_from_directory('.', path)

@app.route('/api/data', methods=['GET'])
def get_data():
    table_key = request.args.get('table', 'consumption')
    draw = int(request.args.get('draw', 1))
    start = int(request.args.get('start', 0))
    length = int(request.args.get('length', 10))
    
    if table_key not in ALLOWED_TABLES:
        return jsonify({"error": "Invalid table"}), 400
        
    table_name = ALLOWED_TABLES[table_key]
    
    # Optional search value (Global search from DataTables)
    search_value = request.args.get('search[value]', '').strip()
    
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        # Get total records
        total_records = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        
        # Build search clause if needed
        where_clause = ""
        params = []
        conditions = []

        
        missing_vendors = request.args.get('missing_vendors') == 'true'
        cons_filter = request.args.get('cons_filter', '')
        po_filter = request.args.get('po_filter', '')

        if missing_vendors and table_key == 'item_master':
            conditions.append("is_missing_vendor_code = true")
            
        if table_key == 'transformed':
            conditions.append("price_variance > 0")
            
        if table_key == 'consumption' and cons_filter:
            conditions.append(f"facility = '{cons_filter.replace(chr(39), chr(39)+chr(39))}'")
            
        if table_key == 'purchase_orders' and po_filter:
            conditions.append(f"facility_name = '{po_filter.replace(chr(39), chr(39)+chr(39))}'")


        if search_value:
            # We will just cast the whole row to a string and search it.
            # In production, we'd search specific columns for performance, but DuckDB is fast.
            columns = [col[0] for col in con.execute(f"DESCRIBE {table_name}").fetchall()]
            search_conditions = [f'CAST("{col}" AS VARCHAR) ILIKE ?' for col in columns]
            conditions.append("(" + " OR ".join(search_conditions) + ")")
            params = [f"%{search_value}%"] * len(columns)

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
            
        # Sorting support for DataTables
        order_clause = ""
        order_col_idx = request.args.get('order[0][column]')
        order_dir = request.args.get('order[0][dir]', 'asc').upper()
        
        if order_col_idx is not None:
            # Get the exact column name requested
            col_name = request.args.get(f'columns[{order_col_idx}][data]')
            # Validate it exists in the schema to prevent SQL injection
            table_cols = [col[0] for col in con.execute(f"DESCRIBE {table_name}").fetchall()]
            if col_name in table_cols and order_dir in ('ASC', 'DESC'):
                order_clause = f' ORDER BY "{col_name}" {order_dir}'
        
        # Get filtered records count
        records_filtered = total_records
        if where_clause:
            records_filtered = con.execute(f"SELECT count(*) FROM {table_name} {where_clause}", params).fetchone()[0]
            
        # Get paginated data
        query = f"SELECT * FROM {table_name} {where_clause}{order_clause} LIMIT {length} OFFSET {start}"
        result = con.execute(query, params)
        columns = [col[0] for col in result.description]
        data = [dict(zip(columns, row)) for row in result.fetchall()]
        
        return jsonify({
            "draw": draw,
            "recordsTotal": total_records,
            "recordsFiltered": records_filtered,
            "data": data,
            "columns": columns
        })
        
    finally:
        con.close()

@app.route('/api/download', methods=['GET'])
def download():
    table_key = request.args.get('table')
    
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        if table_key == 'transformed':
            # Use Pandas to stream or just dump to CSV file temporarily
            df = con.execute("SELECT * FROM main.fct_cost_savings_analysis").fetchdf()
            csv_data = df.to_csv(index=False)
            return Response(
                csv_data,
                mimetype="text/csv",
                headers={"Content-disposition": "attachment; filename=transformed_data.csv"}
            )
        elif table_key in ALLOWED_TABLES:
            table_name = ALLOWED_TABLES[table_key]
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            csv_data = df.to_csv(index=False)
            return Response(
                csv_data,
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename={table_key}_raw.csv"}
            )
        else:
            return jsonify({"error": "Invalid table"}), 400
    finally:
        con.close()

@app.route('/dbt-docs/<path:path>')
def dbt_docs(path):
    if not path or path == '/':
        path = 'index.html'
    return send_from_directory('../uc_health/target', path)

@app.route('/dbt-docs/')
def dbt_docs_index():
    return send_from_directory('../uc_health/target', 'index.html')

@app.route('/api/savings/by-manufacturer', methods=['GET'])
def savings_by_mfr():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        query = """
            SELECT manufacturer_name, SUM(price_variance) as total_savings
            FROM main.fct_cost_savings_analysis
            WHERE price_variance > 0 AND manufacturer_name IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """
        result = con.execute(query).fetchall()
        return jsonify([{"name": r[0], "savings": r[1]} for r in result])
    finally:
        con.close()

@app.route('/api/savings/by-vendor', methods=['GET'])
def savings_by_vendor():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        query = """
            SELECT im_vendor_name, SUM(price_variance) as total_savings
            FROM main.fct_cost_savings_analysis
            WHERE price_variance > 0 AND im_vendor_name IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """
        result = con.execute(query).fetchall()
        return jsonify([{"name": r[0], "savings": r[1]} for r in result])
    finally:
        con.close()

@app.route('/api/savings/by-procedure', methods=['GET'])
def savings_by_procedure():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        query = """
            SELECT primary_procedure_group, SUM(price_variance) as total_savings
            FROM main.fct_cost_savings_analysis
            WHERE price_variance > 0 AND primary_procedure_group IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 5
        """
        result = con.execute(query).fetchall()
        return jsonify([{"name": r[0], "savings": r[1]} for r in result])
    finally:
        con.close()

@app.route('/api/generate_insights', methods=['POST'])
def generate_insights():
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        return jsonify({"error": "OPENAI_API_KEY not found in environment. Please check the .env file."}), 500

    # Get aggregated data to send to the LLM
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        total_savings = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0").fetchone()[0] or 0
        off_contract = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND is_off_contract = TRUE").fetchone()[0] or 0
        mfrs = con.execute("SELECT manufacturer_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND manufacturer_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        vendors = con.execute("SELECT po_vendor_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND po_vendor_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        procedures = con.execute("SELECT primary_procedure_group, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND primary_procedure_group IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        facilities = con.execute("SELECT facility, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND facility IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        items = con.execute("SELECT item_description, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND item_description IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
    finally:
        con.close()

    prompt = f"""Analyze this hospital supply chain cost savings data:
Total Potential Savings: ${total_savings:,.2f}
Off-Contract Maverick Spend Savings: ${off_contract:,.2f}

Top Contributors to Savings:
- Procedures: {procedures}
- Facilities: {facilities}
- Items: {items}
- Vendors: {vendors}

Write exactly 2 short paragraphs summarizing the biggest savings opportunities (mentioning specific facilities or procedures), followed by a bulleted list of 2 concrete, actionable sourcing recommendations.
CRITICAL INSTRUCTIONS:
- Use markdown formatting! Bold (**text**) key entities, numbers, and findings to maximize readability.
- Output ONLY the raw analytical paragraphs and bullets. Do NOT include ANY headers (e.g. # Executive Summary), titles, greetings, or introductions.
"""

    model_name = os.environ.get('OPENAI_MODEL', 'gpt-5.6-luna')

    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}]
        }).encode('utf-8')
    )

    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode('utf-8'))
            return jsonify({"markdown": result['choices'][0]['message']['content']})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/data-health', methods=['GET'])
def data_health():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        health_data = {}
        
        # Consumption (admit_date_time)
        cons_res = con.execute("""
            WITH dates AS (
                SELECT CAST(admit_date_time AS DATE) as d FROM main.int_consumption_profiled WHERE admit_date_time IS NOT NULL GROUP BY 1
            ), min_max AS (
                SELECT MIN(d) as min_d, MAX(d) as max_d FROM dates
            )
            SELECT min_d, max_d, (max_d - min_d + 1) as total_days, (SELECT count(*) FROM dates) as actual_days FROM min_max
        """).fetchone()
        
        health_data['consumption'] = {
            'min_date': str(cons_res[0]) if cons_res[0] else None,
            'max_date': str(cons_res[1]) if cons_res[1] else None,
            'missing_days': int(cons_res[2] - cons_res[3]) if cons_res[2] else 0
        }
        
        # Purchase Orders (po_date)
        po_res = con.execute("""
            WITH dates AS (
                SELECT CAST(po_date AS DATE) as d FROM main.stg_purchase_orders WHERE po_date IS NOT NULL GROUP BY 1
            ), min_max AS (
                SELECT MIN(d) as min_d, MAX(d) as max_d FROM dates
            )
            SELECT min_d, max_d, (max_d - min_d + 1) as total_days, (SELECT count(*) FROM dates) as actual_days FROM min_max
        """).fetchone()
        
        health_data['purchase_orders'] = {
            'min_date': str(po_res[0]) if po_res[0] else None,
            'max_date': str(po_res[1]) if po_res[1] else None,
            'missing_days': int(po_res[2] - po_res[3]) if po_res[2] else 0
        }
        
        # Invoices (invoice_paid_date)
        inv_res = con.execute("""
            WITH dates AS (
                SELECT CAST(invoice_paid_date AS DATE) as d FROM main.stg_invoice WHERE invoice_paid_date IS NOT NULL GROUP BY 1
            ), min_max AS (
                SELECT MIN(d) as min_d, MAX(d) as max_d FROM dates
            )
            SELECT min_d, max_d, (max_d - min_d + 1) as total_days, (SELECT count(*) FROM dates) as actual_days FROM min_max
        """).fetchone()
        
        health_data['invoices'] = {
            'min_date': str(inv_res[0]) if inv_res[0] else None,
            'max_date': str(inv_res[1]) if inv_res[1] else None,
            'missing_days': int(inv_res[2] - inv_res[3]) if inv_res[2] else 0
        }
        
        # Contracts (contract_start_date)
        con_res = con.execute("""
            WITH dates AS (
                SELECT CAST(contract_start_date AS DATE) as d FROM main.stg_contracts WHERE contract_start_date IS NOT NULL GROUP BY 1
            ), min_max AS (
                SELECT MIN(d) as min_d, MAX(d) as max_d FROM dates
            )
            SELECT min_d, max_d, (max_d - min_d + 1) as total_days, (SELECT count(*) FROM dates) as actual_days FROM min_max
        """).fetchone()
        
        health_data['contracts'] = {
            'min_date': str(con_res[0]) if con_res[0] else None,
            'max_date': str(con_res[1]) if con_res[1] else None,
            'missing_days': int(con_res[2] - con_res[3]) if con_res[2] else 0
        }
        
        return jsonify(health_data)
    finally:
        con.close()

@app.route('/api/volume-trend', methods=['GET'])
def volume_trend():
    dataset = request.args.get('dataset', 'consumption')
    
    table_map = {
        'consumption': ('main.int_consumption_profiled', 'admit_date_time'),
        'po': ('main.stg_purchase_orders', 'po_date'),
        'inv': ('main.stg_invoice', 'invoice_paid_date'),
        'con': ('main.stg_contracts', 'contract_start_date')
    }
    
    if dataset not in table_map:
        return jsonify({"error": "Invalid dataset"}), 400
        
    table, date_col = table_map[dataset]
    
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        query = f"""
            SELECT CAST({date_col} AS DATE) as d, COUNT(*) as c
            FROM {table}
            WHERE {date_col} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """
        rows = con.execute(query).fetchall()
        result = [{"date": str(r[0]), "count": int(r[1])} for r in rows]
        return jsonify(result)
    finally:
        con.close()



@app.route('/api/raw_stats', methods=['GET'])
def get_raw_stats():
    cons_filter = request.args.get('cons_filter', '')
    po_filter = request.args.get('po_filter', '')
    
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        cons_where = ""
        po_where = ""
        
        if cons_filter:
            cons_where = f"WHERE facility = '{cons_filter.replace(chr(39), chr(39)+chr(39))}'"
            
        if po_filter:
            po_where = f"WHERE facility_name = '{po_filter.replace(chr(39), chr(39)+chr(39))}'"
            
        cons_vol = con.execute(f"SELECT count(*) FROM main.int_consumption_profiled {cons_where}").fetchone()[0] or 0
        cons_val = con.execute(f"SELECT SUM(total_acquisition_cost) FROM main.int_consumption_profiled {cons_where}").fetchone()[0] or 0
        
        po_vol = con.execute(f"SELECT count(*) FROM main.stg_purchase_orders {po_where}").fetchone()[0] or 0
        po_val = con.execute(f"SELECT SUM(total_value) FROM main.stg_purchase_orders {po_where}").fetchone()[0] or 0
        
        return jsonify({
            "consumption": {"volume": cons_vol, "value": cons_val},
            "purchase_orders": {"volume": po_vol, "value": po_val}
        })
    finally:
        con.close()

@app.route('/api/filter_options', methods=['GET'])
def get_filter_options():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        cons_facilities = [r[0] for r in con.execute("SELECT DISTINCT facility FROM main.int_consumption_profiled WHERE facility IS NOT NULL").fetchall()]
        po_facilities = [r[0] for r in con.execute("SELECT DISTINCT facility_name FROM main.stg_purchase_orders WHERE facility_name IS NOT NULL").fetchall()]
        return jsonify({
            "cons_facilities": cons_facilities,
            "po_facilities": po_facilities
        })
    finally:
        con.close()

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        total_variance = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0").fetchone()[0] or 0
        off_contract = con.execute("SELECT SUM(total_acquisition_cost) FROM main.fct_cost_savings_analysis WHERE is_off_contract = true").fetchone()[0] or 0
        
        consumption = con.execute("SELECT count(*) FROM main.int_consumption_profiled").fetchone()[0] or 0
        item_master = con.execute("SELECT count(*) FROM main.int_item_master_profiled").fetchone()[0] or 0
        invoices = con.execute("SELECT count(*) FROM main.stg_invoice").fetchone()[0] or 0
        purchase_orders = con.execute("SELECT count(*) FROM main.stg_purchase_orders").fetchone()[0] or 0
        contracts = con.execute("SELECT count(*) FROM main.stg_contracts").fetchone()[0] or 0
        
        avg_score = con.execute("SELECT AVG(data_quality_score) FROM main.int_item_master_profiled").fetchone()[0] or 0
        missing_vendors = con.execute("SELECT count(*) FROM main.int_item_master_profiled WHERE is_missing_vendor_code = true").fetchone()[0] or 0
        try:
            total_variance_po_v2 = con.execute("SELECT SUM(price_variance) FROM main.fct_po_cost_savings_analysis_v2 WHERE price_variance > 0").fetchone()[0] or 0
            total_variance_cons_v2 = con.execute("SELECT SUM(price_variance) FROM main.fct_consumption_cost_savings_analysis_v2 WHERE price_variance > 0").fetchone()[0] or 0
            total_variance_po = con.execute("SELECT SUM(price_variance2) FROM main.fct_po_cost_savings_analysis_v3 WHERE price_variance2 > 0").fetchone()[0] or 0
            total_variance_cons = con.execute("SELECT SUM(price_variance2) FROM main.fct_consumption_cost_savings_analysis_v3 WHERE price_variance2 > 0").fetchone()[0] or 0
            total_variance_po_v4 = con.execute("SELECT SUM(price_variance2) FROM main.fct_po_cost_savings_v4 WHERE price_variance2 > 0").fetchone()[0] or 0
            total_variance_cons_v4 = con.execute("SELECT SUM(price_variance2) FROM main.fct_consumption_cost_savings_v4 WHERE price_variance2 > 0").fetchone()[0] or 0
            total_savings_opp_cons_v4 = con.execute("SELECT SUM(savings_opportunity) FROM main.fct_consumption_cost_savings_v4").fetchone()[0] or 0
            total_savings_opp_po_v4 = con.execute("SELECT SUM(savings_opportunity) FROM main.fct_po_cost_savings_v4").fetchone()[0] or 0
        except Exception:
            total_variance_po_v2 = 0
            total_variance_cons_v2 = 0
            total_variance_po = 0
            total_variance_cons = 0
            total_variance_po_v4 = 0
            total_variance_cons_v4 = 0
            total_savings_opp_cons_v4 = 0
            total_savings_opp_po_v4 = 0

        return jsonify({
            "raw_counts": {
                "consumption": consumption,
                "item_master": item_master,
                "invoices": invoices,
                "purchase_orders": purchase_orders,
                "contracts": contracts
            },
            "data_quality": {
                "average_score": round(avg_score, 1),
                "missing_vendors": missing_vendors
            },
            "savings": {
                "total_variance": total_variance,
                "off_contract_spend": off_contract,
                "total_variance_po_v2": total_variance_po_v2,
                "total_variance_cons_v2": total_variance_cons_v2,
                "total_variance_po": total_variance_po,
                "total_variance_cons": total_variance_cons,
                "total_variance_po_v4": total_variance_po_v4,
                "total_variance_cons_v4": total_variance_cons_v4,
                "total_savings_opp_cons_v4": total_savings_opp_cons_v4,
                "total_savings_opp_po_v4": total_savings_opp_po_v4
            }
        })
    finally:
        con.close()

if __name__ == '__main__':

    print("🌟 Starting Flask Dashboard Server on http://localhost:8080...")
    app.run(host='0.0.0.0', port=8080, threaded=True)
