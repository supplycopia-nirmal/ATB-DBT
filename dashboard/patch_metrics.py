with open("server.py", "r") as f:
    content = f.read()

metrics_endpoint = """
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
                "off_contract_spend": off_contract
            }
        })
    finally:
        con.close()

if __name__ == '__main__':
"""

if "/api/metrics" not in content:
    content = content.replace("if __name__ == '__main__':", metrics_endpoint)
    with open("server.py", "w") as f:
        f.write(content)

with open("app.js", "r") as f:
    app_js = f.read()

old_fetch = """        const cacheBuster = '?t=' + new Date().getTime();
        const response = await fetch('data.json' + cacheBuster);
        const data = await response.json();"""

new_fetch = """        const cacheBuster = '?t=' + new Date().getTime();
        const response = await fetch('/api/metrics' + cacheBuster);
        const data = await response.json();"""

app_js = app_js.replace(old_fetch, new_fetch)
with open("app.js", "w") as f:
    f.write(app_js)

print("Patched!")
