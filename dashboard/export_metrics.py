import duckdb
import json
import os

db_path = '../uc_health/uc_health.duckdb'
con = duckdb.connect(db_path)

def execute_query(query):
    return con.execute(query).fetchall()

def query_to_dict(query):
    # Returns list of dicts for JSON serialization
    result = con.execute(query)
    columns = [col[0] for col in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]

print("Gathering Metrics...")
metrics = {
    "raw_counts": {
        "consumption": execute_query("SELECT count(*) FROM main.stg_consumption")[0][0],
        "item_master": execute_query("SELECT count(*) FROM main.stg_item_master")[0][0],
        "invoices": execute_query("SELECT count(*) FROM main.stg_invoice")[0][0],
        "purchase_orders": execute_query("SELECT count(*) FROM main.stg_purchase_orders")[0][0],
        "contracts": execute_query("SELECT count(*) FROM main.stg_contracts")[0][0]
    },
    "classification": {
        "categories": {row[0]: row[1] for row in execute_query("SELECT custom_category, count(*) FROM main.int_item_master_profiled GROUP BY custom_category")}
    },
    "data_quality": {
        "average_score": round(execute_query("SELECT avg(data_quality_score) FROM main.int_item_master_profiled")[0][0], 1),
        "missing_vendors": execute_query("SELECT count(*) FROM main.int_item_master_profiled WHERE is_missing_vendor_code = true")[0][0]
    }
}

total_savings = execute_query("SELECT sum(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0")[0][0]
off_contract = execute_query("SELECT sum(total_acquisition_cost) FROM main.fct_cost_savings_analysis WHERE is_off_contract = true")[0][0]

metrics["savings"] = {
    "total_variance": round(total_savings, 2) if total_savings else 0,
    "off_contract_spend": round(off_contract, 2) if off_contract else 0,
    "by_category": {row[0]: round(row[1], 2) for row in execute_query("SELECT im_custom_category, sum(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 GROUP BY im_custom_category ORDER BY 2 DESC")}
}

with open('data.json', 'w') as f:
    json.dump(metrics, f, indent=4)
print("✅ Exported data.json")

# 1. Export Raw Tables (Top 50 rows)
print("Exporting Raw Table Previews...")
raw_tables = {
    "consumption": query_to_dict("SELECT * FROM main.stg_consumption LIMIT 50"),
    "item_master": query_to_dict("SELECT * FROM main.stg_item_master LIMIT 50"),
    "invoices": query_to_dict("SELECT * FROM main.stg_invoice LIMIT 50"),
    "purchase_orders": query_to_dict("SELECT * FROM main.stg_purchase_orders LIMIT 50"),
    "contracts": query_to_dict("SELECT * FROM main.stg_contracts LIMIT 50")
}
with open('raw_tables.json', 'w') as f:
    json.dump(raw_tables, f, indent=4, default=str)
print("✅ Exported raw_tables.json")

# 2. Export Top Cost Saving Items
print("Exporting Cost Savings Rationale...")
savings_query = """
    SELECT 
        item_number as item_id, item_description, im_custom_category as custom_category, im_vendor_name as vendor_name,
        round(COALESCE(con_contract_ea_price, con_contract_price, im_contract_price), 2) as contract_price,
        round(consumption_unit_price, 2) as actual_price,
        total_quantity,
        round(price_variance, 2) as total_savings
    FROM main.fct_cost_savings_analysis
    WHERE price_variance > 0
    ORDER BY price_variance DESC
    LIMIT 100
"""
item_savings = query_to_dict(savings_query)
with open('item_savings.json', 'w') as f:
    json.dump(item_savings, f, indent=4, default=str)
print("✅ Exported item_savings.json")

# 3. Export Transformed Dataset to CSV
print("Exporting Full Transformed Dataset...")
con.execute("COPY main.fct_cost_savings_analysis TO 'transformed_data.csv' (HEADER, DELIMITER ',')")
print("✅ Exported transformed_data.csv")

con.close()
