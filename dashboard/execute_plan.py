import re

# 1. Update index.html
with open('index.html', 'r') as f:
    html = f.read()

# Remove the subtitle
html = html.replace('<p class="section-desc" style="margin: 5px 0 0 0;">Powered by Proprietary Ask The Bee</p>', '')

with open('index.html', 'w') as f:
    f.write(html)

# 2. Update server.py
with open('server.py', 'r') as f:
    server = f.read()

old_db_logic = """    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        total_savings = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0").fetchone()[0]
        mfrs = con.execute("SELECT manufacturer_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND manufacturer_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        vendors = con.execute("SELECT po_vendor_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND po_vendor_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
    finally:
        con.close()

    prompt = f"Analyze this supply chain cost savings data:\nTotal Potential Savings: ${total_savings:,.2f}\nTop Manufacturers for savings: {mfrs}\nTop Vendors for savings: {vendors}\nWrite 2 paragraphs and a bulleted list of 2 actionable recommendations. Output ONLY the raw analytical content. Do NOT include ANY headers, titles, greetings, or introductions.\""""

new_db_logic = """    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        total_savings = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0").fetchone()[0] or 0
        off_contract_savings = con.execute("SELECT SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND is_off_contract = TRUE").fetchone()[0] or 0
        
        mfrs = con.execute("SELECT manufacturer_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND manufacturer_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        vendors = con.execute("SELECT po_vendor_name, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND po_vendor_name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        procedures = con.execute("SELECT primary_procedure_group, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND primary_procedure_group IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        facilities = con.execute("SELECT facility, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND facility IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
        items = con.execute("SELECT item_description, SUM(price_variance) FROM main.fct_cost_savings_analysis WHERE price_variance > 0 AND item_description IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 3").fetchall()
    finally:
        con.close()

    prompt = f\"\"\"Analyze this hospital supply chain cost savings data:
Total Potential Savings: ${total_savings:,.2f}
Off-Contract Maverick Spend Savings: ${off_contract_savings:,.2f}

Top Contributors to Savings:
- Procedures: {procedures}
- Facilities: {facilities}
- Items: {items}
- Vendors: {vendors}

Write exactly 2 short paragraphs summarizing the biggest savings opportunities (mentioning specific facilities or procedures), followed by a bulleted list of 2 concrete, actionable sourcing recommendations.
CRITICAL INSTRUCTIONS:
- Use markdown formatting! Bold (**text**) key entities, numbers, and findings to maximize readability.
- Output ONLY the raw analytical paragraphs and bullets. Do NOT include ANY headers (e.g. # Executive Summary), titles, greetings, or introductions.
\"\"\""""

server = server.replace(old_db_logic, new_db_logic)

with open('server.py', 'w') as f:
    f.write(server)

print("Execution complete!")
