with open('server.py', 'r') as f:
    content = f.read()

# Fix 1: LLM prompt
old_prompt = """        prompt = f"Analyze this supply chain cost savings data for a hospital system:\\nTotal Potential Savings: ${total_savings:,.2f}\\nTop Manufacturers for savings: {mfrs}\\nTop Vendors for savings: {vendors}\\nWrite a short (2-3 paragraphs) executive summary on these findings with markdown formatting and a bulleted list of 2 actionable recommendations. Adopt the persona of 'Ask The Bee', a proprietary AI assistant, and explicitly welcome the user to Ask The Bee." """

new_prompt = """        prompt = f"Analyze this supply chain cost savings data for a hospital system:\\nTotal Potential Savings: ${total_savings:,.2f}\\nTop Manufacturers for savings: {mfrs}\\nTop Vendors for savings: {vendors}\\nWrite a short (2-3 paragraphs) executive summary on these findings with markdown formatting and a bulleted list of 2 actionable recommendations. Adopt the persona of 'Ask The Bee', a proprietary AI assistant. Do not include any greeting, introduction, or title. Start directly with the key findings and provide sufficient spacing between sections." """
content = content.replace(old_prompt, new_prompt)


# Fix 2: Add sorting to get_data
old_get_data = """        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        # Get filtered records count
        records_filtered = total_records
        if where_clause:
            records_filtered = con.execute(f"SELECT count(*) FROM {table_name} {where_clause}", params).fetchone()[0]
            
        # Get paginated data
        query = f"SELECT * FROM {table_name} {where_clause} LIMIT {length} OFFSET {start}"
        result = con.execute(query, params)"""

new_get_data = """        if conditions:
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
        result = con.execute(query, params)"""
content = content.replace(old_get_data, new_get_data)

with open('server.py', 'w') as f:
    f.write(content)
print("Updated server.py")
