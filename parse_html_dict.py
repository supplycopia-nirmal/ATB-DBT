import json
from bs4 import BeautifulSoup
import sys

def parse_file(filename):
    print(f"\n--- {filename} ---")
    with open(filename, 'r', encoding='utf-8') as f:
        html = f.read()
    
    # Extract the JS variables
    import re
    match = re.search(r'const rawData = (\[.*?\]);', html, re.DOTALL)
    if match:
        data = json.loads(match.group(1))
        # print the list of columns
        for row in data:
            col_name = row.get('column', '')
            desc = row.get('description', '')
            if 'price' in col_name.lower() or 'variance' in col_name.lower():
                print(f"{col_name}: {desc}")

parse_file("Consumption/UC_Health_Mapping_Diagram_and_Data_Dictionary.html")
parse_file("Spend/Mapping_Diagram_and_Data_Dictionary.html")
