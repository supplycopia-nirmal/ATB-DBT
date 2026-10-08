import os
import sys
import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv('/Users/nirmalrayan/Documents/UC_Health/.env')

private_key_path = os.getenv("PRIVATE_KEY_PATH")
private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")

with open(private_key_path, "rb") as key_file:
    p_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_passphrase.encode() if private_key_passphrase else None,
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

ctx = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=pkb,
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

db_name = os.getenv("SNOWFLAKE_DATABASE")
schema_name = os.getenv("SNOWFLAKE_SCHEMA")

tables = [
    "UC_HEALTH_STG_PURCHASE_ORDERS_V1",
    "UC_HEALTH_STG_CONTRACTS_V1",
    "UC_HEALTH_FCT_COST_SAVINGS_ANALYSIS_V1",
    "UC_HEALTH_FCT_PO_COST_SAVINGS_ANALYSIS_V2",
    "UC_HEALTH_FCT_CONSUMPTION_COST_SAVINGS_ANALYSIS_V2"
]

with open("/Users/nirmalrayan/.gemini/antigravity/brain/c3eb8abd-6329-4327-8df8-28d33e86c183/snowflake_schemas.md", "w") as f:
    f.write("# Snowflake Table Schemas\\n\\n")
    for t in tables:
        f.write(f"## `{t}`\\n\\n")
        query = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{t}' AND TABLE_SCHEMA = '{schema_name}' ORDER BY ORDINAL_POSITION"
        cur = ctx.cursor().execute(query)
        rows = cur.fetchall()
        if not rows:
            f.write("Table not found or no columns.\\n\\n")
            continue
        f.write("| Column Name | Data Type | Nullable |\\n")
        f.write("| ----------- | --------- | -------- |\\n")
        for r in rows:
            f.write(f"| {r[0]} | {r[1]} | {r[2]} |\\n")
        f.write("\\n")

print("Schemas extracted successfully.")
