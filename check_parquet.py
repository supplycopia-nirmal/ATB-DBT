import pandas as pd
df = pd.read_parquet('Consumption/consumption_enriched.parquet')
print(df.columns.tolist())
