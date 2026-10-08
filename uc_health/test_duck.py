import duckdb
con = duckdb.connect()
con.execute("SELECT * FROM read_csv_auto('../Data/UHC_CON_20260911020906.csv', delim='|', all_varchar=True, null_padding=True, ignore_errors=True, quote='\"', strict_mode=False) LIMIT 5")
print('Success')
