def model(dbt, session):
    dbt.config(materialized="table", packages=["pandas", "pyarrow"])
    import pandas as pd
    
    # Let's see what happens when we use dbt.source
    df = dbt.source("supplycopia_raw", "UHC_PO_20260930010955").df()
    
    return df
