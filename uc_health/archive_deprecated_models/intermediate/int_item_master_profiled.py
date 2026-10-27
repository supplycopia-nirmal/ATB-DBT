def model(dbt, session):
    # Configure the model to be materialized as a table and ensure pandas is available
    dbt.config(
        materialized="table",
        packages=["pandas"]
    )
    
    # Read the staging item master table into a pandas DataFrame
    df = dbt.ref("stg_item_master").df()
    
    import pandas as pd
    qoe = pd.to_numeric(df['contract_qoe'], errors='coerce')
    qoe = qoe.replace(0, pd.NA)
    df['im_unit_contract_price'] = df['contract_price'] / qoe

    # ---------------------------------------------------------
    # 1. PROFILING
    # ---------------------------------------------------------
    
    # Flag items that are missing critical vendor information
    df['is_missing_vendor_code'] = df['vendor_code'].isnull()
    
    # Flag items missing manufacturer name
    df['is_missing_mfr_name'] = df['mfr_name'].isnull()
    
    # ---------------------------------------------------------
    # 2. CLASSIFICATION (Custom Python Logic)
    # ---------------------------------------------------------
    
    def classify_item(row):
        description = str(row['item_description']).upper()
        unspsc = str(row['unspsc_description']).upper()
        
        # Combined text for broader matching
        search_text = description + ' ' + unspsc
        
        # Medical Supply Taxonomy
        if any(kw in search_text for kw in ['IMPLANT', 'SCREW', 'PLATE', 'NAIL', 'ANCHOR', 'SPINE', 'BONE']):
            return 'Orthopedic / Implants'
        elif any(kw in search_text for kw in ['STENT', 'PACEMAKER', 'BALLOON', 'CATH', 'GUIDEWIRE']):
            return 'Cardiology'
        elif any(kw in search_text for kw in ['GLOVE', 'MASK', 'GOWN', 'DRAPE', 'SHOE COVER', 'PPE']):
            return 'PPE / Apparel'
        elif any(kw in search_text for kw in ['SUTURE', 'STAPLE', 'MESH', 'BLADE', 'SCALPEL']):
            return 'Surgical Supplies'
        elif any(kw in search_text for kw in ['BANDAGE', 'DRESSING', 'GAUZE', 'TAPE', 'WOUND']):
            return 'Wound Care'
        elif any(kw in search_text for kw in ['SYRINGE', 'NEEDLE', 'IV', 'FLUID', 'SALINE', 'INJECTION']):
            return 'IV & Injection'
        elif any(kw in search_text for kw in ['ENDOSCOP', 'TUBE', 'SCOPE']):
            return 'Endoscopy'
        elif row['unspsc_description'] and str(row['unspsc_description']) not in ['nan', 'None', '']:
            # Fallback to UNSPSC description if available, properly cased
            return str(row['unspsc_description']).title()
        else:
            return 'General Medical (Unclassified)'
            
    df['custom_category'] = df.apply(classify_item, axis=1)
    
    # Optional: Calculate a simple data quality score (0 to 100)
    def calculate_dq_score(row):
        score = 100
        if row['is_missing_vendor_code']:
            score -= 20
        if row['is_missing_mfr_name']:
            score -= 20
        if row['custom_category'] == 'Unclassified':
            score -= 10
        return score
        
    df['data_quality_score'] = df.apply(calculate_dq_score, axis=1)
    
    # The return value will be written back to DuckDB as a table
    return df
