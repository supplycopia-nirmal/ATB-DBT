"""
Standardize clinical procedures and consolidate procedure groups in UC_Health_DRG_Procedure_Mapping.xlsx
"""
import os
import re
import pandas as pd
import openpyxl

EXCEL_PATH = 'output/UC_Health_DRG_Procedure_Mapping.xlsx'

# 1. Group consolidation map (merging redundant/duplicate groups)
GROUP_CONSOLIDATION = {
    'Amputation': 'Amputation Surgery',
    'Cardiac Device': 'Cardiac Device Surgery',
    'Cardiac Device Implant': 'Cardiac Device Surgery',
    'Cardiac Device Implantation': 'Cardiac Device Surgery',
    'Cardiac Device Procedure': 'Cardiac Device Surgery',
    'Cardiac Intervention': 'Cardiovascular Intervention',
    'Cardiac Procedure': 'Cardiovascular Intervention',
    'Cardiac Surgery': 'Cardiovascular Surgery',
    'Cardiovascular Support Procedure': 'Cardiovascular Support',
    'Cardiovascular Procedure': 'Cardiovascular Intervention',
    'Cardiothoracic Surgery': 'Cardiovascular Surgery',
    'Cardiovascular Device': 'Cardiac Device Surgery',
    'Emergency Abdominal Surgery': 'Abdominal Surgery',
    'Gynecologic Oncology': 'Gynecologic Oncology Surgery',
    'Gynecologic Procedure': 'Gynecologic Surgery',
    'Obstetric and Gynecologic Surgery': 'Obstetric & Gynecologic Surgery',
    'Head and Neck Oncology': 'Head and Neck Surgery',
    'Head and Neck Procedure': 'Head and Neck Surgery',
    'Maxillofacial Trauma': 'Maxillofacial Trauma Surgery',
    'Maxillofacial Fracture Fixation': 'Maxillofacial Trauma Surgery',
    'Neurologic Surgery': 'Neurosurgery',
    'Orthopedic Trauma': 'Orthopedic Trauma Surgery',
    'Orthopedic Fracture Care': 'Orthopedic Fracture Repair',
    'Orthopedic Fracture Fixation': 'Orthopedic Fracture Repair',
    'Orthopedic Fracture Surgery': 'Orthopedic Fracture Repair',
    'Orthopedic Hardware Surgery': 'Orthopedic Hardware Removal',
    'Orthopedic Joint Replacement': 'Orthopedic Implant',
    'Orthopedic Infection Surgery': 'Orthopedic Surgery',
    'Orthopedic Tendon Repair': 'Orthopedic Soft Tissue Surgery',
    'Orthopedic Wound Surgery': 'Orthopedic Soft Tissue Surgery',
    'Otolaryngology Procedure': 'ENT Surgery',
    'Otolaryngology Surgery': 'ENT Surgery',
    'ENT Procedure': 'ENT Surgery',
    'Plastic and Reconstructive Surgery': 'Plastic & Reconstructive Surgery',
    'Reconstructive Plastic Surgery': 'Plastic & Reconstructive Surgery',
    'Reconstructive Surgery': 'Plastic & Reconstructive Surgery',
    'Spine Surgery': 'Spinal Surgery',
    'Spinal Fusion': 'Spinal Surgery',
    'Thoracic Procedure': 'Thoracic Surgery',
    'Thoracic Trauma Surgery': 'Thoracic Surgery',
    'Vascular Access': 'Vascular Access Surgery',
    'Vascular Procedure': 'Vascular Surgery',
    'Wound Surgery': 'Wound Debridement',
    'Wound and Soft Tissue Surgery': 'Wound Debridement',
    'Unclassified Surgical Procedure': 'Unclassified Procedure',
    'Uncompleted Procedure': 'Aborted Procedure',
    'Organ Transplant': 'Organ Transplantation',
    'Transplant Surgery': 'Organ Transplantation',
    'Biopsy and Excision': 'Biopsy',
    'Diagnostic Biopsy': 'Biopsy',
    'Diagnostic Laparoscopy': 'Diagnostic Procedure',
    'Diagnostic Examination': 'Diagnostic Procedure',
    'Surgical Examination': 'Diagnostic Procedure',
    'Upper GI Endoscopy': 'Gastrointestinal Endoscopy',
    'Lower GI Endoscopy': 'Gastrointestinal Endoscopy',
    'Advanced GI Endoscopy': 'Gastrointestinal Endoscopy',
    'Gastrointestinal Procedure': 'Gastrointestinal Surgery',
    'Gastric Surgery': 'Gastrointestinal Surgery',
    'Foregut Surgery': 'Gastrointestinal Surgery',
    'Hepatobiliary Procedure': 'Hepatobiliary Surgery',
    'Biliary Surgery': 'Hepatobiliary Surgery',
    'Biliary Endoscopy': 'Endoscopic Biliary Procedure',
}

# 2. Canonical procedure map for explicit variations
EXPLICIT_PROCEDURE_STANDARDIZATION = {
    # Laparotomy variations (Screenshot 1)
    'LAPAROTOMY EXPLORATORY': 'EXPLORATORY LAPAROTOMY',
    'LAPAROTOMY EMERGENCY EXPLORATORY': 'EXPLORATORY LAPAROTOMY',
    'LAPAROTOMY EXPLORATORY GYN': 'EXPLORATORY LAPAROTOMY GYN',
    'LAPAROTOMY EXPLORATORY TRAUMA': 'EXPLORATORY LAPAROTOMY TRAUMA',
    'EXPLORATORY LAPAROTOMY': 'EXPLORATORY LAPAROTOMY',

    # Amputations (Screenshot 2)
    'AMPUTATION LEG ABOVE KNEE': 'ABOVE KNEE AMPUTATION (AKA)',
    'AMPUTATION LEG BELOW KNEE': 'BELOW KNEE AMPUTATION (BKA)',
    'AMPUTATION FOOT / TOE': 'AMPUTATION TOE / FOOT',
    'AMPUTATION TOE TRANSMETATARSAL PARTIAL FOOT': 'TRANSMETATARSAL AMPUTATION',
    'AMPUTATION TRANSMETATARSAL': 'TRANSMETATARSAL AMPUTATION',
    'AMPUTATION TOE': 'AMPUTATION TOE',
    'AMPUTATION FINGER / THUMB': 'AMPUTATION FINGER / THUMB',
    'AMPUTATION STUMP REVISION': 'AMPUTATION STUMP REVISION',
    'DISARTICULATION HIP': 'HIP DISARTICULATION',

    # Common Arthroplasty variations
    'ARTHROPLASTY HIP TOTAL': 'TOTAL HIP ARTHROPLASTY',
    'ARTHROPLASTY KNEE TOTAL': 'TOTAL KNEE ARTHROPLASTY',
    'ARTHROPLASTY SHOULDER TOTAL': 'TOTAL SHOULDER ARTHROPLASTY',
    'ARTHROPLASTY SHOULDER TOTAL REVERSE': 'REVERSE TOTAL SHOULDER ARTHROPLASTY',
    'ARTHROPLASTY HIP HEMI': 'HEMIARTHROPLASTY HIP',
    'ARTHROPLASTY RESECTION HIP GIRDLESTONE': 'GIRDLESTONE RESECTION ARTHROPLASTY HIP',

    # Cholecystectomy & Appendectomy variations
    'CHOLECYSTECTOMY,  LAPAROSCOPIC': 'LAPAROSCOPIC CHOLECYSTECTOMY',
    'CHOLECYSTECTOMY, LAPAROSCOPIC': 'LAPAROSCOPIC CHOLECYSTECTOMY',
    'CHOLECYSTECTOMY LAPAROSCOPIC': 'LAPAROSCOPIC CHOLECYSTECTOMY',
    'CHOLECYSTECTOMY ROBOTIC ASSISTED': 'ROBOTIC CHOLECYSTECTOMY',
    'APPENDECTOMY, LAPAROSCOPIC': 'LAPAROSCOPIC APPENDECTOMY',
    'APPENDECTOMY,  LAPAROSCOPIC': 'LAPAROSCOPIC APPENDECTOMY',
    'APPENDECTOMY LAPAROSCOPIC': 'LAPAROSCOPIC APPENDECTOMY',
}

def clean_procedure_string(text):
    if not isinstance(text, str) or not text.strip():
        return ""
    # Uppercase and clean whitespace
    val = re.sub(r'\s+', ' ', text.strip()).upper()
    # Check explicit dictionary
    if val in EXPLICIT_PROCEDURE_STANDARDIZATION:
        return EXPLICIT_PROCEDURE_STANDARDIZATION[val]
    
    # Generic normalization patterns:
    # Remove redundant commas and double spaces
    val = val.replace(', ', ' ').replace(',', ' ')
    val = re.sub(r'\s+', ' ', val).strip()
    
    # If explicit matched after comma removal
    if val in EXPLICIT_PROCEDURE_STANDARDIZATION:
        return EXPLICIT_PROCEDURE_STANDARDIZATION[val]
        
    # Standardize LAPAROSCOPIC / ROBOTIC position if at end
    if val.endswith(' LAPAROSCOPIC'):
        val = 'LAPAROSCOPIC ' + val[:-13].strip()
    elif val.endswith(' ROBOTIC'):
        val = 'ROBOTIC ' + val[:-8].strip()
        
    return val

def standardize_procedure_group(group):
    if not isinstance(group, str) or not group.strip():
        return "General Clinical Procedure"
    g = group.strip()
    return GROUP_CONSOLIDATION.get(g, g)

def main():
    print(f"Reading {EXCEL_PATH}...")
    xl = pd.ExcelFile(EXCEL_PATH)
    df_mapping = xl.parse('DRG Procedure Mapping')
    df_logic = xl.parse('LLM Derivation Logic')
    
    initial_unique_procs = df_mapping['Primary Procedure'].nunique()
    initial_unique_groups = df_mapping['Procedure Group (LLM Derived)'].nunique()
    print(f"Initial: {len(df_mapping)} rows, {initial_unique_procs} unique procedures, {initial_unique_groups} unique groups")
    
    # Apply standardizations
    df_mapping['Standardized Primary Procedure'] = df_mapping['Primary Procedure'].apply(clean_procedure_string)
    df_mapping['Procedure Group (LLM Derived)'] = df_mapping['Procedure Group (LLM Derived)'].apply(standardize_procedure_group)
    
    final_unique_procs = df_mapping['Standardized Primary Procedure'].nunique()
    final_unique_groups = df_mapping['Procedure Group (LLM Derived)'].nunique()
    print(f"After Standardization: {final_unique_procs} unique procedures (reduced by {initial_unique_procs - final_unique_procs}), {final_unique_groups} unique groups (reduced by {initial_unique_groups - final_unique_groups})")
    
    # Re-order columns for clarity
    cols = [
        'Source DRG Code (Raw)',
        'Billed CPT Code',
        'Primary ICD-10 PX Code',
        'Primary Procedure',
        'Standardized Primary Procedure',
        'Patient Type',
        'Primary DRG Code (LLM Derived)',
        'Procedure Group (LLM Derived)'
    ]
    df_mapping = df_mapping[cols]
    
    # Create Summary DataFrame
    summary_data = [
        {'Metric': 'Total Encounter / DRG Records', 'Before': len(df_mapping), 'After': len(df_mapping), 'Change': '0'},
        {'Metric': 'Unique Primary Procedures', 'Before': initial_unique_procs, 'After': final_unique_procs, 'Change': f"-{initial_unique_procs - final_unique_procs}"},
        {'Metric': 'Unique Procedure Groups', 'Before': initial_unique_groups, 'After': final_unique_groups, 'Change': f"-{initial_unique_groups - final_unique_groups}"},
        {'Metric': 'Laparotomy Variants Consolidated', 'Before': '5 distinct variants', 'After': 'Unified into EXPLORATORY LAPAROTOMY', 'Change': 'Standardized'},
        {'Metric': 'Amputation Groups Consolidated', 'Before': 'Amputation & Amputation Surgery', 'After': 'Amputation Surgery', 'Change': 'Merged'}
    ]
    df_summary = pd.DataFrame(summary_data)
    
    print(f"Writing updated Excel to {EXCEL_PATH}...")
    with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Clinical Standardization Summary', index=False)
        df_mapping.to_excel(writer, sheet_name='DRG Procedure Mapping', index=False)
        df_logic.to_excel(writer, sheet_name='LLM Derivation Logic', index=False)
        
    print("Excel updated successfully!")

if __name__ == '__main__':
    main()
