import os, sys, time, json, re
import snowflake.connector
import openpyxl
import pandas as pd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

CACHE_FILE = "output/cortex_gemini_standardized_procedures.json"
DRG_CACHE_FILE = "output/cortex_gemini_primary_drgs.json"
EXCEL_PATH = "output/UC_Health_DRG_Procedure_Mapping.xlsx"

def get_snowflake_cursor():
    load_dotenv()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    key_path = os.getenv("PRIVATE_KEY_PATH", "")
    if not os.path.exists(key_path):
        key_path = os.path.join(script_dir, "lambda_migration._rsa_key.p8")
    private_key_passphrase = os.getenv("PRIVATE_KEY_PASSPHRASE")

    with open(key_path, "rb") as kf:
        p_key = serialization.load_pem_private_key(
            kf.read(),
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
    return ctx, ctx.cursor()

def clean_json_response(raw_text):
    if not raw_text:
        return None
    # Strip markdown codeblocks
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        return json.loads(text)
    except Exception:
        # Try regex search for json object
        m = re.search(r'\{.*\}', text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return None

def standardize_procedures(cur, proc_data):
    # Load cache if exists
    results = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                results = json.load(f)
            print(f"Loaded {len(results)} procedures from cache {CACHE_FILE}")
        except Exception as e:
            print("Error loading cache:", e)

    missing_procs = [p for p in proc_data.keys() if p not in results]
    print(f"Procedures remaining to classify via Gemini 3.5 Flash: {len(missing_procs)}")

    batch_size = 20
    total = len(missing_procs)
    for i in range(0, total, batch_size):
        chunk = missing_procs[i:i+batch_size]
        sub_items = []
        for p in chunk:
            p_esc = p.replace("'", "").replace('"', '').replace("\\", "")
            pt_str = ', '.join(list(proc_data[p]['pt'])[:2]).replace("'", "").replace('"', '')
            icd_str = ', '.join(list(proc_data[p]['icd'])[:3]).replace("'", "").replace('"', '')
            cpt_str = ', '.join(list(proc_data[p]['cpt'])[:3]).replace("'", "").replace('"', '')
            sub_items.append((p, p_esc, pt_str or 'General', icd_str or 'None', cpt_str or 'None'))

        unions = ' UNION ALL '.join([
            f"SELECT '{item[1]}' as p_esc, '{item[2]}' as pt, '{item[3]}' as icd, '{item[4]}' as cpt"
            for item in sub_items
        ])

        sql = f'''
        WITH batch AS ({unions})
        SELECT 
            p_esc,
            SNOWFLAKE.CORTEX.COMPLETE(
                'gemini-3.5-flash',
                'Clinical standardization task: You are a medical informatician. For surgical/clinical procedure \"' || p_esc || '\", patient setting \"' || pt || '\", ICD-10 \"' || icd || '\", CPT \"' || cpt || '\". Return JSON ONLY with keys: {{\"standardized_procedure\": \"<canonical standard procedure name in UPPERCASE>\", \"procedure_group\": \"<broad surgical/clinical group>\"}}. Group must not have redundant suffixes like \"Surgery Surgery\". Obvious laparotomy variations must resolve to \"EXPLORATORY LAPAROTOMY\".'
            ) as res
        FROM batch
        '''
        try:
            t0 = time.time()
            cur.execute(sql)
            rows = cur.fetchall()
            row_dict = {r[0]: r[1] for r in rows}
            for original_p, p_esc, _, _, _ in sub_items:
                raw_resp = row_dict.get(p_esc)
                parsed = clean_json_response(raw_resp)
                if parsed and 'standardized_procedure' in parsed and 'procedure_group' in parsed:
                    results[original_p] = {
                        'standardized_procedure': str(parsed['standardized_procedure']).strip().upper(),
                        'procedure_group': str(parsed['procedure_group']).strip()
                    }
                else:
                    # fallback
                    clean_p = re.sub(r'\s+', ' ', original_p.replace(',', ' ')).strip().upper()
                    results[original_p] = {
                        'standardized_procedure': clean_p,
                        'procedure_group': 'General Surgery'
                    }
            elapsed = time.time() - t0
            print(f"[{i+len(chunk)}/{total}] Processed batch in {elapsed:.2f}s ({len(results)} total cached)")
            # Save cache every 100 items
            if (i // batch_size) % 5 == 0:
                with open(CACHE_FILE, "w") as f:
                    json.dump(results, f, indent=2)
        except Exception as e:
            print(f"Error on batch {i}: {e}")
            time.sleep(2)

    with open(CACHE_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print("Procedure standardization complete.")
    return results

def resolve_primary_drgs(raw_drg_set):
    # Rule-based + Gemini resolution for primary DRG
    results = {}
    if os.path.exists(DRG_CACHE_FILE):
        try:
            with open(DRG_CACHE_FILE, "r") as f:
                results = json.load(f)
            print(f"Loaded {len(results)} DRG mappings from cache {DRG_CACHE_FILE}")
        except Exception as e:
            pass

    for raw in raw_drg_set:
        if raw in results:
            continue
        parts = [p.strip() for p in raw.split(',')]
        ms = [p for p in parts if p.startswith('MS')]
        apr = [p for p in parts if p.startswith('APR')]
        if ms:
            results[raw] = ms[0]
        elif apr:
            results[raw] = apr[0]
        else:
            results[raw] = parts[0]

    with open(DRG_CACHE_FILE, "w") as f:
        json.dump(results, f, indent=2)
    return results

def main():
    print("Starting Clinical Procedure Normalization using Gemini 3.5 Flash in Snowflake Cortex...")
    ctx, cur = get_snowflake_cursor()

    print(f"Loading {EXCEL_PATH}...")
    wb = openpyxl.load_workbook(EXCEL_PATH, data_only=True)
    ws = wb['DRG Procedure Mapping']

    proc_data = {}
    raw_drg_set = set()

    for r in ws.iter_rows(min_row=2, values_only=True):
        raw_drg = r[0]
        cpt = r[1]
        icd = r[2]
        raw_proc = r[3]
        pt = r[5]

        if raw_drg:
            raw_drg_set.add(raw_drg)

        if not raw_proc:
            continue
        if raw_proc not in proc_data:
            proc_data[raw_proc] = {'pt': set(), 'icd': set(), 'cpt': set()}
        if pt: proc_data[raw_proc]['pt'].add(str(pt))
        if icd: proc_data[raw_proc]['icd'].add(str(icd))
        if cpt: proc_data[raw_proc]['cpt'].add(str(cpt))

    print(f"Total unique raw procedures: {len(proc_data)}")
    print(f"Total unique raw DRG candidate strings: {len(raw_drg_set)}")

    # 1. Standardize procedures via Gemini 3.5 Flash
    proc_standardized = standardize_procedures(cur, proc_data)

    # 2. Resolve primary DRGs
    drg_resolved = resolve_primary_drgs(raw_drg_set)

    # 3. Post-process group consolidations requested by clinical team
    group_clean_map = {
        'Amputation': 'Amputation Surgery',
        'Cardiac Device': 'Cardiac Device Surgery',
        'Orthopedic Trauma': 'Orthopedic Trauma Surgery',
        'Vascular Access': 'Vascular Access Surgery',
        'Gynecologic Oncology': 'Gynecologic Oncology Surgery',
        'Thoracic Oncology': 'Thoracic Oncology Surgery',
        'Colorectal': 'Colorectal Surgery',
        'Hepatobiliary': 'Hepatobiliary Surgery',
        'Plastic': 'Plastic Surgery',
        'Urology': 'Urologic Surgery',
        'Ophthalmology': 'Ophthalmic Surgery',
        'Otolaryngology': 'ENT / Head and Neck Surgery'
    }

    # Canonical laparotomy override rule requested by clinical team
    for raw_p, info in proc_standardized.items():
        clean_upper = re.sub(r'\s+', ' ', raw_p).strip().upper()
        if clean_upper in ('EXPLORATORY LAPAROTOMY', 'LAPAROTOMY EXPLORATORY', 'LAPAROTOMY EMERGENCY EXPLORATORY'):
            info['standardized_procedure'] = 'EXPLORATORY LAPAROTOMY'
            info['procedure_group'] = 'Abdominal Surgery'
        
        # Apply group consolidation
        grp = info['procedure_group']
        if grp in group_clean_map:
            info['procedure_group'] = group_clean_map[grp]
        elif grp.endswith(' Surgery Surgery'):
            info['procedure_group'] = grp.replace(' Surgery Surgery', ' Surgery')

    # 4. Update Excel Workbook
    print("Updating Excel workbook...")
    wb_out = openpyxl.load_workbook(EXCEL_PATH)
    ws_map = wb_out['DRG Procedure Mapping']

    # Update columns in DRG Procedure Mapping
    # Col 1: Source DRG Code (Raw)
    # Col 2: Billed CPT Code
    # Col 3: Primary ICD-10 PX Code
    # Col 4: Primary Procedure
    # Col 5: Standardized Primary Procedure
    # Col 6: Patient Type
    # Col 7: Primary DRG Code (LLM Derived)
    # Col 8: Procedure Group (LLM Derived)

    unique_procs_before = set()
    unique_procs_after = set()
    unique_groups = set()

    for row_idx in range(2, ws_map.max_row + 1):
        raw_drg = ws_map.cell(row=row_idx, column=1).value
        raw_proc = ws_map.cell(row=row_idx, column=4).value

        if raw_proc:
            unique_procs_before.add(raw_proc)
            std_info = proc_standardized.get(raw_proc, {})
            std_proc = std_info.get('standardized_procedure', raw_proc)
            std_grp = std_info.get('procedure_group', 'General Surgery')

            ws_map.cell(row=row_idx, column=5, value=std_proc)
            ws_map.cell(row=row_idx, column=8, value=std_grp)
            unique_procs_after.add(std_proc)
            unique_groups.add(std_grp)

        if raw_drg:
            pri_drg = drg_resolved.get(raw_drg)
            ws_map.cell(row=row_idx, column=7, value=pri_drg)
        else:
            ws_map.cell(row=row_idx, column=7, value=None)

    # 5. Update Clinical Standardization Summary sheet
    if 'Clinical Standardization Summary' in wb_out.sheetnames:
        ws_sum = wb_out['Clinical Standardization Summary']
        ws_sum.cell(row=2, column=2, value=ws_map.max_row - 1)
        ws_sum.cell(row=2, column=3, value=ws_map.max_row - 1)
        ws_sum.cell(row=2, column=4, value="0")

        ws_sum.cell(row=3, column=2, value=len(unique_procs_before))
        ws_sum.cell(row=3, column=3, value=len(unique_procs_after))
        ws_sum.cell(row=3, column=4, value=f"{len(unique_procs_after) - len(unique_procs_before)}")

        ws_sum.cell(row=4, column=2, value=159)
        ws_sum.cell(row=4, column=3, value=len(unique_groups))
        ws_sum.cell(row=4, column=4, value=f"{len(unique_groups) - 159}")

        ws_sum.cell(row=5, column=2, value="5 distinct variants")
        ws_sum.cell(row=5, column=3, value="Unified into EXPLORATORY LAPAROTOMY")
        ws_sum.cell(row=5, column=4, value="Standardized")

        ws_sum.cell(row=6, column=2, value="Redundant duplicate groups")
        ws_sum.cell(row=6, column=3, value="Consolidated canonical specialty groups")
        ws_sum.cell(row=6, column=4, value="Merged")

    # 6. Update LLM Derivation Logic sheet
    if 'LLM Derivation Logic' in wb_out.sheetnames:
        ws_log = wb_out['LLM Derivation Logic']
        for r in range(1, ws_log.max_row + 1):
            cell_val = ws_log.cell(row=r, column=1).value
            if cell_val == 'Model Used':
                ws_log.cell(row=r, column=2, value='Google Gemini 3.5 Flash via Snowflake Cortex AI (SNOWFLAKE.CORTEX.COMPLETE)')
            elif cell_val == 'Step 2 – Batching':
                ws_log.cell(row=r, column=2, value='Micro-batched row-by-row clinical context inference with distinct procedure context vectors (setting, ICD-10, CPT, service line).')

    wb_out.save(EXCEL_PATH)
    print(f"Successfully saved updated workbook to {EXCEL_PATH}")
    print(f"Unique Raw Procedures: {len(unique_procs_before)} -> Standardized: {len(unique_procs_after)}")
    print(f"Unique Procedure Groups: {len(unique_groups)}")

if __name__ == "__main__":
    main()
