import pandas as pd
import json
import math
import os
from openai import OpenAI
from dotenv import load_dotenv

def model(dbt, session):
    # Configure model as incremental table
    dbt.config(
        materialized="table",
        packages=["pandas", "openai", "python-dotenv"]
    )
    
    # Load .env file
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), '.env')
    load_dotenv(env_path)
    
    # Source distinct DRG and primary procedure from consumption
    cons_df = dbt.ref("int_consumption_normalized_v4").df()
    unique_pairs = cons_df[['drg_code', 'primary_procedure']].drop_duplicates().dropna()
    
    api_key = os.getenv("OPENAI_API_KEY")
    model_name = os.getenv("OPENAI_MODEL", "gpt-4o")
    
    # Fallback if API key missing
    if not api_key:
        print("WARNING: OPENAI_API_KEY not found. Returning rule-based fallback mapping.")
        unique_pairs['primary_drg_code'] = unique_pairs['drg_code'].apply(lambda x: str(x).split('|')[0].strip())
        unique_pairs['primary_procedure_group'] = 'General Clinical Procedure'
        unique_pairs['llm_model_used'] = 'rule_fallback'
        unique_pairs['llm_prompt_version'] = 'v4.0'
        unique_pairs['llm_generated_at'] = pd.Timestamp.now().isoformat()
        return unique_pairs
        
    client = OpenAI(api_key=api_key, max_retries=1)
    batch_size = 500
    results = []
    pairs_list = unique_pairs.to_dict('records')
    
    for i in range(0, len(pairs_list), batch_size):
        batch = pairs_list[i:i+batch_size]
        prompt = f"""
You are a healthcare analytics coding expert.
Given a list of JSON objects each with 'drg_code' and 'primary_procedure':
1. Resolve multiple or piped DRG codes into a single primary_drg_code.
2. Group the procedure into a concise primary_procedure_group (e.g. 'Cardiovascular Surgery', 'Orthopedic Implant', 'General Surgery').

Return JSON array of objects with keys: drg_code, primary_procedure, primary_drg_code, primary_procedure_group.
Data:
{json.dumps(batch)}
"""
        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that strictly outputs JSON arrays."},
                    {"role": "user", "content": prompt}
                ]
            )
            content = response.choices[0].message.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            parsed = json.loads(content)
            if isinstance(parsed, dict):
                for k in parsed:
                    if isinstance(parsed[k], list):
                        parsed = parsed[k]
                        break
            if isinstance(parsed, list):
                results.extend(parsed)
        except Exception as e:
            print(f"Batch {i//batch_size} LLM call error: {e}")
            for item in batch:
                results.append({
                    'drg_code': item['drg_code'],
                    'primary_procedure': item['primary_procedure'],
                    'primary_drg_code': str(item['drg_code']).split('|')[0].strip(),
                    'primary_procedure_group': 'General Surgery'
                })
                
    mapped_df = pd.DataFrame(results)
    if mapped_df.empty:
        mapped_df = pd.DataFrame(columns=['drg_code', 'primary_procedure', 'primary_drg_code', 'primary_procedure_group'])
        
    mapped_df['llm_model_used'] = model_name
    mapped_df['llm_prompt_version'] = 'v4.0'
    mapped_df['llm_generated_at'] = pd.Timestamp.now().isoformat()
    return mapped_df
