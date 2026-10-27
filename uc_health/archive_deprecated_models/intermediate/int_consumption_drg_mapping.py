import pandas as pd
import json
import math
import os
from openai import OpenAI
from dotenv import load_dotenv

def model(dbt, session):
    # Load .env explicitly since dbt might not run from the root directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), '.env')
    load_dotenv(env_path)
    
    # Configure DBT model
    dbt.config(
        materialized="table"
    )
    
    # Get distinct drg_code and primary_procedure from consumption
    df = dbt.ref("stg_consumption_enriched_v3").df()
    
    # Extract unique combinations where drg_code contains multiple values (assuming pipe or comma separated)
    # We will just process all distinct pairs to ensure complete mapping
    unique_pairs = df[['DRG_CODE', 'PRIMARY_PROCEDURE']].drop_duplicates().dropna()
    unique_pairs = unique_pairs.rename(columns={'DRG_CODE': 'drg_code', 'PRIMARY_PROCEDURE': 'primary_procedure'})
    
    # Initialize OpenAI client
    api_key = os.getenv("OPENAI_API_KEY")
    model_name = os.getenv("OPENAI_MODEL", "gpt-4o")
    
    if not api_key:
        print("WARNING: OPENAI_API_KEY not found. Returning empty mapping.")
        return pd.DataFrame(columns=['drg_code', 'primary_procedure', 'primary_drg_code', 'primary_procedure_group'])
    
    client = OpenAI(api_key=api_key, max_retries=0)
    
    # We will process in batches of 500 to avoid token limits
    batch_size = 500
    results = []
    
    pairs_list = unique_pairs.to_dict('records')
    total_batches = math.ceil(len(pairs_list) / batch_size)
    
    print(f"Starting OpenAI processing for {len(pairs_list)} unique DRG/Procedure combinations across {total_batches} batches.")
    
    for i in range(0, len(pairs_list), batch_size):
        batch = pairs_list[i:i+batch_size]
        
        prompt = f"""
You are a medical coding expert.
I will give you a list of JSON objects containing a 'drg_code' (which often has multiple codes like '001|002') and a 'primary_procedure' description.
For each object, perform the following:
1. Identify the single TRUE primary DRG code from the multiple codes provided, based on the 'primary_procedure'.
2. Create a concise 'primary_procedure_group' string categorizing the procedure (e.g. 'Cardiovascular Surgery', 'Orthopedic Implant').

Return EXACTLY a JSON array of objects with these keys:
- "drg_code" (original string)
- "primary_procedure" (original string)
- "primary_drg_code" (the single identified code)
- "primary_procedure_group" (the categorized group)

Here is the data:
{json.dumps(batch)}
"""
        
        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that only outputs valid JSON arrays."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"} if "gpt-4" in model_name or "gpt-3.5" in model_name else None
            )
            
            content = response.choices[0].message.content
            
            # Extract JSON array from response
            try:
                # Sometimes the LLM wraps in ```json ... ```
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0]
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0]
                    
                parsed_batch = json.loads(content)
                # Handle cases where LLM wraps array in an object like {"data": [...]}
                if isinstance(parsed_batch, dict):
                    for key in parsed_batch:
                        if isinstance(parsed_batch[key], list):
                            parsed_batch = parsed_batch[key]
                            break
                            
                if isinstance(parsed_batch, list):
                    results.extend(parsed_batch)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON for batch {i//batch_size}: {e}")
                
        except Exception as e:
            print(f"API Error in batch {i//batch_size}: {e}")
            
    print(f"Successfully mapped {len(results)} records.")
    
    # Convert results to DataFrame
    if results:
        mapped_df = pd.DataFrame(results)
    else:
        mapped_df = pd.DataFrame(columns=['drg_code', 'primary_procedure', 'primary_drg_code', 'primary_procedure_group'])
        
    return mapped_df
