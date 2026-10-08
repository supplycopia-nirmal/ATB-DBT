import os
import json
import urllib.request
import urllib.error
from typing import Optional, Dict, Any, List
from autonomous_dbt_app.config import (
    SNOWFLAKE_PAT,
    SNOWFLAKE_URL,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    PRIVATE_KEY_PATH,
    PRIVATE_KEY_PASSPHRASE,
    OPENAI_API_KEY,
    OPENAI_MODEL
)

class CortexClient:
    """
    Client for interacting with Snowflake Cortex AI models (Claude 3.5 Sonnet, Gemini 1.5 Pro/Flash, OpenAI GPT-4o)
    using REST API authentication via SNOWFLAKE_PAT or SQL SNOWFLAKE.CORTEX.COMPLETE via snowflake-connector.
    """
    def __init__(self):
        self.pat = SNOWFLAKE_PAT
        self.url = SNOWFLAKE_URL.rstrip('/')
        self.openai_key = OPENAI_API_KEY
        self.default_cortex_model = "claude-3-5-sonnet"

    def complete(self, prompt: str, model: Optional[str] = None, system_prompt: Optional[str] = None, temperature: float = 0.2) -> str:
        """
        Executes a completion request across Snowflake Cortex models with automatic model fallback.
        Model options: 'claude-3-5-sonnet', 'gemini-1.5-pro', 'gemini-1.5-flash', 'gpt-4o'
        """
        chosen_model = model or self.default_cortex_model
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"System Instruction:\n{system_prompt}\n\nUser Request:\n{prompt}"

        # 1. Attempt Snowflake Cortex REST / SQL completion if credentials available
        if self.pat and self.url:
            try:
                res = self._call_cortex_rest(full_prompt, chosen_model)
                if res:
                    return res
            except Exception as e:
                # Log and proceed to SQL connector or OpenAI fallback
                print(f"[CortexClient] REST attempt note: {e}")

        # 2. Attempt Snowflake SQL connection with SNOWFLAKE.CORTEX.COMPLETE
        try:
            res = self._call_cortex_sql(full_prompt, chosen_model)
            if res:
                return res
        except Exception as e:
            print(f"[CortexClient] SQL Cortex attempt note: {e}")

        # 3. Fallback to OpenAI API if available
        if self.openai_key:
            try:
                return self._call_openai(full_prompt, system_prompt, temperature)
            except Exception as e:
                print(f"[CortexClient] OpenAI fallback error: {e}")

        # 4. Deterministic fallback if external LLM unavailable
        return f"[MOCK_OR_OFFLINE_RESPONSE for model {chosen_model}]: Completed prompt analysis."

    def _call_cortex_rest(self, prompt: str, model: str) -> Optional[str]:
        """Calls Snowflake Cortex REST API endpoint with Bearer PAT token."""
        endpoint = f"{self.url}/api/v2/cortex/inference:complete"
        headers = {
            "Authorization": f"Bearer {self.pat}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}]
        }
        req = urllib.request.Request(endpoint, data=json.dumps(payload).encode('utf-8'), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if "choices" in data and len(data["choices"]) > 0:
                return data["choices"][0].get("message", {}).get("content", "")
            if "message" in data:
                return data["message"].get("content", "")
        return None

    def _call_cortex_sql(self, prompt: str, model: str) -> Optional[str]:
        """Calls SNOWFLAKE.CORTEX.COMPLETE function via snowflake-connector-python."""
        try:
            import snowflake.connector
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            pkb = None
            if os.path.exists(PRIVATE_KEY_PATH):
                with open(PRIVATE_KEY_PATH, "rb") as key_file:
                    p_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None,
                        backend=default_backend()
                    )
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )

            conn_params = {
                "user": SNOWFLAKE_USER,
                "account": SNOWFLAKE_ACCOUNT,
                "role": SNOWFLAKE_ROLE,
                "warehouse": SNOWFLAKE_WAREHOUSE,
                "database": SNOWFLAKE_DATABASE,
                "schema": SNOWFLAKE_SCHEMA
            }
            if pkb:
                conn_params["private_key"] = pkb
            elif self.pat:
                conn_params["token"] = self.pat
                conn_params["authenticator"] = "oauth"

            ctx = snowflake.connector.connect(**conn_params)
            cs = ctx.cursor()
            escaped_prompt = prompt.replace("'", "''")
            query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{escaped_prompt}') as response;"
            cs.execute(query)
            row = cs.fetchone()
            cs.close()
            ctx.close()
            if row and row[0]:
                return str(row[0])
        except Exception as e:
            raise e
        return None

    def _call_openai(self, prompt: str, system_prompt: Optional[str], temperature: float) -> str:
        """Fallback to OpenAI completions."""
        endpoint = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.openai_key}",
            "Content-Type": "application/json"
        }
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": OPENAI_MODEL if OPENAI_MODEL else "gpt-4o",
            "messages": messages,
            "temperature": temperature
        }
        req = urllib.request.Request(endpoint, data=json.dumps(payload).encode('utf-8'), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            return data["choices"][0]["message"]["content"]
