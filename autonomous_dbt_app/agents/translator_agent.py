import re
from typing import Dict, Any, Optional
from autonomous_dbt_app.core.memory_store import MemoryStore
from autonomous_dbt_app.core.cortex_client import CortexClient

class TranslatorAgent:
    """
    Translator Hermes: User Feedback & Python-to-DuckDB SQL Transpiler Agent.
    Interprets natural language adjustments and translates custom Python/Pandas logic
    into declarative DuckDB SQL queries or dbt-duckdb Python models.
    """
    def __init__(self, memory_store: Optional[MemoryStore] = None, cortex_client: Optional[CortexClient] = None):
        self.memory = memory_store or MemoryStore()
        self.cortex = cortex_client or CortexClient()

    def translate_feedback(self, user_feedback: str, current_blueprint: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses user natural language feedback or Python code and generates SQL or blueprint adjustments.
        """
        # Determine if input is Python code
        is_python_code = bool(re.search(r'(def |import |df\[|lambda |\.apply|\.groupby|\.merge)', user_feedback))

        if is_python_code:
            prompt = f"""You are Translator Hermes, a specialized data engineering agent that transpiles Python/Pandas code into high-performance DuckDB SQL CTEs.
User Python snippet:
```python
{user_feedback}
```

Task:
Convert this Python data manipulation into an equivalent, pure DuckDB SQL CTE query.
Output ONLY the clean DuckDB SQL statement without markdown fences."""
            translated_sql = self.cortex.complete(prompt, model="claude-3-5-sonnet")
            translated_sql = translated_sql.replace("```sql", "").replace("```", "").strip()

            result = {
                "type": "PYTHON_TRANSPILATION",
                "original_input": user_feedback,
                "translated_sql": translated_sql,
                "action": "INJECT_SQL_CTE",
                "status": "TRANSLATED"
            }
        else:
            prompt = f"""You are Translator Hermes. The user provided feedback on a healthcare supply chain dbt pipeline:
"{user_feedback}"

Determine what pipeline adjustment is requested (e.g., threshold change, column filter, exclusion).
Provide a brief JSON summary with keys: 'parameter_affected', 'new_value', 'sql_condition'."""
            llm_res = self.cortex.complete(prompt, model="gpt-4o")
            result = {
                "type": "NATURAL_LANGUAGE_RULE",
                "original_input": user_feedback,
                "interpretation": llm_res,
                "action": "UPDATE_BLUEPRINT_RULE",
                "status": "APPLIED"
            }

        # Persist feedback to ChromaDB
        self.memory.add_feedback(
            feedback_id=f"fb_{hash(user_feedback) % 1000000}",
            user_text=user_feedback,
            action_taken=result["action"]
        )

        return result
