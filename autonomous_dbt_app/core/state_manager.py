import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
from autonomous_dbt_app.config import STATE_FILE

class PipelineState:
    INIT = "INIT"
    PROFILED = "PROFILED"
    BLUEPRINT_GENERATED = "BLUEPRINT_GENERATED"
    DBT_GENERATED = "DBT_GENERATED"
    DBT_EXECUTING = "DBT_EXECUTING"
    SELF_HEALING = "SELF_HEALING"
    DBT_SUCCESS = "DBT_SUCCESS"
    RECONCILED = "RECONCILED"
    WAITING_USER_REVIEW = "WAITING_USER_REVIEW"
    SNOWFLAKE_PUSHING = "SNOWFLAKE_PUSHING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class StateManager:
    """Manages transactional state machine checkpoints and artifacts for the autonomous pipeline."""
    def __init__(self, state_path: Optional[Path] = None):
        self.state_path = state_path or STATE_FILE
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.state_path.exists():
            self._save_default()

    def _save_default(self):
        data = {
            "status": PipelineState.INIT,
            "current_step": "Initialization",
            "client_folder": "",
            "client_metadata": {
                "client_name": "UC Health",
                "client_id": "CL_UCH_001",
                "client_type": "HealthCare System"
            },
            "profile_manifest": {},
            "blueprint": {},
            "dbt_models_created": [],
            "execution_metrics": {},
            "parity_results": {},
            "healing_history": [],
            "feedback_history": [],
            "swarm_events": [],
            "hitl_checkpoints": {
                "checkpoint_1_joins_approved": False,
                "checkpoint_2_pipeline_approved": False
            },
            "logs": [],
            "last_updated": time.time()
        }
        with open(self.state_path, "w") as f:
            json.dump(data, f, indent=2)

    def log_swarm_event(self, agent_bee: str, agent_role: str, action: str, details: str, icon: str = "🐝"):
        data = self.load()
        if "swarm_events" not in data:
            data["swarm_events"] = []
        event = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "agent": agent_bee,
            "role": agent_role,
            "action": action,
            "details": details,
            "icon": icon
        }
        data["swarm_events"].append(event)
        self.save(data)


    def load(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            self.reset()
        with open(self.state_path, "r") as f:
            data = json.load(f)
        data.setdefault("logs", [])
        data.setdefault("swarm_events", [])
        data.setdefault("hitl_checkpoints", {})
        data.setdefault("feedback_history", [])
        data.setdefault("dbt_models_created", [])
        data.setdefault("healing_history", [])
        return data

    def save(self, data: Dict[str, Any]):
        data["last_updated"] = time.time()
        with open(self.state_path, "w") as f:
            json.dump(data, f, indent=2)

    def update_status(self, status: str, step_name: str, message: str = ""):
        data = self.load()
        data["status"] = status
        data["current_step"] = step_name
        if message:
            data.setdefault("logs", []).append({
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
                "step": step_name,
                "message": message
            })
        self.save(data)

    def log(self, message: str):
        data = self.load()
        data.setdefault("logs", []).append({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": data.get("status", "UNKNOWN"),
            "step": data.get("current_step", "General"),
            "message": message
        })
        self.save(data)
