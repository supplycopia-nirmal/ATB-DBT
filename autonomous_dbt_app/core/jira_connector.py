"""
Jira Issue & Incident Management Engine
Connects to Atlassian Jira Cloud REST API v3 to automate incident ticket creation for
unresolved exceptions and empower operators to log tasks/bugs directly from the UI.
"""

import os
import sys
import json
import time
import hashlib
import base64
import urllib.request
import urllib.error
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from autonomous_dbt_app.config import (
    ATLASSIAN_DOMAIN, ATLASSIAN_EMAIL, JIRA_PROJECT_KEY, JIRA_API_TOKEN
)


class JiraConnector:
    """
    Connects to Atlassian Jira Cloud REST API v3 for project 'ATBDBT'.
    Supports automated incident escalation for unrecoverable errors and manual user ticketing.
    """
    def __init__(
        self,
        domain: str = ATLASSIAN_DOMAIN,
        email: str = ATLASSIAN_EMAIL,
        token: str = JIRA_API_TOKEN,
        project_key: str = JIRA_PROJECT_KEY
    ):
        self.domain = domain.rstrip("/")
        self.email = email
        self.token = token
        self.project_key = project_key
        self.base_url = f"{self.domain}/rest/api/3"

        # Anti-spam cache for recent automated incidents: {hash: timestamp}
        self._recent_incident_cache = {}

    def _get_headers(self) -> Dict[str, str]:
        auth_bytes = f"{self.email}:{self.token}".encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")
        return {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def test_connection(self) -> Dict[str, Any]:
        """Validates Jira connection and retrieves project details."""
        url = f"{self.base_url}/project/{self.project_key}"
        req = urllib.request.Request(url, headers=self._get_headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                issue_types = [t.get("name") for t in data.get("issueTypes", [])]
                return {
                    "status": "SUCCESS",
                    "project_name": data.get("name"),
                    "project_key": data.get("key"),
                    "project_type": data.get("projectTypeKey"),
                    "available_issue_types": issue_types,
                    "authenticated_user": self.email
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "code": e.code, "message": f"Jira API Error ({e.code}): {err_body}"}
        except Exception as e:
            return {"status": "ERROR", "message": f"Jira Connection Error: {str(e)}"}

    def _text_to_adf(self, text: str) -> Dict[str, Any]:
        """Converts plain text or multiline text into Atlassian Document Format (ADF)."""
        paragraphs = []
        for line in (text or "No description provided.").split("\n"):
            line = line.strip()
            if line:
                paragraphs.append({
                    "type": "paragraph",
                    "content": [{"type": "text", "text": line}]
                })
        if not paragraphs:
            paragraphs = [{
                "type": "paragraph",
                "content": [{"type": "text", "text": "No details provided."}]
            }]
        return {
            "type": "doc",
            "version": 1,
            "content": paragraphs
        }

    def create_issue(
        self,
        summary: str,
        description: str,
        issue_type: str = "Task",
        priority: str = "Medium",
        labels: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Creates an issue in Jira project ATBDBT.
        Issue types available: 'Bug', 'Task', 'Story', 'Feature', 'Epic'.
        """
        url = f"{self.base_url}/issue"
        adf_desc = self._text_to_adf(description)

        fields = {
            "project": {"key": self.project_key},
            "summary": summary[:250],
            "description": adf_desc,
            "issuetype": {"name": issue_type or "Task"}
        }

        if labels:
            clean_labels = [l.replace(" ", "-").replace(":", "-") for l in labels if l]
            fields["labels"] = clean_labels

        if priority in ["Highest", "High", "Medium", "Low", "Lowest"]:
            fields["priority"] = {"name": priority}

        payload = {"fields": fields}
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self._get_headers(),
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=25) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                issue_key = data.get("key")
                issue_url = f"{self.domain}/browse/{issue_key}"
                return {
                    "status": "SUCCESS",
                    "key": issue_key,
                    "issue_key": issue_key,
                    "issue_id": data.get("id"),
                    "url": issue_url,
                    "summary": summary,
                    "created_at": datetime.now().isoformat()
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "code": e.code, "message": f"Failed to create Jira issue: {err_body}"}
        except Exception as e:
            return {"status": "ERROR", "message": f"Jira creation error: {str(e)}"}

    def create_incident_ticket(
        self,
        error_message: str,
        context: str = "pipeline_execution",
        details: Optional[Dict[str, Any]] = None,
        client_id: str = "CL_UCH_001"
    ) -> Dict[str, Any]:
        """
        Automated Incident Escalation.
        Triggered when Self-Healing Engine or compilation encounters an unresolved failure.
        Features intelligent deduplication so identical errors don't spam Jira.
        """
        # Deduplication signature (error + context)
        sig = hashlib.sha256(f"{context}:{error_message[:100]}".encode("utf-8")).hexdigest()
        now = time.time()

        # Suppress duplicate tickets within 1 hour (3600 seconds)
        if sig in self._recent_incident_cache:
            last_time, last_key = self._recent_incident_cache[sig]
            if (now - last_time) < 3600:
                return {
                    "status": "DUPLICATE_SUPPRESSED",
                    "message": f"Similar incident already reported within past hour under issue {last_key}",
                    "key": last_key,
                    "issue_key": last_key,
                    "url": f"{self.domain}/browse/{last_key}"
                }

        summary = f"[Auto-Incident] Unresolved Exception in {context}: {error_message[:70]}"
        detail_lines = [
            f"Tenant Context: {client_id}",
            f"Pipeline Execution Stage: {context}",
            f"Detected At: {datetime.now().isoformat()}",
            "---",
            "Error Message Traceback:",
            error_message,
            "---",
            "Self-Healing Engine Escalation Notice:",
            "The autonomous self-healing engine attempted automated AST diagnostic remediation but the pipeline requires human developer review."
        ]

        if details:
            detail_lines.append("--- Additional Diagnostics ---")
            detail_lines.append(json.dumps(details, indent=2))

        description = "\n".join(detail_lines)
        labels = ["autonomous-dbt", "automated-incident", "self-healing-escalation", client_id.lower()]

        res = self.create_issue(
            summary=summary,
            description=description,
            issue_type="Bug",
            priority="High",
            labels=labels
        )

        if res.get("status") == "SUCCESS":
            self._recent_incident_cache[sig] = (now, res.get("issue_key"))

        return res

    def list_issues(self, project_key: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
        """Fetches the latest issues from Jira project ATBDBT using /rest/api/3/search/jql."""
        proj = project_key or self.project_key
        url = f"{self.base_url}/search/jql"
        payload = {
            "jql": f"project = {proj} ORDER BY created DESC",
            "maxResults": limit,
            "fields": ["summary", "status", "issuetype", "priority", "created", "reporter"]
        }

        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=self._get_headers(),
            method="POST"
        )

        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                formatted = []
                for iss in data.get("issues", []):
                    flds = iss.get("fields", {})
                    key = iss.get("key")
                    formatted.append({
                        "key": key,
                        "summary": flds.get("summary", ""),
                        "status": flds.get("status", {}).get("name", "Unknown"),
                        "status_category": flds.get("status", {}).get("statusCategory", {}).get("key", "new"),
                        "issue_type": flds.get("issuetype", {}).get("name", "Task"),
                        "priority": flds.get("priority", {}).get("name", "Medium"),
                        "created": flds.get("created", ""),
                        "url": f"{self.domain}/browse/{key}"
                    })
                return {
                    "status": "SUCCESS",
                    "project_key": proj,
                    "total": len(formatted),
                    "issues": formatted
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "code": e.code, "message": f"Failed to list Jira issues: {err_body}"}
        except Exception as e:
            return {"status": "ERROR", "message": f"Jira query error: {str(e)}"}
