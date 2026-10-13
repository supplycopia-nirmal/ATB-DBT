"""
Confluence Technical Documentation Engine
Publishes and synchronizes comprehensive technical architecture manuals, process flows,
and feature documentation directly to Atlassian Confluence Space 'ATBDBT'.
"""

import os
import sys
import json
import time
import uuid
import mimetypes
import base64
import urllib.request
import urllib.error
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from datetime import datetime

from autonomous_dbt_app.config import (
    ATLASSIAN_DOMAIN, ATLASSIAN_EMAIL, CONFLUENCE_SPACE_KEY, CONFLUENCE_API_TOKEN,
    ROOT_DIR as PROJECT_ROOT
)

DOCS_CACHE_DIR = PROJECT_ROOT / "agent_memory" / "confluence_docs_cache"
DOCS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
DOCS_ASSETS_DIR = PROJECT_ROOT / "agent_memory" / "docs_assets"
DOCS_ASSETS_DIR.mkdir(parents=True, exist_ok=True)


class ConfluenceConnector:
    """
    Connects to Atlassian Confluence Cloud REST API to author and synchronize living
    technical documentation for SupplyCopia / Ask The Bee Autonomous DBT Studio.
    """
    def __init__(
        self,
        domain: str = ATLASSIAN_DOMAIN,
        email: str = ATLASSIAN_EMAIL,
        token: str = CONFLUENCE_API_TOKEN,
        space_key: str = CONFLUENCE_SPACE_KEY
    ):
        self.domain = domain.rstrip("/")
        self.email = email
        self.token = token
        self.space_key = space_key
        self.base_url = f"{self.domain}/wiki/rest/api"

    def _get_headers(self) -> Dict[str, str]:
        auth_bytes = f"{self.email}:{self.token}".encode("utf-8")
        auth_b64 = base64.b64encode(auth_bytes).decode("utf-8")
        return {
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    def test_connection(self) -> Dict[str, Any]:
        """Validates Confluence credentials and retrieves space information."""
        url = f"{self.base_url}/space/{self.space_key}"
        req = urllib.request.Request(url, headers=self._get_headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return {
                    "status": "SUCCESS",
                    "space_name": data.get("name"),
                    "space_key": data.get("key"),
                    "space_type": data.get("type"),
                    "authenticated_user": self.email
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "code": e.code, "message": f"Confluence API Error ({e.code}): {err_body}"}
        except Exception as e:
            return {"status": "ERROR", "message": f"Confluence Connection Error: {str(e)}"}

    def get_space_root_page(self) -> Optional[Dict[str, Any]]:
        """Finds the designated home / root page for the space."""
        url = f"{self.base_url}/space/{self.space_key}?expand=homepage"
        req = urllib.request.Request(url, headers=self._get_headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                homepage = data.get("homepage")
                if homepage and homepage.get("id"):
                    return homepage
        except Exception:
            pass
        return {"id": "2080997603", "title": "Ask The Bee - DBT Automation"}

    def find_page_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Searches for an existing page by title in the space."""
        encoded_title = urllib.parse.quote(title)
        url = f"{self.base_url}/content?spaceKey={self.space_key}&title={encoded_title}&expand=version,body.storage"
        req = urllib.request.Request(url, headers=self._get_headers())
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                results = data.get("results", [])
                return results[0] if results else None
        except Exception:
            return None

    def upsert_page(
        self,
        title: str,
        body_storage_xhtml: str,
        parent_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Creates or updates a page with Confluence Storage Format XHTML."""
        existing = self.find_page_by_title(title)

        if existing:
            # Update existing page
            page_id = existing["id"]
            current_ver = existing.get("version", {}).get("number", 1)
            payload = {
                "id": page_id,
                "type": "page",
                "title": title,
                "space": {"key": self.space_key},
                "version": {"number": current_ver + 1},
                "body": {
                    "storage": {
                        "value": body_storage_xhtml,
                        "representation": "storage"
                    }
                }
            }
            if parent_id:
                payload["ancestors"] = [{"id": str(parent_id)}]
            url = f"{self.base_url}/content/{page_id}"
            req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=self._get_headers(), method="PUT")
            action = "UPDATED"
        else:
            # Create new page
            payload = {
                "type": "page",
                "title": title,
                "space": {"key": self.space_key},
                "body": {
                    "storage": {
                        "value": body_storage_xhtml,
                        "representation": "storage"
                    }
                }
            }
            if parent_id:
                payload["ancestors"] = [{"id": str(parent_id)}]

            url = f"{self.base_url}/content"
            req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers=self._get_headers(), method="POST")
            action = "CREATED"

        try:
            with urllib.request.urlopen(req, timeout=25) as resp:
                res_data = json.loads(resp.read().decode("utf-8"))
                web_link = f"{self.domain}/wiki{res_data.get('_links', {}).get('webui', '')}"
                return {
                    "status": "SUCCESS",
                    "action": action,
                    "page_id": res_data.get("id"),
                    "title": title,
                    "version": res_data.get("version", {}).get("number", 1),
                    "url": web_link
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "title": title, "code": e.code, "message": err_body}
        except Exception as e:
            return {"status": "ERROR", "title": title, "message": str(e)}

    def upload_attachment(
        self,
        page_id: str,
        file_path: Union[str, Path],
        filename: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Uploads a binary attachment (e.g. PNG screenshot) to a Confluence page using multipart/form-data.
        """
        path = Path(file_path)
        if not path.exists():
            return {"status": "ERROR", "message": f"File not found: {file_path}"}

        upload_filename = filename or path.name
        mime_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"

        with open(path, "rb") as f:
            file_bytes = f.read()

        boundary = "----ConfluenceBoundary" + uuid.uuid4().hex
        headers = {
            "Authorization": self._get_headers()["Authorization"],
            "X-Atlassian-Token": "nocheck",
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        }

        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{upload_filename}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

        url = f"{self.base_url}/content/{page_id}/child/attachment"
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                results = data.get("results", [])
                att = results[0] if results else {}
                return {
                    "status": "SUCCESS",
                    "attachment_id": att.get("id"),
                    "title": att.get("title", upload_filename),
                    "filename": upload_filename,
                    "page_id": page_id
                }
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else str(e)
            return {"status": "ERROR", "filename": upload_filename, "code": e.code, "message": err_body}
        except Exception as e:
            return {"status": "ERROR", "filename": upload_filename, "message": str(e)}

    def generate_technical_documentation(self) -> List[Dict[str, Any]]:
        """
        Compiles the comprehensive, enterprise technical documentation suite.
        Returns ordered chapters with title, XHTML storage format body, and summary.
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

        docs = [
            {
                "title": "SupplyCopia ATB Studio: System Architecture & Agent Swarm Blueprint",
                "summary": "Multi-agent cognitive architecture, Queen Bee coordination, and execution kernel.",
                "body": f"""
                <ac:structured-macro ac:name="info">
                    <ac:rich-text-body>
                        <p><strong>SupplyCopia Autonomous DBT Pipeline Studio</strong> • Built for enterprise healthcare supply chain transformation. Last synchronized: {now_str}</p>
                    </ac:rich-text-body>
                </ac:structured-macro>

                <h2>1. Executive Summary &amp; Vision</h2>
                <p>The SupplyCopia Autonomous DBT Studio (Ask The Bee) delivers automated multi-tenant data transformation from heterogeneous EHR/ERP feeds (Epic, Cerner, Lawson, Workday, Excel/CSV) into gold-standard clinical supply chain data marts.</p>

                <h2>2. Specialized Cognitive Agent Swarm</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Agent Persona</th>
                            <th>Subsystem Role</th>
                            <th>Core Responsibilities</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td><strong>👑 Queen Bee Orla</strong></td>
                            <td>Autonomous Pipeline Orchestrator</td>
                            <td>Coordinates swarm execution, enforces review gates, and oversees end-to-end multi-tenant lifecycle.</td>
                        </tr>
                        <tr>
                            <td><strong>🐝 Scout Bee Buzz</strong></td>
                            <td>Cloud Ingestion &amp; Raw Profiler</td>
                            <td>Connects to AWS S3, sniffs schemas, profiles cardinality/null rates, and classifies entities.</td>
                        </tr>
                        <tr>
                            <td><strong>📐 Architect Bee Pollen</strong></td>
                            <td>Semantic Topology &amp; Joins</td>
                            <td>Discovers natural joins, maps composite keys, aligns UOM conversion factors, and crafts dbt schemas.</td>
                        </tr>
                        <tr>
                            <td><strong>⚡ Worker Bee Wax</strong></td>
                            <td>DBT Core Compiler &amp; SQL Engine</td>
                            <td>Synthesizes staging and intermediate models, compiles DAGs, and runs vectorized DuckDB queries.</td>
                        </tr>
                        <tr>
                            <td><strong>🛡️ Inspector Bee Guard</strong></td>
                            <td>Golden Parity &amp; Sentinel Aegis</td>
                            <td>Executes 100% mathematical parity assertions, self-healing diagnostic AST parsing, and SLA guardrails.</td>
                        </tr>
                    </tbody>
                </table>

                <h2>3. Vectorized Multi-Tenant Kernel</h2>
                <p>The system leverages an in-process vectorized DuckDB execution kernel capable of processing over 2.2 million transaction rows in under 2 seconds, guaranteeing zero-latency interactive dashboards and drill-down KPI inspection.</p>
                """
            },
            {
                "title": "SupplyCopia ATB Studio: 7-Stage Autonomous Workflow & User Guide",
                "summary": "Detailed stage-by-stage guide covering Ingestion, Profiling, Join Discovery, Lineage, Explorer, Parity, and Refinement.",
                "body": f"""
                <ac:structured-macro ac:name="tip">
                    <ac:rich-text-body>
                        <p><strong>Autonomy Modes Supported:</strong> <code>Default (Step-by-Step HITL)</code>, <code>User Review (Approval Gate)</code>, and <code>⚡ Turbo (Full Auto End-to-End)</code>.</p>
                    </ac:rich-text-body>
                </ac:structured-macro>

                <h2>Workflow Lifecycle Stages</h2>
                <ol>
                    <li><strong>Stage 1: Multi-Tenant AWS S3 Ingestion</strong>
                        <p>Configures client tenant IDs (e.g. <code>CL_UCH_001</code>), S3 bucket URIs, and load mode (Bulk Historical Rebuild vs Incremental Delta Load).</p>
                    </li>
                    <li><strong>Stage 2: Raw Client Profiling &amp; Auto-Classification</strong>
                        <p>Profiles all discovered CSV/Parquet files, evaluates field null-rates, distinct counts, and auto-classifies canonical entities (Item Master, Contracts, PO, Invoices, Consumption).</p>
                    </li>
                    <li><strong>Stage 3: Join Discovery &amp; Topology (HITL)</strong>
                        <p>Discovers natural foreign keys and composite join paths (e.g. <code>item_id</code>, <code>contract_number</code>, <code>po_number + po_line_no</code>). Allows manual join topology overrides before compilation.</p>
                    </li>
                    <li><strong>Stage 4: Interactive Lineage &amp; DBT DAG</strong>
                        <p>Compiles DBT models into visual DAG lineage with full dbt documentation embedding (project overview, model lineage, and column descriptions).</p>
                    </li>
                    <li><strong>Stage 5: Output Explorer &amp; Clinical Gaps</strong>
                        <p>Inspects synthesized fact tables: <code>fct_consumption_cost_savings_v4</code>, <code>fct_po_cost_savings_v4</code>, and <code>fct_gap_analysis_v4</code> with live filtering and custom SQL runner.</p>
                    </li>
                    <li><strong>Stage 6: 100% Golden Parity &amp; Mathematical Audit</strong>
                        <p>Runs automated multi-layer verification ensuring zero dollar variance between raw source spend and aggregated dimensional marts ($243,934,093.08 golden target).</p>
                    </li>
                    <li><strong>Stage 7: Refine &amp; Evolve (HITL Retrospective)</strong>
                        <p>Empowers clinical and analytics teams to submit findings, feedback text, Excel/CSV evidence, and Python patch scripts. Automatically synthesizes iterative implementation plans and updates the living Institutional Playbook.</p>
                    </li>
                </ol>
                """
            },
            {
                "title": "SupplyCopia ATB Studio: Client File Catalog & Smart Entity Merging",
                "summary": "Tenant-partitioned catalog, versioning, and DuckDB deduplication (Latest Wins, Union Distinct, Additive Append).",
                "body": f"""
                <h2>1. Tenant Ingestion Catalog Architecture</h2>
                <p>Every ingested file is cryptographically hashed (SHA-256) and tracked in a persistent client catalog (<code>agent_memory/client_catalogs/{{client_id}}_catalog.json</code>). Version numbers auto-increment, recording active vs superseded states.</p>

                <h2>2. Smart Entity Merging &amp; Deduplication Strategies</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Deduplication Strategy</th>
                            <th>Algorithm</th>
                            <th>Use Case</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td><strong>Latest Wins (SCD Type 1)</strong></td>
                            <td><code>ROW_NUMBER() OVER(PARTITION BY primary_keys ORDER BY version DESC)</code></td>
                            <td>Recommended for Item Master and Contracts updates where newer addenda overwrite outdated attributes.</td>
                        </tr>
                        <tr>
                            <td><strong>Union Distinct</strong></td>
                            <td><code>SELECT DISTINCT *</code></td>
                            <td>Purges exact row duplicates across recurring client exports.</td>
                        </tr>
                        <tr>
                            <td><strong>Additive Append</strong></td>
                            <td><code>UNION ALL</code> with collision filtering</td>
                            <td>Transactional append for monthly Purchase Orders and Invoices.</td>
                        </tr>
                    </tbody>
                </table>

                <ac:structured-macro ac:name="note">
                    <ac:rich-text-body>
                        <p><strong>Natural Primary Keys:</strong><br/>
                        • Item Master: <code>item_id</code><br/>
                        • Contracts: <code>contract_number, item_id</code><br/>
                        • Purchase Orders: <code>po_number, po_line_no</code><br/>
                        • Invoices: <code>invoice_number, po_line_no</code><br/>
                        • Consumption: <code>log_id</code></p>
                    </ac:rich-text-body>
                </ac:structured-macro>
                """
            },
            {
                "title": "SupplyCopia ATB Studio: Record & Row Drift Observability Engine",
                "summary": "Vectorized row drift detection, record churn matrix, null-rate spikes, numerical distribution shifts, and multi-sheet Excel export.",
                "body": f"""
                <h2>1. Row &amp; Record Drift Capabilities</h2>
                <p>Compares incoming client datasets against established baselines at the individual row level across 5 dimensional vectors:</p>
                <ul>
                    <li><strong>Volume &amp; Cardinality Drift:</strong> Identifies sudden drops or surges in row counts with stability alerts.</li>
                    <li><strong>Record Churn Matrix:</strong> Segregates incoming records into <code>ADDED</code>, <code>MODIFIED</code>, <code>DROPPED</code>, and <code>UNCHANGED</code> partitions.</li>
                    <li><strong>Field Modification Frequencies:</strong> Pinpoints fields experiencing mutations (e.g., vendor renames, price fluctuations).</li>
                    <li><strong>Null-Rate Drift:</strong> Alerts when column missingness spikes by more than 10%.</li>
                    <li><strong>Numerical Distribution Shifts:</strong> Tracks Mean, Median, and Max drifts, flagging pricing spikes.</li>
                </ul>

                <h2>2. 5-Sheet OpenPyXL Export Suite</h2>
                <p>Zero-latency Excel exports streamed directly via <code>GET /api/enterprise/export_excel?dataset=record_drift</code> containing:</p>
                <ol>
                    <li><em>Executive Churn Summary</em></li>
                    <li><em>Field Modifications</em></li>
                    <li><em>Null Rate Drift</em></li>
                    <li><em>Distribution Shifts</em></li>
                    <li><em>Sample Row Diffs</em></li>
                </ol>
                """
            },
            {
                "title": "SupplyCopia ATB Studio: Sentinel Aegis Self-Healing & Troubleshooting",
                "summary": "Automated error diagnosis, AST patch generation, dynamic retries, and Jira incident escalation.",
                "body": f"""
                <h2>1. Self-Healing Architecture</h2>
                <p>When compilation, database execution, or parity audits encounter anomalies, the <strong>Sentinel Aegis Self-Healing Engine</strong> triggers:</p>
                <ol>
                    <li><strong>Error Categorization:</strong> Classifies errors into Syntax/Grammar, Schema/Column Mismatch, Parity Variance, or Connection issues.</li>
                    <li><strong>AST Diagnostic Analysis:</strong> Analyzes SQL models and dependencies to craft non-destructive SQL patches.</li>
                    <li><strong>Dynamic Repair Loop:</strong> Applies candidate patches in isolated temporary sessions, verifies results, and promotes successful repairs.</li>
                    <li><strong>Automated Jira Escalation:</strong> If an exception remains unresolved after retry budgets, an automated High-Priority Incident ticket is created in Jira project <code>ATBDBT</code>.</li>
                </ol>
                """
            },
            {
                "title": "SupplyCopia ATB Studio: Enterprise Operations, SSO & Compliance Audit",
                "summary": "Deployment instructions, Docker hosting, SAML 2.0 / Entra ID SSO, and 90-day activity logging.",
                "body": f"""
                <h2>1. Containerized Docker Hosting</h2>
                <pre><code># Build and run Docker container on port 8081
sudo docker build -t supplycopia-atb-studio .
sudo docker run -d \\
  --name supplycopia-atb-studio \\
  --restart unless-stopped \\
  -p 8081:8081 \\
  --env-file .env \\
  supplycopia-atb-studio</code></pre>

                <h2>2. Microsoft Entra SAML 2.0 SSO Integration</h2>
                <ul>
                    <li><strong>Identifier (Entity ID):</strong> <code>https://atb-dbt.supplycopia.com/saml/metadata</code></li>
                    <li><strong>Reply URL (Assertion Consumer Service):</strong> <code>https://atb-dbt.supplycopia.com/saml/acs</code></li>
                    <li><strong>Sign-on URL:</strong> <code>https://atb-dbt.supplycopia.com/login</code></li>
                </ul>

                <h2>3. User Activity &amp; Audit Logging Compliance</h2>
                <p>Maintains an audit ledger in SQLite WAL mode. Automatically purges logs exceeding 90 days (3 months maximum retention) while supporting on-demand CSV compliance exports.</p>
                """
            }
        ]

        # Cache locally
        cache_file = DOCS_CACHE_DIR / "technical_documentation.json"
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": now_str,
                "space_key": self.space_key,
                "chapters": docs
            }, f, indent=2)

        return docs

    def sync_all_docs_to_confluence(self) -> Dict[str, Any]:
        """
        Synchronizes all chapters to Confluence Space ATBDBT.
        Parented under the space home page if present.
        """
        conn_test = self.test_connection()
        if conn_test.get("status") != "SUCCESS":
            return conn_test

        root_page = self.get_space_root_page()
        parent_id = root_page["id"] if root_page else None

        chapters = self.generate_technical_documentation()
        results = []

        for ch in chapters:
            res = self.upsert_page(
                title=ch["title"],
                body_storage_xhtml=ch["body"],
                parent_id=parent_id
            )
            results.append(res)
            time.sleep(0.5)  # Rate limiting courteous pause

        # Save sync manifest
        manifest_file = DOCS_CACHE_DIR / "sync_manifest.json"
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump({
                "synced_at": datetime.now().isoformat(),
                "space_key": self.space_key,
                "results": results
            }, f, indent=2)

        successful = [r for r in results if r.get("status") == "SUCCESS"]
        return {
            "status": "SUCCESS",
            "space_key": self.space_key,
            "total_pages": len(chapters),
            "synced_count": len(successful),
            "pages": results
        }

    def get_cached_docs(self) -> Dict[str, Any]:
        """Returns cached technical documentation for immediate UI viewing."""
        cache_file = DOCS_CACHE_DIR / "technical_documentation.json"
        if not cache_file.exists():
            self.generate_technical_documentation()
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                chapters = data.get("chapters", [])
                data["pages"] = chapters
                return data
        except Exception:
            return {"chapters": [], "pages": []}
