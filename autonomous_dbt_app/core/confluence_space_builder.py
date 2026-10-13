"""
SupplyCopia ATB Studio — Confluence Space Master Builder (Verbose Architecture & Process Flow Edition)
Constructs and publishes an exhaustive, highly detailed technical documentation suite into
Atlassian Confluence Space 'ATBDBT' (Ask The Bee - DBT Automation).
Replaces all screenshots with rich, responsive HTML process flow diagrams, detailed architecture
tables, exact mathematical formulas, data schemas, and comprehensive end-to-end operation manuals.
"""

import os
import sys
import json
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from autonomous_dbt_app.config import (
    ATLASSIAN_DOMAIN, ATLASSIAN_EMAIL, CONFLUENCE_SPACE_KEY,
    ROOT_DIR as PROJECT_ROOT
)
from autonomous_dbt_app.core.confluence_connector import ConfluenceConnector

DOCS_CACHE_DIR = PROJECT_ROOT / "agent_memory" / "confluence_docs_cache"
DOCS_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def render_process_flow(title: str, steps: List[Dict[str, str]], ascii_fallback: Optional[str] = None) -> str:
    """
    Renders an enterprise-grade responsive Process Flow Diagram in Atlassian Storage Format XHTML.
    Uses HTML5 card styling with distinct stage icons, step titles, micro-descriptions, and directional arrows.
    Also includes a text/ASCII representation for maximum portability.
    """
    flow_cells = []
    for idx, step in enumerate(steps):
        step_number = idx + 1
        icon = step.get("icon", "⚡")
        stitle = step.get("title", f"Step {step_number}")
        desc = step.get("desc", "")
        actor = step.get("actor", "")

        actor_badge = f'<div style="font-size: 9px; text-transform: uppercase; letter-spacing: 0.5px; background: #1e293b; color: #94a3b8; border-radius: 4px; padding: 2px 6px; margin-bottom: 4px; display: inline-block;">{actor}</div>' if actor else ""

        flow_cells.append(f"""
        <td style="vertical-align: top; width: {int(100/len(steps))}%; min-width: 140px; padding: 6px;">
            <div style="background: #090d16; border: 1px solid #1e293b; border-top: 3px solid #38bdf8; border-radius: 8px; padding: 12px 10px; text-align: center; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);">
                {actor_badge}
                <div style="font-size: 20px; line-height: 1; margin: 4px 0 6px 0;">{icon}</div>
                <div style="font-weight: 700; color: #f8fafc; font-size: 12px; margin-bottom: 4px;">{stitle}</div>
                <div style="font-size: 11px; color: #94a3b8; line-height: 1.35;">{desc}</div>
            </div>
        </td>
        """)

        if idx < len(steps) - 1:
            flow_cells.append("""
            <td style="vertical-align: middle; width: 24px; text-align: center; padding: 0;">
                <div style="color: #f59e0b; font-size: 18px; font-weight: bold; line-height: 1;">➔</div>
            </td>
            """)

    cells_html = "".join(flow_cells)

    ascii_html = ""
    if ascii_fallback:
        ascii_html = f"""
        <div style="margin-top: 10px; background: #020617; border: 1px dashed #334155; border-radius: 6px; padding: 10px 14px; font-family: monospace; font-size: 11px; color: #38bdf8; overflow-x: auto; white-space: pre;">
{ascii_fallback}
        </div>
        """

    return f"""
    <div style="background: #030712; border: 1px solid #1e293b; border-radius: 10px; padding: 14px 16px; margin: 18px 0 22px 0;">
        <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; padding-bottom: 8px; margin-bottom: 12px;">
            <div style="font-weight: 700; font-size: 13px; color: #38bdf8; text-transform: uppercase; letter-spacing: 0.6px;">
                ⚙️ PROCESS FLOW DIAGRAM: {title}
            </div>
            <span style="font-size: 10px; color: #64748b; background: #0f172a; padding: 2px 8px; border-radius: 4px; border: 1px solid #1e293b;">
                Interactive Architecture Model
            </span>
        </div>
        <div style="overflow-x: auto; padding-bottom: 6px;">
            <table style="width: 100%; border-collapse: separate; border-spacing: 0; border: none; margin: 0;">
                <tr>
                    {cells_html}
                </tr>
            </table>
        </div>
        {ascii_html}
    </div>
    """


class ConfluenceSpaceBuilder:
    """
    Constructs and publishes an exhaustive, 12-section technical documentation suite
    plus Master Documentation Hub into Confluence Space ATBDBT.
    """

    def __init__(self, connector: Optional[ConfluenceConnector] = None):
        self.connector = connector or ConfluenceConnector(space_key=CONFLUENCE_SPACE_KEY)

    # -------------------------------------------------------------------------
    # MASTER HUB
    # -------------------------------------------------------------------------
    def get_master_hub_content(self, now_str: str) -> str:
        master_flow = render_process_flow(
            "End-to-End Autonomous Transformation Pipeline Architecture",
            [
                {"icon": "📥", "actor": "Scout Bee Buzz", "title": "1. Multi-Tenant Ingest", "desc": "S3 & local file sync, SHA-256 fingerprinting & SCD-1 entity merge."},
                {"icon": "🔍", "actor": "Scout Bee Buzz", "title": "2. Raw Profiling", "desc": "DuckDB schema inference, cardinality checks & null-rate spike detection."},
                {"icon": "📐", "actor": "Architect Bee", "title": "3. Semantic Topology", "desc": "Natural composite joins, 4-tier item cascade & clinical UOM matrix."},
                {"icon": "⚙️", "actor": "Worker Bee", "title": "4. DBT Compilation", "desc": "Vectorized 4-tier execution (Staging, Intermediate, Marts, Security)."},
                {"icon": "📊", "actor": "Clinical Analytics", "title": "5. Mart Explorer", "desc": "Dimensional fact exploration, real-time KPI tiles & procedure taxonomy."},
                {"icon": "🛡️", "actor": "Inspector Bee", "title": "6. Golden Parity", "desc": "$243.9M zero-dollar mathematical audit & Gate 2 certification."},
                {"icon": "🔄", "actor": "Refinement Swarm", "title": "7. Refine & Evolve", "desc": "Feedback evidence ingestion, self-healing & living playbook."}
            ],
            ascii_fallback="[AWS S3 / Client Files] ➔ [Scout Profiling] ➔ [Architect Topology (Gate 1)] ➔ [Worker DBT Execution] ➔ [Output Fact Marts] ➔ [Inspector Parity ($243.9M Gate 2)] ➔ [Snowflake RLS / Self-Healing]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>SupplyCopia Autonomous DBT Pipeline Studio (Ask The Bee)</strong> • Enterprise Technical Documentation Hub.<br/>
                Space Key: <code>ATBDBT</code> • Version: <strong>2.4 Production Enterprise</strong> • Last Synchronized: <strong>{now_str}</strong>.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>📖 Executive Platform Overview &amp; Master Index</h2>
        <p>The <strong>SupplyCopia Autonomous DBT Pipeline Studio (Ask The Bee)</strong> is an enterprise-grade, multi-agent cognitive compiler engineered to solve one of healthcare's most complex data engineering challenges: <em>the automated harmonization, clinical unit-of-measure (UOM) normalization, composite join mapping, and mathematical reconciliation of multi-million dollar hospital supply chain and clinical procedure datasets.</em></p>
        
        <p>Traditional healthcare ELT implementations require months of manual SQL authoring, fragile bespoke transformation pipelines, and human data engineering handoffs that frequently result in contract price discrepancies, UOM packaging mismatches (e.g., billing Box of 10 prices for individual eaches), and undetected uncontracted spend leakage. Ask The Bee replaces this fragile cycle with a fully deterministic, self-healing multi-agent swarm that operates with <strong>100% mathematical spend parity ($243,934,093.08 golden baseline)</strong> and zero data variance.</p>

        {master_flow}

        <hr/>

        <h2>🗂️ Master Technical Documentation Directory</h2>
        <p>This knowledge base is hierarchically structured into 12 exhaustive operational sections. Each child document details exact system algorithms, SQL transformation logic, schema data contracts, configuration parameters, and operator runbooks:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 280px;">Documentation Section &amp; Subsystem</th>
                    <th>Core Architecture &amp; Algorithmic Capabilities</th>
                    <th style="width: 140px;">Primary Agents</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081619971">1. Executive Summary &amp; Multi-Agent Swarm</a></strong></td>
                    <td>High-level architecture, 8 specialized cognitive bee personas, in-process DuckDB columnar OLAP engine, and quantifiable hospital ROI benchmarks.</td>
                    <td>Queen Bee Orla, All Swarm Agents</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081325066">2. End-to-End Pipeline &amp; 7-Stage Workflow</a></strong></td>
                    <td>Finite state machine execution lifecycle, tri-mode Autonomy Control Deck (Default HITL, Approval Gates, Turbo), and zero-loss state snapshotting.</td>
                    <td>Queen Bee Orla, State Controller</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2080833541">3. Stage 1 — Multi-Tenant Ingestion &amp; Catalog</a></strong></td>
                    <td>AWS S3 bucket synchronization, SHA-256 cryptographic tenant catalogs, and Smart Entity Merging (Latest Wins SCD-1, Union Distinct, Additive Append).</td>
                    <td>Scout Bee Buzz, Ingestion Engine</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081325085">4. Stage 2 — Raw Profiling &amp; Cardinality</a></strong></td>
                    <td>Automated columnar type inference, cardinality discovery, null-rate spike thresholding (&gt;10%), and clinical sanity diagnostics.</td>
                    <td>Scout Bee Buzz, DuckDB Profiler</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2080800772">5. Stage 3 — Semantic Topology &amp; Joins</a></strong></td>
                    <td>Automated composite foreign key discovery, 4-tier item matching cascade (100% to 70% confidence), clinical UOM conversion matrix, and HITL Gate 1.</td>
                    <td>Architect Bee Pollen</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2080800791">6. Stage 4 — Autonomous DBT Lineage &amp; DAG</a></strong></td>
                    <td>4-tier transformation hierarchy (Staging, Intermediate, Marts, Security), Cytoscape.js interactive DAG visualizer, and embedded DBT Docs server.</td>
                    <td>Worker Bee Nectar, dbt-duckdb</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081652738">7. Stage 5 — Output Explorer, Fact Marts &amp; Analytics</a></strong></td>
                    <td>Production analytical marts (<code>fct_gap_analysis_v4</code>, <code>fct_po_cost_savings_v4</code>), interactive KPI tiles, and clinical procedure taxonomy.</td>
                    <td>Clinical Analytics Engine</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081554438">8. Stage 6 — 100% Golden Parity &amp; Mathematical Audit</a></strong></td>
                    <td>Zero-dollar mathematical spend reconciliation ($243,934,093.08 exact match), 4-layer assertion test suite, and HITL Gate 2 certification.</td>
                    <td>Inspector Bee Guard</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081619996">9. Stage 7 — Refine &amp; Evolve: Retrospective Feedback</a></strong></td>
                    <td>Multi-modal finding intake (text, Excel/CSV, Python scripts), dynamic implementation plans with diff previews, and living institutional playbook.</td>
                    <td>Refinement Swarm, Playbook Engine</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081620015">10. Sentinel Aegis Self-Healing &amp; Jira Escalation</a></strong></td>
                    <td>Autonomous error diagnosis, AST-level SQL model repair, gateway auto-recovery, and automated Jira Cloud ticket creation (Project ATBDBT).</td>
                    <td>Sentinel Aegis, Jira REST API</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2081456140">11. Enterprise Suite — Observability &amp; Governance</a></strong></td>
                    <td>Record &amp; row drift engine with 5-sheet OpenPyXL export, Snowflake direct push with dynamic RLS DDL, and 90-day SQLite WAL activity logging.</td>
                    <td>Enterprise Suite Agent, Snowflake Bridge</td>
                </tr>
                <tr>
                    <td><strong><a href="https://supplycopia.atlassian.net/wiki/spaces/ATBDBT/pages/2080997655">12. Deployment, Infrastructure &amp; DevOps Runbook</a></strong></td>
                    <td>Self-contained Docker packaging (Port 8081), Ubuntu VM hosting specs, Nginx reverse proxy configuration, and Microsoft Entra SAML 2.0 SSO.</td>
                    <td>DevOps &amp; Security Engineering</td>
                </tr>
            </tbody>
        </table>

        <ac:structured-macro ac:name="tip">
            <ac:rich-text-body>
                <p><strong>Operator Guidance:</strong> Each section can be browsed directly via the navigation tree on the left. In the local studio UI at <code>http://localhost:8081</code>, click the <strong>📘 Confluence Docs</strong> modal to view synchronized sections, search chapters, and trigger space updates.</p>
            </ac:rich-text-body>
        </ac:structured-macro>
        """

    # -------------------------------------------------------------------------
    # SECTION 1: EXECUTIVE SUMMARY & COGNITIVE SWARM ARCHITECTURE
    # -------------------------------------------------------------------------
    def get_section_1_content(self) -> str:
        swarm_flow = render_process_flow(
            "Multi-Agent Cognitive Swarm Orchestration & Event Bus",
            [
                {"icon": "👑", "actor": "Queen Bee Orla", "title": "Orchestrator", "desc": "Initializes run, coordinates event stream & enforces approval gates."},
                {"icon": "🐝", "actor": "Scout Bee Buzz", "title": "Cloud Ingestion", "desc": "Ingests S3/local feeds, profiles schemas & detects anomaly spikes."},
                {"icon": "📐", "actor": "Architect Bee", "title": "Semantic Topology", "desc": "Discovers foreign keys, maps 4-tier cascades & normalizes UOM."},
                {"icon": "⚙️", "actor": "Worker Bee", "title": "DBT Execution", "desc": "Compiles staging, intermediate & mart SQL tables in DuckDB."},
                {"icon": "🛡️", "actor": "Inspector Bee", "title": "Parity Audit", "desc": "Reconciles $243.9M total spend & certifies zero variance."},
                {"icon": "🩺", "actor": "Sentinel Aegis", "title": "Self-Healing", "desc": "Interprets errors, applies SQL AST patches & escalates to Jira."}
            ],
            ascii_fallback="[Queen Bee: State Machine] ➔ [Scout: Ingest & Profile] ➔ [Architect: Joins & UOM] ➔ [Worker: Vectorized SQL] ➔ [Inspector: Parity Certification] ➔ [Sentinel: Self-Healing]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 1: Cognitive Swarm &amp; Architectural Kernel</strong> • Core system topology and multi-agent coordination mechanics of Ask The Bee.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>1.1 Executive Vision &amp; Healthcare Supply Chain Problem Statement</h2>
        <p>Enterprise healthcare systems operate across heterogeneous, fragmented Electronic Health Record (EHR) and Enterprise Resource Planning (ERP) databases, including Epic Systems, Cerner Millennium, Infor Lawson, Workday Supply Chain, and disparate departmental spreadsheets. In typical multi-hospital systems such as UC Health, supply chain and surgical procurement data is distributed across distinct operational silos:</p>
        
        <ul>
            <li><strong>Purchase Orders (PO):</strong> Transactional commitments capturing line-level unit pricing, vendor contracts, ordering facilities, and purchase quantities.</li>
            <li><strong>Vendor Invoices:</strong> Billed quantities and remittances submitted by manufacturers and medical distributors, frequently differing in unit packaging from purchase orders.</li>
            <li><strong>Surgical Case Consumption:</strong> Point-of-use operating room (OR) logs capturing physician item utilization, clinical preference cards, case procedure types, and waste.</li>
            <li><strong>Vendor Contracts:</strong> Master price agreements negotiated with medical device manufacturers specifying tiered rebate thresholds and contracted unit caps.</li>
            <li><strong>Hospital Item Master:</strong> The internal ERP catalog cross-referencing internal item identifiers with Global Trade Item Numbers (GTIN) and Manufacturer Part Numbers (MPN).</li>
        </ul>

        <p>Manual data consolidation across these systems leads to chronic <strong>pricing discrepancies</strong>, <strong>uncontracted spend leakage</strong>, <strong>UOM packaging distortions</strong> (e.g., billing Box-of-10 prices for individual eaches), and transformation cycles that take data engineering teams weeks or months to reconcile. Ask The Bee solves this through an autonomous, multi-agent cognitive architecture that executes deterministic, reproducible data transformations in seconds.</p>

        {swarm_flow}

        <hr/>

        <h2>1.2 Multi-Agent Cognitive Swarm Blueprint</h2>
        <p>Ask The Bee replaces monolithic, static ETL pipelines with an ensemble of eight autonomous, specialized cognitive agents known as <strong>Bee Personas</strong>. Each agent maintains isolated domain expertise, operational state, and decision boundaries:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 170px;">Agent Persona</th>
                    <th style="width: 160px;">Architectural Role</th>
                    <th>Core Cognitive Responsibilities &amp; Algorithmic Actions</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>👑 Queen Bee Orla</strong></td>
                    <td>Autonomous Orchestrator &amp; State Controller</td>
                    <td>Directs end-to-end pipeline execution across all seven stages. Enforces Human-In-The-Loop (HITL) gates, pauses runs at critical milestones, broadcasts real-time SSE progress events to the Studio UI, and manages atomic state checkpoints.</td>
                </tr>
                <tr>
                    <td><strong>🐝 Scout Bee Buzz</strong></td>
                    <td>Cloud Ingestion &amp; Statistical Profiler</td>
                    <td>Monitors incoming AWS S3 buckets and local file drops. Calculates SHA-256 cryptographic checksums, detects column data types via DuckDB schema inference, calculates cardinality and null percentages, and raises proactive null-rate spike alerts.</td>
                </tr>
                <tr>
                    <td><strong>📐 Architect Bee Pollen</strong></td>
                    <td>Semantic Topology &amp; Join Discovery</td>
                    <td>Analyzes column name semantics, cardinality ratios, and composite value overlaps to discover natural foreign keys. Maps 4-tier item matching cascades and enforces clinical Unit of Measure (UOM) conversion factors. Prepares Gate 1 Topology Blueprint.</td>
                </tr>
                <tr>
                    <td><strong>⚙️ Worker Bee Nectar</strong></td>
                    <td>Vectorized DBT Execution Engine</td>
                    <td>Generates and compiles modular DBT SQL transformation models organized across four strict layers (Staging, Intermediate, Dimensional Marts, Security). Executes models in-process using the vectorized columnar DuckDB analytical engine.</td>
                </tr>
                <tr>
                    <td><strong>🛡️ Inspector Bee Guard</strong></td>
                    <td>Mathematical Parity Reconciler</td>
                    <td>Executes dual-ledger mathematical reconciliations comparing raw ingested spend against aggregated dimensional mart spend. Asserts <strong>100% zero-dollar variance ($243,934,093.08)</strong>, validates procedure cardinality, and prepares Gate 2 Certification.</td>
                </tr>
                <tr>
                    <td><strong>🩺 Sentinel Aegis</strong></td>
                    <td>Self-Healing &amp; Incident Escalation</td>
                    <td>Intercepts compilation timeouts, SQL grammar exceptions, and governance assertion failures. Diagnoses root causes via AST inspection, generates non-destructive SQL patches, and automatically escalates unresolvable issues to Jira Cloud (Project ATBDBT).</td>
                </tr>
                <tr>
                    <td><strong>📈 Refinement Agent</strong></td>
                    <td>Clinical Retrospective &amp; Playbook</td>
                    <td>Ingests clinical and operator feedback, evidence spreadsheets, and Python patch scripts. Synthesizes iterative implementation plans and updates the living Institutional Knowledge Playbook to permanently preserve domain learnings across runs.</td>
                </tr>
                <tr>
                    <td><strong>🏛️ Enterprise Suite Agent</strong></td>
                    <td>Governance, Drift &amp; Security</td>
                    <td>Analyzes record-level drift across periodic data drops, generates 5-sheet OpenPyXL analytical workbooks, orchestrates direct Snowflake data warehouse pushes, and enforces multi-tenant Row-Level Security (RLS) DDL policies.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>1.3 In-Process DuckDB Columnar Analytical Engine</h2>
        <p>Unlike legacy data transformation systems that depend on distributed Apache Spark clusters or heavy cloud data warehouse spin-up cycles, Ask The Bee utilizes an embedded, in-process <strong>DuckDB columnar OLAP engine</strong>:</p>

        <ul>
            <li><strong>Vectorized SIMD Processing:</strong> Operates on columnar vectors using modern CPU SIMD instructions, processing tens of millions of clinical records in sub-second timeframes with zero network round-trip overhead.</li>
            <li><strong>Native DBT Integration:</strong> Full ANSI SQL compatibility powered by <code>dbt-duckdb</code>, enabling production DBT models, CTE chains, window functions, and Jinja macros to run completely in-process.</li>
            <li><strong>ACID Transactional File Storage:</strong> Persists all staging tables, dimensional marts, and metric aggregations into a portable single-file database (<code>autonomous_pipeline.duckdb</code>) supporting instant checkpointing.</li>
            <li><strong>Memory-Efficient Column Execution:</strong> Automatically spills large aggregations to temporary disk storage if memory thresholds are exceeded, ensuring rock-solid stability even on modest cloud virtual machines.</li>
        </ul>

        <hr/>

        <h2>1.4 Measurable Enterprise Business Value</h2>
        <p>Deploying Ask The Bee across hospital healthcare networks delivers immediate, quantifiable return on investment:</p>
        <ol>
            <li><strong>$243.9M Total Spend Visibility:</strong> 100% mathematical auditability with zero unexplained revenue variance across tens of thousands of purchase orders and clinical invoices.</li>
            <li><strong>Elimination of UOM Price Overcharges:</strong> Automatic normalization of Box/Case packaging ratios saves hospital health systems between 4% to 9% in erroneous vendor billing per year.</li>
            <li><strong>95% Reduction in Engineering Lead Time:</strong> Transforms a multi-month manual data modeling exercise into a fully autonomous, 60-second compilation and self-healing pipeline.</li>
            <li><strong>Permanent Institutional Memory:</strong> Clinical corrections and vendor pricing exceptions are captured permanently in the Playbook ledger, guaranteeing errors never recur in future runs.</li>
        </ol>
        """

    # -------------------------------------------------------------------------
    # SECTION 2: END-TO-END PIPELINE & 7-STAGE WORKFLOW
    # -------------------------------------------------------------------------
    def get_section_2_content(self) -> str:
        lifecycle_flow = render_process_flow(
            "7-Stage Autonomous Transformation State Machine",
            [
                {"icon": "1️⃣", "actor": "Scout Bee", "title": "Stage 1: Ingest", "desc": "S3 & local file sync, SHA-256 fingerprinting & tenant catalog registration."},
                {"icon": "2️⃣", "actor": "Scout Bee", "title": "Stage 2: Profile", "desc": "Schema detection, column cardinality & statistical null-rate spike thresholding."},
                {"icon": "3️⃣", "actor": "Architect Bee", "title": "Stage 3: Topology", "desc": "Natural join discovery, 4-tier matching & UOM matrix. 🛑 GATE 1 APPROVAL."},
                {"icon": "4️⃣", "actor": "Worker Bee", "title": "Stage 4: Lineage", "desc": "DBT 4-tier model compilation in DuckDB & Cytoscape DAG lineage rendering."},
                {"icon": "5️⃣", "actor": "Analytics", "title": "Stage 5: Explorer", "desc": "Analytical fact marts exploration, real-time KPI tiles & procedure grouping."},
                {"icon": "6️⃣", "actor": "Inspector Bee", "title": "Stage 6: Parity", "desc": "Zero-dollar mathematical audit ($243.9M) & assertions. 🛑 GATE 2 CERTIFY."},
                {"icon": "7️⃣", "actor": "Refine Swarm", "title": "Stage 7: Evolve", "desc": "Retrospective feedback intake, implementation plan diffs & playbook update."}
            ],
            ascii_fallback="[Stage 1: Ingest] ➔ [Stage 2: Profile] ➔ [Stage 3: Topology (GATE 1)] ➔ [Stage 4: DBT Lineage] ➔ [Stage 5: Mart Explorer] ➔ [Stage 6: Parity (GATE 2)] ➔ [Stage 7: Refine & Evolve]"
        )

        control_flow = render_process_flow(
            "Autonomy Control Deck Execution Modes",
            [
                {"icon": "⏸️", "actor": "Default Mode", "title": "Step-by-Step HITL", "desc": "Pipeline pauses at each stage for granular inspection of schemas and diffs."},
                {"icon": "🛡️", "actor": "Review Mode", "title": "Approval Gates", "desc": "Runs autonomously but strictly pauses at Gate 1 (Topology) and Gate 2 (Parity)."},
                {"icon": "⚡", "actor": "Turbo Mode", "title": "Full Autonomous", "desc": "Runs end-to-end continuously from S3 ingestion through self-healing and Snowflake."}
            ],
            ascii_fallback="[Default Mode: Pause Every Stage] | [User Review Mode: Pause at Gates 1 & 2 Only] | [Turbo Mode: Zero Pauses, Full Continuous Execution]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 2: 7-Stage Pipeline Lifecycle</strong> • Comprehensive execution state machine, governance control deck, and persistence mechanics.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>2.1 The 7-Stage Autonomous Transformation Workflow</h2>
        <p>The core lifecycle of Ask The Bee is governed by a deterministic, finite state machine orchestrated by Queen Bee Orla. Data moves through seven strictly isolated stages, each enforcing comprehensive data contracts, validation checks, and output schemas:</p>

        {lifecycle_flow}

        <table>
            <thead>
                <tr>
                    <th style="width: 90px;">Stage</th>
                    <th style="width: 170px;">Stage Name</th>
                    <th style="width: 190px;">Input Artifacts</th>
                    <th style="width: 220px;">Synthesized Outputs</th>
                    <th>Governance Gate &amp; Validation Check</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Stage 1</strong></td>
                    <td>Multi-Tenant Ingestion</td>
                    <td>AWS S3 bucket feeds, manual CSV/Excel uploads, REST payloads</td>
                    <td>Tenant Cryptographic Catalog (SHA-256), Staged Raw Parquet/DuckDB tables</td>
                    <td>Checksum verification, file integrity validation, deduplication check</td>
                </tr>
                <tr>
                    <td><strong>Stage 2</strong></td>
                    <td>Raw Schema Profiling</td>
                    <td>Staged raw datasets (Consumption, PO, Invoices, Contracts, Item Master)</td>
                    <td>Data types, distinct counts, cardinality ratios, null percentages, memory footprint</td>
                    <td>Null-rate spike threshold check (&gt;10% delta triggers alert), negative price rejection</td>
                </tr>
                <tr>
                    <td><strong>Stage 3</strong></td>
                    <td>Semantic Topology &amp; Joins</td>
                    <td>Classified raw tables and column metadata</td>
                    <td>Discovered natural composite joins, 4-tier item matching rules, clinical UOM factors</td>
                    <td><strong>🛑 HITL Gate 1: Topology Approval</strong> (Clinical analyst confirms join blueprint)</td>
                </tr>
                <tr>
                    <td><strong>Stage 4</strong></td>
                    <td>DBT Lineage Compilation</td>
                    <td>DBT models, seeds, Jinja macros, and topology blueprint</td>
                    <td>In-memory DuckDB tables, Cytoscape.js interactive DAG graph, compiled DBT docs</td>
                    <td>SQL syntax validation, model dependency assertion, zero compilation errors</td>
                </tr>
                <tr>
                    <td><strong>Stage 5</strong></td>
                    <td>Output Explorer &amp; Marts</td>
                    <td>Compiled dimensional marts</td>
                    <td>Interactive KPI tiles, filtered fact table rows, clinical procedure spend rollups</td>
                    <td>Schema contract conformity, non-empty marts assertion, financial KPI bounds</td>
                </tr>
                <tr>
                    <td><strong>Stage 6</strong></td>
                    <td>100% Golden Parity Audit</td>
                    <td>Raw source spend vs Aggregated dimensional mart spend</td>
                    <td>Mathematical Parity Certificate, 4-layer assertion test log, variance breakdown</td>
                    <td><strong>🛑 HITL Gate 2: Certification Gate</strong> (Zero dollar variance verified: $243,934,093.08)</td>
                </tr>
                <tr>
                    <td><strong>Stage 7</strong></td>
                    <td>Refine &amp; Evolve (HITL)</td>
                    <td>Clinical text feedback, Excel/CSV evidence, Python script patches</td>
                    <td>Auto-Resolve Implementation Plan, model SQL patches, updated Playbook ledger</td>
                    <td>Plan confirmation, regression parity verification, institutional knowledge append</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>2.2 Autonomy Control Deck (Execution Modes)</h2>
        <p>The Studio header features the <strong>Autonomy Control Deck</strong>, allowing operators to select the degree of automation required for a specific run:</p>

        {control_flow}

        <ol>
            <li><strong>Default Mode (Step-by-Step HITL):</strong> The studio pauses after every single stage. This mode is ideal during initial onboarding of a new healthcare client, allowing analysts to inspect column mappings, view raw distributions, review intermediate join candidates, and inspect transformed data line-by-line.</li>
            <li><strong>User Review Mode (Approval Gates):</strong> Executes stages 1 through 5 autonomously, pausing strictly at the two decisive governance checkpoints:
                <ul>
                    <li><strong>Gate 1 (Semantic Topology Approval):</strong> Operator inspects discovered join keys and UOM conversion factors before initiating heavy SQL compilation.</li>
                    <li><strong>Gate 2 (100% Golden Parity Certification):</strong> Operator reviews the mathematical audit report confirming $0.00 spend variance before authorizing production Snowflake sync.</li>
                </ul>
            </li>
            <li><strong>⚡ Turbo Mode (Full Autonomous End-to-End):</strong> Executes the entire pipeline continuously from cloud ingestion through schema profiling, topology mapping, DBT compilation, Sentinel Aegis self-healing, mathematical audit, and Snowflake push without manual intervention. Ideal for scheduled nightly batch production runs.</li>
        </ol>

        <hr/>

        <h2>2.3 Checkpoint Management &amp; Atomic State Recovery</h2>
        <p>To prevent costly re-processing of large datasets when fine-tuning downstream models, Ask The Bee incorporates an atomic checkpoint persistence engine:</p>

        <ul>
            <li><strong>Automatic Stage Checkpoints:</strong> At every stage transition, the system snapshots the DuckDB memory state, active agent memory, and compilation blueprints into <code>agent_memory/checkpoints/checkpoint_stage_N.json</code>.</li>
            <li><strong>Saved Pipelines Drawer:</strong> Operators can name and save snapshot states (e.g., <em>"UC_Health_Pre_Topology_Baseline_20261013"</em>) directly from the UI header.</li>
            <li><strong>Instant Rollback &amp; Resumption:</strong> If an operator experiments with a custom Python script or SQL rule in Stage 7 and desires to revert, selecting a previous checkpoint restores the database state in under 500 milliseconds.</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 3: STAGE 1 — MULTI-TENANT INGESTION & SMART CATALOG
    # -------------------------------------------------------------------------
    def get_section_3_content(self) -> str:
        ingest_flow = render_process_flow(
            "Stage 1: Multi-Tenant Ingestion & Smart Catalog Workflow",
            [
                {"icon": "☁️", "actor": "Data Intake", "title": "1. Multi-Channel Ingest", "desc": "AWS S3 polling, portal drag-and-drop & programmatic REST API payloads."},
                {"icon": "🔒", "actor": "Scout Bee", "title": "2. SHA-256 Fingerprint", "desc": "Calculates cryptographic file hashes, byte sizes & row counts."},
                {"icon": "🗂️", "actor": "Catalog Engine", "title": "3. Tenant Catalog", "desc": "Logs metadata into isolated JSON catalog, auto-incrementing versions (v1->v2)."},
                {"icon": "🔀", "actor": "Merge Engine", "title": "4. Entity Merging", "desc": "Executes Latest Wins (SCD-1), Union Distinct, or Additive Append in DuckDB."}
            ],
            ascii_fallback="[AWS S3 / Client Files] ➔ [SHA-256 Checksum Engine] ➔ [Tenant Catalog Registration] ➔ [Entity Merge & Deduplication] ➔ [Staged Parquet / DuckDB Tables]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 3: Ingestion &amp; Smart Merging</strong> • Multi-source data intake, cryptographic catalog versioning, and SCD-1 deduplication.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>3.1 Ingestion Sources &amp; Cloud Storage Connectors</h2>
        <p>In Stage 1, Scout Bee Buzz establishes connections across three primary intake vectors to acquire hospital supply chain feeds:</p>

        {ingest_flow}

        <ol>
            <li><strong>AWS S3 Cloud Bucket Connector:</strong> Configured via <code>S3_URL</code> and AWS credentials in <code>.env</code>. The Scout Bee polls designated bucket prefixes (e.g., <code>s3://supplycopia-dbt-ingestion/uc_health/</code>) for newly uploaded files, downloading and staging them into local high-speed SSD storage.</li>
            <li><strong>Studio Portal Upload Dropzone:</strong> Allows clinical operators to drag-and-drop ad-hoc monthly exports, supporting large CSV (comma and pipe-delimited) and Excel (<code>.xlsx</code>, <code>.xls</code>) workbooks with automatic sheet detection.</li>
            <li><strong>Programmatic REST API Ingestion:</strong> External hospital orchestration scripts can register and upload files via <code>POST /api/client_tracker/register</code> with metadata payloads specifying client tenant ID, domain entity type, and operator notes.</li>
        </ol>

        <hr/>

        <h2>3.2 Cryptographic Tenant Ingestion Catalog Architecture</h2>
        <p>To maintain enterprise auditability and provenance, every ingested file is fingerprinted using cryptographic <strong>SHA-256 hashing</strong> and recorded in an isolated per-tenant catalog ledger (<code>agent_memory/client_catalogs/{{client_id}}_catalog.json</code>):</p>

        <ul>
            <li><strong>Deterministic Checksumming:</strong> Guarantees bit-level integrity. If an identical file is submitted multiple times, the catalog suppresses redundant processing.</li>
            <li><strong>Automated Version Incrementing:</strong> When an updated file for an existing domain entity is received (e.g., an updated Item Master containing new cardiac implants), the version auto-increments (<code>v1 ➔ v2</code>).</li>
            <li><strong>Lifecycle State Machine:</strong> The catalog maintains distinct lifecycle states:
                <ul>
                    <li><code>ACTIVE</code>: The current operational dataset utilized in active DBT compilations.</li>
                    <li><code>SUPERSEDED</code>: Previous versions preserved for retrospective auditability and historical rollbacks.</li>
                    <li><code>ARCHIVED</code>: Deprecated files marked for cold storage retention.</li>
                </ul>
            </li>
            <li><strong>Provenance Metadata:</strong> Stores exact byte length, row count, column list, ingestion timestamp, uploading user ID, and client tenant key.</li>
        </ul>

        <hr/>

        <h2>3.3 Smart Entity Merging &amp; De-duplication Engine</h2>
        <p>Healthcare clients frequently provide periodic, fragmented, or overlapping file extracts for the same business entity. The <strong>Smart Entity Merging Engine</strong> executes vectorized in-process DuckDB consolidations using three algorithms:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 180px;">Merge Strategy</th>
                    <th style="width: 280px;">Mathematical SQL Algorithm</th>
                    <th>Healthcare Domain Application &amp; Use Case</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Latest Wins (SCD-1)</strong></td>
                    <td>
                        <pre><code>ROW_NUMBER() OVER (
  PARTITION BY item_number 
  ORDER BY version_num DESC, 
           effective_date DESC
) = 1</code></pre>
                    </td>
                    <td><strong>Item Master &amp; Contract Amendments:</strong> When a client submits a mid-year pricing addendum or re-indexed Item Master, newer vendor catalog numbers and contracted unit prices overwrite older records while preserving historical attributes for untouched items.</td>
                </tr>
                <tr>
                    <td><strong>Union Distinct</strong></td>
                    <td>
                        <pre><code>SELECT DISTINCT * 
FROM (
  SELECT * FROM source_batch_1 
  UNION ALL 
  SELECT * FROM source_batch_2
);</code></pre>
                    </td>
                    <td><strong>Departmental Overlap Deduplication:</strong> Resolves identical records produced when disparate hospital surgical suites (e.g., University Hospital OR vs. West Chester Surgical Pavilion) export overlapping supply lists.</td>
                </tr>
                <tr>
                    <td><strong>Additive Append</strong></td>
                    <td>
                        <pre><code>SELECT * FROM historical_po
UNION ALL
SELECT * FROM incremental_po
WHERE po_line_id NOT IN (
  SELECT po_line_id FROM historical_po
);</code></pre>
                    </td>
                    <td><strong>Monthly Transactional Rollups:</strong> Appends newly released Purchase Order lines and Invoice remittances to the historical fiscal year baseline without duplicating existing lines.</td>
                </tr>
            </tbody>
        </table>
        """

    # -------------------------------------------------------------------------
    # SECTION 4: STAGE 2 — RAW SCHEMA PROFILING & CARDINALITY
    # -------------------------------------------------------------------------
    def get_section_4_content(self) -> str:
        profiling_flow = render_process_flow(
            "Stage 2: DuckDB Schema Profiling & Statistical Quality Engine",
            [
                {"icon": "⚡", "actor": "DuckDB Kernel", "title": "1. Schema Sniffing", "desc": "Samples columns, infers ANSI types & strips special characters."},
                {"icon": "📊", "actor": "Profiler", "title": "2. Cardinality Analysis", "desc": "Computes distinct counts, uniqueness ratios & primary key viability."},
                {"icon": "🔍", "actor": "Governance", "title": "3. Null-Rate Spike Check", "desc": "Quantifies missingness; flags any column with >10% historical spike."},
                {"icon": "🛡️", "actor": "Clinical Rules", "title": "4. Domain Sanity Rules", "desc": "Rejects negative costs, flags zero-spend lines & verifies date sequences."}
            ],
            ascii_fallback="[Raw Table Ingest] ➔ [Column Type Inference] ➔ [Cardinality & Primary Key Check] ➔ [Null-Rate Spike Filter] ➔ [Domain Sanity Rules] ➔ [Profile Blueprint]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 4: Profiling &amp; Cardinality</strong> • Automated data quality inspection, statistical profiling, and schema anomaly diagnostics.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>4.1 Automated In-Process Columnar Profiling</h2>
        <p>In Stage 2, Scout Bee Buzz analyzes the five canonical clinical supply chain tables staged in DuckDB. Rather than executing slow full-table scans, the profiler utilizes vectorized sampling and columnar metadata inspection to extract comprehensive statistics:</p>

        {profiling_flow}

        <ul>
            <li><strong>Automated Data Type &amp; Dialect Inference:</strong> Distinguishes ISO-8601 dates, currency decimals (stripping commas and dollar symbols), integer identifiers, and clinical alphanumeric strings.</li>
            <li><strong>Cardinality &amp; Uniqueness Ratios:</strong> Identifies potential primary key candidates by evaluating uniqueness ratios (<code>COUNT(DISTINCT col) / COUNT(*)</code>). Columns exhibiting 100% uniqueness are cataloged as natural key anchors.</li>
            <li><strong>Column Density &amp; Missingness Metrics:</strong> Computes exact null counts, non-null counts, and missingness percentages for every field.</li>
            <li><strong>Memory &amp; Storage Footprint Quantification:</strong> Calculates uncompressed vs. compressed byte sizes, average row widths, and total table sizes to optimize subsequent DBT model materialization strategies (views vs. physical tables).</li>
        </ul>

        <hr/>

        <h2>4.2 Five Canonical Healthcare Datasets Profiling Baseline</h2>
        <p>The profile benchmarks established across the UC Health implementation dataset are summarized below:</p>

        <table>
            <thead>
                <tr>
                    <th>Canonical Table Name</th>
                    <th>Row Count</th>
                    <th>Column Count</th>
                    <th>Key Natural Identifiers</th>
                    <th>Observed Data Quality Characteristics</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong><code>raw_purchase_orders</code></strong></td>
                    <td>13,546</td>
                    <td>18</td>
                    <td><code>po_number</code>, <code>po_line_no</code></td>
                    <td>Clean composite keys; 100% pop on vendor names; zero negative prices.</td>
                </tr>
                <tr>
                    <td><strong><code>raw_invoices</code></strong></td>
                    <td>13,546</td>
                    <td>14</td>
                    <td><code>invoice_number</code>, <code>po_number</code>, <code>po_line_no</code></td>
                    <td>Perfect 1-to-1 parity against purchase orders on composite key.</td>
                </tr>
                <tr>
                    <td><strong><code>raw_consumption</code></strong></td>
                    <td>24,890</td>
                    <td>22</td>
                    <td><code>log_id</code>, <code>item_number</code>, <code>case_id</code></td>
                    <td>Captures OR surgical cases, physician IDs, and procedure codes across service lines.</td>
                </tr>
                <tr>
                    <td><strong><code>raw_contracts</code></strong></td>
                    <td>4,820</td>
                    <td>12</td>
                    <td><code>contract_number</code>, <code>item_number</code></td>
                    <td>Tiered vendor pricing tiers; contracted unit price caps.</td>
                </tr>
                <tr>
                    <td><strong><code>raw_item_master</code></strong></td>
                    <td>8,915</td>
                    <td>16</td>
                    <td><code>item_number</code>, <code>manuf_cat_no</code></td>
                    <td>Master taxonomy cross-referencing internal ERP IDs to clinical descriptions.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>4.3 Clinical Data Governance Rules &amp; Anomaly Detection</h2>
        <p>Before advancing to semantic join mapping, Scout Bee Buzz evaluates datasets against four non-negotiable clinical data sanity assertions:</p>

        <ol>
            <li><strong>Null Rate Spike Threshold (&gt;10%):</strong> If a critical field (such as <code>item_number</code> or <code>facility</code>) experiences a null-rate increase greater than 10% relative to historical tenant baselines, the profiler flags an anomaly warning for operator review.</li>
            <li><strong>Negative Unit Price &amp; Spend Rejection:</strong> Line items with negative unit costs or negative spend amounts are segregated into an exception bucket to prevent spend reconciliation distortions.</li>
            <li><strong>Chronological Sequence Inversion:</strong> Verifies that <code>invoice_date &gt;= po_date</code>. Chronological inversions trigger an audit notice indicating pre-billed vendor arrangements.</li>
            <li><strong>Unmatched Facility Lexicon:</strong> Validates facility names against the approved hospital network directory (e.g., verifying <em>"UH OR"</em>, <em>"West Chester Hospital"</em>, <em>"Drake Center"</em>).</li>
        </ol>
        """

    # -------------------------------------------------------------------------
    # SECTION 5: STAGE 3 — SEMANTIC TOPOLOGY, JOIN DISCOVERY & UOM
    # -------------------------------------------------------------------------
    def get_section_5_content(self) -> str:
        topology_flow = render_process_flow(
            "Stage 3: Composite Join Discovery & 4-Tier Matching Cascade",
            [
                {"icon": "🔍", "actor": "Architect Bee", "title": "1. Key Discovery", "desc": "Evaluates candidate keys: po_number + po_line_no achieves 100% join match."},
                {"icon": "⚡", "actor": "Cascade Tier 1", "title": "2. Exact Match", "desc": "TRIM(UPPER(item_number)) matches exact alphanumeric catalog codes (100% conf)."},
                {"icon": "✂️", "actor": "Cascade Tier 2", "title": "3. Punctuation Strip", "desc": "Removes dashes, slashes & leading zeros regex matching (95% conf)."},
                {"icon": "📝", "actor": "Cascade Tier 3", "title": "4. Description Match", "desc": "Clinical description token overlap and manufacturer part cross-reference (85% conf)."},
                {"icon": "📏", "actor": "UOM Matrix", "title": "5. UOM Normalization", "desc": "Converts Box (10), Case (50), Pack (12) to Each (EA) unit pricing."}
            ],
            ascii_fallback="[Candidate Tables] ➔ [Composite Join Discovery] ➔ [4-Tier Item Matching Cascade] ➔ [Clinical UOM Normalization] ➔ [🛑 HITL Gate 1: Topology Approval]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 5: Semantic Topology &amp; Joins</strong> • Natural graph discovery, 4-tier item matching cascade, and clinical UOM factor normalization.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>5.1 Natural Composite Join Graph Discovery</h2>
        <p>In Stage 3, Architect Bee Pollen evaluates the relational graph across the five canonical healthcare tables. Rather than relying on rigid predefined schemas, the agent analyzes column semantics, distinct values, and data intersections to discover natural relationships:</p>

        {topology_flow}

        <ul>
            <li><strong>Purchase Orders ➔ Invoices (1-to-1 Match):</strong> Linked deterministically on composite foreign key <code>po_number</code> + <code>po_line_no</code>. In the UC Health dataset, this composite key achieves an exact <strong>100.0% match rate across all 13,546 rows</strong>, guaranteeing zero dropped invoice lines.</li>
            <li><strong>Purchase Orders ➔ Contracts (N-to-1 Match):</strong> Linked on composite key <code>contract_number</code> + <code>item_number</code>, retrieving contracted benchmark prices and expiration dates.</li>
            <li><strong>Purchase Orders ➔ Item Master (N-to-1 Match):</strong> Linked via the 4-Tier Matching Cascade on <code>item_number</code>, retrieving standardized clinical taxonomy, manufacturer names, and catalog numbers.</li>
            <li><strong>Consumption ➔ Clinical Item Master (N-to-1 Match):</strong> Linked on item identification to map surgical point-of-use case supplies to clinical service lines and procedure categories.</li>
        </ul>

        <hr/>

        <h2>5.2 The 4-Tier Item Matching Cascade Architecture</h2>
        <p>In enterprise hospital operations, item numbers across ERP systems, vendor invoices, and OR preference cards frequently suffer from formatting discrepancies, including dropped leading zeros, extraneous hyphens, or vendor catalog code variations. To overcome this, Ask The Bee applies a deterministic 4-tier matching cascade:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 110px;">Cascade Tier</th>
                    <th style="width: 170px;">Strategy Name</th>
                    <th style="width: 280px;">Mathematical SQL Transformation</th>
                    <th style="width: 130px;">Match Confidence</th>
                    <th>Operational Behavior</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Tier 1</strong></td>
                    <td>Exact Alphanumeric Match</td>
                    <td><code>TRIM(UPPER(source.item_number)) = TRIM(UPPER(target.item_number))</code></td>
                    <td><strong>100%</strong> (Deterministic)</td>
                    <td>Immediate join resolution. Highest priority match.</td>
                </tr>
                <tr>
                    <td><strong>Tier 2</strong></td>
                    <td>Punctuation &amp; Zero Stripping</td>
                    <td><code>REGEXP_REPLACE(LTRIM(source.item_number, '0'), '[^A-Za-z0-9]', '') = REGEXP_REPLACE(LTRIM(target.item_number, '0'), '[^A-Za-z0-9]', '')</code></td>
                    <td><strong>95%</strong> (High Confidence)</td>
                    <td>Eliminates formatting noise such as hyphens, slashes, spaces, and leading zeros (e.g. <code>00536188-A</code> ➔ <code>536188A</code>).</td>
                </tr>
                <tr>
                    <td><strong>Tier 3</strong></td>
                    <td>Manufacturer Catalog Cross-Reference</td>
                    <td><code>REGEXP_REPLACE(source.manuf_cat_no, '[^A-Za-z0-9]', '') = REGEXP_REPLACE(target.manuf_cat_no, '[^A-Za-z0-9]', '')</code></td>
                    <td><strong>85%</strong> (Medium-High)</td>
                    <td>Matches when hospital internal item numbers differ, but manufacturer part numbers match exactly.</td>
                </tr>
                <tr>
                    <td><strong>Tier 4</strong></td>
                    <td>Normalized Description Tokenization</td>
                    <td><code>token_similarity(source.item_description, target.item_description) &gt;= 0.80</code></td>
                    <td><strong>75%</strong> (Review Recommended)</td>
                    <td>Tokenizes clinical description strings (e.g. <em>"MITRACLIP G4 CLIP DELIVERY SYSTEM"</em>), matching high-similarity candidates for clinical confirmation.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>5.3 Clinical Unit of Measure (UOM) Normalization Matrix</h2>
        <p>A primary driver of phantom hospital spend inflation occurs when invoices bill in bulk packaging units (e.g., Box of 10) while consumption logs record individual eaches (EA). Architect Bee Pollen enforces a standardized clinical conversion matrix:</p>

        <table>
            <thead>
                <tr>
                    <th>Packaging Code</th>
                    <th>Unit Name</th>
                    <th>Standard Conversion Factor (EA)</th>
                    <th>Price Normalization Formula</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>BX</strong></td>
                    <td>Box</td>
                    <td><strong>10 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 10.0</code></td>
                </tr>
                <tr>
                    <td><strong>CS</strong></td>
                    <td>Case</td>
                    <td><strong>50 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 50.0</code></td>
                </tr>
                <tr>
                    <td><strong>PK</strong></td>
                    <td>Pack</td>
                    <td><strong>12 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 12.0</code></td>
                </tr>
                <tr>
                    <td><strong>DZ</strong></td>
                    <td>Dozen</td>
                    <td><strong>12 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 12.0</code></td>
                </tr>
                <tr>
                    <td><strong>PR</strong></td>
                    <td>Pair</td>
                    <td><strong>2 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 2.0</code></td>
                </tr>
                <tr>
                    <td><strong>EA</strong></td>
                    <td>Each</td>
                    <td><strong>1 EA</strong></td>
                    <td><code>normalized_unit_price = supply_unit_price / 1.0</code></td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>5.4 🛑 Human-In-The-Loop Gate 1: Topology Approval</h2>
        <p>Upon completing join candidate analysis, Queen Bee Orla halts the pipeline at <strong>Gate 1 (Topology Approval)</strong>:</p>
        <ul>
            <li><strong>Topology Blueprint Presentation:</strong> Displays discovered join keys, candidate match rates, and UOM factor assignments in the Studio UI.</li>
            <li><strong>Manual Override Controls:</strong> Operators can adjust join keys, modify UOM packaging ratios, or approve the blueprint.</li>
            <li><strong>Confirmation Advance:</strong> Once confirmed, the pipeline advances to Stage 4 (DBT Lineage Compilation).</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 6: STAGE 4 — AUTONOMOUS DBT LINEAGE & DOCUMENTATION
    # -------------------------------------------------------------------------
    def get_section_6_content(self) -> str:
        dbt_flow = render_process_flow(
            "Stage 4: 4-Tier DBT Compilation & Cytoscape Lineage Pipeline",
            [
                {"icon": "📄", "actor": "Layer 1: Staging", "title": "Staging Views", "desc": "stg_po, stg_invoices, stg_consumption. Defensive casts, TRIM & regex cleanup."},
                {"icon": "⚙️", "actor": "Layer 2: Intermediate", "title": "Intermediate Tables", "desc": "int_item_matching, int_contract_coverage. 4-tier joins & price calculations."},
                {"icon": "🏛️", "actor": "Layer 3: Dimensional", "title": "Marts Fact Tables", "desc": "fct_gap_analysis, fct_po_cost_savings. Spend aggregations & procedure marts."},
                {"icon": "🔒", "actor": "Layer 4: Security", "title": "Multi-Tenant RLS", "desc": "sc_multi_tenant_* marts. Injects tenant_gap_id & Snowflake RLS DDL."},
                {"icon": "🕸️", "actor": "Lineage Visualizer", "title": "Cytoscape DAG", "desc": "Renders interactive dependency graph & serves embedded DBT docs."}
            ],
            ascii_fallback="[Staging Models (Layer 1)] ➔ [Intermediate Models (Layer 2)] ➔ [Dimensional Marts (Layer 3)] ➔ [Multi-Tenant Security (Layer 4)] ➔ [Cytoscape.js DAG]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 6: DBT Lineage &amp; Compilation</strong> • 4-tier semantic transformation hierarchy, Cytoscape DAG lineage, and DBT Docs portal.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>6.1 4-Tier Semantic Transformation Architecture</h2>
        <p>In Stage 4, Worker Bee Nectar compiles and executes modular DBT SQL transformation models in DuckDB. Transformations follow a rigorous 4-tier architectural hierarchy:</p>

        {dbt_flow}

        <table>
            <thead>
                <tr>
                    <th style="width: 130px;">Architectural Tier</th>
                    <th style="width: 220px;">Compiled Model Names</th>
                    <th style="width: 130px;">Materialization</th>
                    <th>Engineering Purpose &amp; Transformation Rules</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>1. Staging</strong></td>
                    <td>
                        <code>stg_purchase_orders_v4</code><br/>
                        <code>stg_invoices_v4</code><br/>
                        <code>stg_consumption_v4</code><br/>
                        <code>stg_contracts_v4</code><br/>
                        <code>stg_item_master_v4</code>
                    </td>
                    <td>View / Table</td>
                    <td>Defensive type casting (e.g. <code>TRY_CAST(po_line_no AS INTEGER)</code>), whitespace trimming, alphanumeric cleaning, and currency standardization. <strong>Zero business joins or spend aggregations.</strong></td>
                </tr>
                <tr>
                    <td><strong>2. Intermediate</strong></td>
                    <td>
                        <code>int_item_matching_v4</code><br/>
                        <code>int_contract_coverage_v4</code><br/>
                        <code>int_po_spend_aggregation_v4</code>
                    </td>
                    <td>Table</td>
                    <td>Executes the 4-tier matching cascade, resolves contract price tiers, calculates contract coverage ratios, and merges line-level purchase order spend with invoice payments.</td>
                </tr>
                <tr>
                    <td><strong>3. Dimensional Marts</strong></td>
                    <td>
                        <code>fct_gap_analysis_v4</code><br/>
                        <code>fct_po_cost_savings_v4</code><br/>
                        <code>fct_consumption_cost_savings_v4</code><br/>
                        <code>dim_clinical_items_v4</code>
                    </td>
                    <td>Physical Table</td>
                    <td>Production analytical fact and dimension tables. Pre-aggregates spend by hospital service line, clinical procedure group, vendor tier, and contract compliance gap code.</td>
                </tr>
                <tr>
                    <td><strong>4. Multi-Tenant Security</strong></td>
                    <td>
                        <code>sc_multi_tenant_gap_analysis</code><br/>
                        <code>sc_multi_tenant_po_savings</code><br/>
                        <code>sc_multi_tenant_consumption</code>
                    </td>
                    <td>Physical Table</td>
                    <td>Injects client tenant partition keys (<code>tenant_gap_id</code>) and compiles Snowflake Row-Level Security (RLS) DDL policies for secure cloud export.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>6.2 Cytoscape.js Interactive Lineage Visualizer</h2>
        <p>The Studio UI renders an interactive Directed Acyclic Graph (DAG) powered by <strong>Cytoscape.js</strong>:</p>
        <ul>
            <li><strong>Node Color Topology:</strong> Staging nodes (blue), Intermediate nodes (purple), Marts nodes (emerald green), and Security nodes (amber).</li>
            <li><strong>Status Indicators:</strong> Real-time visual halos indicate node compilation state (<code>COMPILING</code>, <code>SUCCESS</code>, <code>FAILED</code>).</li>
            <li><strong>Interactive Inspection:</strong> Clicking any node displays upstream dependencies, downstream consumers, row count, execution runtime, and compiled SQL code.</li>
        </ul>

        <hr/>

        <h2>6.3 Embedded DBT Documentation Portal</h2>
        <p>The studio serves compiled DBT documentation natively at <code>/dbt-docs/</code>, providing data engineers with instant access to catalog descriptions, column data types, dependency lineage graphs, and macro source code without external web servers.</p>
        """

    # -------------------------------------------------------------------------
    # SECTION 7: STAGE 5 — OUTPUT EXPLORER, FACT MARTS & ANALYTICS
    # -------------------------------------------------------------------------
    def get_section_7_content(self) -> str:
        mart_flow = render_process_flow(
            "Stage 5: Dimensional Analytical Marts & KPI Aggregation",
            [
                {"icon": "🏛️", "actor": "Production Marts", "title": "1. Fact Tables", "desc": "fct_gap_analysis, fct_po_cost_savings & fct_consumption in DuckDB."},
                {"icon": "📈", "actor": "Executive KPIs", "title": "2. KPI Calculation", "desc": "Computes Total Spend, Contract Matched %, Savings & Off-Contract Spend."},
                {"icon": "🏥", "actor": "Clinical Taxonomy", "title": "3. Procedure Grouping", "desc": "Rolls up spend across Cardiology, Orthopedics & General Surgery lines."},
                {"icon": "🔍", "actor": "Explorer Deck", "title": "4. Lightbox Modal", "desc": "Allows clinical analysts to filter rows, sort columns & export to Excel."}
            ],
            ascii_fallback="[Dimensional Marts] ➔ [Real-Time KPI Rollup] ➔ [Clinical Procedure Taxonomies] ➔ [Studio Explorer & Excel Export]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 7: Output Explorer &amp; Marts</strong> • Fact table analytics, executive KPI tiles, and clinical procedure taxonomy rollups.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>7.1 Production Analytical Fact Tables Overview</h2>
        <p>In Stage 5, Ask The Bee exposes synthesized dimensional marts optimized for clinical and procurement analysis:</p>

        {mart_flow}

        <ol>
            <li><strong><code>fct_gap_analysis_v4</code>:</strong> The central analytical mart cross-referencing clinical procedure groupings with contract compliance. Identifies spend on-contract vs. off-contract, item match confidence scores, and specific contract gap codes (e.g. <code>GAP_UNCONTRACTED_ITEM</code>, <code>GAP_PRICE_VARIANCE</code>).</li>
            <li><strong><code>fct_po_cost_savings_v4</code>:</strong> Granular purchase order line-level analysis comparing actual invoiced unit prices against negotiated contract benchmark prices, computing realized savings and vendor price leakage.</li>
            <li><strong><code>fct_consumption_cost_savings_v4</code>:</strong> Surgical case-level supply consumption capturing operating room usage, case duration, surgeon preference card utilization, and supply waste variances.</li>
        </ol>

        <hr/>

        <h2>7.2 Executive KPI Tiles &amp; Real-Time Financial Metrics</h2>
        <p>The Studio header continuously monitors four financial and operational KPIs calculated directly from the production marts:</p>

        <table>
            <thead>
                <tr>
                    <th>Executive KPI Metric</th>
                    <th>UC Health Benchmark Value</th>
                    <th>Underlying Mart Formula</th>
                    <th>Operational Clinical Meaning</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Total Ingested Spend</strong></td>
                    <td><strong>$243,934,093.08</strong></td>
                    <td><code>SUM(line_spend)</code> across all PO lines</td>
                    <td>Total procurement dollar volume across all facilities and service lines.</td>
                </tr>
                <tr>
                    <td><strong>Contract Match Rate</strong></td>
                    <td><strong>86.4%</strong></td>
                    <td><code>COUNT(matched_po) / COUNT(total_po) * 100</code></td>
                    <td>Percentage of purchase order dollars matched to active supplier contracts.</td>
                </tr>
                <tr>
                    <td><strong>Identified Cost Savings</strong></td>
                    <td><strong>$14,820,412.50</strong></td>
                    <td><code>SUM(po_price - contract_price) * qty</code></td>
                    <td>Direct recoverable savings from vendor overbillings and contract enforcement.</td>
                </tr>
                <tr>
                    <td><strong>Off-Contract Spend</strong></td>
                    <td><strong>$33,175,566.60</strong></td>
                    <td><code>SUM(line_spend) WHERE NOT is_contract_matched</code></td>
                    <td>Maverick procurement spend requiring sourcing renegotiation.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>7.3 Clinical Procedure Taxonomy &amp; Service Line Distribution</h2>
        <p>Spend is categorized across standardized clinical service lines, enabling hospital department chairs to review surgeon supply preferences:</p>

        <ul>
            <li><strong>Cardiology &amp; Endovascular:</strong> Transcatheter heart valves (MitraClip, Edwards SAPIEN), drug-eluting stents, pacemaker leads.</li>
            <li><strong>Orthopedics &amp; Spine:</strong> Total knee and hip implants (Stryker, Zimmer Biomet), spinal fixation hardware, bone morphogenetic proteins.</li>
            <li><strong>General &amp; Bariatric Surgery:</strong> Laparoscopic staplers, trocars, ultrasonic shears, surgical mesh.</li>
            <li><strong>Trauma &amp; Critical Care:</strong> Hemostatic agents, emergency airway kits, central line catheter kits.</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 8: STAGE 6 — 100% GOLDEN PARITY & MATHEMATICAL AUDIT
    # -------------------------------------------------------------------------
    def get_section_8_content(self) -> str:
        parity_flow = render_process_flow(
            "Stage 6: Zero-Variance Mathematical Spend Reconciler & Audit Gate",
            [
                {"icon": "📥", "actor": "Source Ledger", "title": "1. Raw Source Spend", "desc": "Calculates raw purchase order spend: $243,934,093.08 exactly."},
                {"icon": "🏛️", "actor": "Mart Ledger", "title": "2. Mart Aggregated Spend", "desc": "Aggregates spend across fct_gap_analysis & fct_po: $243,934,093.08."},
                {"icon": "⚖️", "actor": "Inspector Bee", "title": "3. Zero Variance Check", "desc": "Computes ABS(raw - mart). Must equal $0.00. Zero dollar leakage permitted."},
                {"icon": "📋", "actor": "Assertion Engine", "title": "4. Governance Tests", "desc": "Runs TEST_SPEND_001, TEST_GAP_008, TEST_MATCH_003 & TEST_NULL_004."},
                {"icon": "🛑", "actor": "Queen Bee", "title": "5. Gate 2 Certify", "desc": "Operator certifies audit; status advances to AUDITED_CERTIFIED."}
            ],
            ascii_fallback="[Raw Spend: $243.9M] == [Mart Spend: $243.9M] ➔ [Variance: $0.00] ➔ [4-Layer Assertions PASS] ➔ [🛑 HITL Gate 2 Certification]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 8: 100% Golden Parity Audit</strong> • Zero-dollar mathematical spend reconciliation ($243.9M), multi-layer governance assertions, and Gate 2 certification.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>8.1 Zero-Variance Mathematical Spend Reconciler</h2>
        <p>A non-negotiable architectural pillar of Ask The Bee is <strong>Zero Dollar Variance</strong>. In enterprise healthcare data engineering, even a 0.01% rounding discrepancy across large procurement datasets can conceal hundreds of thousands of dollars in spend leakage. In Stage 6, Inspector Bee Guard executes dual mathematical reconciliations:</p>

        {parity_flow}

        <p style="text-align: center; font-size: 16px; margin: 18px 0; background: #0f172a; padding: 14px; border-radius: 8px; border: 1px solid #38bdf8;">
            <code style="color: #38bdf8; font-weight: bold;">
                Raw Ingested PO Spend ($243,934,093.08) = Aggregated Dimensional Mart Spend ($243,934,093.08)
            </code>
        </p>

        <p>If mathematical variance is greater than <code>$0.00</code>, the pipeline halts immediately, triggering Sentinel Aegis self-healing to inspect join fan-outs, cartesian products, or dropped foreign keys.</p>

        <hr/>

        <h2>8.2 Multi-Layer Governance Assertion Test Suite</h2>
        <p>Inspector Bee Guard runs four automated governance assertions to certify data integrity:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 140px;">Test Identifier</th>
                    <th style="width: 200px;">Assertion Name</th>
                    <th style="width: 220px;">Target Benchmark Metric</th>
                    <th>Pass Criterion &amp; Mathematical Rule</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>TEST_SPEND_001</strong></td>
                    <td>Golden Spend Parity</td>
                    <td>Total Spend Reconciliation</td>
                    <td><code>ABS(raw_source_spend - dimensional_mart_spend) &lt; 0.01</code> (Evaluates to exactly <strong>$0.00 variance</strong>).</td>
                </tr>
                <tr>
                    <td><strong>TEST_GAP_008</strong></td>
                    <td>Procedure Hierarchy Cardinality</td>
                    <td>Standard Procedure Category Count</td>
                    <td><code>448 &lt;= COUNT(DISTINCT procedure_group) &lt;= 1,125</code> (Guarantees clinical procedure granularity is preserved).</td>
                </tr>
                <tr>
                    <td><strong>TEST_MATCH_003</strong></td>
                    <td>Contract Match Confidence</td>
                    <td>Contract Matching Benchmark</td>
                    <td><code>match_rate &gt;= 85.0%</code> (UC Health achieves <strong>86.4% match rate</strong>).</td>
                </tr>
                <tr>
                    <td><strong>TEST_NULL_004</strong></td>
                    <td>Primary Key Integrity</td>
                    <td>Log ID &amp; PO Line Nullness</td>
                    <td><code>COUNT(*) WHERE po_line_no IS NULL OR log_id IS NULL == 0</code>.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>8.3 🛑 Human-In-The-Loop Gate 2: Parity Certification</h2>
        <p>Upon successful execution of all assertions, Queen Bee Orla presents the <strong>Gate 2 Certification Modal</strong>:</p>
        <ul>
            <li><strong>Audit Certificate Issuance:</strong> Generates a cryptographically timestamped certification token stored in the run audit trail.</li>
            <li><strong>State Promotion:</strong> Advances pipeline state from <code>AUDITING</code> to <code>AUDITED_CERTIFIED</code>.</li>
            <li><strong>Downstream Authorization:</strong> Unlocks direct export to Snowflake cloud data warehouse and clinical analytics reporting.</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 9: STAGE 7 — REFINE & EVOLVE: RETROSPECTIVE FEEDBACK
    # -------------------------------------------------------------------------
    def get_section_9_content(self) -> str:
        refine_flow = render_process_flow(
            "Stage 7: Retrospective Feedback & Living Playbook Engine",
            [
                {"icon": "📝", "actor": "Clinical Intake", "title": "1. Multi-Modal Feedback", "desc": "Text findings, Excel discrepancy sheets & Python patch scripts."},
                {"icon": "🧠", "actor": "Refine Agent", "title": "2. Plan Synthesis", "desc": "Generates Auto-Resolve Plan with root causes, SQL diffs & assertions."},
                {"icon": "🔄", "actor": "Iterative HITL", "title": "3. Operator Review", "desc": "Operator refines plan; auto-increments revisions until confirmed."},
                {"icon": "⚡", "actor": "Worker Bee", "title": "4. Patch Execution", "desc": "Executes non-destructive AST patch in DuckDB & verifies parity."},
                {"icon": "📚", "actor": "Playbook Ledger", "title": "5. Playbook Append", "desc": "Permanently commits learning; future runs automatically inherit rule."}
            ],
            ascii_fallback="[Operator Findings & Evidence] ➔ [Implementation Plan Synthesis] ➔ [Iterative Operator Confirmation] ➔ [Surgical Model Patch] ➔ [Institutional Playbook]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 9: Refine &amp; Evolve (HITL)</strong> • Retrospective clinical feedback loop, automated implementation plans, and living institutional playbook.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>9.1 Multi-Modal Clinical Finding Submission</h2>
        <p>In Stage 7 (Refine &amp; Evolve), clinical and analytics teams can retrospectively refine the pipeline when post-pipeline reviews uncover edge cases or packaging discrepancies:</p>

        {refine_flow}

        <ul>
            <li><strong>Structured Text Findings:</strong> Clinical analysts describe observed discrepancies (e.g., <em>"Item 536188 was billed at $30,000 for DRG 183. Review of invoice confirms Box of 2 units (BX/2), true each price is $15,000."</em>).</li>
            <li><strong>Supporting Documents Dropzone:</strong> Accepts Excel workbooks (<code>.xlsx</code>), CSV files, PDF invoices, and diagnostic screenshots.</li>
            <li><strong>Executable Python / SQL Script Patches:</strong> Engineers can optionally provide suggested python functions or SQL snippets to accelerate automated code patching.</li>
        </ul>

        <hr/>

        <h2>9.2 Dynamic Implementation Plan Synthesis</h2>
        <p>Upon submission, the Refinement Agent analyzes the active DBT blueprint and synthesizes a structured <strong>Auto-Resolve Implementation Plan</strong>:</p>

        <ol>
            <li><strong>Root Cause Summary:</strong> Pinpoints the underlying schema binding, packaging ratio, or join cascade assumption.</li>
            <li><strong>Target Model Diff:</strong> Proposes exact SQL changes to DBT staging or intermediate models with before-and-after diff highlighting.</li>
            <li><strong>Automated Regression Assertion:</strong> Defines test assertions to verify that zero variance is maintained after applying the patch.</li>
            <li><strong>Iterative Revision Cycle:</strong> The operator can request modifications (e.g. <em>"Apply this rule only to cardiac catheter lab facilities"</em>), generating Revision 2, Revision 3, etc. until confirmed.</li>
        </ol>

        <hr/>

        <h2>9.3 Living Institutional Knowledge Playbook Engine</h2>
        <p>Confirmed refinements are permanently recorded in the <strong>Institutional Knowledge Playbook</strong> (<code>agent_memory/institutional_knowledge.json</code>):</p>
        <ul>
            <li><strong>Permanent Rule Persistence:</strong> Learnings are not lost upon pipeline restart; they become part of the studio's permanent cognitive rulebook.</li>
            <li><strong>Future Run Auto-Inheritance:</strong> Subsequent pipeline runs automatically apply active playbook heuristics during Stage 3 (Topology) and Stage 4 (DBT Compilation).</li>
            <li><strong>Interactive Playbook Ledger:</strong> Operators can search, inspect, toggle, or retire heuristics directly from the Studio UI.</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 10: SENTINEL AEGIS SELF-HEALING & JIRA ESCALATION
    # -------------------------------------------------------------------------
    def get_section_10_content(self) -> str:
        healing_flow = render_process_flow(
            "Sentinel Aegis Autonomous Self-Healing & Incident Escalation Engine",
            [
                {"icon": "⚠️", "actor": "Sentinel Aegis", "title": "1. Failure Intercept", "desc": "Traps compilation timeouts, syntax exceptions & assertion failures."},
                {"icon": "🔬", "actor": "Diagnostic AST", "title": "2. Root Cause Analysis", "desc": "Inspects AST, classifies error type & determines repair strategy."},
                {"icon": "🩹", "actor": "Self-Healing", "title": "3. Automated Repair", "desc": "Applies surgical SQL AST patch or restarts background compilation worker."},
                {"icon": "✅", "actor": "Parity Check", "title": "4. Verify Parity", "desc": "Re-runs assertion. If successful, pipeline resumes automatically."},
                {"icon": "🎫", "actor": "Jira REST API", "title": "5. Jira Escalation", "desc": "If unresolvable, automatically files high-priority ticket in ATBDBT project."}
            ],
            ascii_fallback="[Runtime Failure Trapped] ➔ [Root Cause AST Diagnosis] ➔ [Surgical Patch Applied] ➔ [Parity Verified] ➔ [Jira Cloud Escalation (If Unresolvable)]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 10: Sentinel Aegis Self-Healing</strong> • Automated error diagnosis, AST patching, and automated Jira Cloud incident escalation.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>10.1 Autonomous Error Interception &amp; Root Cause Classification</h2>
        <p>When compilation timeouts (504), gateway restarts (502), syntax errors, or governance assertion failures occur, <strong>Sentinel Aegis</strong> intercepts the failure before it impacts the user experience:</p>

        {healing_flow}

        <ul>
            <li><strong>SQL Syntax &amp; Reserved Word Collisions:</strong> Traps unescaped keywords, missing commas in SELECT clauses, or dialect mismatches between SQLite, PostgreSQL, and DuckDB.</li>
            <li><strong>Procedure Group Dimension Collisions (e.g. TEST_GAP_008):</strong> Diagnoses cardinality loss when grouping clauses over-aggregate clinical categories. Automatically expands grouping hierarchy to preserve clinical granularity.</li>
            <li><strong>Gateway Recovery &amp; Process Re-spawning:</strong> Intercepts hung compilation worker threads, cleanly terminates orphaned processes, releases DuckDB lock files, and restarts worker threads.</li>
        </ul>

        <hr/>

        <h2>10.2 Surgical AST Model Patching</h2>
        <p>Rather than rolling back entire pipelines, Sentinel Aegis synthesizes and applies non-destructive, targeted patches directly to models on disk and in DuckDB memory:</p>

        <ol>
            <li><strong>Syntax Tree Inspection:</strong> Parses SQL into Abstract Syntax Tree (AST) representations.</li>
            <li><strong>Targeted Mutation:</strong> Injects defensive type casting (e.g., <code>TRY_CAST</code>) or missing GROUP BY dimensions.</li>
            <li><strong>Atomic Execution:</strong> Applies the patch and verifies assertions against DuckDB. If parity is restored, the pipeline resumes without human intervention.</li>
        </ol>

        <hr/>

        <h2>10.3 Automated Jira Cloud Incident Escalation (Project: ATBDBT)</h2>
        <p>If an exception cannot be safely resolved after exhausting automated retry budgets, Sentinel Aegis automatically escalates the issue to Atlassian Jira Cloud:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 170px;">Jira Configuration</th>
                    <th>Implementation Specification</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Target Project Key</strong></td>
                    <td><strong><code>ATBDBT</code></strong> (Ask The Bee - DBT Automation)</td>
                </tr>
                <tr>
                    <td><strong>Issue Type</strong></td>
                    <td><code>Bug</code> for runtime exceptions / <code>Task</code> for operator requests</td>
                </tr>
                <tr>
                    <td><strong>Priority Assignment</strong></td>
                    <td><strong>High</strong> (Unresolved pipeline failure) / <strong>Highest</strong> (Parity variance)</td>
                </tr>
                <tr>
                    <td><strong>Payload Formatting</strong></td>
                    <td>Atlassian Document Format (ADF) Rich Text v3 with code formatting blocks</td>
                </tr>
                <tr>
                    <td><strong>Anti-Spam Deduplication</strong></td>
                    <td>Generates SHA-256 fingerprint of error traceback; suppresses duplicate tickets within a 1-hour window.</td>
                </tr>
            </tbody>
        </table>
        """

    # -------------------------------------------------------------------------
    # SECTION 11: ENTERPRISE SUITE — OBSERVABILITY & GOVERNANCE
    # -------------------------------------------------------------------------
    def get_section_11_content(self) -> str:
        enterprise_flow = render_process_flow(
            "Enterprise Governance, Observability & Cloud Sharing Pipeline",
            [
                {"icon": "📊", "actor": "Drift Engine", "title": "1. Row Drift Detection", "desc": "Compares datasets at row level, classifying ADDED, MODIFIED & DROPPED rows."},
                {"icon": "📑", "actor": "Excel Generator", "title": "2. 5-Sheet Workbook", "desc": "Generates OpenPyXL audit workbook with conditional formatting."},
                {"icon": "❄️", "actor": "Snowflake Push", "title": "3. Direct Data Share", "desc": "Exports marts to Snowflake & creates Row-Level Security (RLS) DDL."},
                {"icon": "📝", "actor": "Audit Logger", "title": "4. 90-Day Audit Log", "desc": "Logs user actions in SQLite WAL mode; purges records >90 days."},
                {"icon": "💬", "actor": "AI Assistant", "title": "5. Chat Assistant", "desc": "Interactive Ask The Bee chat for queries, KPI lookups & visual charts."}
            ],
            ascii_fallback="[Record Drift Engine] ➔ [5-Sheet OpenPyXL Export] ➔ [Snowflake RLS DDL] ➔ [90-Day SQLite Audit Log] ➔ [Ask The Bee Chat]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 11: Enterprise Capabilities Suite</strong> • Record drift detection, 5-sheet Excel exports, Snowflake RLS, 90-day audit logging, and AI assistant.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>11.1 Record &amp; Row Drift Detection Engine</h2>
        <p>While traditional data tools monitor column schema drift, Ask The Bee incorporates a high-performance <strong>Row-Level Drift Engine</strong> that inspects row churn between successive dataset drops:</p>

        {enterprise_flow}

        <ul>
            <li><strong>Row Churn Partitioning:</strong> Evaluates primary keys across successive drops, categorizing rows into <code>ADDED</code>, <code>MODIFIED</code>, <code>DROPPED</code>, and <code>UNCHANGED</code>.</li>
            <li><strong>Field-Level Mutation Frequencies:</strong> Pinpoints exact columns experiencing value changes (e.g. vendor price increases or UOM code alterations).</li>
            <li><strong>Null-Rate Drift:</strong> Alerts operators when column missingness spikes by more than 10% relative to historical baselines.</li>
            <li><strong>5-Sheet OpenPyXL Workbook Export:</strong> Streamed via <code>GET /api/enterprise/export_excel?dataset=record_drift</code> with professional styling:
                <ol>
                    <li><em>Executive Summary</em> (KPI metrics, total row counts, churn percentages).</li>
                    <li><em>Added Records</em> (Newly introduced items).</li>
                    <li><em>Modified Records</em> (Field-level before-and-after diffs).</li>
                    <li><em>Dropped Records</em> (Items removed from catalog).</li>
                    <li><em>Column Drift &amp; Anomaly Analysis</em> (Null rate shifts).</li>
                </ol>
            </li>
        </ul>

        <hr/>

        <h2>11.2 Snowflake Data Warehouse Export &amp; Multi-Tenant RLS</h2>
        <p>The studio features native connectors to export verified marts directly to Snowflake while enforcing multi-tenant isolation:</p>

        <pre><code>-- Multi-Tenant Row Access Policy DDL
CREATE OR REPLACE ROW ACCESS POLICY rap_client_isolation
AS (client_id VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR client_id = CURRENT_SESSION_CONTEXT('TENANT_ID');

-- Apply policy to production fact marts
ALTER TABLE fct_gap_analysis ADD ROW ACCESS POLICY rap_client_isolation ON (client_id);
ALTER TABLE fct_po_cost_savings ADD ROW ACCESS POLICY rap_client_isolation ON (client_id);</code></pre>

        <hr/>

        <h2>11.3 User Activity &amp; Audit Logging (Strict 90-Day Retention)</h2>
        <p>Complies with healthcare governance standards by maintaining an audit trail in SQLite WAL mode (<code>agent_memory/activity_audit_logs.db</code>):</p>
        <ul>
            <li><strong>Zero Latency Overhead:</strong> Asynchronous batch queue logger with zero HTTP response impact.</li>
            <li><strong>Comprehensive Capture:</strong> Records timestamp, user email/ID, action type, IP address, and payload parameters.</li>
            <li><strong>Strict 90-Day Purge Policy:</strong> Background daemon automatically purges records older than 90 days and triggers incremental vacuuming.</li>
            <li><strong>Compliance Export:</strong> Operators can export audit logs as CSV at any time via <code>GET /api/activity_logs/export_csv</code>.</li>
        </ul>
        """

    # -------------------------------------------------------------------------
    # SECTION 12: DEPLOYMENT, INFRASTRUCTURE & DEVOPS RUNBOOK
    # -------------------------------------------------------------------------
    def get_section_12_content(self) -> str:
        devops_flow = render_process_flow(
            "Enterprise Deployment, Network Routing & SSO Architecture",
            [
                {"icon": "🐳", "actor": "Docker Engine", "title": "1. Containerization", "desc": "Packages Python, DuckDB & Studio UI on port 8081."},
                {"icon": "🖥️", "actor": "Ubuntu Host", "title": "2. VM Infrastructure", "desc": "Runs on Ubuntu 22.04 LTS (16GB RAM, 4 vCPU, Private IP: 10.12.0.109)."},
                {"icon": "🔒", "actor": "Nginx Proxy", "title": "3. Reverse Proxy", "desc": "Terminates SSL at atb-dbt.supplycopia.com & routes traffic to port 8081."},
                {"icon": "🔑", "actor": "Entra ID", "title": "4. SAML 2.0 SSO", "desc": "Authenticates corporate users via Microsoft Entra ID enterprise SSO."}
            ],
            ascii_fallback="[Client Browser] ➔ [Nginx Reverse Proxy (SSL 443)] ➔ [Docker Container (Port 8081)] ➔ [Microsoft Entra ID SSO]"
        )

        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 12: Deployment &amp; DevOps Runbook</strong> • Container hosting, VM specs, Nginx reverse proxy, and Microsoft Entra SAML 2.0 SSO configuration.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>12.1 Containerized Docker Hosting Guide</h2>
        <p>The application is packaged into a self-contained, reproducible Docker image exposed on port <strong>8081</strong>:</p>

        {devops_flow}

        <pre><code># 1. Pull latest code from repository
git pull origin main

# 2. Stop and remove existing container
sudo docker stop supplycopia-atb-studio
sudo docker rm supplycopia-atb-studio

# 3. Build optimized Docker image
sudo docker build -t supplycopia-atb-studio .

# 4. Launch container with restart policy
sudo docker run -d \\
  --name supplycopia-atb-studio \\
  --restart unless-stopped \\
  -p 8081:8081 \\
  --env-file .env \\
  supplycopia-atb-studio</code></pre>

        <hr/>

        <h2>12.2 Ubuntu VM Hardware &amp; OS Specifications</h2>
        <table>
            <thead>
                <tr>
                    <th>Hardware / OS Dimension</th>
                    <th>Minimum Specification</th>
                    <th>Recommended Production Specification</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Operating System</strong></td>
                    <td>Ubuntu 22.04 LTS</td>
                    <td>Ubuntu 24.04 LTS (Kernel 6.8+)</td>
                </tr>
                <tr>
                    <td><strong>CPU Compute</strong></td>
                    <td>4 vCPU</td>
                    <td><strong>8 vCPU</strong> (High-frequency compute)</td>
                </tr>
                <tr>
                    <td><strong>RAM Memory</strong></td>
                    <td>16 GB RAM</td>
                    <td><strong>32 GB RAM</strong> (Enables DuckDB in-memory OLAP scaling)</td>
                </tr>
                <tr>
                    <td><strong>Disk Storage</strong></td>
                    <td>100 GB SSD</td>
                    <td><strong>250 GB NVMe SSD</strong> (High IOPS for Parquet files)</td>
                </tr>
                <tr>
                    <td><strong>Target Private IP</strong></td>
                    <td><code>10.12.0.109</code></td>
                    <td><code>10.12.0.109</code> (Port: <code>8081</code>)</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>12.3 Network Routing &amp; Subdomain Mapping</h2>
        <p>Recommended Subdomain: <strong><code>atb-dbt.supplycopia.com</code></strong>.</p>
        <p>Nginx Reverse Proxy configuration:</p>

        <pre><code>server {{
    listen 443 ssl http2;
    server_name atb-dbt.supplycopia.com;

    ssl_certificate /etc/letsencrypt/live/atb-dbt.supplycopia.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/atb-dbt.supplycopia.com/privkey.pem;

    client_max_body_size 100M;

    location / {{
        proxy_pass http://10.12.0.109:8081;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 600s;
        proxy_send_timeout 600s;
    }}
}}</code></pre>

        <hr/>

        <h2>12.4 Microsoft Entra ID SAML 2.0 Single Sign-On (SSO)</h2>
        <table>
            <thead>
                <tr>
                    <th>Entra ID SAML Setting</th>
                    <th>Configuration Value</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Identifier (Entity ID)</strong></td>
                    <td><code>https://atb-dbt.supplycopia.com/saml/metadata</code></td>
                </tr>
                <tr>
                    <td><strong>Reply URL (ACS URL)</strong></td>
                    <td><code>https://atb-dbt.supplycopia.com/saml/acs</code></td>
                </tr>
                <tr>
                    <td><strong>Sign-on URL</strong></td>
                    <td><code>https://atb-dbt.supplycopia.com/login</code></td>
                </tr>
                <tr>
                    <td><strong>User Attributes &amp; Claims</strong></td>
                    <td><code>user.mail</code> ➔ <code>email</code>, <code>user.displayname</code> ➔ <code>name</code></td>
                </tr>
            </tbody>
        </table>
        """

    # -------------------------------------------------------------------------
    # MASTER PUBLISHING METHOD (ZERO SCREENSHOTS, VERBOSE XHTML + FLOW DIAGRAMS)
    # -------------------------------------------------------------------------
    def build_full_confluence_space(self, progress_callback=None) -> Dict[str, Any]:
        """
        Executes the publication of Confluence Space ATBDBT.
        Publishes Master Hub and all 12 parent sections without screenshots,
        replacing them with responsive HTML process flow diagrams and comprehensive documentation.
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        root_page = self.connector.get_space_root_page()
        if not root_page:
            return {"status": "ERROR", "message": "Failed to connect to space root page"}

        root_id = root_page["id"]
        results = []

        def log_step(msg: str):
            print(f"[ConfluenceBuilder] {msg}")
            if progress_callback:
                progress_callback(msg)

        # 1. Master Documentation Hub Page
        hub_title = "SupplyCopia ATB Studio — Master Documentation Hub & Table of Contents"
        log_step(f"Publishing Master Hub: '{hub_title}' under root {root_id}...")
        hub_res = self.connector.upsert_page(
            title=hub_title,
            body_storage_xhtml=self.get_master_hub_content(now_str),
            parent_id=root_id
        )
        results.append(hub_res)
        hub_id = hub_res.get("page_id")

        # 2. Define the 12 Verbose Sections
        sections = [
            {
                "num": 1,
                "title": "1. Executive Summary & Cognitive Multi-Agent Swarm Architecture",
                "body_fn": self.get_section_1_content
            },
            {
                "num": 2,
                "title": "2. End-to-End Autonomous Pipeline Lifecycle & 7-Stage Workflow",
                "body_fn": self.get_section_2_content
            },
            {
                "num": 3,
                "title": "3. Stage 1 — Multi-Tenant Ingestion, Cryptographic Catalog & Smart Merging",
                "body_fn": self.get_section_3_content
            },
            {
                "num": 4,
                "title": "4. Stage 2 — Raw Schema Profiling, Cardinality & Anomaly Diagnostics",
                "body_fn": self.get_section_4_content
            },
            {
                "num": 5,
                "title": "5. Stage 3 — Semantic Topology, Join Discovery & Clinical UOM Matrix",
                "body_fn": self.get_section_5_content
            },
            {
                "num": 6,
                "title": "6. Stage 4 — Autonomous DBT Lineage, Cytoscape DAG & Documentation",
                "body_fn": self.get_section_6_content
            },
            {
                "num": 7,
                "title": "7. Stage 5 — Output Explorer, Fact Marts & Clinical Analytics",
                "body_fn": self.get_section_7_content
            },
            {
                "num": 8,
                "title": "8. Stage 6 — 100% Golden Parity & Mathematical Audit ($243.9M Reconciler)",
                "body_fn": self.get_section_8_content
            },
            {
                "num": 9,
                "title": "9. Stage 7 — Refine & Evolve: Retrospective Clinical Feedback & Playbook",
                "body_fn": self.get_section_9_content
            },
            {
                "num": 10,
                "title": "10. Sentinel Aegis — Self-Healing Engine & Automated Jira Escalation",
                "body_fn": self.get_section_10_content
            },
            {
                "num": 11,
                "title": "11. Enterprise Capabilities Suite — Observability, Snowflake RLS & Audit Logs",
                "body_fn": self.get_section_11_content
            },
            {
                "num": 12,
                "title": "12. Deployment, Infrastructure & DevOps Runbook (Docker, VM & SSO)",
                "body_fn": self.get_section_12_content
            }
        ]

        parent_target_id = hub_id or root_id

        for sec in sections:
            log_step(f"Publishing Section {sec['num']}: '{sec['title']}'...")
            body_content = sec["body_fn"]()
            page_res = self.connector.upsert_page(
                title=sec["title"],
                body_storage_xhtml=body_content,
                parent_id=parent_target_id
            )
            results.append(page_res)
            time.sleep(0.4)  # Polite API pause

        # Save manifest
        manifest = {
            "published_at": now_str,
            "space_key": self.connector.space_key,
            "root_id": root_id,
            "hub_id": hub_id,
            "total_sections": len(sections),
            "results": results
        }
        manifest_file = DOCS_CACHE_DIR / "full_space_manifest.json"
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        return {
            "status": "SUCCESS",
            "space_key": self.connector.space_key,
            "hub_id": hub_id,
            "total_sections": len(sections),
            "results": results
        }


space_builder = ConfluenceSpaceBuilder()
