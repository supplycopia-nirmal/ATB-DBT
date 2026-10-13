"""
SupplyCopia ATB Studio — Confluence Space Master Builder
Constructs and publishes a comprehensive, hierarchical Table of Contents (TOC) and technical documentation
suite into Atlassian Confluence Space 'ATBDBT' (Ask The Bee - DBT Automation).
Includes complete technical specs, process flows, and embedded UI screenshot attachments.
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
DOCS_ASSETS_DIR = PROJECT_ROOT / "agent_memory" / "docs_assets"


def get_image_macro(filename: str, width: int = 760) -> str:
    """Generates Confluence storage format for embedding an uploaded image attachment."""
    return f"""
    <p style="text-align: center;">
        <ac:image ac:align="center" ac:width="{width}">
            <ri:attachment ri:filename="{filename}"/>
        </ac:image>
    </p>
    """


class ConfluenceSpaceBuilder:
    """
    Builds and publishes the complete multi-tier technical documentation hierarchy
    into Confluence Space ATBDBT.
    """

    def __init__(self, connector: Optional[ConfluenceConnector] = None):
        self.connector = connector or ConfluenceConnector(space_key=CONFLUENCE_SPACE_KEY)
        self.assets_dir = DOCS_ASSETS_DIR

    def get_master_hub_content(self, now_str: str) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>SupplyCopia Autonomous DBT Pipeline Studio (Ask The Bee)</strong> • Enterprise Technical Documentation Hub. Space: <code>ATBDBT</code>. Synchronized: <strong>{now_str}</strong>.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("supplycopia_brand_logo.png", width=360)}

        <h2>📖 Master Documentation Index &amp; System Manual</h2>
        <p>Welcome to the central technical documentation repository for the <strong>SupplyCopia Autonomous DBT Pipeline Studio</strong>. This knowledge base provides an exhaustive, end-to-end technical reference detailing our multi-agent cognitive architecture, 7-stage automated data transformation pipeline, mathematical parity reconciler, Sentinel Aegis self-healing engine, and enterprise DevOps hosting runbooks.</p>

        <ac:structured-macro ac:name="toc">
            <ac:parameter ac:name="maxLevel">3</ac:parameter>
            <ac:parameter ac:name="type">list</ac:parameter>
        </ac:structured-macro>

        <hr/>

        <h2>🗂️ Complete Documentation Sections Overview</h2>
        <table>
            <thead>
                <tr>
                    <th style="width: 250px;">Section &amp; Module</th>
                    <th>Core Focus &amp; Functional Highlights</th>
                    <th style="width: 140px;">Primary UI Artifact</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>1. Executive Summary &amp; Architecture</strong></td>
                    <td>High-level platform vision, 8 cognitive bee personas, in-process DuckDB OLAP kernel, and enterprise ROI.</td>
                    <td>Brand Identity</td>
                </tr>
                <tr>
                    <td><strong>2. End-to-End Autonomous Pipeline</strong></td>
                    <td>7-Stage state machine lifecycle, Autonomy Control Deck (Default, Approval Gate, Turbo), and checkpoint resumption.</td>
                    <td>Studio Run Deck</td>
                </tr>
                <tr>
                    <td><strong>3. Stage 1 — Multi-Tenant Ingestion</strong></td>
                    <td>AWS S3 automated connector, cryptographic SHA-256 tenant catalogs, and Smart Entity Merging (Latest Wins, Union Distinct, Append).</td>
                    <td>Ingestion Catalog</td>
                </tr>
                <tr>
                    <td><strong>4. Stage 2 — Raw Profiling &amp; Cardinality</strong></td>
                    <td>DuckDB automated schema inference, null-rate spike detection, cardinality validation, and clinical anomaly rules.</td>
                    <td>Schema Profiler</td>
                </tr>
                <tr>
                    <td><strong>5. Stage 3 — Semantic Topology &amp; Joins</strong></td>
                    <td>Automated multi-entity join discovery, 4-tier item matching cascades, clinical UOM conversion matrix, and HITL Gate 1.</td>
                    <td>Topology Matrix</td>
                </tr>
                <tr>
                    <td><strong>6. Stage 4 — Autonomous DBT Lineage</strong></td>
                    <td>4-tier semantic transformation models (Staging, Intermediate, Marts, Security), Cytoscape DAG lineage, and DBT Docs portal.</td>
                    <td>Cytoscape Lineage</td>
                </tr>
                <tr>
                    <td><strong>7. Stage 5 — Output Explorer &amp; Marts</strong></td>
                    <td>Dimensional analytical fact tables, real-time KPI tiles with lightbox inspection, and clinical procedure taxonomy.</td>
                    <td>KPI Inspector</td>
                </tr>
                <tr>
                    <td><strong>8. Stage 6 — 100% Golden Parity Audit</strong></td>
                    <td>Zero-dollar mathematical spend reconciliation ($243,934,093.08), multi-layer governance assertions, and HITL Gate 2 certification.</td>
                    <td>Parity Reconciler</td>
                </tr>
                <tr>
                    <td><strong>9. Stage 7 — Refine &amp; Evolve (HITL)</strong></td>
                    <td>Retrospective finding submission (text feedback, evidence spreadsheets, Python scripts), dynamic implementation plans, and Playbook.</td>
                    <td>Plan Dialog</td>
                </tr>
                <tr>
                    <td><strong>10. Sentinel Aegis Self-Healing Engine</strong></td>
                    <td>Automated root-cause diagnostics, surgical AST SQL model repair, and automated high-priority Jira incident escalation (ATBDBT).</td>
                    <td>Self-Healing Deck</td>
                </tr>
                <tr>
                    <td><strong>11. Enterprise Suite &amp; Integrations</strong></td>
                    <td>Row &amp; record drift engine (5-sheet Excel export), Snowflake direct push &amp; RLS DDL, 90-day activity logs, and Ask The Bee chat.</td>
                    <td>Enterprise Suite</td>
                </tr>
                <tr>
                    <td><strong>12. Deployment &amp; DevOps Runbook</strong></td>
                    <td>Containerized Docker hosting (port 8081), Ubuntu VM specifications, reverse proxy routing, and Microsoft Entra SAML 2.0 SSO.</td>
                    <td>DevOps Runbook</td>
                </tr>
            </tbody>
        </table>

        <ac:structured-macro ac:name="tip">
            <ac:rich-text-body>
                <p><strong>Navigation Tip:</strong> Use the left-hand page tree in Confluence to expand and explore the child pages within each section.</p>
            </ac:rich-text-body>
        </ac:structured-macro>
        """

    def get_section_1_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 1: Cognitive Swarm &amp; Architectural Kernel</strong> • Core system topology of Ask The Bee.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("supplycopia_brand_logo.png", width=340)}

        <h2>1.1 Executive Vision &amp; Clinical Supply Chain Problem Statement</h2>
        <p>Enterprise healthcare systems operate across heterogeneous, fragmented Electronic Health Record (EHR) and Enterprise Resource Planning (ERP) databases, including Epic, Cerner, Infor Lawson, Workday, and disparate departmental spreadsheets. Manual data consolidation leads to pricing discrepancies, uncontracted spend leakage, UOM unit conversion distortions, and lengthy transformation cycles.</p>
        <p>The <strong>SupplyCopia Autonomous DBT Pipeline Studio (Ask The Bee)</strong> delivers an AI-driven, multi-agent cognitive compiler that autonomously discovers schemas, maps complex composite joins, validates clinical Unit of Measure (UOM) conversions, executes DBT models with <strong>100% mathematical parity ($243.9M golden baseline)</strong>, and self-heals runtime errors.</p>

        <hr/>

        <h2>1.2 Multi-Agent Cognitive Swarm Blueprint</h2>
        <p>The system is coordinated by an ensemble of specialized autonomous agents (Bee Personas), each dedicated to an isolated stage of the data engineering lifecycle:</p>

        <table>
            <thead>
                <tr>
                    <th>Agent Persona</th>
                    <th>Subsystem Role</th>
                    <th>Core Cognitive Responsibilities</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>👑 Queen Bee Orla</strong></td>
                    <td>Autonomous Orchestrator</td>
                    <td>Controls end-to-end pipeline state machine, enforces Human-In-The-Loop (HITL) review gates, and broadcasts system-wide events.</td>
                </tr>
                <tr>
                    <td><strong>🐝 Scout Bee Buzz</strong></td>
                    <td>Cloud Ingestion &amp; Profiler</td>
                    <td>Sniffs cloud storage (AWS S3), detects CSV/Excel schemas, profiles cardinality and null rates, and classifies domain entities.</td>
                </tr>
                <tr>
                    <td><strong>📐 Architect Bee Pollen</strong></td>
                    <td>Semantic Topology &amp; Joins</td>
                    <td>Discovers foreign keys, maps composite joins (PO to Invoices, Contracts, Item Master), and enforces Clinical UOM matrix rules.</td>
                </tr>
                <tr>
                    <td><strong>⚙️ Worker Bee Nectar</strong></td>
                    <td>Vectorized DBT Execution</td>
                    <td>Compiles modular DBT SQL models into in-process DuckDB tables across staging, intermediate, and dimensional mart tiers.</td>
                </tr>
                <tr>
                    <td><strong>🛡️ Inspector Bee Guard</strong></td>
                    <td>Mathematical Parity Reconciler</td>
                    <td>Executes multi-layer assertion verification, asserting zero dollar variance ($243,934,093.08) and clinical procedure cardinality.</td>
                </tr>
                <tr>
                    <td><strong>🩺 Sentinel Aegis</strong></td>
                    <td>Self-Healing Engine</td>
                    <td>Performs AST diagnosis on compilation errors, generates non-destructive SQL patches, and escalates unresolved failures to Jira.</td>
                </tr>
                <tr>
                    <td><strong>📈 Refinement Agent</strong></td>
                    <td>Clinical Retrospective &amp; Playbook</td>
                    <td>Translates human operator feedback and evidence into iterative implementation plans, updating the living institutional memory.</td>
                </tr>
                <tr>
                    <td><strong>🏛️ Enterprise Suite Agent</strong></td>
                    <td>Enterprise Governance &amp; Security</td>
                    <td>Detects record-level drift, exports 5-sheet OpenPyXL workbooks, manages Snowflake data sharing, and generates multi-tenant RLS policies.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>1.3 In-Process DuckDB Analytical Architecture</h2>
        <p>Unlike conventional heavyweight ETL engines that require spinning up distributed clusters for mid-sized hospital data lakes, Ask The Bee leverages an in-process, vectorized columnar <strong>DuckDB OLAP kernel</strong>:</p>
        <ul>
            <li><strong>Zero Network Overhead:</strong> Analytical queries execute directly in memory at gigabytes per second.</li>
            <li><strong>ANSI SQL &amp; DBT Integration:</strong> Full native support for standard DBT adapters (<code>dbt-duckdb</code>) and vectorized SQL execution.</li>
            <li><strong>ACID State Persistence:</strong> Pipelines persist to single-file databases (<code>autonomous_pipeline.duckdb</code>) with instant checkpoint snapshotting.</li>
        </ul>
        """

    def get_section_2_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 2: 7-Stage Pipeline Lifecycle</strong> • Comprehensive execution flow and governance control deck.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("studio_run_deck_overview.png", width=760)}

        <h2>2.1 The 7-Stage Autonomous Transformation Workflow</h2>
        <p>Ask The Bee transitions clinical and supply chain datasets through seven sequential execution stages:</p>

        <table>
            <thead>
                <tr>
                    <th style="width: 120px;">Stage</th>
                    <th>Stage Name</th>
                    <th>Input Artifacts</th>
                    <th>Output Artifacts</th>
                    <th>Governance Gate</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Stage 1</strong></td>
                    <td>Multi-Tenant Ingestion</td>
                    <td>AWS S3 / Local CSV files</td>
                    <td>Tenant Cryptographic Catalog (SHA-256)</td>
                    <td>File Integrity Check</td>
                </tr>
                <tr>
                    <td><strong>Stage 2</strong></td>
                    <td>Raw Schema Profiling</td>
                    <td>Staged raw datasets</td>
                    <td>Cardinality, null rates &amp; memory profiles</td>
                    <td>Null Spike Threshold</td>
                </tr>
                <tr>
                    <td><strong>Stage 3</strong></td>
                    <td>Semantic Topology</td>
                    <td>Classified tables</td>
                    <td>Discovered natural joins &amp; UOM factors</td>
                    <td><strong>HITL Gate 1 (Topology)</strong></td>
                </tr>
                <tr>
                    <td><strong>Stage 4</strong></td>
                    <td>DBT Lineage Compilation</td>
                    <td>DBT models &amp; seeds</td>
                    <td>DuckDB marts &amp; Cytoscape DAG lineage</td>
                    <td>Compilation Assertion</td>
                </tr>
                <tr>
                    <td><strong>Stage 5</strong></td>
                    <td>Output Explorer &amp; Gaps</td>
                    <td>Dimensional marts</td>
                    <td>Interactive KPI cards &amp; procedure gaps</td>
                    <td>Data Inspection</td>
                </tr>
                <tr>
                    <td><strong>Stage 6</strong></td>
                    <td>100% Golden Parity</td>
                    <td>Raw vs Mart spend</td>
                    <td>Mathematical Audit Certificate</td>
                    <td><strong>HITL Gate 2 (Certification)</strong></td>
                </tr>
                <tr>
                    <td><strong>Stage 7</strong></td>
                    <td>Refine &amp; Evolve</td>
                    <td>Operator feedback &amp; scripts</td>
                    <td>Iterative Auto-Resolve Implementation Plan</td>
                    <td>Plan Confirmation</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>2.2 Autonomy Control Deck (Execution Modes)</h2>
        <p>The application supports three autonomy levels selectable from the top toolbar:</p>
        <ol>
            <li><strong>Default (Step-by-Step HITL):</strong> The studio pauses at each milestone, allowing clinical analysts to inspect schemas, review join relationships, and verify intermediate numbers.</li>
            <li><strong>User Review (Approval Gates):</strong> Executes stages 1-5 automatically, pausing at critical checkpoints: <em>Gate 1 (Semantic Topology Approval)</em> and <em>Gate 2 (100% Mathematical Parity Certification)</em>.</li>
            <li><strong>⚡ Turbo Mode (Full Auto End-to-End):</strong> Executes the full pipeline continuously from cloud ingestion through DBT compilation, self-healing, mathematical audit, and Snowflake push without manual pauses.</li>
        </ol>

        <hr/>

        <h2>2.3 Checkpoint Management &amp; State Persistence</h2>
        <p>Operators can snapshot the entire pipeline state at any stage via the <strong>Saved Pipelines Drawer</strong>. Each checkpoint packages DuckDB binary state, agent memory, and compilation blueprints, allowing instantaneous restoration without re-running upstream ingestion.</p>
        """

    def get_section_3_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 3: Ingestion &amp; Smart Merging</strong> • Multi-source data intake, catalog versioning, and SCD-1 deduplication.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage1_ingestion_and_catalog.png", width=760)}

        <h2>3.1 Ingestion Sources &amp; AWS S3 Cloud Connector</h2>
        <p>The studio ingests clinical supply chain feeds from three primary vectors:</p>
        <ul>
            <li><strong>AWS S3 Cloud Bucket Connector:</strong> Configured via <code>S3_URL</code> and AWS credentials in <code>.env</code>. The Scout Bee polls <code>s3://supplycopia-dbt-ingestion/</code>, automatically syncing incoming files into local staging.</li>
            <li><strong>Portal Upload:</strong> Direct file drag-and-drop supporting large CSV and Excel files.</li>
            <li><strong>REST API Ingestion:</strong> Programmatic registration via <code>POST /api/client_tracker/register</code>.</li>
        </ul>

        <hr/>

        <h2>3.2 Tenant Cryptographic Ingestion Catalog</h2>
        <p>Every ingested file is cryptographically fingerprinted using <strong>SHA-256</strong> and logged to an isolated JSON catalog per client tenant (<code>agent_memory/client_catalogs/{{client_id}}_catalog.json</code>):</p>
        <ul>
            <li><strong>Version Incrementing:</strong> When updated files are submitted for an existing entity (e.g. newer Item Master addendum), the version auto-increments (<code>v1 -> v2</code>).</li>
            <li><strong>Lifecycle State Tracking:</strong> Previous files transition to <code>SUPERSEDED</code> status while latest files remain <code>ACTIVE</code>.</li>
            <li><strong>Provenance Metadata:</strong> Captures exact byte size, row count, checksum, ingestion timestamp, and operator notes.</li>
        </ul>

        <hr/>

        <h2>3.3 Smart Entity Merging &amp; De-duplication Engine</h2>
        <p>When clients supply multiple periodic or fragmented files for the same entity, the <strong>Entity Merge Engine</strong> executes in-process DuckDB consolidations using three algorithms:</p>

        <table>
            <thead>
                <tr>
                    <th>Merge Strategy</th>
                    <th>Mathematical SQL Algorithm</th>
                    <th>Target Healthcare Use Case</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Latest Wins (SCD-1)</strong></td>
                    <td><code>ROW_NUMBER() OVER(PARTITION BY natural_keys ORDER BY version DESC) = 1</code></td>
                    <td>Item Master and Contract catalog revisions where newer price and vendor attributes overwrite outdated baseline values.</td>
                </tr>
                <tr>
                    <td><strong>Union Distinct</strong></td>
                    <td><code>SELECT DISTINCT * FROM source_files</code></td>
                    <td>Exact row-level deduplication across overlapping hospital departmental exports.</td>
                </tr>
                <tr>
                    <td><strong>Additive Append</strong></td>
                    <td><code>UNION ALL</code> with collision filtering</td>
                    <td>Transactional rollups for monthly Purchase Orders and Invoices.</td>
                </tr>
            </tbody>
        </table>
        """

    def get_section_4_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 4: Profiling &amp; Cardinality</strong> • Automated data quality inspection and schema inference.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage2_profiling_and_cardinality.png", width=760)}

        <h2>4.1 Automated Schema Profiling in DuckDB</h2>
        <p>In Stage 2, Scout Bee Buzz analyzes raw datasets and extracts column-level statistics:</p>
        <ul>
            <li><strong>Column Typing &amp; Cast Inference:</strong> Distinguishes strings, dates, integers, and floating-point currencies.</li>
            <li><strong>Cardinality &amp; Distinct Counts:</strong> Identifies potential primary key candidates and category columns.</li>
            <li><strong>Null-Rate Quantification:</strong> Computes null count and percentage for every column.</li>
            <li><strong>Memory &amp; Storage Footprint:</strong> Monitors row density and DuckDB memory utilization.</li>
        </ul>

        <hr/>

        <h2>4.2 Clinical Data Anomaly Rules &amp; Null Spike Alerts</h2>
        <p>The profiler executes heuristic clinical governance checks:</p>
        <ol>
            <li><strong>Null Rate Spike Alert:</strong> Flags columns where missingness exceeds 10% compared to historical baselines.</li>
            <li><strong>Negative Cost Detection:</strong> Rejects or flags line items with negative unit costs or negative spend amounts.</li>
            <li><strong>Date Sequence Inversion:</strong> Verifies that Invoice Date is greater than or equal to Purchase Order Date.</li>
        </ol>
        """

    def get_section_5_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 5: Semantic Topology &amp; Joins</strong> • Graph discovery, 4-tier matching, and clinical UOM factors.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage3_topology_and_uom_matrix.png", width=760)}

        <h2>5.1 Natural Composite Join Graph Discovery</h2>
        <p>Architect Bee Pollen inspects column names, cardinality, and data intersections across the five canonical clinical supply chain tables to automatically discover natural relationships:</p>
        <ul>
            <li><strong>Purchase Orders ➔ Invoices:</strong> Linked deterministically on composite key <code>po_number</code> + <code>po_line_no</code> (achieving 100.0% match rate).</li>
            <li><strong>Purchase Orders ➔ Contracts:</strong> Linked on <code>contract_number</code> + <code>item_number</code>.</li>
            <li><strong>Purchase Orders ➔ Item Master:</strong> Linked on <code>item_number</code> via the 4-Tier Matching Cascade.</li>
            <li><strong>Consumption ➔ Clinical Item Master:</strong> Linked on item identification with clinical procedure taxonomy.</li>
        </ul>

        <hr/>

        <h2>5.2 The 4-Tier Item Matching Cascade Architecture</h2>
        <p>Because item catalog numbers across hospital ERP systems often suffer from punctuation mismatches, leading zeros, or vendor vendor code discrepancies, Ask The Bee employs a 4-tier matching cascade:</p>

        <table>
            <thead>
                <tr>
                    <th>Matching Tier</th>
                    <th>Strategy</th>
                    <th>Transformation Heuristic</th>
                    <th>Match Confidence</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Tier 1</strong></td>
                    <td>Exact Alphanumeric</td>
                    <td><code>TRIM(UPPER(source.item_number)) = TRIM(UPPER(target.item_number))</code></td>
                    <td>100% (Deterministic)</td>
                </tr>
                <tr>
                    <td><strong>Tier 2</strong></td>
                    <td>Stripped Punctuation</td>
                    <td><code>REGEXP_REPLACE(item_number, '[^A-Za-z0-9]', '')</code> (removes dashes, slashes, spaces)</td>
                    <td>95% (High)</td>
                </tr>
                <tr>
                    <td><strong>Tier 3</strong></td>
                    <td>Normalized Description</td>
                    <td>Standardized clinical description token matching.</td>
                    <td>85% (Medium-High)</td>
                </tr>
                <tr>
                    <td><strong>Tier 4</strong></td>
                    <td>Fuzzy Soundex / Levenshtein</td>
                    <td>Phonetic Soundex and Levenshtein distance string similarity.</td>
                    <td>70% (Review Recommended)</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>5.3 Clinical Unit of Measure (UOM) Normalization Matrix</h2>
        <p>A major source of phantom spend inflation in hospital procurement occurs when invoices bill in bulk packaging (e.g. Box of 10) while consumption logs record individual eaches (EA). Architect Bee Pollen enforces standard clinical conversion factors:</p>
        <ul>
            <li><strong>BX (Box):</strong> 10 EA</li>
            <li><strong>CS (Case):</strong> 50 EA</li>
            <li><strong>PK (Pack):</strong> 12 EA</li>
            <li><strong>DZ (Dozen):</strong> 12 EA</li>
            <li><strong>PR (Pair):</strong> 2 EA</li>
            <li><strong>RL (Roll):</strong> 1 EA</li>
        </ul>
        <p>Normalized unit price formula: <code>normalized_unit_price = (raw_unit_price / conversion_factor)</code>.</p>
        """

    def get_section_6_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 6: DBT Lineage &amp; Compilation</strong> • 4-tier semantic transformation hierarchy and Cytoscape DAG.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage4_lineage_dag_and_dbt_docs.png", width=760)}

        <h2>6.1 4-Tier Semantic Transformation Architecture</h2>
        <p>The autonomous DBT compilation engine organizes SQL transformations into four strict architectural layers:</p>

        <table>
            <thead>
                <tr>
                    <th>Layer</th>
                    <th>Model Names</th>
                    <th>Materialization</th>
                    <th>Engineering Purpose</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>1. Staging</strong></td>
                    <td><code>stg_consumption_v4</code>, <code>stg_purchase_orders_v4</code>, <code>stg_contracts_v4</code>, <code>stg_item_master_v4</code>, <code>stg_invoices_v4</code></td>
                    <td>View / Table</td>
                    <td>Defensive casting, TRIM, regex cleanup, and UOM factor standardization. Zero business logic.</td>
                </tr>
                <tr>
                    <td><strong>2. Intermediate</strong></td>
                    <td><code>int_item_matching_v4</code>, <code>int_contract_coverage_v4</code>, <code>int_po_spend_aggregation_v4</code></td>
                    <td>Table</td>
                    <td>Executes 4-tier cascades, calculates contract coverage ratios, and merges line-level spend.</td>
                </tr>
                <tr>
                    <td><strong>3. Dimensional Marts</strong></td>
                    <td><code>fct_gap_analysis_v4</code>, <code>fct_po_cost_savings_v4</code>, <code>fct_consumption_cost_savings_v4</code>, <code>dim_clinical_items_v4</code></td>
                    <td>Table</td>
                    <td>Production analytical tables aggregating spend, savings, overpayments, and clinical procedure groupings.</td>
                </tr>
                <tr>
                    <td><strong>4. Multi-Tenant Security</strong></td>
                    <td><code>sc_multi_tenant_gap_analysis</code>, <code>sc_multi_tenant_po_savings</code>, <code>sc_multi_tenant_consumption_savings</code></td>
                    <td>Table</td>
                    <td>Injects client tenant partition keys (<code>tenant_gap_id</code>) and enables Snowflake Row-Level Security policies.</td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>6.2 Cytoscape.js Directed Acyclic Graph (DAG)</h2>
        <p>The interactive lineage visualizer renders dependencies from raw source nodes through intermediate join transforms to final analytical marts with real-time status highlights.</p>
        <p>The embedded DBT documentation viewer is served directly inside the studio at <code>/dbt-docs/</code>.</p>
        """

    def get_section_7_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 7: Output Explorer &amp; Marts</strong> • Fact table analytics, KPI tiles, and clinical procedure taxonomy.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage5_output_explorer_fact_tables.png", width=760)}

        <h2>7.1 Production Analytical Fact Tables</h2>
        <p>Stage 5 exposes the synthesized analytical data marts:</p>
        <ul>
            <li><strong><code>fct_gap_analysis_v4</code>:</strong> Clinical procedure grouping, contract matching status, spend with contract vs without, and contract gap codes.</li>
            <li><strong><code>fct_po_cost_savings_v4</code>:</strong> Line-level purchase order price variance versus active contract benchmark price.</li>
            <li><strong><code>fct_consumption_cost_savings_v4</code>:</strong> Case-level surgical supply consumption and waste variance across OR facilities.</li>
        </ul>

        <hr/>

        <h2>7.2 Interactive KPI Tiles &amp; Lightbox Inspector</h2>
        <p>The studio header features real-time financial KPI tiles:</p>

        {get_image_macro("executive_kpi_tiles_dashboard.png", width=760)}

        <p>Clicking any KPI tile opens an interactive lightbox modal with full SQL filtering, column sorting, and instant Excel export.</p>

        <hr/>

        <h2>7.3 Clinical Procedure Taxonomy &amp; Service Lines</h2>
        <p>Procedure spend is grouped across hospital service lines (Cardiology, Orthopedics, General Surgery, Podiatry) and facilities (UH OR, West Chester Hospital), enabling surgical chiefs to identify off-contract supply choices.</p>
        """

    def get_section_8_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 8: 100% Golden Parity Audit</strong> • Mathematical spend reconciler and certification gate.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("stage6_golden_parity_mathematical_audit.png", width=760)}

        <h2>8.1 Zero-Variance Mathematical Spend Reconciler</h2>
        <p>A core architectural pillar of Ask The Bee is <strong>Zero Dollar Variance</strong>. In Stage 6, Inspector Bee Guard executes dual mathematical reconciliations:</p>
        <p style="text-align: center; font-size: 16px;">
            <code>Raw Source Ingested Spend ($243,934,093.08) = Aggregated Dimensional Mart Spend ($243,934,093.08)</code>
        </p>
        <p>If variance is greater than $0.00, the pipeline halts immediately, triggering Sentinel Aegis self-healing.</p>

        <hr/>

        <h2>8.2 Multi-Layer Governance Assertion Test Suite</h2>
        <table>
            <thead>
                <tr>
                    <th>Test ID</th>
                    <th>Assertion Name</th>
                    <th>Target Metric / Benchmark</th>
                    <th>Pass Criterion</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>TEST_SPEND_001</strong></td>
                    <td>Golden Spend Parity</td>
                    <td>Total Spend Reconciliation</td>
                    <td><code>ABS(source_spend - mart_spend) &lt; 0.01</code></td>
                </tr>
                <tr>
                    <td><strong>TEST_GAP_008</strong></td>
                    <td>Procedure Hierarchy Cardinality</td>
                    <td>Standard Procedure Count</td>
                    <td><code>448 &lt;= COUNT(*) &lt;= 1,125</code></td>
                </tr>
                <tr>
                    <td><strong>TEST_MATCH_003</strong></td>
                    <td>PO Contract Match Confidence</td>
                    <td>Item Matching Rate</td>
                    <td><code>match_rate &gt;= 85.0%</code></td>
                </tr>
                <tr>
                    <td><strong>TEST_NULL_004</strong></td>
                    <td>Primary Key Integrity</td>
                    <td>Log ID &amp; PO Line Nullness</td>
                    <td><code>null_count == 0</code></td>
                </tr>
            </tbody>
        </table>

        <hr/>

        <h2>8.3 HITL Gate 2 Certification</h2>
        <p>Upon passing all assertions, the operator approves <strong>Gate 2 Certification</strong>, advancing the pipeline status to <code>AUDITED_CERTIFIED</code>, enabling Snowflake export.</p>
        """

    def get_section_9_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 9: Refine &amp; Evolve (HITL)</strong> • Retrospective feedback loop and living institutional playbook.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("autoresolve_implementation_plan_dialog.png", width=760)}

        <h2>9.1 Multi-Modal Finding Submission</h2>
        <p>In Stage 7 (Refine &amp; Evolve), clinical and analytics teams can retrospectively refine the pipeline by submitting:</p>
        <ul>
            <li><strong>Text Findings:</strong> Operator observations (e.g. <em>"Standardize laparoscopic trocar UOM from Box to Each"</em>).</li>
            <li><strong>Supporting Documents:</strong> Excel spreadsheets, CSV data diffs, or clinical screenshots.</li>
            <li><strong>Python Patch Scripts:</strong> Custom executable scripts with algorithmic repairs.</li>
        </ul>

        <hr/>

        <h2>9.2 Dynamic Implementation Plan Synthesis</h2>
        <p>The Refinement Agent analyzes the submitted evidence against the active DBT blueprint and synthesizes a structured <strong>Auto-Resolve Implementation Plan</strong>:</p>
        <ol>
            <li><strong>Root Cause Summary:</strong> Identifies underlying schema binding or aggregation discrepancy.</li>
            <li><strong>Surgical Model Patch:</strong> Generates targeted SQL or configuration adjustment.</li>
            <li><strong>Verification Assertion:</strong> Specifies exact assertion to verify parity after patch execution.</li>
            <li><strong>Iterative Revision Cycle:</strong> Operators can request modifications, automatically generating revision 2, 3, etc. until confirmed.</li>
        </ol>

        <hr/>

        <h2>9.3 Living Institutional Knowledge Playbook Engine</h2>
        <p>Confirmed refinements are permanently appended to the <strong>Institutional Knowledge Playbook</strong> (<code>agent_memory/institutional_knowledge.json</code>). Future pipeline executions automatically inherit these learnings, preventing recurring errors.</p>
        """

    def get_section_10_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 10: Sentinel Aegis Self-Healing</strong> • Automated error diagnosis, AST patching, and Jira escalation.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        {get_image_macro("sentinel_aegis_diagnostics_lightbox.png", width=760)}

        <h2>10.1 Autonomous Error Diagnosis &amp; Root Cause Classification</h2>
        <p>When compilation timeouts (504), gateway restarts (502), or assertion failures occur, <strong>Sentinel Aegis</strong> intercepts the failure:</p>
        <ul>
            <li><strong>Syntax &amp; Grammar Errors:</strong> Traps missing commas, reserved keyword collisions, and dialect mismatches.</li>
            <li><strong>Cardinality Failures (e.g. TEST_GAP_008):</strong> Diagnoses dimension compression during aggregation, expanding grouping levels to preserve clinical procedure granularity.</li>
            <li><strong>Gateway Recovery:</strong> Automatically restarts background compilation workers and re-synchronizes DuckDB locks.</li>
        </ul>

        <hr/>

        <h2>10.2 Surgical AST Model Patching</h2>
        <p>Sentinel Aegis synthesizes and executes targeted patches directly to DuckDB tables and DBT SQL models on disk:</p>

        {get_image_macro("autoresolve_execution_and_patching.png", width=760)}

        <hr/>

        <h2>10.3 Automated Jira Cloud Incident Escalation (Project: ATBDBT)</h2>
        <p>If an error cannot be auto-resolved after exhausting automated retry budgets, Sentinel Aegis escalates the failure directly into Jira project <strong><code>ATBDBT</code></strong>:</p>
        <ul>
            <li><strong>High-Priority Bug Ticket:</strong> Created via Jira REST API v3 formatted in Atlassian Document Format (ADF).</li>
            <li><strong>Anti-Spam Deduplication:</strong> Generates a SHA-256 hash of error context, suppressing duplicate tickets within a 1-hour window.</li>
            <li><strong>Diagnostic Blueprints:</strong> Includes error traceback, active tenant context, and reproduction steps for human engineers.</li>
        </ul>
        """

    def get_section_11_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 11: Enterprise Capabilities Suite</strong> • Drift detection, Snowflake RLS, 90-day audit logging, and AI assistant.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>11.1 Record &amp; Row Drift Detection Engine</h2>
        <p>Compares incoming client datasets against established baselines at the individual row level:</p>
        <ul>
            <li><strong>Record Churn Matrix:</strong> Segregates records into <code>ADDED</code>, <code>MODIFIED</code>, <code>DROPPED</code>, and <code>UNCHANGED</code> partitions.</li>
            <li><strong>Field Modification Frequencies:</strong> Pinpoints fields experiencing mutations (e.g. price fluctuations).</li>
            <li><strong>Null-Rate Drift:</strong> Alerts when column missingness spikes by more than 10%.</li>
            <li><strong>5-Sheet OpenPyXL Workbook Export:</strong> Streamed via <code>GET /api/enterprise/export_excel?dataset=record_drift</code>.</li>
        </ul>

        <hr/>

        <h2>11.2 Snowflake Data Warehouse Export &amp; Multi-Tenant RLS</h2>
        <p>Exports verified fact marts to Snowflake while generating multi-tenant Row-Level Security (RLS) DDL policies:</p>
        <pre><code>CREATE OR REPLACE ROW ACCESS POLICY rap_client_isolation
AS (client_id VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() = 'ACCOUNTADMIN'
  OR client_id = CURRENT_SESSION_CONTEXT('TENANT_ID');</code></pre>

        <hr/>

        <h2>11.3 User Activity &amp; Audit Logging Engine (Strict 90-Day Retention)</h2>
        <p>Maintains an audit ledger in SQLite WAL mode (<code>agent_memory/activity_audit_logs.db</code>) with zero HTTP latency overhead. Automatically purges records older than 90 days with incremental vacuum and supports on-demand CSV compliance export.</p>

        <hr/>

        <h2>11.4 Ask The Bee Multi-Agent Chat Assistant</h2>
        <p>Floating chat widget powered by specialized swarm personas for interactive schema queries, savings lookups, and visual chart requests.</p>
        """

    def get_section_12_content(self) -> str:
        return f"""
        <ac:structured-macro ac:name="info">
            <ac:rich-text-body>
                <p><strong>Section 12: Deployment &amp; DevOps Runbook</strong> • Container hosting, VM setup, subdomain mapping, and SAML 2.0 SSO.</p>
            </ac:rich-text-body>
        </ac:structured-macro>

        <h2>12.1 Containerized Docker Hosting Guide</h2>
        <p>The application is packaged into a self-contained Docker container exposed on port <strong>8081</strong>:</p>
        <pre><code># Build and run Docker container on port 8081
sudo docker build -t supplycopia-atb-studio .
sudo docker run -d \\
  --name supplycopia-atb-studio \\
  --restart unless-stopped \\
  -p 8081:8081 \\
  --env-file .env \\
  supplycopia-atb-studio</code></pre>

        <hr/>

        <h2>12.2 Ubuntu VM Hardware &amp; OS Requirements</h2>
        <ul>
            <li><strong>Operating System:</strong> Ubuntu 22.04 LTS / 24.04 LTS</li>
            <li><strong>vCPU:</strong> 4 vCPU minimum (8 vCPU recommended for large hospital systems)</li>
            <li><strong>RAM:</strong> 16 GB RAM minimum (32 GB recommended for DuckDB in-memory OLAP)</li>
            <li><strong>Storage:</strong> 100 GB SSD (NVMe preferred)</li>
            <li><strong>Target VM Private IP:</strong> <code>10.12.0.109</code> (Port: <code>8081</code>)</li>
        </ul>

        <hr/>

        <h2>12.3 Network Routing &amp; Subdomain Mapping</h2>
        <p>Recommended Subdomain: <strong><code>atb-dbt.supplycopia.com</code></strong>.</p>
        <p>Nginx Reverse Proxy configuration snippet:</p>
        <pre><code>server {{
    listen 443 ssl http2;
    server_name atb-dbt.supplycopia.com;

    location / {{
        proxy_pass http://10.12.0.109:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }}
}}</code></pre>

        <hr/>

        <h2>12.4 Microsoft Entra ID SAML 2.0 SSO Configuration</h2>
        <ul>
            <li><strong>Identifier (Entity ID):</strong> <code>https://atb-dbt.supplycopia.com/saml/metadata</code></li>
            <li><strong>Reply URL (Assertion Consumer Service):</strong> <code>https://atb-dbt.supplycopia.com/saml/acs</code></li>
            <li><strong>Sign-on URL:</strong> <code>https://atb-dbt.supplycopia.com/login</code></li>
        </ul>
        """

    def build_full_confluence_space(self, progress_callback=None) -> Dict[str, Any]:
        """
        Executes the phased population of Confluence Space ATBDBT.
        Publishes Master Hub, 12 parent sections, and uploads screenshot attachments.
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

        if hub_id:
            # Upload logo attachment to Master Hub
            logo_path = self.assets_dir / "supplycopia_brand_logo.png"
            if logo_path.exists():
                self.connector.upload_attachment(hub_id, logo_path, "supplycopia_brand_logo.png")

        # 2. Define the 12 Sections
        sections = [
            {
                "num": 1,
                "title": "1. Executive Summary & Cognitive Multi-Agent Swarm Architecture",
                "body_fn": self.get_section_1_content,
                "attachments": ["supplycopia_brand_logo.png"]
            },
            {
                "num": 2,
                "title": "2. End-to-End Autonomous Pipeline Lifecycle & 7-Stage Workflow",
                "body_fn": self.get_section_2_content,
                "attachments": ["studio_run_deck_overview.png"]
            },
            {
                "num": 3,
                "title": "3. Stage 1 — Multi-Tenant Ingestion, Cryptographic Catalog & Smart Merging",
                "body_fn": self.get_section_3_content,
                "attachments": ["stage1_ingestion_and_catalog.png"]
            },
            {
                "num": 4,
                "title": "4. Stage 2 — Raw Schema Profiling, Cardinality & Anomaly Diagnostics",
                "body_fn": self.get_section_4_content,
                "attachments": ["stage2_profiling_and_cardinality.png"]
            },
            {
                "num": 5,
                "title": "5. Stage 3 — Semantic Topology, Join Discovery & Clinical UOM Matrix",
                "body_fn": self.get_section_5_content,
                "attachments": ["stage3_topology_and_uom_matrix.png"]
            },
            {
                "num": 6,
                "title": "6. Stage 4 — Autonomous DBT Lineage, Cytoscape DAG & Documentation",
                "body_fn": self.get_section_6_content,
                "attachments": ["stage4_lineage_dag_and_dbt_docs.png"]
            },
            {
                "num": 7,
                "title": "7. Stage 5 — Output Explorer, Fact Marts & Clinical Analytics",
                "body_fn": self.get_section_7_content,
                "attachments": ["stage5_output_explorer_fact_tables.png", "executive_kpi_tiles_dashboard.png"]
            },
            {
                "num": 8,
                "title": "8. Stage 6 — 100% Golden Parity & Mathematical Audit ($243.9M Reconciler)",
                "body_fn": self.get_section_8_content,
                "attachments": ["stage6_golden_parity_mathematical_audit.png"]
            },
            {
                "num": 9,
                "title": "9. Stage 7 — Refine & Evolve: Retrospective Clinical Feedback & Playbook",
                "body_fn": self.get_section_9_content,
                "attachments": ["autoresolve_implementation_plan_dialog.png"]
            },
            {
                "num": 10,
                "title": "10. Sentinel Aegis — Self-Healing Engine & Automated Jira Escalation",
                "body_fn": self.get_section_10_content,
                "attachments": ["sentinel_aegis_diagnostics_lightbox.png", "autoresolve_execution_and_patching.png"]
            },
            {
                "num": 11,
                "title": "11. Enterprise Capabilities Suite — Observability, Snowflake RLS & Audit Logs",
                "body_fn": self.get_section_11_content,
                "attachments": ["studio_run_deck_overview.png", "executive_kpi_tiles_dashboard.png"]
            },
            {
                "num": 12,
                "title": "12. Deployment, Infrastructure & DevOps Runbook (Docker, VM & SSO)",
                "body_fn": self.get_section_12_content,
                "attachments": ["supplycopia_brand_logo.png"]
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
            page_id = page_res.get("page_id")

            # Upload and attach screenshots
            if page_id and sec.get("attachments"):
                for att_name in sec["attachments"]:
                    att_path = self.assets_dir / att_name
                    if att_path.exists():
                        log_step(f"  Uploading attachment '{att_name}' to page {page_id}...")
                        self.connector.upload_attachment(page_id, att_path, att_name)
                    else:
                        print(f"  Notice: Asset not found {att_path}")

            time.sleep(0.5)  # Courteous rate limit pause

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
