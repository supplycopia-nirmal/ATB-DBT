# 🐝 Ask The Bee (ATB-DBT): Autonomous Multi-Tenant DBT Studio

![SupplyCopia Banner](autonomous_dbt_app/dashboard_v2/static/logo.png)

An enterprise-grade, agentic AI platform engineered for **SupplyCopia** to autonomously profile, reconcile, topology-map, and generate production **dbt Core (DuckDB & Snowflake)** pipelines from heterogeneous client EHR / ERP supply chain datasets.

---

## 🌟 Key Capabilities

1. **Autonomous Bee Swarm Architecture**:
   - **Queen Bee Orla**: Chief Swarm Coordinator & Orchestrator. Directs stages, checkpoints, and human-in-the-loop (HITL) approval gates.
   - **Scout Bee Buzz**: Cloud Ingestion (AWS S3) & Schema Profiler. Discovers canonical datasets and detects schema drift across hospitals.
   - **Architect Bee Pollen**: Relationship Inference & Join Topology Synthesizer. Auto-constructs multi-tier foreign key joins and matching cascades.
   - **Worker Bee Stitch**: CodeCraft Engineer. Generates modular Jinja2 SQL models, analytical marts, and multi-tenant metadata tagging.
   - **Inspector Bee Guard (Sentinel Aegis)**: Parity Assurance, SLA Anomaly Detector & Auto-Resolve Diagnostics Engine.
   - **Carrier Bee Nectar**: Enterprise Cloud Warehouse Publisher. Manages high-throughput RSA-authenticated loading into Snowflake (`SC_MULTI_TENANT_*` and `SC_FCT_*`).

2. **Enterprise Multi-Tenancy & Zero-Downtime Warehouse**:
   - Standardized `SC_MULTI_TENANT_*` warehouse schema housing multiple hospital networks, GPOs, and ambulatory systems with row-level security (RLS).
   - Standardized common analytics marts:
     - `SC_FCT_CONSUMPTION_COST_SAVINGS`
     - `SC_FCT_PO_COST_SAVINGS`
     - `SC_FCT_GAP_ANALYSIS`
     - `SC_MULTI_TENANT_CONSUMPTION_SAVINGS`
     - `SC_MULTI_TENANT_PO_SAVINGS`
     - `SC_MULTI_TENANT_GAP_ANALYSIS`

3. **Incremental Load Historic Mutation Reconciliation**:
   - Detects late-arriving updates to historical records (e.g. retroactive price discounts, returned quantities).
   - Surfaces record-level and aggregate differences in an interactive modal with 3 configurable operator resolution modes:
     - `UPSERT_OVERWRITE`: Reconciles the baseline with late-arriving unit prices and quantities.
     - `SPLIT_AUDIT_TRAIL`: Preserves original historical rows marked as `SUPERSEDED` and appends new `ACTIVE` versions with valid date ranges.
     - `PRESERVE_HISTORIC_RECORD`: Retains historical baseline figures and routes delta duplicates to an audit exception ledger.

4. **Interactive KPI Drill-Down & Zero-Latency Raw Excel Export**:
   - Top KPI tiles (*Total Clinical Spend*, *Contract Match Rate*, *Savings Opportunity*, *Identified Overpayment*) feature interactive drill-down.
   - High-density Lightbox Dialog renders sample underlying records with instant column sorting.
   - Integrated zero-latency streaming exports full raw datasets directly to Excel (.xlsx) using Apache Arrow / OpenPyXL without browser or server memory bloat.

5. **Enterprise Readiness Guardrails**:
   - **Schema Contract Locks**: Strict JSON schema contracts enforce mandatory primary keys, field presence, and data types before staging.
   - **Automated Cost Budget Guardrails**: Hard limits on Snowflake Cortex and external LLM credit consumption per tenant to protect operational spend.
   - **Chaos Simulator & SLA Testing**: Synthetic perturbation engines stress-test pipelines against Epic EHR, Cerner Millennium, and MEDITECH ERP schema shifts.

---

## 🗂️ AWS S3 Storage & Ingestion Directory Specification

For multi-tenant ingestion, AWS S3 buckets are organized under client identifiers with segregated partitions for initial historical bulk loads and scheduled incremental feeds:

```text
s3://supplycopia-dbt-ingestion/
├── clients/
│   ├── <client_id>/                       # e.g., CL_UCH_001 (UC Health)
│   │   ├── initial_bulk_load/             # Initial historical multi-year baseline data
│   │   │   ├── UHC_Consumption.csv
│   │   │   ├── UHC_PO.csv
│   │   │   ├── UHC_CON.csv
│   │   │   ├── UHC_IM.csv
│   │   │   └── UHC_INV.csv
│   │   └── incremental_loads/             # Scheduled delta feeds partitioned by cadence
│   │       ├── daily/
│   │       │   └── YYYY-MM-DD/
│   │       │       └── consumption_delta_20261012.csv
│   │       ├── weekly/
│   │       │   └── YYYY-WW/
│   │       │       └── po_delta_2026_w41.csv
│   │       └── monthly/
│   │           └── YYYY-MM/
│   │               └── contract_delta_202610.csv
```

---

## 🏗️ Repository Architecture

```text
ATB-DBT/
├── clients/                         # Multi-tenant client workspace configurations
│   ├── uc_health/                   # UC Health System configuration & seeds
│   │   └── client_config.json
│   └── template_client/             # Scaffolding template for onboarding new clients
│       └── client_config.json
├── autonomous_dbt_app/              # Core Agentic Platform Engine
│   ├── agents/                      # Specialized Autonomous Bee Swarm Agents
│   │   ├── scout_bee.py             # Scout Bee Buzz: S3 connector & profiling
│   │   ├── architect_bee.py         # Architect Bee Pollen: Relationship & join topology
│   │   ├── codecraft_bee.py         # Worker Bee Stitch: dbt model synthesis
│   │   ├── enterprise_agent.py      # SLA detection, drift matrix, telemetry
│   │   └── snowflake_agent.py       # Carrier Bee Nectar: Snowflake RSA warehouse publisher
│   ├── core/                        # Core enterprise engine
│   │   ├── schema_contracts.py      # Strict JSON schema contracts & header validator
│   │   ├── cortex_client.py         # Snowflake Cortex AI client & budget guardrails
│   │   ├── checkpoint_manager.py    # Auto-save & pipeline snapshot versioning
│   │   ├── s3_connector.py          # AWS S3 multi-tenant bucket connector
│   │   └── state_manager.py         # Swarm event bus & status persistence
│   ├── orchestrator/                # Pipeline orchestrator & incremental engine
│   │   └── pipeline_orchestrator.py # Bulk & incremental reconciliation orchestrator
│   └── dashboard_v2/                # Real-time Web Studio (Port 8081)
│       ├── server.py                # REST API, KPI preview & Excel streaming engine
│       ├── app.js                   # Interactive client application
│       ├── index.html               # 6-Stage Web Studio layout & modal dialogs
│       └── style.css                # Dark enterprise UI styles & interactive effects
├── verify_snowflake_parity.py       # Automated 100% apples-to-apples parity certification
├── requirements.txt                 # Python dependencies
└── README.md                        # Platform documentation
```

---

## 🚀 Quickstart & Setup

### 1. Prerequisites
- Python 3.9+
- Git

### 2. Installation
```bash
git clone https://github.com/supplycopia-nirmal/ATB-DBT.git
cd ATB-DBT
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Environment Variables
Configure your `.env` file in the project root:
```bash
# Snowflake Enterprise Credentials
SNOWFLAKE_USER=SA_MS_Lambda
SNOWFLAKE_ACCOUNT=NBNAETU-CS50192
SNOWFLAKE_ROLE=DB_SUPPLYCOPIA_TRANSFORM_ETL
SNOWFLAKE_WAREHOUSE=DATAMANAGEMENT_WH
SNOWFLAKE_DATABASE=SUPPLYCOPIA_TRANSFORM
SNOWFLAKE_SCHEMA=SCRETAIL_IMPORT
PRIVATE_KEY_PATH=/absolute/path/to/lambda_migration._rsa_key.p8

# AWS S3 Ingestion Credentials
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-2
S3_DEFAULT_BUCKET=supplycopia-dbt-ingestion
```

### 4. Running the Autonomous Studio
Launch the Ask The Bee Web Studio on port 8081:
```bash
python3 -m autonomous_dbt_app.dashboard_v2.server
```
Open your browser to: **`http://localhost:8081`**

### 5. Running Snowflake Parity Verification
Verify 100% apples-to-apples parity against baseline Snowflake tables:
```bash
python3 verify_snowflake_parity.py
```

---

## 🔒 Security & Data Governance
- Sensitive credentials (`.env`, `*.p8`, `*.pem`, `*.key`) are strictly gitignored.
- Raw clinical and spend datasets (`*.csv`, `*.parquet`, DuckDB binaries) remain outside version control.
- Automated Row-Level Security (RLS) policies isolate client data in shared Snowflake tables.

---

© 2026 SupplyCopia Inc. All rights reserved.
