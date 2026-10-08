# 🐝 Ask The Bee (ATB-DBT): Autonomous Multi-Tenant DBT Studio

![SupplyCopia Banner](autonomous_dbt_app/dashboard_v2/static/logo.png)

An enterprise-grade, agentic AI platform engineered for **SupplyCopia** to autonomously profile, reconcile, topology-map, and generate production **dbt Core (DuckDB & Snowflake)** pipelines from heterogeneous client EHR / ERP supply chain datasets.

---

## 🌟 Key Capabilities

1. **Autonomous Bee Swarm Architecture**:
   - **Scout Bee**: Cloud ingestion (AWS S3) and deep schema profiling.
   - **Architect Bee**: Autonomous relationship inference, schema drift detection, and join topology synthesis.
   - **CodeCraft Bee**: Jinja2 & dbt SQL model generation with multi-tenant client metadata tagging.
   - **Validation Bee**: Automated test assertions, SLA anomaly detection, and Golden Parity certification.
2. **Multi-Tenancy Out of the Box**:
   - Standardized `sc_multi_tenant_*` warehouse schema housing multiple hospital networks, GPOs, and ambulatory systems.
   - Isolated client configurations under `/clients/<client_name>/`.
3. **Execution Modes**:
   - **Standard Mode**: Interactive step-by-step review across all stages.
   - **User Review Mode**: Pauses at critical join and schema-change decisions for explicit data engineer sign-off.
   - **⚡ Turbo Autonomy Mode**: End-to-end zero-click execution from raw files directly to certified marts.
4. **Resilience & Chaos Simulation**:
   - Built-in Chaos Simulator for testing schema perturbations across **Epic Systems**, **Oracle Cerner**, and **MEDITECH**.
   - Automated 8-point SLA and regression test suite matrix.
5. **Enterprise Audit & Underlying Data Export**:
   - Multi-tab Excel export engine streaming tens of thousands of underlying exception records, item crosswalks, price spikes, and LLM telemetry.

---

## 🏗️ Repository Architecture

```text
ATB-DBT/
├── clients/                         # Multi-tenant client workspace directories
│   ├── uc_health/                   # UC Health System configuration & seeds
│   │   └── client_config.json
│   └── template_client/             # Scaffolding template for onboarding new clients
│       └── client_config.json
├── autonomous_dbt_app/              # Core Agentic Platform Engine
│   ├── agents/                      # Specialized Autonomous Bee Agents
│   │   ├── scout_bee.py             # Schema profiling & S3 connector
│   │   ├── architect_bee.py         # Relationship inference & topology mapping
│   │   ├── codecraft_bee.py         # dbt model synthesis & SQL generation
│   │   ├── enterprise_suite_agent.py# SLA detection, drift matrix, telemetry
│   │   └── validation_bee.py        # Parity engine & quality certification
│   ├── core/                        # Persistence & checkpoints
│   │   ├── checkpoint_manager.py    # Auto-save & pipeline version control
│   │   └── state_manager.py         # Reactive swarm event bus
│   ├── orchestrator/                # Autonomous workflow pipeline orchestrators
│   └── dashboard_v2/                # Real-time Web Studio (Port 8081)
│       ├── server.py                # REST API & underlying data export engine
│       └── static/                  # Interactive visual studio (HTML5/CSS3/Vanilla JS)
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
Create a local `.env` file in the root directory (never commit this file):
```bash
# OpenAI LLM Key (Optional - Fallbacks to deterministic fuzzy matching if omitted)
OPENAI_API_KEY=your_openai_key
OPENAI_MODEL=gpt-5.6-luna

# Snowflake Enterprise Credentials
SNOWFLAKE_USER=SA_MS_Lambda
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_ROLE=DB_SUPPLYCOPIA_TRANSFORM_ETL
SNOWFLAKE_WAREHOUSE=DATAMANAGEMENT_WH
SNOWFLAKE_DATABASE=SUPPLYCOPIA_TRANSFORM
SNOWFLAKE_SCHEMA=SCRETAIL_IMPORT
```

### 4. Running the Autonomous Studio
Launch the Ask The Bee Web Studio on port 8081:
```bash
python3 -m autonomous_dbt_app.dashboard_v2.server
```
Open your browser to: **`http://localhost:8081`**

---

## 🏢 Onboarding a New Client

To onboard a new health system (e.g., `mercy_health`):

1. **Create the client profile**:
   ```bash
   cp -r clients/template_client clients/mercy_health
   ```
2. **Update `clients/mercy_health/client_config.json`**:
   - Set `client_id`, `client_name`, and `client_type`.
   - Specify the incoming raw file names or S3 bucket path.
3. **Run Ingestion in the Studio**:
   - Provide the S3 folder or local directory in Stage 1.
   - Scout Bee will automatically profile columns and recommend mappings.
   - Run in **⚡ Turbo Mode** for hands-off autonomous compilation and Snowflake delivery!

---

## 🔒 Security & Data Governance
- Sensitive credentials (`.env`, `*.p8`, `*.pem`, `*.key`) are strictly gitignored.
- Raw clinical and spend files (`*.csv`, `*.parquet`, DuckDB binary lakes) are kept outside source control.
- All marts feature automated SupplyCopia multi-tenant identifiers (`tenant_client_id`, `tenant_client_name`, `tenant_client_type`) ensuring cross-tenant isolation in shared Snowflake tables.

---

© 2026 SupplyCopia Inc. All rights reserved.
