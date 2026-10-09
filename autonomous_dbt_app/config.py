import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

# Snowflake Configuration
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT", "")
SNOWFLAKE_URL = os.getenv("SNOWFLAKE_URL", "https://NBNAETU-CS50192.snowflakecomputing.com")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "SA_MS_Lambda")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "NBNAETU-CS50192")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "DB_SUPPLYCOPIA_TRANSFORM_ETL")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DATAMANAGEMENT_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "SUPPLYCOPIA_TRANSFORM")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "SCRETAIL_IMPORT")
PRIVATE_KEY_PATH = os.getenv("PRIVATE_KEY_PATH", str(ROOT_DIR / "lambda_migration._rsa_key.p8"))
PRIVATE_KEY_PASSPHRASE = os.getenv("PRIVATE_KEY_PASSPHRASE", "")

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "")
S3_DEFAULT_BUCKET = os.getenv("S3_DEFAULT_BUCKET", "supplycopia-dbt-ingestion")

# Multi-Tenancy Agnostic Settings
DEFAULT_CLIENT_NAME = os.getenv("DEFAULT_CLIENT_NAME", "UC Health")
DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "CL_UCH_001")
DEFAULT_CLIENT_TYPE = os.getenv("DEFAULT_CLIENT_TYPE", "HealthCare System")

# OpenAI / Fallback LLM Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# Application Settings
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8081"))
BASE_DATA_DIR = ROOT_DIR / "Data"
OUTPUT_DIR = ROOT_DIR / "output"
CHROMA_PERSIST_DIR = ROOT_DIR / "agent_memory" / "chroma_db"
STATE_FILE = ROOT_DIR / "agent_memory" / "pipeline_state.json"

# SupplyCopia Canonical Required vs Optional Entities
REQUIRED_ENTITIES = ["consumption", "purchase_order"]
OPTIONAL_ENTITIES = ["item_master", "contracts", "invoice", "inventory", "vendor_alias", "facility_mapping"]
