# ==============================================================================
# SupplyCopia / Ask The Bee - Autonomous Multi-Tenant DBT Studio
# Production Dockerfile
# ==============================================================================
FROM python:3.10-slim

# Optimize Python execution in containers
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DASHBOARD_PORT=8081

# Install system dependencies needed for compiling C extensions & dbt
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache layer: install dependencies first
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app/

# Create runtime state directories
RUN mkdir -p /app/output /app/Data /app/agent_memory

# Expose Studio Port
EXPOSE 8081

# Container Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8081/health || exit 1

# Launch Studio Daemon
CMD ["python3", "-m", "autonomous_dbt_app.dashboard_v2.server"]
