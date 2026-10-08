#!/bin/bash
echo "🚀 Exporting latest summary data from DuckDB..."
source ../.venv/bin/activate
python3 export_metrics.py
echo "🌟 Starting Flask Dashboard Server..."
python3 server.py
