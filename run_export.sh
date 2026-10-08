#!/bin/bash
while true; do
  source .venv/bin/activate && python3 export_to_snowflake.py
  if [ $? -eq 0 ]; then
    break
  fi
  echo "Database locked, waiting 10s..."
  sleep 10
done
