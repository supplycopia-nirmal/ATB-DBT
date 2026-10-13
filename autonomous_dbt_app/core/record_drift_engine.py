"""
Record & Row Drift Detection Engine
Vectorized, in-process DuckDB engine that analyzes record-level drift, entity key churn
(added, modified, dropped records), field-level modification frequencies, null-rate shifts,
and statistical distribution drift between baseline and incoming client feeds.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import duckdb

from autonomous_dbt_app.config import OUTPUT_DIR, ROOT_DIR as PROJECT_ROOT, BASE_DATA_DIR
from autonomous_dbt_app.core.client_file_tracker import ENTITY_NATURAL_KEYS


class RecordDriftEngine:
    """
    Sentinel Aegis Record Observability:
    Detects record-level additions, modifications, drop-offs, and numerical/distribution drift.
    """
    def __init__(self, duckdb_path: Optional[Path] = None):
        self.db_path = duckdb_path or (OUTPUT_DIR / "autonomous_pipeline.duckdb")

    def analyze_record_drift(
        self,
        baseline_file: Path,
        current_file: Path,
        entity_type: str = "item_master",
        key_columns: Optional[List[str]] = None,
        sample_diff_limit: int = 15
    ) -> Dict[str, Any]:
        """
        Runs comprehensive record and row drift analysis comparing baseline vs current CSV.
        """
        baseline_file = Path(baseline_file)
        current_file = Path(current_file)

        if not baseline_file.exists() or not current_file.exists():
            raise FileNotFoundError(f"Missing file: {baseline_file} or {current_file}")

        t0 = time.time()
        con = duckdb.connect()

        # 1. Load datasets into DuckDB
        con.execute(f"CREATE TEMP TABLE df_base AS SELECT * FROM read_csv_auto('{baseline_file}', ignore_errors=true);")
        con.execute(f"CREATE TEMP TABLE df_curr AS SELECT * FROM read_csv_auto('{current_file}', ignore_errors=true);")

        base_count = con.execute("SELECT count(*) FROM df_base;").fetchone()[0]
        curr_count = con.execute("SELECT count(*) FROM df_curr;").fetchone()[0]

        # 2. Volume & Cardinality Drift
        row_delta = curr_count - base_count
        row_delta_pct = round((row_delta / max(1, base_count)) * 100.0, 2)
        
        if abs(row_delta_pct) < 5.0:
            volume_status = "STABLE"
            volume_severity = "LOW"
        elif abs(row_delta_pct) < 20.0:
            volume_status = "MODERATE_DRIFT"
            volume_severity = "MEDIUM"
        else:
            volume_status = "SEVERE_DRIFT"
            volume_severity = "HIGH"

        # 3. Identify Key Columns & Column Alignment
        base_cols_info = con.execute("PRAGMA table_info('df_base');").fetchall()
        curr_cols_info = con.execute("PRAGMA table_info('df_curr');").fetchall()

        base_cols = [c[1] for c in base_cols_info]
        curr_cols = [c[1] for c in curr_cols_info]
        common_cols = [c for c in base_cols if c in curr_cols]

        keys = key_columns or ENTITY_NATURAL_KEYS.get(entity_type, ["item_id"])
        valid_keys = [k for k in keys if k in common_cols]
        if not valid_keys:
            valid_keys = common_cols[:1] if common_cols else [base_cols[0]]

        non_key_cols = [c for c in common_cols if c not in valid_keys]

        # 4. Record Churn Matrix (Added, Modified, Dropped, Unchanged)
        join_clause = " AND ".join([f"b.{k} = c.{k}" for k in valid_keys])
        b_key_null = f"b.{valid_keys[0]} IS NULL"
        c_key_null = f"c.{valid_keys[0]} IS NULL"

        added_cnt = con.execute(f"""
            SELECT count(*) 
            FROM df_curr c 
            LEFT JOIN df_base b ON {join_clause}
            WHERE {b_key_null};
        """).fetchone()[0]

        dropped_cnt = con.execute(f"""
            SELECT count(*) 
            FROM df_base b 
            LEFT JOIN df_curr c ON {join_clause}
            WHERE {c_key_null};
        """).fetchone()[0]

        # Check modifications among common keys
        if non_key_cols:
            diff_conditions = " OR ".join([
                f"(b.{col} IS DISTINCT FROM c.{col})" for col in non_key_cols
            ])
            modified_cnt = con.execute(f"""
                SELECT count(*) 
                FROM df_base b 
                INNER JOIN df_curr c ON {join_clause}
                WHERE {diff_conditions};
            """).fetchone()[0]
        else:
            modified_cnt = 0

        common_keys_cnt = con.execute(f"""
            SELECT count(*) 
            FROM df_base b 
            INNER JOIN df_curr c ON {join_clause};
        """).fetchone()[0]

        unchanged_cnt = max(0, common_keys_cnt - modified_cnt)

        # 5. Field-Level Modification Frequency
        field_shift_rates = []
        if non_key_cols and modified_cnt > 0:
            for col in non_key_cols:
                col_mod_cnt = con.execute(f"""
                    SELECT count(*) 
                    FROM df_base b 
                    INNER JOIN df_curr c ON {join_clause}
                    WHERE b.{col} IS DISTINCT FROM c.{col};
                """).fetchone()[0]
                
                if col_mod_cnt > 0:
                    pct = round((col_mod_cnt / max(1, common_keys_cnt)) * 100.0, 1)
                    field_shift_rates.append({
                        "field_name": col,
                        "records_modified": col_mod_cnt,
                        "modification_pct": pct,
                        "severity": "HIGH" if pct > 20.0 else ("MEDIUM" if pct > 5.0 else "LOW")
                    })
            field_shift_rates.sort(key=lambda x: x["records_modified"], reverse=True)

        # 6. Null-Rate Drift
        null_rate_shifts = []
        for col in common_cols:
            b_nulls = con.execute(f"SELECT count(*) FROM df_base WHERE {col} IS NULL;").fetchone()[0]
            c_nulls = con.execute(f"SELECT count(*) FROM df_curr WHERE {col} IS NULL;").fetchone()[0]
            
            b_null_pct = round((b_nulls / max(1, base_count)) * 100.0, 2)
            c_null_pct = round((c_nulls / max(1, curr_count)) * 100.0, 2)
            delta_null = round(c_null_pct - b_null_pct, 2)

            if abs(delta_null) >= 1.0 or c_null_pct > 10.0:
                null_rate_shifts.append({
                    "column": col,
                    "baseline_null_pct": b_null_pct,
                    "current_null_pct": c_null_pct,
                    "null_rate_delta": delta_null,
                    "status": "SEVERE" if delta_null > 10.0 else ("WARNING" if delta_null > 3.0 else "STABLE")
                })
        null_rate_shifts.sort(key=lambda x: abs(x["null_rate_delta"]), reverse=True)

        # 7. Numerical Distribution Drift (Numeric stats)
        distribution_shifts = []
        numeric_types = ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC']
        for col_info in curr_cols_info:
            col_name = col_info[1]
            col_type = col_info[2].upper()
            if any(t in col_type for t in numeric_types) and col_name in common_cols:
                try:
                    b_stats = con.execute(f"""
                        SELECT 
                            round(coalesce(avg(cast({col_name} as double)), 0), 2),
                            round(coalesce(median(cast({col_name} as double)), 0), 2),
                            round(coalesce(min(cast({col_name} as double)), 0), 2),
                            round(coalesce(max(cast({col_name} as double)), 0), 2)
                        FROM df_base WHERE {col_name} IS NOT NULL;
                    """).fetchone()

                    c_stats = con.execute(f"""
                        SELECT 
                            round(coalesce(avg(cast({col_name} as double)), 0), 2),
                            round(coalesce(median(cast({col_name} as double)), 0), 2),
                            round(coalesce(min(cast({col_name} as double)), 0), 2),
                            round(coalesce(max(cast({col_name} as double)), 0), 2)
                        FROM df_curr WHERE {col_name} IS NOT NULL;
                    """).fetchone()

                    if b_stats and c_stats:
                        b_avg, b_med, b_min, b_max = b_stats
                        c_avg, c_med, c_min, c_max = c_stats
                        avg_drift_pct = round(((c_avg - b_avg) / max(0.001, abs(b_avg))) * 100.0, 1) if b_avg != 0 else 0.0

                        distribution_shifts.append({
                            "column": col_name,
                            "baseline_mean": b_avg,
                            "current_mean": c_avg,
                            "baseline_median": b_med,
                            "current_median": c_med,
                            "baseline_max": b_max,
                            "current_max": c_max,
                            "mean_drift_pct": avg_drift_pct,
                            "is_spike": abs(avg_drift_pct) > 50.0 or (c_max > (b_max * 5.0) if b_max > 0 else False)
                        })
                except Exception:
                    pass

        # 8. Representative Row Diffs (Top changed records)
        sample_diffs = []
        if non_key_cols and modified_cnt > 0:
            diff_cases = ", ".join([f"b.{c} as base_{c}, c.{c} as curr_{c}" for c in non_key_cols[:5]])
            key_select = ", ".join([f"c.{k}" for k in valid_keys])
            sample_query = f"""
                SELECT {key_select}, {diff_cases}
                FROM df_base b 
                INNER JOIN df_curr c ON {join_clause}
                WHERE {diff_conditions}
                LIMIT {sample_diff_limit};
            """
            rows = con.execute(sample_query).fetchdf()
            for _, r in rows.iterrows():
                key_val = str(r[valid_keys[0]])
                changed_fields = []
                for col in non_key_cols[:5]:
                    old_v = r[f"base_{col}"]
                    new_v = r[f"curr_{col}"]
                    if str(old_v) != str(new_v):
                        changed_fields.append({
                            "field": col,
                            "baseline_val": str(old_v),
                            "current_val": str(new_v)
                        })
                sample_diffs.append({
                    "record_key": key_val,
                    "changes": changed_fields
                })

        con.close()
        elapsed = round(time.time() - t0, 3)

        return {
            "status": "SUCCESS",
            "entity_type": entity_type,
            "baseline_file": baseline_file.name,
            "current_file": current_file.name,
            "key_columns": valid_keys,
            "execution_time_sec": elapsed,
            "baseline_row_count": base_count,
            "latest_row_count": curr_count,
            "volume_delta": row_delta,
            "volume_pct_change": row_delta_pct,
            "volume_drift": {
                "baseline_rows": base_count,
                "current_rows": curr_count,
                "row_delta": row_delta,
                "row_delta_pct": row_delta_pct,
                "status": volume_status,
                "severity": volume_severity
            },
            "record_churn": {
                "added": added_cnt,
                "added_records": added_cnt,
                "added_pct": round((added_cnt / max(1, curr_count)) * 100.0, 1),
                "modified": modified_cnt,
                "modified_records": modified_cnt,
                "modified_pct": round((modified_cnt / max(1, curr_count)) * 100.0, 1),
                "dropped": dropped_cnt,
                "dropped_records": dropped_cnt,
                "dropped_pct": round((dropped_cnt / max(1, base_count)) * 100.0, 1),
                "unchanged": unchanged_cnt,
                "unchanged_records": unchanged_cnt,
                "unchanged_pct": round((unchanged_cnt / max(1, curr_count)) * 100.0, 1)
            },
            "field_modifications": [
                {"field": f["field_name"], "modified_records": f["records_modified"], "modification_pct": f["modification_pct"]}
                for f in field_shift_rates[:8]
            ],
            "field_modification_frequencies": field_shift_rates[:8],
            "null_rate_drift": [
                {
                    "column": n["column"],
                    "baseline_null_pct": n["baseline_null_pct"],
                    "incoming_null_pct": n["current_null_pct"],
                    "delta_pct": n["null_rate_delta"],
                    "spike_alert": abs(n["null_rate_delta"]) > 10.0
                }
                for n in null_rate_shifts[:8]
            ],
            "null_rate_shifts": null_rate_shifts[:8],
            "numerical_shifts": [
                {
                    "column": d["column"],
                    "baseline_mean": d["baseline_mean"],
                    "incoming_mean": d["current_mean"],
                    "mean_shift_pct": d["mean_drift_pct"],
                    "max_shift_pct": round(((d["current_max"] - d["baseline_max"]) / max(1.0, d["baseline_max"])) * 100.0, 1),
                    "shift_alert": d["is_spike"]
                }
                for d in distribution_shifts
            ],
            "distribution_shifts": distribution_shifts,
            "sample_diffs": sample_diffs
        }

    def compare_datasets(
        self,
        baseline_path: Path,
        incoming_path: Path,
        entity_type: str = "item_master",
        primary_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Convenience alias for analyze_record_drift."""
        return self.analyze_record_drift(
            baseline_file=baseline_path,
            current_file=incoming_path,
            entity_type=entity_type,
            key_columns=primary_keys
        )
