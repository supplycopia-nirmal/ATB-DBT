"""
User Activity & Audit Logging Engine
High-performance, non-blocking activity tracker using SQLite WAL mode and async batch worker.
Enforces strict 90-day (3-month maximum) automated data retention and space reclamation.
Designed to seamlessly track user interactions under both Basic Auth and Enterprise SAML/SSO.
"""

import os
import sys
import json
import time
import uuid
import queue
import sqlite3
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path
import io

from autonomous_dbt_app.config import ACTIVITY_LOG_DB_PATH, ACTIVITY_LOG_RETENTION_DAYS


class UserActivityLogger:
    """
    Enterprise Activity & Compliance Audit Logger.
    Buffers events in memory and writes asynchronously to a compact SQLite database in WAL mode.
    Maintains bounded disk footprint via automated 90-day retention purging.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(UserActivityLogger, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path: Optional[Path] = None, retention_days: int = ACTIVITY_LOG_RETENTION_DAYS):
        if self._initialized:
            return

        self.db_path = Path(db_path or ACTIVITY_LOG_DB_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_days = min(90, max(1, retention_days))  # Maximum 3 months (90 days)

        self._queue = queue.Queue(maxsize=10000)
        self._shutdown_event = threading.Event()
        self._flush_requested = threading.Event()

        self._init_sqlite_schema()

        # Start asynchronous writer thread
        self._writer_thread = threading.Thread(target=self._worker_loop, daemon=True, name="ActivityLogWriter")
        self._writer_thread.start()

        # Run startup purge and schedule periodic purge
        self.purge_older_than(self.retention_days)

        self._initialized = True

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL;")
        return conn

    def _init_sqlite_schema(self):
        conn = self._get_connection()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_activity_logs (
                    log_id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_email TEXT,
                    user_role TEXT DEFAULT 'Operator',
                    action TEXT NOT NULL,
                    resource TEXT,
                    ip_address TEXT DEFAULT '127.0.0.1',
                    user_agent TEXT,
                    status TEXT DEFAULT 'SUCCESS',
                    duration_ms REAL DEFAULT 0.0,
                    details TEXT
                );
            """)
            # Fast index lookups for compliance queries and TTL purge
            conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON user_activity_logs(timestamp);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_user ON user_activity_logs(user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_action ON user_activity_logs(action);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_activity_status ON user_activity_logs(status);")
            conn.commit()
        finally:
            conn.close()

    def log(
        self,
        action: str,
        user_id: str = "admin",
        user_email: Optional[str] = None,
        user_role: str = "Operator",
        resource: Optional[str] = None,
        ip_address: str = "127.0.0.1",
        user_agent: Optional[str] = None,
        status: str = "SUCCESS",
        duration_ms: float = 0.0,
        details: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Enqueues an activity record non-blockingly (<0.05ms) so HTTP requests stay zero-latency.
        """
        log_id = f"LOG_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        entry = {
            "log_id": log_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_id": user_id or "admin",
            "user_email": user_email or ("admin@supplycopia.com" if user_id == "admin" else ""),
            "user_role": user_role or "Operator",
            "action": action,
            "resource": resource or "",
            "ip_address": ip_address or "127.0.0.1",
            "user_agent": (user_agent or "")[:250],
            "status": status.upper(),
            "duration_ms": round(float(duration_ms), 2),
            "details": json.dumps(details or {})
        }
        try:
            self._queue.put_nowait(entry)
        except queue.Full:
            # Under extreme load, drop or direct fallback to avoid choking the HTTP server
            pass
        return log_id

    def _worker_loop(self):
        """Background daemon pulling queued logs and executing batch SQLite inserts."""
        batch = []
        last_flush = time.time()
        last_purge = time.time()

        while not self._shutdown_event.is_set():
            try:
                # Wait up to 100ms for items
                item = self._queue.get(timeout=0.1)
                batch.append(item)
                # Pull any available items
                while len(batch) < 50:
                    try:
                        batch.append(self._queue.get_nowait())
                    except queue.Empty:
                        break
            except queue.Empty:
                pass

            now = time.time()
            if batch and (len(batch) >= 20 or (now - last_flush) >= 0.25 or self._flush_requested.is_set()):
                self._flush_batch(batch)
                batch = []
                last_flush = now
                self._flush_requested.clear()

            # Periodic purge every 6 hours
            if (now - last_purge) >= 21600:
                self.purge_older_than(self.retention_days)
                last_purge = now

        # Final flush on exit
        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch: List[Dict[str, Any]]):
        if not batch:
            return
        conn = self._get_connection()
        try:
            conn.executemany("""
                INSERT OR IGNORE INTO user_activity_logs (
                    log_id, timestamp, user_id, user_email, user_role,
                    action, resource, ip_address, user_agent, status,
                    duration_ms, details
                ) VALUES (
                    :log_id, :timestamp, :user_id, :user_email, :user_role,
                    :action, :resource, :ip_address, :user_agent, :status,
                    :duration_ms, :details
                );
            """, batch)
            conn.commit()
        except Exception as e:
            print(f"[ActivityLogger] Warning flushing batch: {e}", file=sys.stderr)
        finally:
            conn.close()

    def _get_db(self) -> sqlite3.Connection:
        """Alias for _get_connection for convenience."""
        return self._get_connection()

    def flush(self):
        """Immediately flushes any items currently waiting in the queue to disk."""
        self._flush_requested.set()
        time.sleep(0.12)
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self._flush_batch(batch)

    def shutdown(self, timeout: float = 3.0):
        """Signals shutdown, flushes pending logs, and joins background worker thread."""
        self._shutdown_event.set()
        self.flush()
        if hasattr(self, '_writer_thread') and self._writer_thread.is_alive():
            self._writer_thread.join(timeout=timeout)
        with UserActivityLogger._lock:
            UserActivityLogger._instance = None

    def purge_older_than(self, days: int = 90) -> int:
        """
        Purges records older than specified retention duration (capped at 90 days).
        Reclaims disk space using incremental vacuum to keep storage compact.
        """
        effective_days = min(90, max(1, days))
        cutoff = (datetime.utcnow() - timedelta(days=effective_days)).isoformat() + "Z"

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_activity_logs WHERE timestamp < ?;", (cutoff,))
            deleted = cur.rowcount
            conn.commit()
            if deleted > 0:
                conn.execute("PRAGMA incremental_vacuum(50);")
                conn.execute("PRAGMA optimize;")
            return {
                "status": "SUCCESS",
                "purged_count": deleted,
                "retention_days": effective_days,
                "message": f"Purged {deleted} records older than {effective_days} days."
            }
        except Exception as e:
            print(f"[ActivityLogger] Purge notice: {e}", file=sys.stderr)
            return {
                "status": "ERROR",
                "purged_count": 0,
                "message": str(e)
            }
        finally:
            conn.close()

    def query_logs(
        self,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        status: Optional[str] = None,
        search_query: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Queries stored logs with filtering, search, and pagination."""
        # Ensure any pending in-memory batch is flushed before querying
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self._flush_batch(batch)

        conn = self._get_connection()
        try:
            clauses = ["1=1"]
            params = []

            if user_id:
                clauses.append("user_id = ?")
                params.append(user_id)
            if action:
                clauses.append("action = ?")
                params.append(action)
            if status:
                clauses.append("status = ?")
                params.append(status.upper())
            if search_query:
                clauses.append("(resource LIKE ? OR action LIKE ? OR user_id LIKE ? OR details LIKE ?)")
                like_term = f"%{search_query}%"
                params.extend([like_term, like_term, like_term, like_term])

            where_clause = " AND ".join(clauses)

            # Count total matching
            count_sql = f"SELECT count(*) FROM user_activity_logs WHERE {where_clause};"
            total = conn.execute(count_sql, params).fetchone()[0]

            # Query page
            data_sql = f"""
                SELECT log_id, timestamp, user_id, user_email, user_role,
                       action, resource, ip_address, user_agent, status,
                       duration_ms, details
                FROM user_activity_logs
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?;
            """
            rows = conn.execute(data_sql, params + [limit, offset]).fetchall()

            logs = []
            for r in rows:
                row_dict = dict(r)
                try:
                    row_dict["details_parsed"] = json.loads(row_dict["details"] or "{}")
                except Exception:
                    row_dict["details_parsed"] = {}
                logs.append(row_dict)

            return {
                "status": "SUCCESS",
                "total": total,
                "limit": limit,
                "offset": offset,
                "retention_days": self.retention_days,
                "db_size_kb": round(self.db_path.stat().st_size / 1024, 1) if self.db_path.exists() else 0,
                "logs": logs
            }
        finally:
            conn.close()

    def get_activity_metrics(self) -> Dict[str, Any]:
        """Provides aggregate metrics on activity logs for executive review."""
        conn = self._get_connection()
        try:
            total_logs = conn.execute("SELECT count(*) FROM user_activity_logs;").fetchone()[0]
            unique_users = conn.execute("SELECT count(DISTINCT user_id) FROM user_activity_logs;").fetchone()[0]
            recent_24h = conn.execute(
                "SELECT count(*) FROM user_activity_logs WHERE timestamp >= datetime('now', '-1 day');"
            ).fetchone()[0]

            action_counts = conn.execute("""
                SELECT action, count(*) as count 
                FROM user_activity_logs 
                GROUP BY action 
                ORDER BY count DESC 
                LIMIT 5;
            """).fetchall()

            status_counts = conn.execute("""
                SELECT status, count(*) as count 
                FROM user_activity_logs 
                GROUP BY status;
            """).fetchall()

            return {
                "total_logs": total_logs,
                "unique_users": unique_users,
                "active_users": unique_users,
                "events_today": recent_24h,
                "recent_24h": recent_24h,
                "retention_days": self.retention_days,
                "db_size_kb": round(self.db_path.stat().st_size / 1024, 1) if self.db_path.exists() else 0,
                "db_size_human": f"{round(self.db_path.stat().st_size / 1024, 1)} KB" if self.db_path.exists() else "0 KB",
                "top_actions": [{r[0]: r[1]} for r in action_counts],
                "statuses": {r[0]: r[1] for r in status_counts}
            }
        finally:
            conn.close()

    def export_csv_bytes(self, limit: int = 1000) -> bytes:
        """Exports activity logs as CSV bytes for compliance audits."""
        data = self.query_logs(limit=limit, offset=0)
        logs = data.get("logs", [])
        if not logs:
            return b"log_id,timestamp,user_id,action,status,resource,duration_ms\n"

        import pandas as pd
        df = pd.DataFrame([{
            "Log ID": l["log_id"],
            "Timestamp": l["timestamp"],
            "User": l["user_id"],
            "Email": l.get("user_email", ""),
            "Role": l.get("user_role", ""),
            "Action": l["action"],
            "Status": l["status"],
            "Resource": l.get("resource", ""),
            "Duration (ms)": l.get("duration_ms", 0),
            "IP Address": l.get("ip_address", "")
        } for l in logs])

        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        return buf.getvalue()
