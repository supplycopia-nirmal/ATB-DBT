import unittest
import tempfile
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

from autonomous_dbt_app.core.user_activity_logger import UserActivityLogger
from autonomous_dbt_app.core.confluence_connector import ConfluenceConnector
from autonomous_dbt_app.core.jira_connector import JiraConnector
from autonomous_dbt_app.core.self_healing_engine import SelfHealingEngine


class TestUserActivityLogger(unittest.TestCase):
    def setUp(self):
        # Reset singleton to ensure test isolation
        with UserActivityLogger._lock:
            UserActivityLogger._instance = None
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_activity_logs.db"
        self.logger = UserActivityLogger(db_path=self.db_path, retention_days=90)

    def tearDown(self):
        self.logger.shutdown()
        self.tmp_dir.cleanup()

    def test_log_and_query(self):
        self.logger.log(
            action="RUN_PIPELINE",
            user_id="nrayan@supplycopia.com",
            details={"notes": "Triggered full autonomous DBT pipeline"},
            status="SUCCESS",
            ip_address="10.12.0.109"
        )
        self.logger.flush()

        res = self.logger.query_logs(user_id="nrayan@supplycopia.com")
        self.assertEqual(res["total"], 1)
        self.assertEqual(len(res["logs"]), 1)
        log = res["logs"][0]
        self.assertEqual(log["user_id"], "nrayan@supplycopia.com")
        self.assertEqual(log["action"], "RUN_PIPELINE")
        self.assertEqual(log["status"], "SUCCESS")
        self.assertEqual(log["ip_address"], "10.12.0.109")

    def test_search_and_filters(self):
        self.logger.log(action="LOGIN", user_id="admin", details={"protocol": "SAML SSO"}, status="SUCCESS")
        self.logger.log(action="UPDATE_JOIN", user_id="operator", details={"msg": "Changed join key"}, status="WARNING")
        self.logger.log(action="FAILED_AUTH", user_id="admin", details={"err": "Invalid password attempt"}, status="ERROR")
        self.logger.flush()

        # Filter by status
        err_res = self.logger.query_logs(status="ERROR")
        self.assertEqual(err_res["total"], 1)
        self.assertEqual(err_res["logs"][0]["action"], "FAILED_AUTH")

        # Filter by search query
        search_res = self.logger.query_logs(search_query="SAML")
        self.assertEqual(search_res["total"], 1)
        self.assertEqual(search_res["logs"][0]["user_id"], "admin")

    def test_retention_purge_strict_90_days(self):
        # Insert an old record directly with a timestamp 100 days in the past
        old_time = (datetime.utcnow() - timedelta(days=100)).isoformat() + "Z"
        recent_time = (datetime.utcnow() - timedelta(days=10)).isoformat() + "Z"

        with self.logger._get_db() as conn:
            conn.execute(
                "INSERT INTO user_activity_logs (log_id, timestamp, user_id, action, status, ip_address) VALUES (?, ?, ?, ?, ?, ?)",
                ("LOG_OLD", old_time, "old_user", "OLD_ACTION", "SUCCESS", "127.0.0.1")
            )
            conn.execute(
                "INSERT INTO user_activity_logs (log_id, timestamp, user_id, action, status, ip_address) VALUES (?, ?, ?, ?, ?, ?)",
                ("LOG_RECENT", recent_time, "current_user", "RECENT_ACTION", "SUCCESS", "127.0.0.1")
            )
            conn.commit()

        # Purge older than 90 days
        purge_res = self.logger.purge_older_than(days=90)
        self.assertEqual(purge_res["status"], "SUCCESS")
        self.assertEqual(purge_res["purged_count"], 1)

        # Confirm only recent remains
        all_logs = self.logger.query_logs()["logs"]
        self.assertEqual(len(all_logs), 1)
        self.assertEqual(all_logs[0]["user_id"], "current_user")

    def test_metrics_and_csv_export(self):
        self.logger.log(action="EXPORT_MART", user_id="user_a", details={"target": "Snowflake"}, status="SUCCESS")
        self.logger.log(action="APPLY_PATCH", user_id="user_b", details={"engine": "Self-healing"}, status="SUCCESS")
        self.logger.flush()

        metrics = self.logger.get_activity_metrics()
        self.assertGreaterEqual(metrics["total_logs"], 2)
        self.assertGreaterEqual(metrics["active_users"], 2)
        self.assertIn("KB", metrics["db_size_human"])

        csv_bytes = self.logger.export_csv_bytes()
        csv_str = csv_bytes.decode("utf-8")
        self.assertIn("Log ID,Timestamp,User", csv_str)
        self.assertIn("user_a", csv_str)
        self.assertIn("EXPORT_MART", csv_str)


class TestConfluenceConnector(unittest.TestCase):
    def setUp(self):
        self.connector = ConfluenceConnector(space_key="ATBDBT")

    def test_generate_technical_documentation_chapters(self):
        docs = self.connector.generate_technical_documentation()
        self.assertEqual(len(docs), 6)

        titles = [c["title"] for c in docs]
        self.assertIn("SupplyCopia ATB Studio: System Architecture & Agent Swarm Blueprint", titles)
        self.assertIn("SupplyCopia ATB Studio: 7-Stage Autonomous Workflow & User Guide", titles)
        self.assertIn("SupplyCopia ATB Studio: Enterprise Operations, SSO & Compliance Audit", titles)

        # Check XHTML storage format formatting
        first_chapter = docs[0]
        self.assertIn("ac:structured-macro", first_chapter["body"])
        self.assertIn("<table>", first_chapter["body"])
        self.assertIn("Queen Bee Orla", first_chapter["body"])

    def test_cached_docs_retrieval(self):
        cached = self.connector.get_cached_docs()
        self.assertEqual(cached["space_key"], "ATBDBT")
        self.assertEqual(len(cached["chapters"]), 6)
        self.assertEqual(len(cached["pages"]), 6)

    @patch("urllib.request.urlopen")
    def test_upsert_page_mocked(self, mock_urlopen):
        # Mock GET search returning empty (no existing page)
        mock_response_get = MagicMock()
        mock_response_get.__enter__.return_value = mock_response_get
        mock_response_get.status = 200
        mock_response_get.read.return_value = b'{"results": []}'

        # Mock POST create page
        mock_response_post = MagicMock()
        mock_response_post.__enter__.return_value = mock_response_post
        mock_response_post.status = 200
        mock_response_post.read.return_value = b'{"id": "123456", "title": "Test Doc", "version": {"number": 1}}'

        mock_urlopen.side_effect = [mock_response_get, mock_response_post]

        res = self.connector.upsert_page("Test Doc", "<p>Test Content</p>")
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["action"], "CREATED")
        self.assertEqual(res["page_id"], "123456")

    @patch("urllib.request.urlopen")
    def test_upload_attachment_mocked(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.status = 200
        mock_response.read.return_value = b'{"results": [{"id": "att-99", "title": "test_logo.png"}]}'
        mock_urlopen.return_value = mock_response

        with tempfile.NamedTemporaryFile(suffix=".png") as tmp_img:
            tmp_img.write(b"\x89PNG\r\n\x1a\nfakeimagecontent")
            tmp_img.flush()
            res = self.connector.upload_attachment("123456", Path(tmp_img.name), "test_logo.png")
            self.assertEqual(res["status"], "SUCCESS")
            self.assertEqual(res["filename"], "test_logo.png")


class TestConfluenceSpaceBuilder(unittest.TestCase):
    def test_manifest_and_section_blueprints(self):
        from autonomous_dbt_app.core.confluence_space_builder import ConfluenceSpaceBuilder, space_builder
        builder = space_builder
        # Check master hub and sections generate valid storage xhtml
        hub_content = builder.get_master_hub_content("2026-10-13 12:00:00 UTC")
        self.assertIn("Master Documentation Index", hub_content)
        self.assertIn("Executive Summary", hub_content)

        sec1 = builder.get_section_1_content()
        self.assertIn("Queen Bee Orla", sec1)
        self.assertIn("Sentinel Aegis", sec1)

        sec8 = builder.get_section_8_content()
        self.assertIn("243,934,093.08", sec8)
        self.assertIn("100% Golden Parity", sec8)

        # Check published manifest exists and has all 13 entries
        manifest_file = Path("agent_memory/confluence_docs_cache/full_space_manifest.json")
        self.assertTrue(manifest_file.exists())
        import json
        with open(manifest_file, "r") as f:
            manifest = json.load(f)
        self.assertEqual(manifest["space_key"], "ATBDBT")
        self.assertEqual(manifest["total_sections"], 12)
        self.assertEqual(len(manifest["results"]), 13)


class TestJiraConnector(unittest.TestCase):
    def setUp(self):
        self.connector = JiraConnector(project_key="ATBDBT")

    def test_adf_formatting(self):
        adf = self.connector._text_to_adf("Line 1\nLine 2")
        self.assertEqual(adf["type"], "doc")
        self.assertEqual(adf["version"], 1)
        self.assertEqual(len(adf["content"]), 2)
        self.assertEqual(adf["content"][0]["content"][0]["text"], "Line 1")

    @patch("urllib.request.urlopen")
    def test_create_issue_mocked(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.status = 201
        mock_response.read.return_value = b'{"id": "10001", "key": "ATBDBT-99", "self": "https://supplycopia.atlassian.net/rest/api/3/issue/10001"}'
        mock_urlopen.return_value = mock_response

        res = self.connector.create_issue(
            summary="Unit Test Issue",
            description="Testing issue creation",
            issue_type="Task",
            priority="Medium"
        )
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["key"], "ATBDBT-99")

    @patch("urllib.request.urlopen")
    def test_incident_deduplication(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.status = 201
        mock_response.read.return_value = b'{"id": "10002", "key": "ATBDBT-100"}'
        mock_urlopen.return_value = mock_response

        err_msg = "Database deadlocked on DBT execution table"
        plan = "Restart worker thread"

        # First call creates
        res1 = self.connector.create_incident_ticket(err_msg, plan)
        self.assertEqual(res1["status"], "SUCCESS")
        self.assertEqual(res1["key"], "ATBDBT-100")

        # Second immediate identical call dedupes within 1 hour
        res2 = self.connector.create_incident_ticket(err_msg, plan)
        self.assertEqual(res2["status"], "DUPLICATE_SUPPRESSED")
        self.assertEqual(res2["key"], "ATBDBT-100")

    @patch("urllib.request.urlopen")
    def test_list_issues_mocked(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.__enter__.return_value = mock_response
        mock_response.status = 200
        mock_response.read.return_value = b'{"issues": [{"key": "ATBDBT-1", "fields": {"summary": "Setup", "status": {"name": "To Do"}, "issuetype": {"name": "Task"}, "priority": {"name": "Medium"}, "created": "2026-10-13T12:00:00.000Z"}}]}'
        mock_urlopen.return_value = mock_response

        res = self.connector.list_issues(limit=5)
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(len(res["issues"]), 1)
        self.assertEqual(res["issues"][0]["key"], "ATBDBT-1")


class TestSelfHealingJiraEscalation(unittest.TestCase):
    def test_self_healing_escalation_on_unresolvable_failure(self):
        with tempfile.NamedTemporaryFile(suffix=".duckdb") as tmp_db:
            db_path = Path(tmp_db.name)
            engine = SelfHealingEngine(duckdb_path=db_path)
            engine.jira.create_incident_ticket = MagicMock(return_value={
                "status": "SUCCESS",
                "key": "ATBDBT-105",
                "issue_key": "ATBDBT-105",
                "url": "https://supplycopia.atlassian.net/browse/ATBDBT-105"
            })

            # Mock duckdb.connect to return a connection whose execute raises an error during patch
            with patch("duckdb.connect") as mock_conn:
                mock_con_obj = MagicMock()
                mock_con_obj.execute.side_effect = RuntimeError("Fatal compilation corrupted AST state")
                mock_conn.return_value = mock_con_obj

                res = engine.execute_healing_patch(
                    "Fatal compilation corrupt state in stg_consumption",
                    "Rebuild dbt staging model"
                )
                self.assertEqual(res["status"], "ERROR")
                self.assertIn("escalated to jira", res.get("message", "").lower())
                self.assertEqual(res.get("jira_ticket"), "ATBDBT-105")
                engine.jira.create_incident_ticket.assert_called_once()


if __name__ == "__main__":
    unittest.main()
