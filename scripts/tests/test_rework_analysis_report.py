from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.e2e.rework_analysis_report import (
    build_bootstrap_failure_analysis,
    build_rework_analysis,
    classify_provider_timeout,
    provider_timeout_blockers,
    structured_guidance_warnings,
    summarize_validator_waves,
)


class ReworkAnalysisReportTests(unittest.TestCase):
    def test_build_bootstrap_failure_analysis_should_classify_bootstrap_process_kill_when_signal_text_present(self) -> None:
        analysis = build_bootstrap_failure_analysis(
            "fast-iteration",
            "runtime error: bootstrap runtime: provider agent \"builder\" is not resolvable: signal: killed",
        )
        self.assertEqual(analysis["profile"], "fast-iteration")
        self.assertEqual(analysis["failure_classification"], "bootstrap_process_killed")
        self.assertEqual(
            analysis["attempt_metrics"]["failure_reasons"][0]["reason_code"],
            "bootstrap_runtime",
        )

    def test_classify_provider_timeout_should_detect_first_event_when_error_mentions_it(self) -> None:
        self.assertEqual(
            classify_provider_timeout("provider first_event_timeout exceeded after 1m0s", None),
            "first_event_timeout",
        )

    def test_classify_provider_timeout_should_prefer_explicit_timeout_type_when_present(self) -> None:
        self.assertEqual(
            classify_provider_timeout("provider timeout", "stream_inactivity_timeout"),
            "stream_inactivity_timeout",
        )

    def test_provider_timeout_blockers_should_distinguish_first_event_from_stream_when_only_first_event_present(self) -> None:
        blockers = provider_timeout_blockers(
            [{"timeout_type": "first_event_timeout"}, {"timeout_type": "first_event_timeout"}]
        )
        self.assertTrue(blockers["provider_first_event_timeout"])
        self.assertFalse(blockers["provider_stream_inactivity_timeout"])

    def test_build_rework_analysis_should_classify_canonical_invalid_response(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_dir = root / "report"
            report_dir.mkdir()
            (report_dir / "run-final.json").write_text(json.dumps({"status": "failed"}), encoding="utf-8")
            db_path = root / "runtime.sqlite"
            connection = sqlite3.connect(db_path)
            connection.executescript(
                """
                CREATE TABLE workflow_runs (run_id TEXT, run_status TEXT, plan_state TEXT, created_at TEXT, updated_at TEXT, rework_cycle_count INTEGER, max_rework_cycles INTEGER);
                CREATE TABLE node_runs (run_id TEXT, node_id TEXT, attempt INTEGER, node_status TEXT, started_at TEXT, finished_at TEXT);
                CREATE TABLE token_usage (run_id TEXT, required_role TEXT, model TEXT, created_at TEXT, usage_id TEXT);
                CREATE TABLE events (run_id TEXT, attempt_id TEXT, reason_code TEXT, reason_text TEXT, payload_json TEXT, created_at TEXT, event_type TEXT);
                CREATE TABLE mailbox_messages (run_id TEXT, message_id TEXT, from_agent_id TEXT, to_agent_id TEXT, created_at TEXT, payload_json TEXT);
                INSERT INTO workflow_runs VALUES ('run-1', 'failed', '', '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', 0, 0);
                INSERT INTO events VALUES ('run-1', 'attempt-1', 'llm_invalid_response', 'invalid provider response', '{}', '2026-01-01T00:01:00Z', 'attempt.failed');
                """
            )
            connection.close()

            analysis = build_rework_analysis("test", "run-1", report_dir, db_path, "", "", "")

            self.assertEqual(analysis["failure_classification"], "invalid_response")
            self.assertTrue(analysis["blockers"]["invalid_response"])

    def test_summarize_validator_waves_should_ignore_non_validator_retries_when_retry_context_missing(self) -> None:
        failures = {1: ["2026-03-24T20:10:21Z", "2026-03-24T20:10:29Z"]}
        instructions = [
            {
                "created_at": "2026-03-24T20:10:29Z",
                "includes_validation_failure_context": True,
            },
            {
                "created_at": "2026-03-24T20:14:55Z",
                "includes_validation_failure_context": False,
            },
            {
                "created_at": "2026-03-24T20:16:49Z",
                "includes_validation_failure_context": False,
            },
        ]
        summary = summarize_validator_waves(failures, instructions)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["retries_after_wave"], 0)
        self.assertTrue(summary[0]["coalesced"])

    def test_summarize_validator_waves_should_mark_wave_non_coalesced_when_multiple_retries_follow_failure(self) -> None:
        failures = {1: ["2026-03-24T20:10:21Z"]}
        instructions = [
            {
                "created_at": "2026-03-24T20:10:30Z",
                "includes_validation_failure_context": True,
            },
            {
                "created_at": "2026-03-24T20:10:40Z",
                "includes_validation_failure_context": True,
            },
        ]
        summary = summarize_validator_waves(failures, instructions)
        self.assertEqual(summary[0]["retries_after_wave"], 2)
        self.assertFalse(summary[0]["coalesced"])

    def test_structured_guidance_warnings_should_remain_warning_only_when_sections_missing(self) -> None:
        warning = structured_guidance_warnings(
            review_failures=3,
            validator_reworks=2,
            review_rework_items_present=0,
            validator_blocking_gates_present=0,
            validator_fix_items_present=0,
        )
        self.assertTrue(warning["has_warnings"])
        self.assertGreaterEqual(len(warning["items"]), 1)


if __name__ == "__main__":
    unittest.main()
