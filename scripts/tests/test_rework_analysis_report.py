from __future__ import annotations

import unittest

from scripts.e2e.rework_analysis_report import (
    build_bootstrap_failure_analysis,
    classify_provider_timeout,
    provider_timeout_blockers,
    structured_guidance_warnings,
    summarize_validator_waves,
)


class ReworkAnalysisReportTests(unittest.TestCase):
    def test_build_bootstrap_failure_analysis_should_classify_provider_probe_failure_when_probe_text_present(self) -> None:
        analysis = build_bootstrap_failure_analysis(
            "fast-iteration",
            "runtime error: bootstrap runtime: provider strict probe failed: opencode probe execute failed: opencode run failed: signal: killed",
        )
        self.assertEqual(analysis["profile"], "fast-iteration")
        self.assertEqual(analysis["failure_classification"], "bootstrap_provider_probe_failed")
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
