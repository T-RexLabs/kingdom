from __future__ import annotations

import unittest

from scripts.e2e.in_existing_repo_report import (
    build_bootstrap_failure_analysis,
    parse_changed_paths,
    touches_target_paths,
)


class InExistingRepoReportTests(unittest.TestCase):
    def test_parse_changed_paths_should_ignore_branch_lines_when_status_contains_branch_header(self) -> None:
        lines = [
            "## main",
            " M src/main.js",
            "?? docs/upgrade-notes.md",
        ]
        self.assertEqual(parse_changed_paths(lines), ["src/main.js", "docs/upgrade-notes.md"])

    def test_touches_target_paths_should_return_true_when_seeded_targets_change(self) -> None:
        self.assertTrue(touches_target_paths(["src/main.js"]))
        self.assertTrue(touches_target_paths(["docs/upgrade-notes.md"]))
        self.assertFalse(touches_target_paths(["package-lock.json"]))

    def test_build_bootstrap_failure_analysis_should_mark_failed_checks_when_runtime_never_started(self) -> None:
        analysis = build_bootstrap_failure_analysis("bootstrap log output", "git_worktree")
        self.assertFalse(analysis["pass"])
        self.assertEqual(analysis["publish_mode"], "git_worktree")
        self.assertIn("bootstrap_error", analysis["observations"])


if __name__ == "__main__":
    unittest.main()
