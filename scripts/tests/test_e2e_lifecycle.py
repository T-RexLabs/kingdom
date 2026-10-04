from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts.e2e import in_existing_repo, rework_analysis
from scripts.e2e.models import CommonRunOptions, InExistingRepoConfig, PollResult, ReworkAnalysisConfig, WorkspacePaths
from scripts.e2e.runtime import CommandError, RuntimeService, Scenario
from scripts.e2e.rework_analysis_report import acceptance_checks, write_summary as rework_summary
from scripts.e2e.in_existing_repo_report import write_summary as existing_repo_summary


class HarnessLifecycleTests(unittest.TestCase):
    def passing_analysis(self):
        return {
            "profile": "fixture", "run_id": "run", "terminal_status": "succeeded", "failure_classification": "none",
            "message_metrics": {"instruction_count": 1, "response_count": 1, "message_contract_violations": [], "authority_violations": [], "section_growth_ok": True, "builder_overlap_detected": False},
            "model_routing": {"ok": True},
            "rework_propagation": {"reviewer_failures": 0, "validator_reworks": 0, "reviewer_detail_propagated": False, "validator_detail_propagated": False, "coalesced_retry_after_wave_terminal": True},
        }

    def test_rework_summary_requires_stop_confirmation_and_all_requested_profiles(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "fixture/report"
            report.mkdir(parents=True)
            (report / "analysis.json").write_text(json.dumps(self.passing_analysis()))
            (report / "harness-result.json").write_text(json.dumps({"status": "failed", "runtime_stopped": False}))
            self.assertFalse(rework_summary(root, ["fixture"])[0]["overall"]["pass"])
            (report / "harness-result.json").write_text(json.dumps({"status": "passed", "runtime_stopped": True}))
            self.assertTrue(rework_summary(root, ["fixture"])[0]["overall"]["pass"])
            self.assertFalse(rework_summary(root, ["fixture", "missing"])[0]["overall"]["pass"])

    def test_mixed_feedback_links_cannot_pass_acceptance(self):
        for kind, marker in (("reviewer", "contains_review_failure_marker"), ("validator", "contains_validator_node_marker")):
            with self.subTest(kind=kind):
                analysis = self.passing_analysis()
                propagation = analysis["rework_propagation"]
                propagation["reviewer_failures" if kind == "reviewer" else "validator_reworks"] = 2
                propagation[f"{kind}_detail_propagated"] = True
                propagation[f"{kind}_links"] = [
                    {"has_builder_retry": True, "snippet_match": True, marker: True},
                    {"has_builder_retry": True, "snippet_match": False, marker: False},
                ]
                self.assertFalse(acceptance_checks(analysis)[f"{kind}_feedback_propagated"])
                propagation[f"{kind}_links"][1]["snippet_match"] = True
                self.assertTrue(acceptance_checks(analysis)[f"{kind}_feedback_propagated"])
                propagation[f"{kind}_links"].pop()
                self.assertFalse(acceptance_checks(analysis)[f"{kind}_feedback_propagated"])

    def test_final_report_write_failure_cannot_leave_saved_pass(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            write_json = runtime.write_json
            result_writes = 0

            def fail_final_result(path, payload):
                nonlocal result_writes
                if path.name == "harness-result.json":
                    result_writes += 1
                    if result_writes == 2:
                        raise OSError("final report unavailable")
                write_json(path, payload)

            runtime.write_json = mock.Mock(side_effect=fail_final_result)
            with self.assertRaisesRegex(RuntimeError, "final report unavailable"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
                    scenario.require_acceptance({"observed": True})
            result = json.loads((paths.report_dir / "harness-result.json").read_text())
            self.assertNotEqual(result["status"], "passed")

    def test_bootstrap_report_fault_preserves_original_failure_in_both_workflows(self):
        for workflow in (in_existing_repo, rework_analysis):
            for fault in ("load", "write_text", "write_json"):
                with self.subTest(workflow=workflow.__name__, fault=fault), TemporaryDirectory() as directory:
                    root = Path(directory)
                    runtime, paths, common = self.setup_runtime(root)
                    # Only bootstrap text collection fails; initial Git evidence writes succeed.
                    write_text = runtime.write_text
                    if fault == "write_text":
                        def fail_bootstrap(path, text):
                            if path.name == "bootstrap-error.txt":
                                raise OSError("bootstrap evidence unavailable")
                            write_text(path, text)
                        runtime.write_text = mock.Mock(side_effect=fail_bootstrap)
                    if fault == "write_json":
                        runtime.write_json = mock.Mock(side_effect=OSError("bootstrap evidence unavailable"))
                    with mock.patch.object(workflow, "RuntimeService", return_value=runtime), mock.patch.object(workflow, "load_bootstrap_error", side_effect=OSError("bootstrap evidence unavailable") if fault == "load" else None, return_value="bootstrap primary failure"):
                        with self.assertRaisesRegex(RuntimeError, "bootstrap primary failure") as raised:
                            if workflow is in_existing_repo:
                                workflow.run_in_existing_repo(InExistingRepoConfig(common, 18180, "direct"), root)
                            else:
                                config = ReworkAnalysisConfig(common, 18180, (rework_analysis.FAST_ITERATION_PROFILE,))
                                workflow.run_profile(runtime, config, config.profiles[0], 18180)
                    self.assertTrue(any("evidence unavailable" in note for note in raised.exception.__notes__))
                    self.assertTrue(paths.workspace_dir.exists())

    def test_rework_setup_report_fault_preserves_setup_failure(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, _paths, common = self.setup_runtime(root)
            runtime.ensure_required_commands.side_effect = RuntimeError("setup primary failure")
            runtime.write_json = mock.Mock(side_effect=OSError("setup evidence unavailable"))
            config = ReworkAnalysisConfig(common, 18180, (rework_analysis.FAST_ITERATION_PROFILE,))
            with mock.patch.object(rework_analysis, "RuntimeService", return_value=runtime):
                with self.assertRaisesRegex(RuntimeError, "setup primary failure") as raised:
                    rework_analysis.run_rework_analysis(config, root)
            self.assertTrue(any("evidence unavailable" in note for note in raised.exception.__notes__))

    def test_existing_repo_summary_reports_shutdown_failure_even_if_analysis_passes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "report"
            report.mkdir()
            (report / "analysis.json").write_text(json.dumps({"pass": True}))
            (report / "harness-result.json").write_text(json.dumps({"status": "failed", "runtime_stopped": False}))
            self.assertFalse(existing_repo_summary(root)[0]["pass"])

    def test_unrecorded_acceptance_cannot_report_success(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            with self.assertRaisesRegex(RuntimeError, "acceptance failed"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
            self.assertTrue(paths.workspace_dir.exists())
            self.assertEqual(json.loads((paths.report_dir / "harness-result.json").read_text())["status"], "failed")

    def test_evidence_write_failure_does_not_mask_primary_failure(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.write_json = mock.Mock(side_effect=OSError("disk full"))
            with self.assertRaisesRegex(RuntimeError, "primary") as raised:
                with Scenario(runtime, paths.workspace_dir, False):
                    raise RuntimeError("primary")
            self.assertIn("disk full", raised.exception.__notes__[0])
            self.assertTrue(paths.workspace_dir.exists())

    def test_evidence_write_failure_prevents_successful_workspace_removal(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            runtime.write_json = mock.Mock(side_effect=OSError("disk full"))
            with self.assertRaisesRegex(RuntimeError, "disk full"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
                    scenario.require_acceptance({"observed": True})
            self.assertTrue(paths.workspace_dir.exists())

    def test_cli_setup_failure_is_nonzero_and_persists_report(self):
        for workflow in ("in-existing-repo", "rework-analysis"):
            with self.subTest(workflow=workflow), TemporaryDirectory() as directory:
                root = Path(directory)
                result = subprocess.run(
                    [sys.executable, "-m", "scripts.e2e.cli", workflow, "--output-dir", str(root), "--skip-install", "--global-model", "fixture"],
                    env={**os.environ, "PATH": str(root)}, capture_output=True, text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                report = root / "report/harness-result.json" if workflow == "in-existing-repo" else root / "setup-result.json"
                self.assertEqual(json.loads(report.read_text())["status"], "failed")

    def test_successful_stop_is_required_before_workspace_removal(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            runtime.stop_summon.side_effect = RuntimeError("shutdown failure")
            with self.assertRaisesRegex(RuntimeError, "shutdown failure"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
                    scenario.require_acceptance({"observed": True})
            self.assertTrue(paths.workspace_dir.exists())

    def test_successful_scenario_can_remove_workspace_after_stop(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            with Scenario(runtime, paths.workspace_dir, False) as scenario:
                scenario.start(paths.report_dir / "bootstrap.log")
                scenario.require_acceptance({"observed": True})
            self.assertFalse(paths.workspace_dir.exists())
            self.assertEqual(json.loads((paths.report_dir / "harness-result.json").read_text())["status"], "passed")

    def test_acceptance_failure_is_nonzero_and_retains_report(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            with self.assertRaisesRegex(RuntimeError, "model_routing_ok"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
                    scenario.require_acceptance({"model_routing_ok": False})
            self.assertTrue(paths.workspace_dir.exists())
            self.assertFalse(json.loads((paths.report_dir / "harness-result.json").read_text())["acceptance"]["model_routing_ok"])

    def test_rework_acceptance_requires_collected_evidence_even_on_success(self):
        checks = acceptance_checks({"terminal_status": "succeeded"})
        self.assertTrue(checks["terminal_ok"])
        self.assertFalse(checks["communication_evidence_present"])
        self.assertFalse(checks["authority_ok"])
        self.assertFalse(checks["model_routing_ok"])

    def test_existing_repo_failed_analysis_exits_and_saves_acceptance(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, paths, common = self.setup_runtime(root)
            runtime.start_summon.side_effect = None
            runtime.create_work_order_run = mock.Mock(return_value="run")
            runtime.poll_run = mock.Mock(return_value=PollResult("failed", paths.report_dir / "history.json"))
            runtime.capture_run_api_artifacts = mock.Mock()
            runtime.load_run_worktree_record = mock.Mock(return_value=None)
            with (
                mock.patch.object(in_existing_repo, "RuntimeService", return_value=runtime),
                mock.patch.object(in_existing_repo, "capture_db_evidence"),
                mock.patch.object(in_existing_repo, "build_in_existing_repo_analysis", return_value={"pass": False, "checks": {"terminal_ok": False}}),
            ):
                with self.assertRaisesRegex(RuntimeError, "terminal_ok"):
                    in_existing_repo.run_in_existing_repo(InExistingRepoConfig(common, 18180, "direct"), root)
            self.assertTrue(paths.workspace_dir.exists())
            self.assertFalse(json.loads((root / "summary.json").read_text())["pass"])
            runtime.stop_summon.assert_called_once()

    def test_rework_failed_analysis_exits_and_saves_acceptance(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, paths, common = self.setup_runtime(root)
            runtime.start_summon.side_effect = None
            runtime.create_work_order_run = mock.Mock(return_value="run")
            runtime.poll_run = mock.Mock(return_value=PollResult("succeeded", paths.report_dir / "history.json"))
            runtime.capture_run_api_artifacts = mock.Mock()
            analysis = self.passing_analysis()
            analysis["message_metrics"]["builder_overlap_detected"] = True
            config = ReworkAnalysisConfig(common, 18180, (rework_analysis.FAST_ITERATION_PROFILE,))
            with mock.patch.object(rework_analysis, "build_rework_analysis", return_value=analysis):
                with self.assertRaisesRegex(RuntimeError, "builder_execution_serial"):
                    rework_analysis.run_profile(runtime, config, config.profiles[0], 18180)
            self.assertTrue(paths.workspace_dir.exists())
            result = json.loads((paths.report_dir / "harness-result.json").read_text())
            self.assertFalse(result["acceptance"]["builder_execution_serial"])
            runtime.stop_summon.assert_called_once()

    def test_shutdown_command_failure_saves_command_and_output(self):
        with TemporaryDirectory() as directory:
            runtime, paths, _common = self.setup_runtime(Path(directory))
            runtime.start_summon.side_effect = None
            runtime.stop_summon.side_effect = CommandError(["kd", "summon", "stop"], 3, "still running", "quiescence unproved")
            with self.assertRaisesRegex(RuntimeError, "quiescence unproved"):
                with Scenario(runtime, paths.workspace_dir, False) as scenario:
                    scenario.start(paths.report_dir / "bootstrap.log")
                    scenario.require_acceptance({"observed": True})
            result = json.loads((paths.report_dir / "summon-stop.json").read_text())
            self.assertEqual(result["exit_code"], 3)
            self.assertEqual(result["stdout"], "still running")
            self.assertEqual(result["stderr"], "quiescence unproved")
            self.assertTrue(paths.workspace_dir.exists())

    def setup_runtime(self, root: Path):
        workspace = root / "workspace"
        workspace.mkdir()
        (workspace / "evidence.txt").write_text("retain")
        report = root / "report"
        report.mkdir()
        paths = WorkspacePaths(root, workspace, report, workspace / ".kingdom/config.yaml", workspace / ".kingdom/kingdom.db", workspace / "token")
        runtime = RuntimeService(root)
        runtime.ensure_required_commands = mock.Mock()
        runtime.ensure_port_available = mock.Mock()
        runtime.copy_tree = mock.Mock()
        runtime.run_command = mock.Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        runtime.git_head = mock.Mock(return_value="seed")
        runtime.init_workspace = mock.Mock(return_value=paths)
        runtime.start_summon = mock.Mock(side_effect=RuntimeError("bootstrap primary failure"))
        runtime.stop_summon = mock.Mock()
        common = CommonRunOptions(root, 10, 1, "model", "", 1, 1, False, False)
        return runtime, paths, common

    def test_existing_repo_bootstrap_failure_is_nonzero_and_keeps_evidence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, paths, common = self.setup_runtime(root)
            with mock.patch.object(in_existing_repo, "RuntimeService", return_value=runtime), mock.patch.object(in_existing_repo, "load_bootstrap_error", return_value="bootstrap primary failure"):
                with self.assertRaisesRegex(RuntimeError, "bootstrap primary failure"):
                    in_existing_repo.run_in_existing_repo(InExistingRepoConfig(common, 18180, "direct"), root)
            self.assertTrue(paths.workspace_dir.exists())
            self.assertEqual(json.loads((paths.report_dir / "harness-result.json").read_text())["status"], "failed")
            runtime.stop_summon.assert_called_once()

    def test_rework_bootstrap_failure_is_nonzero_and_keeps_evidence(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, paths, common = self.setup_runtime(root)
            config = ReworkAnalysisConfig(common, 18180, (rework_analysis.FAST_ITERATION_PROFILE,))
            with mock.patch.object(rework_analysis, "load_bootstrap_error", return_value="bootstrap primary failure"):
                with self.assertRaisesRegex(RuntimeError, "bootstrap primary failure"):
                    rework_analysis.run_profile(runtime, config, config.profiles[0], 18180)
            self.assertTrue(paths.workspace_dir.exists())
            runtime.stop_summon.assert_called_once()

    def test_cleanup_failure_does_not_replace_primary_failure(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, paths, common = self.setup_runtime(root)
            runtime.stop_summon.side_effect = RuntimeError("shutdown failure")
            with mock.patch.object(in_existing_repo, "RuntimeService", return_value=runtime), mock.patch.object(in_existing_repo, "load_bootstrap_error", return_value="bootstrap primary failure"):
                with self.assertRaisesRegex(RuntimeError, "bootstrap primary failure"):
                    in_existing_repo.run_in_existing_repo(InExistingRepoConfig(common, 18180, "direct"), root)
            result = json.loads((paths.report_dir / "harness-result.json").read_text())
            self.assertIn("shutdown failure", result["cleanup_error"])
            self.assertFalse(result["runtime_stopped"])
            self.assertTrue(paths.workspace_dir.exists())
