from __future__ import annotations

import shutil
from pathlib import Path

from scripts.e2e.in_existing_repo_report import brief_from_analysis, build_bootstrap_failure_analysis, build_in_existing_repo_analysis, write_summary
from scripts.e2e.models import DEFAULT_WORKFLOW_ID, InExistingRepoConfig, RunRequest, WorkspacePaths, WorktreeRecord
from scripts.e2e.runtime import RuntimeService


DEFAULT_DECREE_TITLE = "Improve Existing Repository Baseline"
DEFAULT_DECREE_TEXT = (
    "You are working inside an existing git repository. Upgrade this Node project while preserving "
    "current behavior: improve project scripts and package metadata, refactor src/main.js and "
    "src/util.js for clearer structure, add or update tests under test/, and update "
    "docs/upgrade-notes.md with a concise migration summary and verification steps. Keep changes "
    "scoped, runnable, and production-safe."
)


def run_in_existing_repo(config: InExistingRepoConfig, root_dir: Path) -> None:
    runtime = RuntimeService(root_dir)
    runtime.ensure_required_commands(
        include_git=True,
        include_go=config.common.run_install,
    )
    config.common.output_dir.mkdir(parents=True, exist_ok=True)
    if config.common.run_install:
        print("[setup] installing kd")
        runtime.install_binaries()

    fixture_dir = root_dir / "scripts" / "e2e" / "fixtures" / "in_existing_repo_seed"
    workspace_dir = config.common.output_dir / "workspace"
    report_dir = config.common.output_dir / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    runtime.copy_tree(fixture_dir, workspace_dir)
    init_seed_git_repo(runtime, workspace_dir)
    runtime.write_text(report_dir / "git-main-status-before.txt", runtime.run_command(["git", "-C", str(workspace_dir), "status", "--short", "--branch"]).stdout)
    runtime.write_text(report_dir / "git-main-head-before.txt", runtime.git_head(workspace_dir))

    runtime.ensure_port_available(config.server_port)
    paths = runtime.init_workspace(workspace_dir, config.server_port, config.publish_mode)
    bootstrap_log_path = paths.report_dir / "summon-bootstrap.log"
    server_url = f"http://127.0.0.1:{config.server_port}/api/v1"
    analysis_written = False

    try:
        print(f"[in-existing-repo] summon runtime on {server_url}")
        runtime.start_summon(paths.config_path, bootstrap_log_path)
    except Exception:
        runtime.stop_summon(paths.config_path)
        error_text = load_bootstrap_error(paths, bootstrap_log_path)
        runtime.write_text(paths.report_dir / "bootstrap-error.txt", error_text + "\n")
        analysis = build_bootstrap_failure_analysis(error_text, config.publish_mode)
        runtime.write_json(paths.report_dir / "analysis.json", analysis)
        runtime.write_json(paths.report_dir / "analysis-brief.json", brief_from_analysis(analysis))
        analysis_written = True
        finish_in_existing_repo(runtime, config, paths)
        return

    try:
        run_request = RunRequest(
            title=DEFAULT_DECREE_TITLE,
            text=DEFAULT_DECREE_TEXT,
            workflow_id=DEFAULT_WORKFLOW_ID,
            publish_mode=config.publish_mode,
            global_model=config.common.global_model,
            agent_model_overrides=config.common.agent_model_overrides,
            max_execution_review_cycles=config.common.max_execution_review_cycles,
            max_validation_rework_cycles=config.common.max_validation_rework_cycles,
        )
        print("[in-existing-repo] create decree")
        run_id = runtime.create_decree_run(
            server_url,
            config.common.actor_id,
            config.common.role_id,
            run_request,
            paths.report_dir / "decree.json",
        )
        print(f"[in-existing-repo] run_id={run_id}")
        poll_result = runtime.poll_run(
            server_url,
            config.common.actor_id,
            config.common.role_id,
            run_id,
            config.common.timeout_seconds,
            config.common.poll_interval_seconds,
            paths.report_dir / "run-status-history.json",
        )
        print(f"[in-existing-repo] terminal poll status={poll_result.terminal_status}")
        runtime.capture_run_api_artifacts(
            server_url,
            config.common.actor_id,
            config.common.role_id,
            run_id,
            paths.report_dir,
        )
        capture_db_evidence(runtime, paths.db_path, run_id, paths.report_dir)
        worktree_record = runtime.load_run_worktree_record(paths.db_path, run_id)
        runtime.write_json(paths.report_dir / "run-worktree.json", runtime.serialize_worktree_record(worktree_record))
        runtime.write_text(
            paths.report_dir / "git-main-status-after.txt",
            runtime.run_command(["git", "-C", str(workspace_dir), "status", "--short", "--branch"]).stdout,
        )
        runtime.write_text(
            paths.report_dir / "git-main-diff-stat-after.txt",
            runtime.run_command(["git", "-C", str(workspace_dir), "diff", "--stat"]).stdout,
        )
        runtime.write_text(
            paths.report_dir / "git-main-diff-after.patch",
            runtime.run_command(["git", "-C", str(workspace_dir), "diff"]).stdout,
        )
        capture_worktree_git_artifacts(runtime, worktree_record, paths.report_dir)
        analysis = build_in_existing_repo_analysis(
            paths.report_dir,
            run_id,
            poll_result.terminal_status,
            config.publish_mode,
            config.common.global_model,
            config.common.agent_model_overrides,
        )
        runtime.write_json(paths.report_dir / "analysis.json", analysis)
        runtime.write_json(paths.report_dir / "analysis-brief.json", brief_from_analysis(analysis))
        analysis_written = True
    finally:
        print("[in-existing-repo] stopping summon")
        runtime.stop_summon(paths.config_path)
        if analysis_written:
            finish_in_existing_repo(runtime, config, paths)
        elif not config.common.keep_workspaces:
            shutil.rmtree(paths.workspace_dir, ignore_errors=True)


def capture_db_evidence(
    runtime: RuntimeService,
    db_path: Path,
    run_id: str,
    report_dir: Path,
) -> None:
    runtime.write_json(
        report_dir / "db-workflow-run.json",
        runtime.query_rows(
            db_path,
            (
                "SELECT run_id, run_status, plan_state, created_at, updated_at FROM workflow_runs "
                "WHERE run_id = ?"
            ),
            (run_id,),
        ),
    )
    runtime.write_json(
        report_dir / "db-node-runs.json",
        runtime.query_rows(
            db_path,
            (
                "SELECT node_id, attempt, node_status, started_at, finished_at FROM node_runs "
                "WHERE run_id = ? ORDER BY node_id, attempt"
            ),
            (run_id,),
        ),
    )
    runtime.write_json(
        report_dir / "db-events.json",
        runtime.query_rows(
            db_path,
            (
                "SELECT event_id, event_type, reason_code, created_at FROM events "
                "WHERE run_id = ? ORDER BY created_at"
            ),
            (run_id,),
        ),
    )
    runtime.write_json(
        report_dir / "db-artifacts.json",
        runtime.query_rows(
            db_path,
            (
                "SELECT artifact_id, artifact_kind, path, producer_role, created_at FROM artifacts "
                "WHERE run_id = ? ORDER BY created_at"
            ),
            (run_id,),
        ),
    )
    runtime.write_json(
        report_dir / "db-token-usage.json",
        runtime.query_rows(
            db_path,
            (
                "SELECT required_role, model, created_at FROM token_usage "
                "WHERE run_id = ? ORDER BY created_at"
            ),
            (run_id,),
        ),
    )


def capture_worktree_git_artifacts(
    runtime: RuntimeService,
    worktree_record: WorktreeRecord | None,
    report_dir: Path,
) -> None:
    worktree_path = Path(worktree_record.worktree_path) if worktree_record is not None else Path()
    if worktree_record is None or not worktree_path.is_dir():
        runtime.write_text(
            report_dir / "git-worktree-status.txt",
            f"worktree path does not exist: {worktree_path}\n",
        )
        runtime.write_text(report_dir / "git-worktree-diff-stat.txt", "")
        runtime.write_text(report_dir / "git-worktree-diff.patch", "")
        return
    runtime.capture_git_artifacts(worktree_path, "git-worktree", report_dir)


def init_seed_git_repo(runtime: RuntimeService, repo_dir: Path) -> None:
    runtime.run_command(["git", "-C", str(repo_dir), "init"])
    runtime.run_command(["git", "-C", str(repo_dir), "add", "."])
    runtime.run_command(
        [
            "git",
            "-C",
            str(repo_dir),
            "-c",
            "user.email=e2e@example.com",
            "-c",
            "user.name=Kingdom E2E",
            "commit",
            "-m",
            "seed baseline repository",
        ]
    )


def load_bootstrap_error(paths: WorkspacePaths, bootstrap_log_path: Path) -> str:
    summon_log_path = paths.workspace_dir / ".kingdom" / "logs" / "summon.log"
    if summon_log_path.exists():
        return summon_log_path.read_text(encoding="utf-8").strip()
    return bootstrap_log_path.read_text(encoding="utf-8").strip()


def finish_in_existing_repo(
    runtime: RuntimeService,
    config: InExistingRepoConfig,
    paths: WorkspacePaths,
) -> None:
    summary, text = write_summary(config.common.output_dir)
    runtime.write_json(config.common.output_dir / "summary.json", summary)
    runtime.write_text(config.common.output_dir / "summary.txt", text)
    if not config.common.keep_workspaces:
        shutil.rmtree(paths.workspace_dir, ignore_errors=True)
    print(text, end="")
    print(f"done: {config.common.output_dir}")
