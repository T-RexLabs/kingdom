from __future__ import annotations

import shutil
from pathlib import Path

from scripts.e2e.models import DEFAULT_WORKFLOW_ID, PUBLISH_MODE_DIRECT, ReworkAnalysisConfig, ReworkProfile, RunRequest, WorkspacePaths
from scripts.e2e.rework_analysis_report import brief_from_analysis, build_bootstrap_failure_analysis, build_rework_analysis, write_summary
from scripts.e2e.runtime import RuntimeService


FAST_ITERATION_PROFILE = ReworkProfile(
    profile_id="fast-iteration",
    title="Fast Iteration Profile",
    prompt_text=(
        "Implement a minimal auth dashboard quickly and iterate based on feedback "
        "while keeping core paths coherent."
    ),
)

QUALITY_FIRST_PROFILE = ReworkProfile(
    profile_id="quality-first",
    title="Quality First Profile",
    prompt_text=(
        "Implement a minimal auth dashboard with clean structure and testability; "
        "prioritize correctness but keep delivery pragmatic."
    ),
)


def run_rework_analysis(config: ReworkAnalysisConfig, root_dir: Path) -> None:
    runtime = RuntimeService(root_dir)
    runtime.ensure_required_commands(include_go=config.common.run_install)
    config.common.output_dir.mkdir(parents=True, exist_ok=True)
    if config.common.run_install:
        print("[setup] installing kd")
        runtime.install_binaries()

    for index, profile in enumerate(config.profiles):
        run_profile(runtime, config, profile, config.server_port_base + index)

    summary, text = write_summary(config.common.output_dir)
    runtime.write_json(config.common.output_dir / "summary.json", summary)
    runtime.write_text(config.common.output_dir / "summary.txt", text)
    print(text, end="")
    print(f"done: {config.common.output_dir}")


def run_profile(
    runtime: RuntimeService,
    config: ReworkAnalysisConfig,
    profile: ReworkProfile,
    port: int,
) -> None:
    scenario_dir = config.common.output_dir / profile.profile_id
    workspace_dir = scenario_dir / "workspace"
    runtime.ensure_port_available(port)
    paths = runtime.init_workspace(workspace_dir, port, PUBLISH_MODE_DIRECT)
    paths.report_dir.mkdir(parents=True, exist_ok=True)

    bootstrap_log_path = paths.report_dir / "summon-bootstrap.log"
    server_url = f"http://127.0.0.1:{port}/api/v1"

    try:
        print(f"[{profile.profile_id}] summon runtime on {server_url}")
        runtime.start_summon(paths.config_path, bootstrap_log_path)
    except Exception:
        runtime.stop_summon(paths.config_path)
        error_text = load_bootstrap_error(paths, bootstrap_log_path)
        runtime.write_text(paths.report_dir / "bootstrap-error.txt", error_text + "\n")
        analysis = build_bootstrap_failure_analysis(profile.profile_id, error_text)
        runtime.write_json(paths.report_dir / "analysis.json", analysis)
        runtime.write_json(paths.report_dir / "analysis-brief.json", brief_from_analysis(analysis))
        cleanup_workspace(config, paths)
        return

    try:
        run_request = RunRequest(
            title=profile.title,
            text=profile.prompt_text,
            workflow_id=DEFAULT_WORKFLOW_ID,
            publish_mode=PUBLISH_MODE_DIRECT,
            global_model=config.common.global_model,
            agent_model_overrides=config.common.agent_model_overrides,
            max_execution_review_cycles=config.common.max_execution_review_cycles,
            max_validation_rework_cycles=config.common.max_validation_rework_cycles,
        )
        print(f"[{profile.profile_id}] create decree")
        run_id = runtime.create_decree_run(
            server_url,
            paths.credential_path,
            run_request,
            paths.report_dir / "decree.json",
        )
        print(f"[{profile.profile_id}] run_id={run_id}")
        poll_result = runtime.poll_run(
            server_url,
            paths.credential_path,
            run_id,
            config.common.timeout_seconds,
            config.common.poll_interval_seconds,
            paths.report_dir / "run-status-history.json",
        )
        print(f"[{profile.profile_id}] terminal poll status={poll_result.terminal_status}")
        runtime.capture_run_api_artifacts(
            server_url,
            paths.credential_path,
            run_id,
            paths.report_dir,
        )
        analysis = build_rework_analysis(
            profile.profile_id,
            run_id,
            paths.report_dir,
            paths.db_path,
            poll_result.terminal_status,
            config.common.global_model,
            config.common.agent_model_overrides,
        )
        runtime.write_json(paths.report_dir / "analysis.json", analysis)
        runtime.write_json(paths.report_dir / "analysis-brief.json", brief_from_analysis(analysis))
    finally:
        print(f"[{profile.profile_id}] stopping summon")
        runtime.stop_summon(paths.config_path)
        cleanup_workspace(config, paths)


def load_bootstrap_error(paths: WorkspacePaths, bootstrap_log_path: Path) -> str:
    summon_log_path = paths.workspace_dir / ".kingdom" / "logs" / "summon.log"
    if summon_log_path.exists():
        return summon_log_path.read_text(encoding="utf-8").strip()
    return bootstrap_log_path.read_text(encoding="utf-8").strip()


def cleanup_workspace(config: ReworkAnalysisConfig, paths: WorkspacePaths) -> None:
    if not config.common.keep_workspaces:
        shutil.rmtree(paths.workspace_dir, ignore_errors=True)
