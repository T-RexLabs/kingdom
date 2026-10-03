from __future__ import annotations

from pathlib import Path

from scripts.e2e.models import PUBLISH_MODE_DIRECT, ReworkAnalysisConfig, ReworkProfile, RunRequest, WorkspacePaths
from scripts.e2e.rework_analysis_report import brief_from_analysis, build_bootstrap_failure_analysis, build_rework_analysis, acceptance_checks, write_summary
from scripts.e2e.runtime import RuntimeService, Scenario, preserve_failure


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
    config.common.output_dir.mkdir(parents=True, exist_ok=True)
    try:
        runtime.ensure_required_commands(include_go=config.common.run_install)
        if config.common.run_install:
            print("[setup] installing kd")
            runtime.install_binaries()
    except Exception as exc:
        with preserve_failure(exc):
            runtime.write_json(config.common.output_dir / "setup-result.json", {"status": "failed", "error": str(exc)})
        raise
    runtime.write_json(config.common.output_dir / "setup-result.json", {"status": "passed"})

    try:
        for index, profile in enumerate(config.profiles):
            run_profile(runtime, config, profile, config.server_port_base + index)
    except Exception as exc:
        try:
            finish_rework_analysis(runtime, config)
        except Exception as report_error:
            exc.add_note(f"summary write failed: {report_error}")
        raise
    finish_rework_analysis(runtime, config)


def finish_rework_analysis(runtime: RuntimeService, config: ReworkAnalysisConfig) -> None:
    summary, text = write_summary(config.common.output_dir, [profile.profile_id for profile in config.profiles])
    runtime.write_json(config.common.output_dir / "summary.json", summary)
    runtime.write_text(config.common.output_dir / "summary.txt", text)
    print(text, end="")
    print(f"reports: {config.common.output_dir}")


def run_profile(
    runtime: RuntimeService,
    config: ReworkAnalysisConfig,
    profile: ReworkProfile,
    port: int,
) -> None:
    workspace_dir = config.common.output_dir / profile.profile_id / "workspace"
    with Scenario(runtime, workspace_dir, config.common.keep_workspaces) as scenario:
        runtime.ensure_port_available(port)
        paths = runtime.init_workspace(workspace_dir, port, PUBLISH_MODE_DIRECT)
        scenario.paths = paths
        paths.report_dir.mkdir(parents=True, exist_ok=True)

        bootstrap_log_path = paths.report_dir / "summon-bootstrap.log"
        server_url = f"http://127.0.0.1:{port}/api/v1"

        try:
            print(f"[{profile.profile_id}] summon runtime on {server_url}")
            scenario.start(bootstrap_log_path)
        except Exception as exc:
            with preserve_failure(exc):
                error_text = load_bootstrap_error(paths, bootstrap_log_path) or str(exc)
                runtime.write_text(paths.report_dir / "bootstrap-error.txt", error_text + "\n")
                analysis = build_bootstrap_failure_analysis(profile.profile_id, error_text)
                runtime.write_json(paths.report_dir / "analysis.json", analysis)
                runtime.write_json(paths.report_dir / "analysis-brief.json", brief_from_analysis(analysis))
            raise

        run_request = RunRequest(
            title=profile.title,
            text=profile.prompt_text,
            publish_mode=PUBLISH_MODE_DIRECT,
            global_model=config.common.global_model,
            agent_model_overrides=config.common.agent_model_overrides,
            max_execution_review_cycles=config.common.max_execution_review_cycles,
            max_validation_rework_cycles=config.common.max_validation_rework_cycles,
        )
        print(f"[{profile.profile_id}] create work order")
        run_id = runtime.create_work_order_run(
            server_url,
            paths.credential_path,
            run_request,
            paths.report_dir / "work-order.json",
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
        scenario.require_acceptance(acceptance_checks(analysis))


def load_bootstrap_error(paths: WorkspacePaths, bootstrap_log_path: Path) -> str:
    summon_log_path = paths.workspace_dir / ".kingdom" / "logs" / "summon.log"
    if summon_log_path.exists():
        return summon_log_path.read_text(encoding="utf-8").strip()
    return bootstrap_log_path.read_text(encoding="utf-8").strip() if bootstrap_log_path.exists() else ""
