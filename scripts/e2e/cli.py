from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from scripts.e2e.in_existing_repo import run_in_existing_repo
from scripts.e2e.models import CommonRunOptions, InExistingRepoConfig, ReworkAnalysisConfig, ReworkProfile, PUBLISH_MODE_DIRECT, PUBLISH_MODE_GIT_WORKTREE
from scripts.e2e.rework_analysis import FAST_ITERATION_PROFILE, QUALITY_FIRST_PROFILE, run_rework_analysis


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_REWORK_OUTPUT_PREFIX = "rework-analysis"
DEFAULT_EXISTING_REPO_OUTPUT_PREFIX = "in-existing-repo-e2e"
PROFILE_BY_ID = {
    FAST_ITERATION_PROFILE.profile_id: FAST_ITERATION_PROFILE,
    QUALITY_FIRST_PROFILE.profile_id: QUALITY_FIRST_PROFILE,
}


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    command = str(args.command)
    if command == "rework-analysis":
        run_rework_analysis(build_rework_analysis_config(args), ROOT_DIR)
        return
    if command == "in-existing-repo":
        run_in_existing_repo(build_in_existing_repo_config(args), ROOT_DIR)
        return
    raise RuntimeError(f"unsupported command: {command}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Kingdom E2E maintenance workflows.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    rework_parser = subparsers.add_parser(
        "rework-analysis",
        help="Run profile-based rework analysis E2E flows.",
    )
    add_common_arguments(
        rework_parser,
        default_output_dir=default_output_dir(DEFAULT_REWORK_OUTPUT_PREFIX),
    )
    rework_parser.add_argument(
        "--profile",
        dest="profiles",
        action="append",
        choices=sorted(PROFILE_BY_ID.keys()),
        help="Profile(s) to run. Defaults to all built-in profiles.",
    )
    rework_parser.add_argument(
        "--server-port-base",
        type=positive_int,
        default=18080,
        help="Base port for summon instances.",
    )

    existing_repo_parser = subparsers.add_parser(
        "in-existing-repo",
        help="Run the existing-repo upgrade E2E scenario.",
    )
    add_common_arguments(
        existing_repo_parser,
        default_output_dir=default_output_dir(DEFAULT_EXISTING_REPO_OUTPUT_PREFIX),
    )
    existing_repo_parser.add_argument(
        "--server-port",
        type=positive_int,
        default=18180,
        help="Port for the summon instance.",
    )
    existing_repo_parser.add_argument(
        "--publish-mode",
        choices=[PUBLISH_MODE_DIRECT, PUBLISH_MODE_GIT_WORKTREE],
        default=PUBLISH_MODE_GIT_WORKTREE,
        help="Publish mode for the existing-repo scenario.",
    )
    return parser


def add_common_arguments(
    parser: argparse.ArgumentParser,
    *,
    default_output_dir: Path,
) -> None:
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir,
        help="Output directory for reports and workspaces.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=positive_int,
        default=1800,
        help="Per-run timeout in seconds.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=positive_int,
        default=2,
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--global-model",
        required=True,
        help="Required global model for run creation.",
    )
    parser.add_argument(
        "--agent-model-overrides",
        default="",
        help="Optional role=model overrides.",
    )
    parser.add_argument(
        "--max-execution-review-cycles",
        type=positive_int,
        default=5,
        help="Max execution review cycles.",
    )
    parser.add_argument(
        "--max-validation-rework-cycles",
        type=positive_int,
        default=5,
        help="Max validation rework cycles.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip make install and go install kingdomd.",
    )
    parser.add_argument(
        "--no-keep-workspaces",
        action="store_true",
        help="Remove workspace folders after report generation.",
    )


def build_rework_analysis_config(args: argparse.Namespace) -> ReworkAnalysisConfig:
    common = build_common_options(args)
    profile_ids = args.profiles or sorted(PROFILE_BY_ID.keys())
    profiles = tuple(PROFILE_BY_ID[profile_id] for profile_id in profile_ids)
    return ReworkAnalysisConfig(
        common=common,
        server_port_base=int(args.server_port_base),
        profiles=profiles,
    )


def build_in_existing_repo_config(args: argparse.Namespace) -> InExistingRepoConfig:
    return InExistingRepoConfig(
        common=build_common_options(args),
        server_port=int(args.server_port),
        publish_mode=str(args.publish_mode),
    )


def build_common_options(args: argparse.Namespace) -> CommonRunOptions:
    return CommonRunOptions(
        output_dir=Path(args.output_dir).resolve(),
        timeout_seconds=int(args.timeout_seconds),
        poll_interval_seconds=int(args.poll_interval_seconds),
        global_model=str(args.global_model).strip(),
        agent_model_overrides=str(args.agent_model_overrides).strip(),
        max_execution_review_cycles=int(args.max_execution_review_cycles),
        max_validation_rework_cycles=int(args.max_validation_rework_cycles),
        run_install=not bool(args.skip_install),
        keep_workspaces=not bool(args.no_keep_workspaces),
    )


def positive_int(raw: str) -> int:
    value = int(raw)
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return value


def default_output_dir(prefix: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return ROOT_DIR / "kd-sandbox" / f"{prefix}-{timestamp}"


if __name__ == "__main__":
    main()
