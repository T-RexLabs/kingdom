from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_WORKFLOW_ID = "work-order-v1"
DEFAULT_BASE_PATH = "/api/v1"
PUBLISH_MODE_DIRECT = "direct"
PUBLISH_MODE_GIT_WORKTREE = "git_worktree"


@dataclass(frozen=True)
class CommonRunOptions:
    output_dir: Path
    timeout_seconds: int
    poll_interval_seconds: int
    global_model: str
    agent_model_overrides: str
    max_execution_review_cycles: int
    max_validation_rework_cycles: int
    run_install: bool
    keep_workspaces: bool


@dataclass(frozen=True)
class ReworkProfile:
    profile_id: str
    title: str
    prompt_text: str


@dataclass(frozen=True)
class ReworkAnalysisConfig:
    common: CommonRunOptions
    server_port_base: int
    profiles: tuple[ReworkProfile, ...]


@dataclass(frozen=True)
class InExistingRepoConfig:
    common: CommonRunOptions
    server_port: int
    publish_mode: str


@dataclass(frozen=True)
class WorkspacePaths:
    scenario_dir: Path
    workspace_dir: Path
    report_dir: Path
    config_path: Path
    db_path: Path
    credential_path: Path


@dataclass(frozen=True)
class RunRequest:
    title: str
    text: str
    workflow_id: str
    publish_mode: str
    global_model: str
    agent_model_overrides: str
    max_execution_review_cycles: int
    max_validation_rework_cycles: int


@dataclass(frozen=True)
class PollResult:
    terminal_status: str
    history_path: Path


@dataclass(frozen=True)
class WorktreeRecord:
    run_id: str
    repo_common_dir: str
    main_worktree_root: str
    project_relpath: str
    worktree_name: str
    worktree_path: str
    seed_commit_oid: str
    state: str
    created_at: str
    last_used_at: str
    cleanup_after: str | None
    cleaned_at: str | None
    failure_reason: str
