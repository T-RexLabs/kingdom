from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Sequence


TARGET_PATTERNS = (
    "README.md",
    "package.json",
    "src/",
    "test/",
    "docs/upgrade-notes.md",
    "config/",
)


def parse_changed_paths(lines: Sequence[str]) -> list[str]:
    changed: list[str] = []
    valid_status_chars = set(" MADRCU?!")
    for raw_line in lines:
        line = raw_line.rstrip("\n")
        if not line or line.startswith("## "):
            continue
        if len(line) < 4 or line[2] != " ":
            continue
        if line[0] not in valid_status_chars or line[1] not in valid_status_chars:
            continue
        path = line[3:].strip()
        if path:
            changed.append(path)
    return changed


def touches_target_paths(paths: Sequence[str]) -> bool:
    for path in paths:
        for pattern in TARGET_PATTERNS:
            if pattern.endswith("/"):
                if path.startswith(pattern):
                    return True
                continue
            if path == pattern or path.startswith(f"{pattern} -> "):
                return True
    return False


def build_bootstrap_failure_analysis(error_text: str, publish_mode: str) -> dict[str, Any]:
    error = error_text.strip()
    checks = {
        "terminal_ok": False,
        "execution_not_timeout": False,
        "communication_or_event_evidence_present": False,
        "model_routing_ok": False,
        "publish_mode_matches_request": False,
    }
    if publish_mode == "git_worktree":
        checks.update(
            {
                "run_worktree_record_present": False,
                "worktree_exists": False,
                "main_repo_non_kingdom_clean": False,
                "worktree_has_non_kingdom_changes": False,
                "seeded_targets_touched": False,
            }
        )
    else:
        checks.update(
            {
                "main_repo_has_non_kingdom_changes": False,
                "seeded_targets_touched": False,
            }
        )
    return {
        "run_id": "",
        "publish_mode": publish_mode,
        "terminal_status": "failed",
        "timeout_status": "bootstrap_failed",
        "pass": False,
        "checks": checks,
        "observations": {
            "message_count": 0,
            "event_count": 0,
            "main_changed_paths": [],
            "main_changed_non_kingdom": [],
            "worktree_changed_paths": [],
            "worktree_changed_non_kingdom": [],
            "model_routing": [],
            "worktree_record": {"exists": False},
            "bootstrap_error": error,
        },
    }


def build_in_existing_repo_analysis(
    report_dir: Path,
    run_id: str,
    timeout_status: str,
    requested_publish_mode: str,
    global_model: str,
    agent_model_overrides_raw: str,
) -> dict[str, Any]:
    run_final = json.loads((report_dir / "run-final.json").read_text(encoding="utf-8"))
    run_payload = run_final.get("run") or run_final
    terminal_status = str(run_payload.get("run_status") or run_payload.get("status") or "").strip()
    actual_publish_mode = str(run_payload.get("publish_mode") or "").strip()

    main_status_lines = (report_dir / "git-main-status-after.txt").read_text(encoding="utf-8").splitlines()
    worktree_status_lines = (report_dir / "git-worktree-status.txt").read_text(encoding="utf-8").splitlines()
    worktree_record = json.loads((report_dir / "run-worktree.json").read_text(encoding="utf-8"))
    messages = json.loads((report_dir / "messages.json").read_text(encoding="utf-8"))
    db_events = json.loads((report_dir / "db-events.json").read_text(encoding="utf-8"))
    token_usage = json.loads((report_dir / "db-token-usage.json").read_text(encoding="utf-8"))

    main_changed = parse_changed_paths(main_status_lines)
    worktree_changed = parse_changed_paths(worktree_status_lines)
    main_changed_non_kingdom = [
        path for path in main_changed if not (path == ".kingdom" or path.startswith(".kingdom/"))
    ]
    worktree_changed_non_kingdom = [
        path
        for path in worktree_changed
        if not (path == ".kingdom" or path.startswith(".kingdom/"))
    ]

    message_items = messages.get("items") or []
    has_instruction_message = any(
        (item.get("payload") or {}).get("communication_type") == "instruction_dispatched"
        for item in message_items
    )
    has_response_message = any(
        (item.get("payload") or {}).get("communication_type") == "response_received"
        for item in message_items
    )
    event_count = len(db_events if isinstance(db_events, list) else [])
    terminal_ok = terminal_status in {"succeeded", "awaiting_human_confirmation"}
    execution_ok = timeout_status != "timeout"
    mailbox_ok = (has_instruction_message and has_response_message) or event_count > 0
    publish_mode_matches_request = actual_publish_mode == requested_publish_mode

    expected_overrides = parse_agent_model_overrides(agent_model_overrides_raw)
    observed_by_role: dict[str, set[str]] = {}
    if isinstance(token_usage, list):
        for row in token_usage:
            role = str(row.get("required_role") or "").strip()
            model = str(row.get("model") or "").strip()
            if role and model:
                observed_by_role.setdefault(role, set()).add(model)

    model_routing_checks: list[dict[str, Any]] = []
    if global_model.strip() or expected_overrides:
        expected_roles = set(observed_by_role.keys()) | set(expected_overrides.keys())
        for role in sorted(expected_roles):
            expected = expected_overrides.get(role) or global_model.strip()
            if not expected:
                continue
            observed = sorted(observed_by_role.get(role, set()))
            model_routing_checks.append(
                {
                    "role": role,
                    "expected_model": expected,
                    "observed_models": observed,
                    "matched": expected in observed,
                }
            )
    model_routing_ok = all(item["matched"] for item in model_routing_checks) if model_routing_checks else not (global_model.strip() or expected_overrides)

    checks: dict[str, bool] = {
        "terminal_ok": terminal_ok,
        "execution_not_timeout": execution_ok,
        "communication_or_event_evidence_present": mailbox_ok,
        "model_routing_ok": model_routing_ok,
        "publish_mode_matches_request": publish_mode_matches_request,
    }
    if requested_publish_mode == "git_worktree":
        checks.update(
            {
                "run_worktree_record_present": bool(worktree_record.get("exists")),
                "worktree_exists": bool(worktree_record.get("exists"))
                and Path(str(worktree_record.get("worktree_path") or "")).is_dir(),
                "main_repo_non_kingdom_clean": len(main_changed_non_kingdom) == 0,
                "worktree_has_non_kingdom_changes": len(worktree_changed_non_kingdom) > 0,
                "seeded_targets_touched": touches_target_paths(worktree_changed_non_kingdom),
            }
        )
    else:
        checks.update(
            {
                "main_repo_has_non_kingdom_changes": len(main_changed_non_kingdom) > 0,
                "seeded_targets_touched": touches_target_paths(main_changed_non_kingdom),
            }
        )

    return {
        "run_id": run_id,
        "publish_mode": requested_publish_mode,
        "terminal_status": terminal_status,
        "timeout_status": timeout_status,
        "pass": all(checks.values()),
        "checks": checks,
        "observations": {
            "actual_publish_mode": actual_publish_mode,
            "main_changed_paths": main_changed,
            "main_changed_non_kingdom": main_changed_non_kingdom,
            "worktree_changed_paths": worktree_changed,
            "worktree_changed_non_kingdom": worktree_changed_non_kingdom,
            "message_count": len(message_items),
            "event_count": event_count,
            "model_routing": model_routing_checks,
            "worktree_record": worktree_record,
        },
    }


def brief_from_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": analysis.get("run_id"),
        "terminal_status": analysis.get("terminal_status"),
        "publish_mode": analysis.get("publish_mode"),
        "pass": analysis.get("pass"),
    }


def write_summary(output_dir: Path) -> tuple[dict[str, Any], str]:
    analysis_path = output_dir / "report" / "analysis.json"
    analysis = json.loads(analysis_path.read_text(encoding="utf-8")) if analysis_path.exists() else {}
    harness_path = output_dir / "report" / "harness-result.json"
    harness = json.loads(harness_path.read_text(encoding="utf-8")) if harness_path.exists() else {}
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "output_dir": str(output_dir),
        "scenario": "in-existing-repo",
        "run_id": analysis.get("run_id"),
        "publish_mode": analysis.get("publish_mode"),
        "pass": analysis.get("pass") is True and harness.get("status") == "passed" and harness.get("runtime_stopped") is True,
        "checks": analysis.get("checks"),
        "harness_result": harness,
    }
    lines = [
        f"In-existing-repo E2E summary: {output_dir}",
        f"run_id={summary['run_id']} publish_mode={summary['publish_mode']} pass={summary['pass']}",
    ]
    for key, value in (summary.get("checks") or {}).items():
        lines.append(f"- {key}={value}")
    return summary, "\n".join(lines) + "\n"


def parse_agent_model_overrides(raw: str) -> dict[str, str]:
    expected_overrides: dict[str, str] = {}
    for pair in raw.split(","):
        entry = pair.strip()
        if not entry or "=" not in entry:
            continue
        role, model = entry.split("=", 1)
        role = role.strip()
        model = model.strip()
        if role and model:
            expected_overrides[role] = model
    return expected_overrides
