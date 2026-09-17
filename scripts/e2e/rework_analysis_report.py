from __future__ import annotations

import datetime as dt
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_ts(raw: str) -> dt.datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


def summarize_validator_waves(
    validator_reworks_by_attempt: dict[int, list[str]],
    builder_instruction_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    validator_attempts_sorted = sorted(validator_reworks_by_attempt.keys())

    parsed_builder_rows: list[dict[str, Any]] = []
    for item in builder_instruction_rows:
        created_at = parse_ts(str(item.get("created_at") or ""))
        if created_at is None:
            continue
        parsed_builder_rows.append(
            {
                "created_at": created_at,
                "includes_validation_failure_context": bool(
                    item.get("includes_validation_failure_context", False)
                ),
            }
        )

    for index, attempt in enumerate(validator_attempts_sorted):
        reworks = sorted(
            stamp
            for stamp in (
                parse_ts(value)
                for value in validator_reworks_by_attempt.get(attempt, [])
            )
            if stamp is not None
        )
        if not reworks:
            continue

        first_rework = reworks[0]
        last_rework = reworks[-1]

        next_wave_first_failure: dt.datetime | None = None
        if index + 1 < len(validator_attempts_sorted):
            next_attempt = validator_attempts_sorted[index + 1]
            next_wave_first_failure = min(
                (
                    stamp
                    for stamp in (
                        parse_ts(value)
                        for value in validator_reworks_by_attempt.get(next_attempt, [])
                    )
                    if stamp is not None
                ),
                default=None,
            )

        retries_between_first_and_last_failure = 0
        retries_after_wave = 0
        for item in parsed_builder_rows:
            if not item["includes_validation_failure_context"]:
                continue
            created_at = item["created_at"]
            if first_rework <= created_at < last_rework:
                retries_between_first_and_last_failure += 1
            if created_at > last_rework and (
                next_wave_first_failure is None or created_at < next_wave_first_failure
            ):
                retries_after_wave += 1

        summary.append(
            {
                "attempt": attempt,
                "rework_count": len(reworks),
                "first_rework_at": first_rework.isoformat().replace("+00:00", "Z"),
                "last_rework_at": last_rework.isoformat().replace("+00:00", "Z"),
                "retries_between_first_and_last_failure": retries_between_first_and_last_failure,
                "retries_after_wave": retries_after_wave,
                "coalesced": retries_between_first_and_last_failure == 0
                and retries_after_wave <= 1,
            }
        )

    return summary


def classify_provider_timeout(error_text: str, timeout_type: str | None) -> str:
    error = (error_text or "").strip().lower()
    timeout = (timeout_type or "").strip().lower()
    if timeout in {"first_event_timeout", "stream_inactivity_timeout"}:
        return timeout
    if "first_event_timeout" in error:
        return "first_event_timeout"
    if "stream_inactivity_timeout" in error:
        return "stream_inactivity_timeout"
    return ""


def provider_timeout_blockers(timeout_failures: list[dict[str, Any]]) -> dict[str, bool]:
    timeout_types = {
        str(item.get("timeout_type") or "").strip().lower()
        for item in timeout_failures
    }
    return {
        "provider_stream_inactivity_timeout": "stream_inactivity_timeout" in timeout_types,
        "provider_first_event_timeout": "first_event_timeout" in timeout_types,
    }


def build_bootstrap_failure_analysis(profile_id: str, error_text: str) -> dict[str, Any]:
    error = (error_text or "").strip()
    provider_timeout_type = classify_provider_timeout(error, None)
    failure_classification = "bootstrap_failed"
    error_lower = error.lower()
    if provider_timeout_type == "first_event_timeout":
        failure_classification = "provider_first_event_timeout"
    elif provider_timeout_type == "stream_inactivity_timeout":
        failure_classification = "provider_stream_inactivity_timeout"
    elif "signal: killed" in error_lower:
        failure_classification = "bootstrap_process_killed"

    failure_reasons: list[dict[str, Any]] = []
    if error:
        failure_reasons.append(
            {
                "attempt_id": "",
                "reason_code": "bootstrap_runtime",
                "reason_text": "runtime bootstrap failed",
                "error": error,
                "timeout_type": provider_timeout_type or None,
                "stream_diagnostics": None,
                "execution_path_diagnostics": None,
                "execution_diagnostics": None,
                "cancel_diagnostics": None,
                "latest_progress": None,
                "created_at": None,
            }
        )

    return {
        "profile": profile_id,
        "run_id": "",
        "terminal_status": "failed",
        "timeout_status": "bootstrap_failed",
        "failure_classification": failure_classification,
        "historical_failure_reasons": ["bootstrap_runtime"] if error else [],
        "run_metrics": {
            "run_duration_seconds": None,
            "plan_state": "",
            "rework_cycle_count": 0,
            "max_rework_cycles": 0,
        },
        "attempt_metrics": {
            "attempt_count_by_node": {},
            "attempt_durations": [],
            "failure_reasons": failure_reasons,
        },
        "message_metrics": {
            "instruction_count": 0,
            "response_count": 0,
            "mailbox_row_counts": {
                "total_rows": 0,
                "communication_rows": 0,
                "event_style_rows": 0,
            },
            "message_contract_violations": [],
            "authority_violations": [],
            "builder_instruction_sections": [],
            "section_growth_ok": True,
            "builder_overlap_detected": False,
        },
        "rework_propagation": {
            "reviewer_failures": 0,
            "validator_reworks": 0,
            "reviewer_links": [],
            "validator_links": [],
            "reviewer_detail_propagated": False,
            "validator_detail_propagated": False,
            "validator_wave_summary": [],
            "dual_validator_reworks_same_wave": False,
            "coalesced_retry_after_wave_terminal": True,
            "structured_guidance": {
                "review_rework_items_present": 0,
                "review_rework_items_coverage": None,
                "validator_blocking_gates_present": 0,
                "validator_blocking_gates_coverage": None,
                "validator_fix_items_present": 0,
                "validator_fix_items_coverage": None,
            },
            "structured_guidance_warning": {"has_warnings": False, "items": []},
        },
        "model_routing": {
            "global_model": None,
            "agent_model_overrides": {},
            "checks": [],
            "ok": True,
        },
        "blockers": {
            "review_attempt_ceiling_hit": False,
            "no_builder_retry_after_validator_rework": False,
            "invalid_validation_verdict": False,
            "provider_stream_inactivity_timeout": provider_timeout_type
            == "stream_inactivity_timeout",
            "provider_first_event_timeout": provider_timeout_type
            == "first_event_timeout",
        },
        "timeouts": [
            {
                "attempt_id": "",
                "error": error,
                "timeout_type": provider_timeout_type,
                "created_at": None,
            }
        ]
        if provider_timeout_type
        else [],
    }


def structured_guidance_warnings(
    review_failures: int,
    validator_reworks: int,
    review_rework_items_present: int,
    validator_blocking_gates_present: int,
    validator_fix_items_present: int,
) -> dict[str, Any]:
    warnings: list[str] = []
    if review_failures > 0 and review_rework_items_present == 0:
        warnings.append("Reviewer rework responses omitted optional REWORK_ITEMS sections.")
    if validator_reworks > 0 and validator_blocking_gates_present == 0:
        warnings.append(
            "Validator rework responses omitted optional BLOCKING_GATES sections."
        )
    if validator_reworks > 0 and validator_fix_items_present == 0:
        warnings.append("Validator rework responses omitted optional FIX_ITEMS sections.")
    return {"has_warnings": len(warnings) > 0, "items": warnings}


def build_rework_analysis(
    profile_id: str,
    run_id: str,
    report_dir: Path,
    db_path: Path,
    timeout_status: str,
    global_model: str,
    agent_model_overrides_raw: str,
) -> dict[str, Any]:
    run_final = json.loads((report_dir / "run-final.json").read_text(encoding="utf-8"))
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    run_row = connection.execute(
        (
            "SELECT run_status, plan_state, created_at, updated_at, rework_cycle_count, "
            "max_rework_cycles FROM workflow_runs WHERE run_id = ?"
        ),
        (run_id,),
    ).fetchone()
    node_rows = connection.execute(
        (
            "SELECT node_id, attempt, node_status, started_at, finished_at FROM node_runs "
            "WHERE run_id = ? ORDER BY node_id, attempt"
        ),
        (run_id,),
    ).fetchall()
    token_rows = connection.execute(
        "SELECT required_role, model FROM token_usage WHERE run_id = ? ORDER BY created_at, usage_id",
        (run_id,),
    ).fetchall()
    event_fail_rows = connection.execute(
        (
            "SELECT attempt_id, reason_code, reason_text, payload_json, created_at FROM events "
            "WHERE run_id = ? AND event_type = 'attempt.failed' ORDER BY created_at, attempt_id"
        ),
        (run_id,),
    ).fetchall()
    msg_rows = connection.execute(
        (
            "SELECT message_id, from_agent_id, to_agent_id, created_at, "
            "COALESCE(json_extract(payload_json, '$.communication_type'), '') AS communication_type, "
            "COALESCE(json_extract(payload_json, '$.node_id'), '') AS node_id, "
            "COALESCE(json_extract(payload_json, '$.attempt_id'), '') AS attempt_id, "
            "COALESCE(json_extract(payload_json, '$.reason_code'), '') AS reason_code, "
            "COALESCE(json_extract(payload_json, '$.instruction_full'), '') AS instruction_full, "
            "COALESCE(json_extract(payload_json, '$.response_full'), '') AS response_full, "
            "COALESCE(json_extract(payload_json, '$.delegator_instruction_scoped'), '') AS delegator_instruction_scoped "
            "FROM mailbox_messages WHERE run_id = ? ORDER BY created_at, message_id"
        ),
        (run_id,),
    ).fetchall()
    connection.close()

    expected_overrides = parse_agent_model_overrides(agent_model_overrides_raw)
    observed_models_by_role: dict[str, list[str]] = defaultdict(list)
    for row in token_rows:
        role = str(row["required_role"] or "").strip()
        model = str(row["model"] or "").strip()
        if role and model:
            observed_models_by_role[role].append(model)

    model_routing_checks: list[dict[str, Any]] = []
    if global_model.strip() or expected_overrides:
        expected_roles = set(observed_models_by_role.keys()) | set(expected_overrides.keys())
        for role in sorted(expected_roles):
            expected_model = expected_overrides.get(role) or global_model.strip()
            if not expected_model:
                continue
            observed = observed_models_by_role.get(role, [])
            model_routing_checks.append(
                {
                    "role": role,
                    "expected_model": expected_model,
                    "observed_models": observed,
                    "matched": expected_model in observed,
                }
            )
    model_routing_ok = all(item["matched"] for item in model_routing_checks) if model_routing_checks else True

    run_started = parse_ts(str(run_row["created_at"] if run_row else ""))
    run_ended = parse_ts(str(run_row["updated_at"] if run_row else ""))
    run_duration_seconds: float | None = None
    if run_started is not None and run_ended is not None:
        run_duration_seconds = round((run_ended - run_started).total_seconds(), 3)

    attempt_durations: list[dict[str, Any]] = []
    attempt_count_by_node: dict[str, int] = {}
    latest_status_by_node: dict[str, str] = {}
    for row in node_rows:
        node_id = str(row["node_id"])
        attempt = int(row["attempt"])
        attempt_count_by_node[node_id] = max(attempt_count_by_node.get(node_id, 0), attempt)
        latest_status_by_node[node_id] = str(row["node_status"])
        started = parse_ts(str(row["started_at"] or ""))
        finished = parse_ts(str(row["finished_at"] or ""))
        duration_seconds: float | None = None
        if started is not None and finished is not None:
            duration_seconds = round((finished - started).total_seconds(), 3)
        attempt_durations.append(
            {
                "node_id": node_id,
                "attempt": attempt,
                "status": str(row["node_status"]),
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "duration_seconds": duration_seconds,
            }
        )

    failure_reasons: list[dict[str, Any]] = []
    timeout_failures: list[dict[str, Any]] = []
    for row in event_fail_rows:
        payload: dict[str, Any] = {}
        raw_payload = str(row["payload_json"] or "")
        if raw_payload:
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                payload = {"raw": raw_payload}
        error_text = str(payload.get("error", ""))
        failure_reasons.append(
            {
                "attempt_id": row["attempt_id"],
                "reason_code": row["reason_code"],
                "reason_text": row["reason_text"],
                "error": error_text,
                "timeout_type": payload.get("timeout_type"),
                "stream_diagnostics": payload.get("stream_diagnostics"),
                "execution_path_diagnostics": payload.get("execution_path_diagnostics"),
                "execution_diagnostics": payload.get("execution_diagnostics"),
                "cancel_diagnostics": payload.get("cancel_diagnostics"),
                "latest_progress": payload.get("latest_progress"),
                "created_at": row["created_at"],
            }
        )
        provider_timeout_type = classify_provider_timeout(error_text, payload.get("timeout_type"))
        if provider_timeout_type:
            timeout_failures.append(
                {
                    "attempt_id": row["attempt_id"],
                    "error": error_text,
                    "timeout_type": provider_timeout_type,
                    "created_at": row["created_at"],
                }
            )

    instructions: dict[tuple[str, int | None], sqlite3.Row] = {}
    responses: dict[tuple[str, int | None], sqlite3.Row] = {}
    message_contract_violations: list[dict[str, Any]] = []
    authority_violations: list[dict[str, Any]] = []
    builder_instruction_sections: list[dict[str, Any]] = []
    review_failures: list[sqlite3.Row] = []
    validator_reworks: list[sqlite3.Row] = []
    communication_rows: list[sqlite3.Row] = []
    event_style_rows: list[sqlite3.Row] = []

    for row in msg_rows:
        if row["communication_type"] in {"instruction_dispatched", "response_received"}:
            communication_rows.append(row)
        else:
            event_style_rows.append(row)

    for row in communication_rows:
        communication_type = str(row["communication_type"])
        node_id = str(row["node_id"])
        attempt_id = str(row["attempt_id"])
        attempt_number = attempt_from_attempt_id(attempt_id)
        key = (node_id, attempt_number)
        reason = str(row["reason_code"] or "").strip().lower()
        if communication_type == "instruction_dispatched":
            instructions[key] = row
            if node_id.startswith("delegated_execution"):
                text = str(row["instruction_full"] or "")
                builder_instruction_sections.append(
                    {
                        "message_id": row["message_id"],
                        "attempt": attempt_number,
                        "primary_plan_count": text.count("Primary Council Rework Plan:"),
                        "failure_context_count": text.count("Failure Context:"),
                        "scoped_instruction_count": text.count("Scoped Delegator Instruction:"),
                    }
                )
                if "council" not in str(row["from_agent_id"] or ""):
                    authority_violations.append(
                        {
                            "message_id": row["message_id"],
                            "node_id": node_id,
                            "expected_from_contains": "council",
                            "actual_from": row["from_agent_id"],
                        }
                    )
            if node_id.startswith("execution_review") and "council" not in str(row["from_agent_id"] or ""):
                authority_violations.append(
                    {
                        "message_id": row["message_id"],
                        "node_id": node_id,
                        "expected_from_contains": "council",
                        "actual_from": row["from_agent_id"],
                    }
                )
            if node_id.startswith("validation_gates") and "council" not in str(row["from_agent_id"] or ""):
                authority_violations.append(
                    {
                        "message_id": row["message_id"],
                        "node_id": node_id,
                        "expected_from_contains": "council",
                        "actual_from": row["from_agent_id"],
                    }
                )
        if communication_type == "response_received":
            responses[key] = row
            if node_id.startswith("execution_review") and reason == "execution_review_rework":
                review_failures.append(row)
            if node_id.startswith("validation_gates") and reason == "validation_gate_rework":
                validator_reworks.append(row)

    for key, instruction in instructions.items():
        node_id, attempt_number = key
        if attempt_number is None:
            continue
        if key not in responses and latest_status_by_node.get(node_id, "") not in {"running", "pending"}:
            message_contract_violations.append(
                {
                    "node_id": node_id,
                    "attempt": attempt_number,
                    "instruction_message_id": instruction["message_id"],
                    "issue": "missing_response_received",
                }
            )

    builder_instruction_rows: list[dict[str, Any]] = []
    for key, row in instructions.items():
        node_id, attempt_number = key
        if node_id.startswith("delegated_execution") and attempt_number is not None:
            builder_instruction_rows.append(
                {
                    "attempt": attempt_number,
                    "created_at": row["created_at"],
                    "created_ts": parse_ts(str(row["created_at"] or "")),
                    "row": row,
                }
            )
    builder_instruction_rows.sort(
        key=lambda item: (item["created_ts"] is None, item["created_ts"], item["attempt"])
    )

    builder_overlap_detected = False
    builder_attempt_windows: list[dict[str, Any]] = []
    for row in attempt_durations:
        node_id = str(row.get("node_id") or "")
        if not node_id.startswith("delegated_execution"):
            continue
        started = parse_ts(str(row.get("started_at") or ""))
        finished = parse_ts(str(row.get("finished_at") or ""))
        if started is None:
            continue
        builder_attempt_windows.append(
            {"attempt": row.get("attempt"), "started": started, "finished": finished}
        )
    for index, left in enumerate(builder_attempt_windows):
        for right in builder_attempt_windows[index + 1 :]:
            if left["finished"] is None or right["finished"] is None:
                continue
            if left["started"] < right["finished"] and right["started"] < left["finished"]:
                builder_overlap_detected = True
                break
        if builder_overlap_detected:
            break

    reviewer_links: list[dict[str, Any]] = []
    review_rework_items_present = 0
    for row in review_failures:
        attempt_number = attempt_from_attempt_id(str(row["attempt_id"] or ""))
        if attempt_number is None:
            continue
        next_builder_item = next_builder_instruction_after(builder_instruction_rows, str(row["created_at"] or ""))
        next_builder = next_builder_item["row"] if next_builder_item is not None else None
        reviewer_snippet = first_detail_line(str(row["response_full"] or ""), {"VERDICT:"})
        if "REWORK_ITEMS:" in str(row["response_full"] or ""):
            review_rework_items_present += 1
        instruction_text = str(next_builder["instruction_full"] if next_builder is not None else "")
        reviewer_links.append(
            {
                "review_message_id": row["message_id"],
                "review_attempt": attempt_number,
                "failure_created_at": row["created_at"],
                "builder_retry_attempt": next_builder_item["attempt"] if next_builder_item is not None else None,
                "builder_instruction_created_at": next_builder_item["created_at"] if next_builder_item is not None else "",
                "builder_instruction_message_id": next_builder["message_id"] if next_builder is not None else "",
                "has_builder_retry": next_builder is not None,
                "contains_review_failure_marker": "[execution_review" in instruction_text,
                "snippet_match": bool(reviewer_snippet and reviewer_snippet in instruction_text),
                "snippet": reviewer_snippet,
            }
        )

    validator_links: list[dict[str, Any]] = []
    validator_blocking_gates_present = 0
    validator_fix_items_present = 0
    for row in validator_reworks:
        attempt_number = attempt_from_attempt_id(str(row["attempt_id"] or ""))
        if attempt_number is None:
            continue
        next_builder_item = next_builder_instruction_after(builder_instruction_rows, str(row["created_at"] or ""))
        next_builder = next_builder_item["row"] if next_builder_item is not None else None
        validator_snippet = first_detail_line(str(row["response_full"] or ""), {"VERDICT:"})
        if "BLOCKING_GATES:" in str(row["response_full"] or ""):
            validator_blocking_gates_present += 1
        if "FIX_ITEMS:" in str(row["response_full"] or ""):
            validator_fix_items_present += 1
        instruction_text = str(next_builder["instruction_full"] if next_builder is not None else "")
        node_marker = f"[{row['node_id']} (validation_gate_rework)]"
        validator_links.append(
            {
                "validator_message_id": row["message_id"],
                "validator_node": row["node_id"],
                "validator_attempt": attempt_number,
                "failure_created_at": row["created_at"],
                "builder_retry_attempt": next_builder_item["attempt"] if next_builder_item is not None else None,
                "builder_instruction_created_at": next_builder_item["created_at"] if next_builder_item is not None else "",
                "builder_instruction_message_id": next_builder["message_id"] if next_builder is not None else "",
                "has_builder_retry": next_builder is not None,
                "contains_validator_node_marker": node_marker in instruction_text,
                "snippet_match": bool(validator_snippet and validator_snippet in instruction_text),
                "snippet": validator_snippet,
            }
        )

    validator_reworks_by_attempt: dict[int, list[dt.datetime]] = defaultdict(list)
    for row in validator_reworks:
        attempt_number = attempt_from_attempt_id(str(row["attempt_id"] or ""))
        created_ts = parse_ts(str(row["created_at"] or ""))
        if attempt_number is not None and created_ts is not None:
            validator_reworks_by_attempt[attempt_number].append(created_ts)

    validator_nodes = sorted(
        {
            str(row.get("node_id"))
            for row in attempt_durations
            if str(row.get("node_id") or "").startswith("validation_gates")
        }
    )
    terminal_attempt_statuses = {"succeeded", "failed", "cancelled"}
    for item in validator_links:
        attempt_number = item.get("validator_attempt")
        status_by_node = {
            str(row.get("node_id")): str(row.get("status") or "").strip().lower()
            for row in attempt_durations
            if str(row.get("node_id") or "").startswith("validation_gates")
            and row.get("attempt") == attempt_number
        }
        item["rework_wave_settled"] = bool(validator_nodes) and len(status_by_node) == len(
            validator_nodes
        ) and all(
            status_by_node.get(node_id, "") in terminal_attempt_statuses
            for node_id in validator_nodes
        )

    validator_reworks_by_attempt_iso = {
        attempt: [stamp.isoformat().replace("+00:00", "Z") for stamp in stamps]
        for attempt, stamps in validator_reworks_by_attempt.items()
    }
    builder_retry_rows = [
        {
            "created_at": item["created_ts"].isoformat().replace("+00:00", "Z")
            if item["created_ts"] is not None
            else "",
            "includes_validation_failure_context": "Validation Rework"
            in str(item["row"]["instruction_full"] or ""),
        }
        for item in builder_instruction_rows
    ]
    validator_wave_summary = summarize_validator_waves(
        validator_reworks_by_attempt_iso,
        builder_retry_rows,
    )
    coalesced_retry_after_wave_terminal = all(
        item["coalesced"] for item in validator_wave_summary
    ) if validator_wave_summary else True
    dual_validator_reworks_same_wave = any(
        item["rework_count"] > 1 for item in validator_wave_summary
    )

    section_growth_ok = all(
        row["primary_plan_count"] <= 1
        and row["failure_context_count"] <= 1
        and row["scoped_instruction_count"] <= 1
        for row in builder_instruction_sections
    )

    terminal_status = str(
        run_final.get("status") or (run_row["run_status"] if run_row is not None else "")
    ).strip()
    historical_failure_reasons = sorted(
        {
            str(item.get("reason_code") or "").strip()
            for item in failure_reasons
            if str(item.get("reason_code") or "").strip()
        }
    )
    review_attempt_ceiling_hit = any(
        "max attempts reached" in str(item.get("error") or "").lower()
        and "execution_review" in str(item.get("error") or "").lower()
        for item in failure_reasons
    )
    no_builder_retry_after_validator_rework = any(
        bool(item.get("rework_wave_settled")) and not bool(item["has_builder_retry"])
        for item in validator_links
    )
    invalid_response_hit = any(
        str(item.get("reason_code") or "") == "llm_invalid_response"
        for item in failure_reasons
    )
    node_wall_clock_timeout_hit = any(
        str(item.get("reason_code") or "") == "timeout" for item in failure_reasons
    )

    structured_guidance = {
        "review_rework_items_present": review_rework_items_present,
        "review_rework_items_coverage": review_rework_items_present / len(review_failures)
        if review_failures
        else None,
        "validator_blocking_gates_present": validator_blocking_gates_present,
        "validator_blocking_gates_coverage": validator_blocking_gates_present
        / len(validator_reworks)
        if validator_reworks
        else None,
        "validator_fix_items_present": validator_fix_items_present,
        "validator_fix_items_coverage": validator_fix_items_present / len(validator_reworks)
        if validator_reworks
        else None,
    }
    structured_guidance_warning = structured_guidance_warnings(
        review_failures=len(review_failures),
        validator_reworks=len(validator_reworks),
        review_rework_items_present=review_rework_items_present,
        validator_blocking_gates_present=validator_blocking_gates_present,
        validator_fix_items_present=validator_fix_items_present,
    )

    failure_classification = "none"
    if terminal_status.lower() == "succeeded":
        failure_classification = "none"
    elif timeout_status == "timeout":
        failure_classification = "script_timeout"
    elif review_attempt_ceiling_hit:
        failure_classification = "review_attempt_ceiling_hit"
    elif no_builder_retry_after_validator_rework:
        failure_classification = "no_builder_retry_after_validator_rework"
    elif invalid_response_hit:
        failure_classification = "invalid_response"
    elif timeout_failures:
        timeout_types = {str(item.get("timeout_type") or "") for item in timeout_failures}
        if timeout_types == {"first_event_timeout"}:
            failure_classification = "provider_first_event_timeout"
        elif timeout_types == {"stream_inactivity_timeout"}:
            failure_classification = "provider_stream_inactivity_timeout"
        else:
            failure_classification = "timeout"
    elif node_wall_clock_timeout_hit:
        failure_classification = "node_wall_clock_timeout"
    elif any(str(item.get("reason_code") or "") == "validation_gate_rework" for item in failure_reasons):
        failure_classification = "validation_gate_rework"
    elif any(str(item.get("reason_code") or "") == "execution_review_rework" for item in failure_reasons):
        failure_classification = "execution_review_rework"
    elif terminal_status == "failed":
        failure_classification = "failed_other"

    analysis = {
        "profile": profile_id,
        "run_id": run_id,
        "terminal_status": terminal_status,
        "timeout_status": timeout_status,
        "failure_classification": failure_classification,
        "historical_failure_reasons": historical_failure_reasons,
        "run_metrics": {
            "run_duration_seconds": run_duration_seconds,
            "plan_state": run_row["plan_state"] if run_row is not None else "",
            "rework_cycle_count": int(run_row["rework_cycle_count"] or 0) if run_row is not None else 0,
            "max_rework_cycles": int(run_row["max_rework_cycles"] or 0) if run_row is not None else 0,
        },
        "attempt_metrics": {
            "attempt_count_by_node": attempt_count_by_node,
            "attempt_durations": attempt_durations,
            "failure_reasons": failure_reasons,
        },
        "message_metrics": {
            "instruction_count": len(instructions),
            "response_count": len(responses),
            "mailbox_row_counts": {
                "total_rows": len(msg_rows),
                "communication_rows": len(communication_rows),
                "event_style_rows": len(event_style_rows),
            },
            "message_contract_violations": message_contract_violations,
            "authority_violations": authority_violations,
            "builder_instruction_sections": builder_instruction_sections,
            "section_growth_ok": section_growth_ok,
            "builder_overlap_detected": builder_overlap_detected,
        },
        "rework_propagation": {
            "reviewer_failures": len(review_failures),
            "validator_reworks": len(validator_reworks),
            "reviewer_links": reviewer_links,
            "validator_links": validator_links,
            "reviewer_detail_propagated": any(
                item["snippet_match"] or item["contains_review_failure_marker"]
                for item in reviewer_links
            ),
            "validator_detail_propagated": any(
                item["snippet_match"] or item["contains_validator_node_marker"]
                for item in validator_links
            ),
            "validator_wave_summary": validator_wave_summary,
            "dual_validator_reworks_same_wave": dual_validator_reworks_same_wave,
            "coalesced_retry_after_wave_terminal": coalesced_retry_after_wave_terminal,
            "structured_guidance": structured_guidance,
            "structured_guidance_warning": structured_guidance_warning,
        },
        "model_routing": {
            "global_model": global_model.strip() or None,
            "agent_model_overrides": expected_overrides,
            "checks": model_routing_checks,
            "ok": model_routing_ok,
        },
        "blockers": {
            "review_attempt_ceiling_hit": review_attempt_ceiling_hit,
            "no_builder_retry_after_validator_rework": no_builder_retry_after_validator_rework,
            "invalid_response": invalid_response_hit,
            "provider_stream_inactivity_timeout": False,
            "provider_first_event_timeout": False,
        },
        "timeouts": timeout_failures,
    }

    timeout_blockers = provider_timeout_blockers(timeout_failures)
    analysis["blockers"]["provider_stream_inactivity_timeout"] = timeout_blockers[
        "provider_stream_inactivity_timeout"
    ]
    analysis["blockers"]["provider_first_event_timeout"] = timeout_blockers[
        "provider_first_event_timeout"
    ]
    return analysis


def brief_from_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "profile": analysis.get("profile"),
        "run_id": analysis.get("run_id"),
        "terminal_status": analysis.get("terminal_status"),
        "failure_classification": analysis.get("failure_classification"),
        "section_growth_ok": analysis.get("message_metrics", {}).get("section_growth_ok", True),
        "model_routing_ok": analysis.get("model_routing", {}).get("ok", True),
    }


def write_summary(output_dir: Path) -> tuple[dict[str, Any], str]:
    flow_reports: list[dict[str, Any]] = []
    for analysis_path in sorted(output_dir.glob("*/report/analysis.json")):
        flow_reports.append(json.loads(analysis_path.read_text(encoding="utf-8")))
    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "profiles": flow_reports,
        "overall": {
            "profile_count": len(flow_reports),
            "all_section_growth_ok": all(
                profile["message_metrics"]["section_growth_ok"] for profile in flow_reports
            )
            if flow_reports
            else False,
            "any_provider_timeout": any(
                str(profile["failure_classification"]).startswith("provider_")
                for profile in flow_reports
            ),
        },
    }
    lines = [
        f"Rework analysis summary: {output_dir}",
        f"Profiles analyzed: {summary['overall']['profile_count']}",
        f"All section growth checks passed: {summary['overall']['all_section_growth_ok']}",
        f"Any provider timeout: {summary['overall']['any_provider_timeout']}",
    ]
    for profile in flow_reports:
        lines.append(
            "- "
            + f"{profile['profile']} run_id={profile['run_id']} status={profile['terminal_status']} "
            + f"failure={profile['failure_classification']} "
            + f"reviewer_propagated={profile['rework_propagation']['reviewer_detail_propagated']} "
            + f"validator_propagated={profile['rework_propagation']['validator_detail_propagated']} "
            + f"validator_wave_coalesced={profile['rework_propagation'].get('coalesced_retry_after_wave_terminal', True)} "
            + f"builder_overlap={profile['message_metrics'].get('builder_overlap_detected', False)} "
            + f"structured_warnings={len((profile['rework_propagation'].get('structured_guidance_warning') or {}).get('items', []))}"
        )
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


def first_detail_line(text: str, control_prefixes: set[str]) -> str:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if any(line.upper().startswith(prefix) for prefix in control_prefixes):
            continue
        return line
    return ""


def attempt_from_attempt_id(attempt_id: str) -> int | None:
    match = re.search(r":(\d+)$", attempt_id or "")
    if match is None:
        return None
    return int(match.group(1))


def next_builder_instruction_after(
    builder_instruction_rows: list[dict[str, Any]],
    created_at_raw: str,
) -> dict[str, Any] | None:
    created_ts = parse_ts(created_at_raw)
    if created_ts is None:
        return None
    for item in builder_instruction_rows:
        item_ts = item["created_ts"]
        if item_ts is None:
            continue
        if item_ts >= created_ts:
            return item
    return None
