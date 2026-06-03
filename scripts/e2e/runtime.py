from __future__ import annotations

import json
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

from scripts.e2e.models import DEFAULT_BASE_PATH, DEFAULT_WORKFLOW_ID, PollResult, RunRequest, WorktreeRecord, WorkspacePaths


class CommandError(RuntimeError):
    def __init__(
        self,
        args: Sequence[str],
        return_code: int,
        stdout: str,
        stderr: str,
    ) -> None:
        self.args_list = list(args)
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr
        command = " ".join(self.args_list)
        details = stderr.strip() or stdout.strip() or f"exit code {return_code}"
        super().__init__(f"command failed: {command}: {details}")


class RuntimeService:
    """Orchestrates local `kd` runtime setup and evidence capture."""

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir

    def ensure_required_commands(
        self,
        *,
        include_git: bool = False,
        include_go: bool = False,
    ) -> None:
        required = ["kd"]
        if include_go:
            required.append("go")
        if include_git:
            required.append("git")
        missing = [command for command in required if shutil.which(command) is None]
        if missing:
            joined = ", ".join(sorted(missing))
            raise RuntimeError(f"required command not found: {joined}")

    def install_binaries(self) -> None:
        self.run_command(["make", "-C", str(self._root_dir), "install"])
        self.run_command(["go", "install", "./cmd/kingdomd"], cwd=self._root_dir)

    def ensure_port_available(self, port: int) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError as exc:
                raise RuntimeError(f"port 127.0.0.1:{port} is unavailable") from exc

    def init_workspace(self, workspace_dir: Path, port: int, publish_mode: str) -> WorkspacePaths:
        workspace_dir.mkdir(parents=True, exist_ok=True)
        self.run_command(
            [
                "kd",
                "init",
                "--path",
                str(workspace_dir),
                "--publish-mode",
                publish_mode,
                "--provider-engine",
                "opencode",
                "--default-workflow-id",
                DEFAULT_WORKFLOW_ID,
                "--listen",
                f"127.0.0.1:{port}",
                "--base-path",
                DEFAULT_BASE_PATH,
            ]
        )
        return WorkspacePaths(
            scenario_dir=workspace_dir.parent,
            workspace_dir=workspace_dir,
            report_dir=workspace_dir.parent / "report",
            config_path=workspace_dir / ".kingdom" / "config.yaml",
            db_path=workspace_dir / ".kingdom" / "kingdom.db",
        )

    def start_summon(self, config_path: Path, bootstrap_log_path: Path) -> None:
        bootstrap_log_path.parent.mkdir(parents=True, exist_ok=True)
        with bootstrap_log_path.open("w", encoding="utf-8") as handle:
            result = subprocess.run(
                ["kd", "summon", "--config", str(config_path), "--detach", "--ready-timeout", "120s"],
                text=True,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
        if result.returncode != 0:
            raise RuntimeError(f"summon bootstrap failed for {config_path}")

    def stop_summon(self, config_path: Path) -> None:
        subprocess.run(
            ["kd", "summon", "stop", "--config", str(config_path)],
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def create_decree_run(
        self,
        server_url: str,
        actor_id: str,
        role_id: str,
        request: RunRequest,
        decree_path: Path,
    ) -> str:
        decree_payload = self.post_json(
            f"{server_url}/decrees",
            {
                "decree_title": request.title,
                "decree_text": request.text,
            },
            actor_id,
            role_id,
            f"decree-create-{uuid.uuid4()}",
        )
        decree_id = str(decree_payload.get("decree_id") or "").strip()
        if not decree_id:
            raise RuntimeError("create decree response missing decree_id")
        run_payload = self.post_json(
            f"{server_url}/runs",
            self.build_run_payload(decree_id, request),
            actor_id,
            role_id,
            f"run-start-{uuid.uuid4()}",
        )
        payload = {
            "decree_id": decree_id,
            "decree_at": decree_payload.get("created_at"),
            "run_id": run_payload.get("run_id"),
            "workflow_id": run_payload.get("workflow_id"),
            "status": run_payload.get("status"),
            "created_at": run_payload.get("created_at"),
            "updated_at": run_payload.get("updated_at"),
        }
        decree_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_json(decree_path, payload)
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id:
            raise RuntimeError("create decree response missing run_id")
        return run_id

    def build_run_payload(self, decree_id: str, request: RunRequest) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "decree_id": decree_id,
            "workflow_id": request.workflow_id,
            "publish_mode": request.publish_mode,
            "max_execution_review_cycles": request.max_execution_review_cycles,
            "max_validation_rework_cycles": request.max_validation_rework_cycles,
            "model_selection": {
                "global": {
                    "model": request.global_model,
                }
            },
        }
        role_overrides = self.build_role_overrides(request.agent_model_overrides)
        if role_overrides:
            payload["model_selection"]["role_overrides"] = role_overrides
        return payload

    def build_role_overrides(self, raw: str) -> dict[str, dict[str, str]]:
        overrides: dict[str, dict[str, str]] = {}
        for pair in raw.split(","):
            entry = pair.strip()
            if not entry or "=" not in entry:
                continue
            role, model = entry.split("=", 1)
            role = role.strip()
            model = model.strip()
            if role and model:
                overrides[role] = {"model": model}
        return overrides

    def poll_run(
        self,
        server_url: str,
        actor_id: str,
        role_id: str,
        run_id: str,
        timeout_seconds: int,
        poll_interval_seconds: int,
        history_path: Path,
    ) -> PollResult:
        terminal_statuses = {"succeeded", "failed", "cancelled", "awaiting_human_confirmation"}
        start = time.time()
        history: list[dict[str, Any]] = []
        last_payload: dict[str, Any] = {}
        terminal_status = "timeout"

        while True:
            payload = json.loads(
                self.run_command(
                    [
                        "kd",
                        "--server",
                        server_url,
                        "--actor-id",
                        actor_id,
                        "--role-id",
                        role_id,
                        "--json",
                        "run",
                        "status",
                        "--run-id",
                        run_id,
                    ]
                ).stdout
            )
            last_payload = payload
            status_value = payload.get("run", {}).get("run_status") or payload.get("status") or "unknown"
            terminal_status = str(status_value).strip() or "unknown"
            history.append({"at": self.utcnow(), "status": terminal_status})
            if terminal_status in terminal_statuses:
                break
            if time.time() - start > timeout_seconds:
                terminal_status = "timeout"
                history.append({"at": self.utcnow(), "status": terminal_status})
                break
            time.sleep(poll_interval_seconds)

        self.write_json(
            history_path,
            {"run_id": run_id, "history": history, "last_status_payload": last_payload},
        )
        return PollResult(terminal_status=terminal_status, history_path=history_path)

    def capture_run_api_artifacts(
        self,
        server_url: str,
        actor_id: str,
        role_id: str,
        run_id: str,
        report_dir: Path,
    ) -> None:
        endpoints = {
            "run-final.json": f"{server_url}/runs/{run_id}",
            "graph.json": f"{server_url}/runs/{run_id}/graph",
            "timeline.json": f"{server_url}/runs/{run_id}/timeline",
            "messages.json": f"{server_url}/runs/{run_id}/mailbox/messages?limit=200",
        }
        for filename, url in endpoints.items():
            payload = self.fetch_json(url, actor_id, role_id)
            self.write_json(report_dir / filename, payload)

    def fetch_json(self, url: str, actor_id: str, role_id: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "X-Actor-Id": actor_id,
                "X-Role-Id": role_id,
            },
        )
        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"request failed: {url}: {exc.code} {body}") from exc

    def post_json(
        self,
        url: str,
        payload: dict[str, Any],
        actor_id: str,
        role_id: str,
        idempotency_key: str,
    ) -> Any:
        encoded = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=encoded,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Actor-Id": actor_id,
                "X-Role-Id": role_id,
                "Idempotency-Key": idempotency_key,
            },
        )
        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"request failed: {url}: {exc.code} {body}") from exc

    def query_rows(
        self,
        db_path: Path,
        query: str,
        parameters: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        with sqlite3.connect(db_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(query, tuple(parameters)).fetchall()
        return [dict(row) for row in rows]

    def query_one(
        self,
        db_path: Path,
        query: str,
        parameters: Sequence[Any] = (),
    ) -> dict[str, Any] | None:
        rows = self.query_rows(db_path, query, parameters)
        if not rows:
            return None
        return rows[0]

    def load_run_worktree_record(self, db_path: Path, run_id: str) -> WorktreeRecord | None:
        row = self.query_one(
            db_path,
            (
                "SELECT run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, "
                "seed_commit_oid, state, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason "
                "FROM run_worktrees WHERE run_id = ?"
            ),
            (run_id,),
        )
        if row is None:
            return None
        worktree_path = Path(row["main_worktree_root"]).parent / "kingdom-worktrees" / Path(row["main_worktree_root"]).name / row["worktree_name"]
        return WorktreeRecord(
            run_id=str(row["run_id"]),
            repo_common_dir=str(row["repo_common_dir"]),
            main_worktree_root=str(row["main_worktree_root"]),
            project_relpath=str(row["project_relpath"]),
            worktree_name=str(row["worktree_name"]),
            worktree_path=str(worktree_path),
            seed_commit_oid=str(row["seed_commit_oid"]),
            state=str(row["state"]),
            created_at=str(row["created_at"]),
            last_used_at=str(row["last_used_at"]),
            cleanup_after=self._nullable_text(row.get("cleanup_after")),
            cleaned_at=self._nullable_text(row.get("cleaned_at")),
            failure_reason=str(row.get("failure_reason") or ""),
        )

    def capture_git_artifacts(self, repo_dir: Path, prefix: str, report_dir: Path) -> None:
        status = self.run_command(["git", "-C", str(repo_dir), "status", "--short", "--branch"]).stdout
        diff_stat = self.run_command(["git", "-C", str(repo_dir), "diff", "--stat"]).stdout
        diff_patch = self.run_command(["git", "-C", str(repo_dir), "diff"]).stdout
        self.write_text(report_dir / f"{prefix}-status.txt", status)
        self.write_text(report_dir / f"{prefix}-diff-stat.txt", diff_stat)
        self.write_text(report_dir / f"{prefix}-diff.patch", diff_patch)

    def git_head(self, repo_dir: Path) -> str:
        return self.run_command(["git", "-C", str(repo_dir), "log", "-1", "--oneline"]).stdout

    def run_command(
        self,
        args: Sequence[str],
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            list(args),
            cwd=str(cwd) if cwd is not None else None,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise CommandError(args, result.returncode, result.stdout, result.stderr)
        return result

    def write_json(self, path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def write_text(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def copy_tree(self, source: Path, destination: Path) -> None:
        shutil.copytree(source, destination)

    def serialize_worktree_record(self, record: WorktreeRecord | None) -> dict[str, Any]:
        if record is None:
            return {"exists": False}
        payload = asdict(record)
        payload["exists"] = True
        return payload

    def read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    @staticmethod
    def utcnow() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    @staticmethod
    def _nullable_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
