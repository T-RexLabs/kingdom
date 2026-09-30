from __future__ import annotations

import unittest
import http.client
import urllib.error
from tempfile import TemporaryDirectory
from unittest import mock
from pathlib import Path

from scripts.e2e.models import RunRequest
from scripts.e2e.runtime import RuntimeService, new_idempotency_key


class RuntimeServiceTests(unittest.TestCase):
    def test_new_idempotency_key_rejects_invalid_scope(self) -> None:
        with self.assertRaises(ValueError):
            new_idempotency_key("Invalid Scope")
        with self.assertRaises(ValueError):
            new_idempotency_key("a" * 41)

    def test_build_run_payload_should_omit_thinking_effort_when_using_model_defaults(self) -> None:
        runtime = RuntimeService(Path("/tmp/kingdom"))
        request = RunRequest(
            title="Title",
            text="Text",
            workflow_id="work-order-v1",
            publish_mode="direct",
            global_model="github-copilot/gpt-5.4-mini",
            agent_model_overrides="reviewer=github-copilot/gpt-5.4-mini",
            max_execution_review_cycles=5,
            max_validation_rework_cycles=5,
        )

        payload = runtime.build_run_payload("work_order_123", request)

        self.assertEqual(payload["model_selection"]["global"], {"model": "github-copilot/gpt-5.4-mini"})
        self.assertEqual(
            payload["model_selection"]["role_overrides"],
            {"reviewer": {"model": "github-copilot/gpt-5.4-mini"}},
        )

    def test_post_json_retries_ambiguous_transport_with_same_key(self) -> None:
        runtime = RuntimeService(Path("/tmp/kingdom"))

        class Response:
            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return b'{"ok":true}'

        with TemporaryDirectory() as directory:
            credential_path = Path(directory) / "credential"
            credential_path.write_text("secret", encoding="utf-8")
            with (
                mock.patch("urllib.request.urlopen", side_effect=[urllib.error.URLError("dropped"), Response()]) as urlopen,
                mock.patch("time.sleep"),
            ):
                result = runtime.post_json("http://127.0.0.1/api/v1/runs", {"value": 1}, credential_path, "stable-key")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(urlopen.call_args_list[0].args[0].get_header("Idempotency-key"), "stable-key")
        self.assertEqual(urlopen.call_args_list[1].args[0].get_header("Idempotency-key"), "stable-key")

    def test_post_json_retries_truncated_response_with_same_key(self) -> None:
        runtime = RuntimeService(Path("/tmp/kingdom"))

        class Response:
            def __init__(self, truncated: bool) -> None:
                self.truncated = truncated

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                if self.truncated:
                    raise http.client.IncompleteRead(b"{")
                return b'{"ok":true}'

        with TemporaryDirectory() as directory:
            credential_path = Path(directory) / "credential"
            credential_path.write_text("secret", encoding="utf-8")
            with (
                mock.patch("urllib.request.urlopen", side_effect=[Response(True), Response(False)]) as urlopen,
                mock.patch("time.sleep"),
            ):
                result = runtime.post_json("http://127.0.0.1/api/v1/runs", {"value": 1}, credential_path, "stable-key")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(urlopen.call_args_list[0].args[0].get_header("Idempotency-key"), "stable-key")
        self.assertEqual(urlopen.call_args_list[1].args[0].get_header("Idempotency-key"), "stable-key")

    def test_post_json_retries_invalid_json_response_with_same_key(self) -> None:
        runtime = RuntimeService(Path("/tmp/kingdom"))

        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return self.body

        with TemporaryDirectory() as directory:
            credential_path = Path(directory) / "credential"
            credential_path.write_text("secret", encoding="utf-8")
            with (
                mock.patch("urllib.request.urlopen", side_effect=[Response(b"{"), Response(b'{"ok":true}')]) as urlopen,
                mock.patch("time.sleep"),
            ):
                result = runtime.post_json("http://127.0.0.1/api/v1/runs", {"value": 1}, credential_path, "stable-key")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(urlopen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
