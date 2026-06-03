from __future__ import annotations

import unittest
from pathlib import Path

from scripts.e2e.models import RunRequest
from scripts.e2e.runtime import RuntimeService


class RuntimeServiceTests(unittest.TestCase):
    def test_build_run_payload_should_omit_thinking_effort_when_using_model_defaults(self) -> None:
        runtime = RuntimeService(Path("/tmp/kingdom"))
        request = RunRequest(
            title="Title",
            text="Text",
            workflow_id="royal-decree-v1",
            publish_mode="direct",
            global_model="github-copilot/gpt-5.4-mini",
            agent_model_overrides="reviewer-shieldmaiden=github-copilot/gpt-5.4-mini",
            max_execution_review_cycles=5,
            max_validation_rework_cycles=5,
        )

        payload = runtime.build_run_payload("decree_123", request)

        self.assertEqual(payload["model_selection"]["global"], {"model": "github-copilot/gpt-5.4-mini"})
        self.assertEqual(
            payload["model_selection"]["role_overrides"],
            {"reviewer-shieldmaiden": {"model": "github-copilot/gpt-5.4-mini"}},
        )


if __name__ == "__main__":
    unittest.main()
