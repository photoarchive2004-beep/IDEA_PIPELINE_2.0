# -*- coding: utf-8 -*-
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "b_lit_scout.py"
SMOKE_IDEA = ROOT / "tests" / "stage_b_smoke_ideas" / "biomed" / "in" / "idea.txt"


def test_emit_anchors_prompt_only_creates_prompt_and_summary_paths() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-ANCHORS-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        shutil.copy2(SMOKE_IDEA, idea_dir / "in" / "idea.txt")

        cmd = [
            sys.executable,
            str(SCRIPT),
            "--idea-dir",
            str(idea_dir),
            "--scope",
            "balanced",
            "--emit-anchors-prompt-only",
        ]
        cp = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=False)
        assert cp.returncode == 0, cp.stdout + "\n" + cp.stderr

        prompt_path = idea_dir / "out" / "llm_prompt_B1_anchors.txt"
        response_path = idea_dir / "in" / "llm_response_B1_anchors.json"
        summary_path = idea_dir / "out" / "stageB_summary.txt"

        assert prompt_path.exists(), "Prompt file must exist"
        assert prompt_path.read_text(encoding="utf-8").strip(), "Prompt file must be non-empty"
        assert response_path.exists(), "Response template must exist"
        assert summary_path.exists(), "Summary must exist"

        summary = summary_path.read_text(encoding="utf-8")
        assert f"llm_prompt_path = {prompt_path.resolve()}" in summary
        assert f"llm_response_path = {response_path.resolve()}" in summary
        assert f"PROMPT_FILE = {prompt_path.resolve()}" in summary
        assert f"WAIT_FILE = {response_path.resolve()}" in summary
