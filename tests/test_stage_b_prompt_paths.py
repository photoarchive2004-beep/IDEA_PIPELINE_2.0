# -*- coding: utf-8 -*-
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "b_lit_scout.py"


def run_emit(idea_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--idea-dir",
            str(idea_dir),
            "--scope",
            "balanced",
            "--emit-anchors-prompt-only",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_stage_b1_prompt_paths_and_response_template_preserved() -> None:
    with tempfile.TemporaryDirectory(prefix="stage-b1-") as tmp:
        repo_like = Path(tmp)
        idea_dir = repo_like / "ideas" / "IDEA-TEST-PATHS"
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        (idea_dir / "in" / "idea.txt").write_text("Test idea for Stage B1 prompt regeneration", encoding="utf-8")

        first = run_emit(idea_dir)
        assert first.returncode == 0, first.stdout + "\n" + first.stderr

        prompt_path = idea_dir / "out" / "llm_prompt_B_anchors.txt"
        response_path = idea_dir / "in" / "llm_response_B_anchors.json"
        summary_path = idea_dir / "out" / "stageB_summary.txt"

        assert prompt_path.exists()
        assert prompt_path.read_text(encoding="utf-8").strip()
        assert response_path.exists()
        assert summary_path.exists()

        user_payload = {
            "refined_primary_token": "custom_primary",
            "refined_keywords_tokens": ["custom_keyword"],
        }
        response_path.write_text(json.dumps(user_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        second = run_emit(idea_dir)
        assert second.returncode == 0, second.stdout + "\n" + second.stderr

        after = json.loads(response_path.read_text(encoding="utf-8"))
        assert after == user_payload, "User edited JSON must not be overwritten"

        summary = summary_path.read_text(encoding="utf-8")
        assert "Этап: Stage B1" in summary
        assert f"PROMPT_FILE = {prompt_path.resolve()}" in summary
        assert f"WAIT_FILE = {response_path.resolve()}" in summary
        assert "Шаг 1" not in summary  # summary should keep paths/state only
