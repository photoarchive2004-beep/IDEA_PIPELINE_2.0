# -*- coding: utf-8 -*-
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "b_lit_scout.py"
SMOKE_IDEA = ROOT / "tests" / "stage_b_smoke_ideas" / "biomed" / "in" / "idea.txt"


def test_b1_emit_prompt_contains_schema_and_paths() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-B1-PROMPT-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        shutil.copy2(SMOKE_IDEA, idea_dir / "in" / "idea.txt")

        cp = subprocess.run([
            sys.executable, str(SCRIPT), "--idea-dir", str(idea_dir), "--scope", "balanced", "--emit-anchors-prompt-only"
        ], cwd=ROOT, capture_output=True, text=True, check=False)
        assert cp.returncode == 0, cp.stdout + "\n" + cp.stderr

        prompt_path = idea_dir / "out" / "llm_prompt_B1_anchors.txt"
        summary_path = idea_dir / "out" / "stageB1_summary.txt"
        assert prompt_path.exists()
        text = prompt_path.read_text(encoding="utf-8")
        assert "Return ONLY valid JSON" in text
        assert "JSON SCHEMA" in text
        assert summary_path.exists()
        summary = summary_path.read_text(encoding="utf-8")
        assert f"PROMPT_FILE = {prompt_path.resolve()}" in summary
