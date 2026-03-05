import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "b_lit_scout.py"
FIX = ROOT / "tests" / "fixtures"
SMOKE_IDEA = ROOT / "tests" / "stage_b_smoke_ideas" / "biomed" / "in" / "idea.txt"


def test_stage_b1_outputs_do_not_expose_api_key_tokens() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-B1-NO-LEAK-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        (idea_dir / "in" / "idea.txt").write_text(SMOKE_IDEA.read_text(encoding="utf-8"), encoding="utf-8")

        run = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--idea-dir",
                str(idea_dir),
                "--scope",
                "balanced",
                "--offline-fixtures",
                str(FIX),
                "--clean-out",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert run.returncode == 0, run.stdout + "\n" + run.stderr

        forbidden = ["super_secret_key_1234"]
        assert "api_key_present" in (idea_dir / "out" / "search_log.json").read_text(encoding="utf-8")
        for name in ["search_log.json", "prisma_lite.md", "stageB1_summary.txt"]:
            text = (idea_dir / "out" / name).read_text(encoding="utf-8")
            for token in forbidden:
                assert token not in text

