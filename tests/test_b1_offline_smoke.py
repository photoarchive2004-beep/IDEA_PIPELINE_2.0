# -*- coding: utf-8 -*-
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "b_lit_scout.py"
FIX = ROOT / "tests" / "fixtures"
SMOKE_IDEA = ROOT / "tests" / "stage_b_smoke_ideas" / "biomed" / "in" / "idea.txt"


def _run(idea_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
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


def test_clean_out_archives_on_second_run() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-B1-ARCHIVE-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        (idea_dir / "out").mkdir(parents=True, exist_ok=True)
        shutil.copy2(SMOKE_IDEA, idea_dir / "in" / "idea.txt")

        first = _run(idea_dir)
        assert first.returncode == 0, first.stdout + "\n" + first.stderr
        assert (idea_dir / "out" / "corpus.csv").exists()

        (idea_dir / "out" / "marker.txt").write_text("old", encoding="utf-8")
        second = _run(idea_dir)
        assert second.returncode == 0, second.stdout + "\n" + second.stderr

        archives = list((idea_dir / "out" / "_archive").glob("*"))
        assert archives, "Second run must archive previous out"


def test_budget_migration_from_3_to_10() -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-B1-BUDGET-") as tmp:
        idea_dir = Path(tmp)
        (idea_dir / "in").mkdir(parents=True, exist_ok=True)
        out_dir = idea_dir / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        (idea_dir / "in" / "idea.txt").write_text("test idea", encoding="utf-8")
        old = {"limit": 3, "used": 3}
        (out_dir / "llm_requests_B1.json").write_text(json.dumps(old), encoding="utf-8")

        sys.path.insert(0, str(ROOT / "tools"))
        from module_b_lit_scout import StageB  # type: ignore

        stage = StageB(idea_dir, "BALANCED", offline_fixtures=None)
        assert stage.llm_budget == 10
        assert int(stage.llm_budget_state.get("limit", 0)) >= 10


def test_llm_limit_env_override(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STAGE_B1_LLM_LIMIT", "12")
    idea = tmp_path / "IDEA-B1-LIMIT-OVERRIDE"
    (idea / "in").mkdir(parents=True)
    (idea / "out").mkdir(parents=True)

    sys.path.insert(0, str(ROOT / "tools"))
    from module_b_lit_scout import StageB  # type: ignore

    stage = StageB(idea, "BALANCED", offline_fixtures=None)
    assert stage.llm_budget == 12
    assert int(stage.llm_budget_state.get("limit", 0)) >= 12
