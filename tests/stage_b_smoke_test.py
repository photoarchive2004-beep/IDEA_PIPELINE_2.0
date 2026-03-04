# -*- coding: utf-8 -*-
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures"
IDEAS = ROOT / "tests" / "stage_b_smoke_ideas"
SCRIPT = ROOT / "tools" / "b_lit_scout.py"


def run_one(src_idea: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="IDEA-TEST-") as tmp:
        idea = Path(tmp)
        (idea / "in").mkdir(parents=True, exist_ok=True)
        (idea / "out").mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_idea, idea / "in" / "idea.txt")
        cmd = [sys.executable, str(SCRIPT), "--idea-dir", str(idea), "--scope", "balanced", "--offline-fixtures", str(FIX)]
        cp = subprocess.run(cmd, cwd=ROOT, check=False)
        if cp.returncode not in (0, 2):
            raise AssertionError(f"Stage B exited with {cp.returncode}")
        for rel in ["out/corpus.csv", "out/search_log.json", "out/prisma_lite.md", "out/stageB_summary.txt"]:
            if not (idea / rel).exists():
                raise AssertionError(f"Missing {rel}")
        rows = (idea / "out" / "corpus.csv").read_text(encoding="utf-8").strip().splitlines()
        if len(rows) < 6:
            raise AssertionError("corpus.csv expected at least 5 rows + header")


if __name__ == "__main__":
    for sub in ("biomed", "social", "tech"):
        run_one(IDEAS / sub / "in" / "idea.txt")
    print("OK: stage_b smoke")
