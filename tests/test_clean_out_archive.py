from pathlib import Path
from module_b_lit_scout import StageB


def test_clean_out_archive(tmp_path: Path) -> None:
    idea = tmp_path / "IDEA-ARCHIVE"
    (idea / "in").mkdir(parents=True)
    (idea / "out").mkdir(parents=True)
    (idea / "in" / "idea.txt").write_text("battery health estimation", encoding="utf-8")
    (idea / "out" / "old.txt").write_text("old", encoding="utf-8")

    stage = StageB(idea, "BALANCED", None, clean_out=True)
    stage.apply_cleaning()

    archived = list((idea / "out" / "_archive").glob("*"))
    assert archived
    assert (archived[0] / "old.txt").exists()
